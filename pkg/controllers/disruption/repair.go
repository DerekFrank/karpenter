/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// agingConstant (τ) is the time a node must wait past its toleration to earn one rank tier of standing. It sets the
// starvation bound: a node overtakes a steadily-refreshed rival Δrank tiers up after Δrank·τ. See resiliency §3.1.2.
const agingConstant = 30 * time.Minute

// Repair is a voluntary disruption method that remediates unhealthy nodes. It replaces the standalone node.health
// controller: repair rides the shared disruption budget (reason "Unhealthy"), pre-spins a replacement before terminating
// (replace-then-terminate), orders candidates by rank + age/τ − backoff, and is vetoed by the do-not-repair annotation.
type Repair struct {
	kubeClient    client.Client
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder
	clock         clock.Clock
	queue         *Queue
	restraint     RepairRestraint
	probes        map[string]*repairProbe // providerID -> in-flight probe repair is tracking to an outcome
	acted         map[string]*Candidate   // providerID -> candidate repair has acted on, watched for fault clearance
}

// repairProbe is a candidate repair admitted and is now watching to a terminal outcome, so it can report Proven/Failed
// to the restraint policy. Repair (not the policy) owns this I/O: whether the replacement came up, and the success
// dwell. The candidate snapshot is retained so the outcome can be attributed to the same failure domains that were
// admitted, even after the original node is gone.
type repairProbe struct {
	candidate     *Candidate
	nodeClaimName string
	succeededAt   time.Time // when the original was observed terminated (replacement came up); zero until then
}

// repairDwell is how long a replacement must hold Ready+Healthy before a probe counts as a durable success.
const repairDwell = 5 * time.Minute

func NewRepair(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, cp cloudprovider.CloudProvider, recorder events.Recorder, clk clock.Clock, queue *Queue) *Repair {
	return &Repair{
		kubeClient:    kubeClient,
		cluster:       cluster,
		provisioner:   provisioner,
		cloudProvider: cp,
		recorder:      recorder,
		clock:         clk,
		queue:         queue,
		probes:        map[string]*repairProbe{},
		acted:         map[string]*Candidate{},
		// The correlated-failure AIMD policy is the default RepairRestraint; swap for another implementation (or
		// noRestraint) without touching this method.
		restraint: newRestraint(clk),
	}
}

// ShouldDisrupt is a predicate that filters candidates to nodes that have an unhealthy condition matching a
// RepairPolicy, have waited past that policy's toleration, and are not vetoed by the do-not-repair annotation.
func (r *Repair) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	// Repair is behind the NodeRepair feature gate, matching the old node.health controller's gating.
	if !options.FromContext(ctx).FeatureGates.NodeRepair || c.Node == nil {
		return false
	}
	// do-not-repair is the operator's escape hatch: it blocks all repair on this node, whatever the drain bound.
	// TODO: the veto shape is deliberately unsettled by the RFC (node vs. NodePool scope; whether do-not-disrupt should
	// imply don't-repair). Revisit per kubernetes-sigs/karpenter#2424.
	if c.Annotations()[v1.DoNotRepairAnnotationKey] == "true" {
		return false
	}
	policies := r.cloudProvider.RepairPolicies()
	_, cond := matchingPolicy(policies, c.Node)
	if cond == nil {
		return false
	}
	// Reason-level eligibility (F1): a policy applies only when the condition matches AND some reason on the node
	// matches its ReasonMatcher, and eligibility is measured from that reason's own onset. When no policy carries a
	// ReasonMatcher this collapses to condition-only — a clean superset of the pre-F1 behavior.
	_, eligible := resolveReasonPolicy(policies, *cond, r.clock.Now())
	return eligible
}

// resolution is a candidate's matched repair policy, resolved once per pass and reused for ordering and the drain
// bound (rather than recomputing the policy scan + reason merge at each step).
type resolution struct {
	cond   corev1.NodeCondition
	merged mergedRepairPolicy
	score  float64
}

// ComputeCommands orders eligible candidates by the repair score and returns one replace-then-terminate command for the
// highest-scoring candidate whose NodePool has budget. Only one command per pass, mirroring drift.
func (r *Repair) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	// Detect the outcomes of prior probes (proven / failed) and report them to the restraint policy.
	r.observeProbes(ctx)

	// Resolve each candidate's matched policy ONCE (the policy scan + per-reason onset merge), then reuse it for
	// ordering and the drain bound below.
	policies := r.cloudProvider.RepairPolicies()
	ranks := denseRanks(policies)
	res := make(map[*Candidate]resolution, len(candidates))
	for _, c := range candidates {
		policy, cond := matchingPolicy(policies, c.Node)
		if policy == nil {
			continue
		}
		merged, _ := resolveReasonPolicy(policies, *cond, r.clock.Now())
		res[c] = resolution{
			cond:   *cond,
			merged: merged,
			score:  r.score(*cond, policy.TolerationDuration, merged, ranks, c.NodePool),
		}
	}

	// Order by the precomputed score (total order via the name tie-break, so a plain sort is deterministic).
	sort.Slice(candidates, func(i, j int) bool {
		si, sj := res[candidates[i]].score, res[candidates[j]].score
		if si != sj {
			return si > sj // higher score repairs first
		}
		if candidates[i].DisruptionCost != candidates[j].DisruptionCost {
			return candidates[i].DisruptionCost < candidates[j].DisruptionCost // lower disruption cost first
		}
		return candidates[i].Name() < candidates[j].Name()
	})

	for _, candidate := range candidates {
		rr, ok := res[candidate]
		if !ok || disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Correlated-failure restraint (F3): even within budget, only admit this candidate if the restraint policy
		// allows it. The default policy paces per failure domain (NodePool ∧ Zone ∧ Policy), cold-starting a correlated
		// burst at one probe and widening only as probes prove out.
		if !r.restraint.CanDisrupt(candidate) {
			continue
		}
		// Terminate-first (F2), static case: a static NodePool (fixed Spec.Replicas) structurally has no room to grow, so
		// repair issues a delete-only command and reactive provisioning refills the freed slot. This is known before any
		// simulation, so short-circuit. The budget still paces it and the drain still honors PDBs/TGP.
		if candidate.OwnedByStaticNodePool() {
			return r.terminateFirstCommand(ctx, candidate, rr)
		}
		// Pre-spin: simulate scheduling to build the replacement(s). The queue launches them first and only
		// terminates the original once they are healthy — so a replacement that boots unhealthy (bad AMI, partitioned
		// zone) is never followed by terminating the original. Replace-then-terminate is the loop's circuit breaker.
		results, err := SimulateScheduling(ctx, r.kubeClient, r.cluster, r.provisioner, r.clock, r.recorder, nil, candidate)
		if err != nil {
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return []Command{}, err
		}
		if !results.AllNonPendingPodsScheduled() {
			r.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}
		// Terminate-first (F2), dynamic/reserved case: the simulation ran as if the candidate were already gone. If it
		// can only place the replacement back into the same reservation (every new NodeClaim is reserved capacity), the
		// pool can't grow, so terminate-first; any other option (incl. on-demand) means replace-first as usual.
		if candidate.capacityType == v1.CapacityTypeReserved && onlyReservedReplacements(results.NewNodeClaims) {
			return r.terminateFirstCommand(ctx, candidate, rr)
		}
		// Bound the drain per the matched policy before the queue terminates the candidate, so repair is never an
		// unbounded hang and a forceful (0) policy skips the drain for conditions the kubelet can't evict through.
		if err := r.stampDrainBound(ctx, candidate, rr.merged); err != nil {
			return []Command{}, err
		}
		r.trackProbe(candidate)
		return []Command{{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		}}, nil
	}
	return []Command{}, nil
}

// terminateFirstCommand builds the delete-only repair command (no pre-spun replacement) for a capacity-constrained
// candidate: the drain is bounded/stamped and the candidate is recorded as an in-flight probe, exactly as the
// replace-first path does, but reactive provisioning fills the freed slot afterward (Terminate-First Disruption,
// RFC #3203). The budget still paces it; PDBs and TGP still bound the drain.
func (r *Repair) terminateFirstCommand(ctx context.Context, candidate *Candidate, rr resolution) ([]Command, error) {
	if err := r.stampDrainBound(ctx, candidate, rr.merged); err != nil {
		return []Command{}, err
	}
	r.trackProbe(candidate)
	return []Command{{
		Candidates:          []*Candidate{candidate},
		PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
	}}, nil
}

// trackProbe records a just-issued repair as an in-flight probe (CanDisrupt already counted it in-flight in the
// restraint policy) so observeProbes can later attribute its outcome. It is also remembered in acted so, once its
// fault later clears, repair can tell the policy the domain's episode is over (RepairCleared).
func (r *Repair) trackProbe(c *Candidate) {
	r.probes[c.ProviderID()] = &repairProbe{candidate: c, nodeClaimName: c.NodeClaim.Name}
	r.acted[c.ProviderID()] = c
}

// observeProbes drives each in-flight probe to a terminal outcome and reports it to the restraint policy. Repair owns
// this I/O so the policy stays pure:
//   - still enqueued (queue holds it until the replacement initializes) → pending, no report.
//   - dequeued and the original NodeClaim is gone → the replacement came up and the original was terminated; once it
//     has held for the dwell, report RepairProven.
//   - dequeued and the original NodeClaim still exists → the repair did not produce a healthy replacement (bad-AMI
//     loop, partitioned zone, or command timeout) → report RepairFailed.
func (r *Repair) observeProbes(ctx context.Context) {
	now := r.clock.Now()
	for providerID, p := range r.probes {
		if r.queue.HasAny(providerID) {
			continue // replacement still launching/waiting; outcome not yet known
		}
		nc := &v1.NodeClaim{}
		err := r.kubeClient.Get(ctx, types.NamespacedName{Name: p.nodeClaimName}, nc)
		switch {
		case apierrors.IsNotFound(err):
			// Original terminated → replacement proved healthy enough for the queue to proceed. Require the dwell to
			// elapse before counting it as a durable success (invariant: success = healthy for a while).
			if p.succeededAt.IsZero() {
				p.succeededAt = now
			}
			if !now.Before(p.succeededAt.Add(repairDwell)) {
				r.restraint.Record(p.candidate, RepairProven)
				delete(r.probes, providerID)
				delete(r.acted, providerID) // outcome settled; the original is gone, nothing left to watch for clearance
			}
		case err == nil:
			r.restraint.Record(p.candidate, RepairFailed)
			delete(r.probes, providerID)
			// keep in acted: a failed repair leaves the original unhealthy; watch for it to clear on its own.
		default:
			// transient get error; leave the probe to be re-observed next pass
		}
	}

	// Fault-episode clearance (invariant: past success doesn't mask a new correlated failure). A domain's width is only
	// raised above the floor by a proven probe, and every probe is remembered in acted, so a domain that could have
	// elevated width always has an acted candidate. When that candidate's node is still around but no longer unhealthy —
	// the fault resolved on its own, WITHOUT a completed repair (a terminated original is instead handled above as
	// proven/failed) — tell the policy the episode is over so it forgets the earned width. A fresh correlated burst then
	// starts slow again.
	policies := r.cloudProvider.RepairPolicies()
	for providerID, c := range r.acted {
		if _, stillProbing := r.probes[providerID]; stillProbing {
			continue // an in-flight probe still owns the outcome; not cleared
		}
		freshNode := &corev1.Node{}
		if err := r.kubeClient.Get(ctx, types.NamespacedName{Name: c.Node.Name}, freshNode); err != nil {
			// The node is gone entirely — the repair already terminated it (handled as proven/failed above). Stop
			// tracking it; its domains were already updated by the probe outcome.
			delete(r.acted, providerID)
			continue
		}
		if _, cond := matchingPolicy(policies, freshNode); cond == nil {
			// Node exists and is healthy again: the episode cleared without a repair.
			r.restraint.Record(c, RepairCleared)
			delete(r.acted, providerID)
		}
	}
}

// onlyReservedReplacements reports whether every simulated replacement resolved to reserved capacity — i.e. the pool
// can only grow back into the same reservation, so there is no headroom and repair must terminate-first. An empty set
// (no replacement needed) is not "reserved-only": that is an ordinary empty-node delete, handled on the normal path.
func onlyReservedReplacements(newNodeClaims []*pscheduling.NodeClaim) bool {
	if len(newNodeClaims) == 0 {
		return false
	}
	for _, nc := range newNodeClaims {
		if !nc.Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeReserved) {
			return false
		}
	}
	return true
}

// score computes E = rank + age/τ − backoff for an already-resolved candidate. Age is time past toleration
// (post-eligibility), so a flakier signal's longer toleration never leaks into its standing. Backoff demotes a whole
// NodePool whose launches keep failing by one tier.
func (r *Repair) score(cond corev1.NodeCondition, toleration time.Duration, merged mergedRepairPolicy, ranks map[int]int, np *v1.NodePool) float64 {
	rank := float64(ranks[merged.priority])
	age := r.clock.Now().Sub(cond.LastTransitionTime.Add(toleration))
	if age < 0 {
		age = 0
	}
	backoff := 0.0
	// A failed launch is almost never node-specific (the AMI/capacity/config is bad for the whole pool), so backoff is
	// keyed to the NodePool: the existing NodeRegistrationHealthy=False signal demotes the pool by one rank tier.
	if np != nil && np.StatusConditions().Get(v1.ConditionTypeNodeRegistrationHealthy).IsFalse() {
		backoff = 1
	}
	return rank + age.Minutes()/agingConstant.Minutes() - backoff
}

// denseRanks compresses the configured policy priorities into contiguous tiers (adjacent tiers one apart), so arbitrary
// priority magnitudes can't change what τ means — only the ordering of priorities matters.
func denseRanks(policies []cloudprovider.RepairPolicy) map[int]int {
	priorities := lo.Uniq(lo.Map(policies, func(p cloudprovider.RepairPolicy, _ int) int { return p.Priority }))
	sort.Ints(priorities)
	ranks := make(map[int]int, len(priorities))
	for i, p := range priorities {
		ranks[p] = i // lowest priority -> rank 0, ascending
	}
	return ranks
}

// matchingPolicy returns the RepairPolicy whose (type,status) matches an unhealthy condition on the node, choosing the
// one closest to (or furthest past) its toleration deadline, plus the matched condition.
func matchingPolicy(policies []cloudprovider.RepairPolicy, node *corev1.Node) (*cloudprovider.RepairPolicy, *corev1.NodeCondition) {
	var best *cloudprovider.RepairPolicy
	var bestCond *corev1.NodeCondition
	deadline := time.Time{}
	for i := range policies {
		cond := nodeutils.GetCondition(node, policies[i].ConditionType)
		if cond.Status != policies[i].ConditionStatus {
			continue
		}
		terminationTime := cond.LastTransitionTime.Add(policies[i].TolerationDuration)
		if deadline.IsZero() || terminationTime.Before(deadline) {
			best, bestCond, deadline = &policies[i], &cond, terminationTime
		}
	}
	return best, bestCond
}

// stampDrainBound sets the NodeClaim termination-timestamp annotation the termination controller reads as the hard
// drain deadline: now + effective TGP (min of the merged policy bound and the NodeClaim's own TGP), or now for a
// forceful (0) bound. The merged bound is the eligible-only lattice-join across the node's matched reasons (F1:
// drain-ability is a conjunction — every eligible reason must permit the drain — so TGP merges by min). A nil merged
// bound leaves the NodeClaim's TGP untouched (inherit).
func (r *Repair) stampDrainBound(ctx context.Context, c *Candidate, merged mergedRepairPolicy) error {
	if !merged.tgpSet {
		return nil // inherit the NodeClaim's own TerminationGracePeriod
	}
	effective := *merged.terminationGracePeriod
	if ncTGP := c.NodeClaim.Spec.TerminationGracePeriod; ncTGP != nil && ncTGP.Duration < effective {
		effective = ncTGP.Duration
	}
	stored := c.NodeClaim.DeepCopy()
	deadline := r.clock.Now().Add(effective).Format(time.RFC3339)
	c.NodeClaim.Annotations = lo.Assign(c.NodeClaim.Annotations, map[string]string{v1.NodeClaimTerminationTimestampAnnotationKey: deadline})
	if equality.Semantic.DeepEqual(stored, c.NodeClaim) {
		return nil
	}
	// Optimistic lock: the nodeclaim lifecycle controller also stamps this annotation, so a plain MergeFrom could race
	// and clobber. Matches lifecycle's MergeFromWithOptimisticLock. (Follow-up: extract a shared stamping helper.)
	return r.kubeClient.Patch(ctx, c.NodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (r *Repair) Reason() v1.DisruptionReason { return v1.DisruptionReasonUnhealthy }

func (r *Repair) Class() string { return RepairDisruptionClass }

func (r *Repair) ConsolidationType() string { return "" }
