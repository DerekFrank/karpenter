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
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
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
// controller: repair rides the shared disruption budget (reason "Repair"), pre-spins a replacement before terminating
// (replace-then-terminate), orders candidates by rank + age/τ − backoff, and is vetoed by the do-not-repair annotation.
type Repair struct {
	kubeClient    client.Client
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder
	clock         clock.Clock
	queue         *Queue
	restraint     *restraint
}

func NewRepair(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, cp cloudprovider.CloudProvider, recorder events.Recorder, clk clock.Clock, queue *Queue) *Repair {
	return &Repair{
		kubeClient:    kubeClient,
		cluster:       cluster,
		provisioner:   provisioner,
		cloudProvider: cp,
		recorder:      recorder,
		clock:         clk,
		queue:         queue,
		restraint:     newRestraint(),
	}
}

// ShouldDisrupt is a predicate that filters candidates to nodes that have an unhealthy condition matching a
// RepairPolicy, have waited past that policy's toleration, and are not vetoed by the do-not-repair annotation.
func (r *Repair) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	// Repair is behind the NodeRepair feature gate, matching the old node.health controller's gating.
	if !options.FromContext(ctx).FeatureGates.NodeRepair {
		return false
	}
	if c.Node == nil {
		return false
	}
	// do-not-repair is the operator's escape hatch: it blocks all repair on this node, whatever the drain bound.
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

// resolution is a candidate's matched repair policy, resolved once per pass and reused for ordering, restraint, and the
// drain bound (rather than recomputing the policy scan + reason merge at each step).
type resolution struct {
	conditionType string
	cond          corev1.NodeCondition
	merged        mergedRepairPolicy
	score         float64
}

// ComputeCommands orders eligible candidates by the repair score and returns one replace-then-terminate command for the
// highest-scoring candidate whose NodePool has budget. Only one command per pass, mirroring drift.
func (r *Repair) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	// Feed the outcomes of prior probes back into the per-domain restraint dials before deciding this pass: proven
	// successes widen, failures cut and cool. Correlated-failure restraint (F3) layers strictly beneath the budget.
	r.restraint.observe(ctx, r.kubeClient, r.queue, r.clock.Now())

	// Resolve each candidate's matched policy ONCE (the policy scan + per-reason onset merge), then reuse it for
	// ordering, restraint admission, and the drain bound below.
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
			conditionType: string(policy.ConditionType),
			cond:          *cond,
			merged:        merged,
			score:         r.score(*cond, policy.TolerationDuration, merged, ranks, c.NodePool),
		}
	}

	// Any width earned in a past fault episode must not carry into a new one: reset dials for domains with no current
	// fault, so a fresh correlated burst starts slow even after a healthy stretch (past success ≠ predictor of new
	// correlated failure). The active set is the domains of this pass's resolved candidates.
	activeDomains := map[failureDomain]struct{}{}
	for c, rr := range res {
		for _, d := range domainsOf(c, rr.conditionType) {
			activeDomains[d] = struct{}{}
		}
	}
	r.restraint.resetIdleDomains(activeDomains)

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
		// Correlated-failure restraint (F3): even within budget, only admit this candidate if its failure domains
		// (NodePool ∧ Zone ∧ Policy) have width headroom and are out of cooldown. A correlated burst collapses into
		// shared domains that cold-start at width 1 → one probe first; widen only as probes prove out.
		if !r.restraint.admit(candidate, rr.conditionType, r.clock.Now()) {
			continue
		}
		// Terminate-first (F2): a capacity-constrained pool has no room to pre-spin a replacement, so repair issues a
		// delete-only command and reactive provisioning refills the freed slot. The budget still paces it and the drain
		// still honors PDBs/TGP.
		if r.terminateFirst(candidate) {
			if err := r.stampDrainBound(ctx, candidate, rr.merged); err != nil {
				return []Command{}, err
			}
			r.restraint.recordProbe(candidate, rr.conditionType)
			return []Command{{
				Candidates:          []*Candidate{candidate},
				PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
			}}, nil
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
		// Bound the drain per the matched policy before the queue terminates the candidate, so repair is never an
		// unbounded hang and a forceful (0) policy skips the drain for conditions the kubelet can't evict through.
		if err := r.stampDrainBound(ctx, candidate, rr.merged); err != nil {
			return []Command{}, err
		}
		r.restraint.recordProbe(candidate, rr.conditionType)
		return []Command{{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		}}, nil
	}
	return []Command{}, nil
}

// terminateFirst reports whether repair must delete this candidate before it can replace, because the candidate's
// NodePool has no room to grow a replacement first (Terminate-First Disruption, RFC #3203). It is derived from the
// operator's existing capacity posture — no new API: a static NodePool (Spec.Replicas set) runs a fixed node count, so
// a pre-spun replacement would be an (N+1)th node the operator capped out; free the slot first and let reactive
// provisioning refill it. A launch *failure* must never flip a pool to terminate-first — that holds by construction
// here, since this reads static configuration, never launch outcomes.
func (r *Repair) terminateFirst(c *Candidate) bool {
	return c.OwnedByStaticNodePool()
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
	return r.kubeClient.Patch(ctx, c.NodeClaim, client.MergeFrom(stored))
}

func (r *Repair) Reason() v1.DisruptionReason { return v1.DisruptionReasonRepair }

func (r *Repair) Class() string { return RepairDisruptionClass }

func (r *Repair) ConsolidationType() string { return "" }
