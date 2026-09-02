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
	"math"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/awslabs/operatorpkg/option"
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
	"sigs.k8s.io/karpenter/pkg/scheduling"
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
	watching      map[string]*repairProbe // providerID -> optimistically-credited probe still watched for a re-break (claw-back)
	acted         map[string]*Candidate   // providerID -> candidate repair has acted on, watched for fault clearance
	// Repair-behavior variants (sim experiments, selected by REPAIR_OPTION), all default off:
	//   - optimistic: credit success the instant the replacement is healthy (dwell 0), so the AIMD widens fast.
	//   - clawback>0: pair with optimistic — after crediting, keep watching the replacement for this window; if it
	//     re-breaks within it, retroactively claw back the credit (RepairClawedBack) so the domain undoes its widen and
	//     backs off. Without it, optimistic re-credits a re-flapping replacement as a fresh success (width grows unbounded).
	//   - dwellJitter>0: randomize each probe's dwell by a uniform [0, dwellJitter) offset (robustness to dwell timing).
	//   - pinned: force each replacement into the CANDIDATE's zone (no escape) — models a replacement that must come from
	//     the same failure domain, so a domain-scoped fault re-inherits and repair churns in-place instead of escaping.
	optimistic  bool
	clawback    time.Duration
	dwellJitter time.Duration
	pinned      bool
	// breaker models the shipped node.health circuit breaker layered on top of the modern machinery: gate ALL repair in
	// a NodePool while its unhealthy fraction exceeds breakerUnhealthyFraction (a fraction-keyed trip that, unlike the
	// per-domain restraint, cannot tell an isolated fault in a small fleet from a correlated fault in a large one).
	breaker   bool
	dwellBase time.Duration // provider RepairTiming.Dwell, snapshotted at construction (freezes the timescale)
}

// breakerUnhealthyFraction is the shipped node.health breaker's allowedUnhealthyPercent (20%): if more than this
// fraction of a NodePool's nodes are unhealthy, the breaker trips and blocks all repair in that pool.
const breakerUnhealthyFraction = 0.20

// repairProbe is a candidate repair admitted and is now watching to a terminal outcome, so it can report Proven/Failed
// to the restraint policy. Repair (not the policy) owns this I/O: whether the replacement came up, and the success
// dwell. The candidate snapshot is retained so the outcome can be attributed to the same failure domains that were
// admitted, even after the original node is gone.
type repairProbe struct {
	candidate     *Candidate
	nodeClaimName string
	replacements  []*Replacement // the pre-spun replacement(s) to watch for durable health; empty for terminate-first
	succeededAt   time.Time      // when the replacement was first observed healthy after the original terminated; zero until then
	dwell         time.Duration  // this probe's success dwell, fixed when succeededAt is set (base ± jitter, or 0 if optimistic)
	creditedAt    time.Time      // when RepairProven was reported; set only for claw-back watching, zero otherwise
}

// RepairOptions carries optional overrides for NewRepair. The only override today is the restraint policy, which
// defaults to the correlated-failure AIMD policy; a test (or an alternate build) can swap in noRestraint or any other
// RepairRestraint without touching the constructor's callers.
type RepairOptions struct {
	restraint RepairRestraint
}

// WithRestraint overrides the RepairRestraint policy repair paces with (default: the correlated-failure AIMD policy).
// Passing noRestraint recovers the budget-only behavior, which is useful as a control arm when demonstrating what
// restraint changes.
func WithRestraint(rr RepairRestraint) option.Function[RepairOptions] {
	return func(o *RepairOptions) { o.restraint = rr }
}

func NewRepair(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, cp cloudprovider.CloudProvider, recorder events.Recorder, clk clock.Clock, queue *Queue, opts ...option.Function[RepairOptions]) *Repair {
	// The restraint policy + dwell handling default from REPAIR_OPTION (a sim knob to compare implementations from one
	// build); WithRestraint still overrides for tests. Cooldowns come from the provider's RepairTiming so the whole
	// restraint timescale scales together with the dwell.
	t := cp.RepairTiming()
	tuning := repairOptionDefaults(clk, t)
	o := option.Resolve(append([]option.Function[RepairOptions]{WithRestraint(tuning.restraint)}, opts...)...)
	return &Repair{
		kubeClient:    kubeClient,
		cluster:       cluster,
		provisioner:   provisioner,
		cloudProvider: cp,
		recorder:      recorder,
		clock:         clk,
		queue:         queue,
		probes:        map[string]*repairProbe{},
		watching:      map[string]*repairProbe{},
		acted:         map[string]*Candidate{},
		restraint:     o.restraint,
		optimistic:    tuning.optimistic,
		clawback:      tuning.clawback,
		dwellJitter:   tuning.dwellJitter,
		pinned:        tuning.pinned,
		breaker:       tuning.breaker,
		dwellBase:     t.Dwell,
	}
}

// repairTuning is the repair-behavior configuration selected from REPAIR_OPTION (restraint policy + dwell/credit +
// replacement placement). Bundling it keeps repairOptionDefaults a single return and NewRepair readable.
type repairTuning struct {
	restraint   RepairRestraint
	optimistic  bool
	clawback    time.Duration
	dwellJitter time.Duration
	pinned      bool
	breaker     bool
}

// repairOptionDefaults selects the repair-behavior config from the REPAIR_OPTION env var — a sim knob so one build can
// be deployed as several implementations to compare. Unset/unknown ⇒ the correlated-failure AIMD policy.
//
//	norestraint                    → noRestraint (only the disruption budget paces repair — the aggressive baseline;
//	                                 NOT the 20%-trip circuit breaker, which is a separate implementation not in this tree)
//	restraint-unpinned (or unset)  → AIMD (default): replacement may land in any zone (can escape a bad domain)
//	restraint-pinned               → AIMD, but each replacement is pinned to the candidate's zone (no escape)
//	jittered                       → AIMD, each probe's dwell randomized by a one-sided [0,+Dwell) offset → [Dwell,2·Dwell)
//	optimistic                     → AIMD, credit on first-healthy (dwell 0). Widens fast; does NOT claw back — the
//	                                 no-dwell control (a re-flapping replacement is re-credited as a fresh success).
//	clawback                       → AIMD, credit on first-healthy (dwell 0) AND watch the credited replacement for
//	                                 ClawbackWindow; a re-break in that window retroactively undoes the credit + backs off.
//	budgeted-breaker               → the 20%-unhealthy circuit-breaker trip layered on the modern budget + replace-first
//	                                 machinery (no AIMD): repair when the pool is under the trip, freeze when over it.
//	breaker-pinned                 → the breaker trip + budget + AIMD restraint + zone-pinned replacements (all mechanisms).
//	(The bare "breaker" arm — the shipped node.health build, force-delete + trip, no budget/restraint — is a SEPARATE
//	 image, not this unified binary; deploy it via REPAIR_OPTION=breaker → the :breaker ECR tag.)
func repairOptionDefaults(clk clock.Clock, t cloudprovider.RepairTiming) repairTuning {
	aimd := func() RepairRestraint { return newRestraint(clk, t.CooldownFloor, t.CooldownCeiling) }
	switch os.Getenv("REPAIR_OPTION") {
	case "norestraint":
		return repairTuning{restraint: noRestraint{}}
	case "restraint-pinned":
		return repairTuning{restraint: aimd(), pinned: true}
	case "budgeted-breaker":
		return repairTuning{restraint: noRestraint{}, breaker: true}
	case "breaker-pinned":
		return repairTuning{restraint: aimd(), breaker: true, pinned: true}
	case "jittered":
		return repairTuning{restraint: aimd(), dwellJitter: t.Dwell}
	case "jittered-pinned":
		return repairTuning{restraint: aimd(), dwellJitter: t.Dwell, pinned: true}
	case "optimistic":
		return repairTuning{restraint: aimd(), optimistic: true}
	case "optimistic-pinned":
		return repairTuning{restraint: aimd(), optimistic: true, pinned: true}
	case "clawback":
		return repairTuning{restraint: aimd(), optimistic: true, clawback: t.ClawbackWindow}
	case "clawback-pinned":
		return repairTuning{restraint: aimd(), optimistic: true, clawback: t.ClawbackWindow, pinned: true}
	default:
		return repairTuning{restraint: aimd()}
	}
}

// probeDwell is this probe's success dwell: 0 when optimistic (credit the instant it's healthy), else the provider's
// base dwell plus a uniform [0, dwellJitter) offset when jitter is on.
func (r *Repair) probeDwell(base time.Duration) time.Duration {
	if r.optimistic {
		return 0
	}
	if r.dwellJitter > 0 {
		return base + time.Duration(rand.Int63n(int64(r.dwellJitter)))
	}
	return base
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
	// Reason-level eligibility (F1), across ALL of the node's unhealthy conditions: a policy applies when its condition
	// matches AND some reason on the node matches its ReasonMatcher, measured from that reason's own onset. Considering
	// every condition (not just the nearest-deadline one) ensures a false-positive flood on one condition can't mask a
	// genuine fault on another. When no policy carries a ReasonMatcher this collapses to condition-only.
	_, eligible := resolveNodePolicy(r.cloudProvider.RepairPolicies(), c.Node, r.clock.Now())
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
		// merged folds priority + drain bound across ALL the node's unhealthy conditions/reasons (so a flood on one
		// condition can't mask a real fault on another). The nearest-deadline condition is the representative aging
		// basis for the score (time past its toleration).
		merged, eligible := resolveNodePolicy(policies, c.Node, r.clock.Now())
		if !eligible {
			continue
		}
		policy, cond := matchingPolicy(policies, c.Node)
		if policy == nil {
			continue
		}
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

	// Circuit-breaker trip (budgeted-breaker / breaker-pinned): mirror the shipped node.health breaker by freezing ALL
	// repair in a NodePool whose unhealthy fraction exceeds breakerUnhealthyFraction. Unhealthy ≈ the repair-eligible
	// candidates in the pool; the denominator is the pool's registered nodes. This is fraction-keyed, so it trips the
	// same on a lone fault in a tiny fleet (1/3 = 33%) and a correlated burst in a huge one (350/1000 = 35%) — the
	// scale/correlation blindness the per-domain restraint avoids.
	var breakerTripped map[string]bool
	if r.breaker {
		poolNodes := map[string]int{}
		for n := range r.cluster.Nodes() {
			if np := n.Labels()[v1.NodePoolLabelKey]; np != "" {
				poolNodes[np]++
			}
		}
		poolUnhealthy := map[string]int{}
		for _, c := range candidates {
			poolUnhealthy[c.NodePool.Name]++
		}
		breakerTripped = map[string]bool{}
		for np, u := range poolUnhealthy {
			total := poolNodes[np]
			if total == 0 {
				continue
			}
			// Mirror node.health EXACTLY: threshold = ceil(allowedUnhealthyPercent * N) (round up), and the pool is
			// unhealthy only when unhealthyCount EXCEEDS it. The round-up is why a lone fault in a small pool never trips
			// (3 nodes ⇒ threshold 1, so 1 unhealthy is tolerated even though it's 33%).
			threshold := int(math.Ceil(breakerUnhealthyFraction * float64(total)))
			if u > threshold {
				breakerTripped[np] = true
			}
		}
	}

	for _, candidate := range candidates {
		rr, ok := res[candidate]
		if !ok || disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Breaker trip: while the pool is over the unhealthy-fraction line, block every repair in it (latched until
		// enough nodes recover on their own to drop back under the line — which, for a durable correlated fault, never
		// happens, so the pool stays frozen).
		if r.breaker && breakerTripped[candidate.NodePool.Name] {
			continue
		}
		// Correlated-failure restraint (F3): even within budget, only admit this candidate if the restraint policy
		// allows it. The default policy paces per failure domain (NodePool ∧ Zone ∧ Policy), cold-starting a correlated
		// burst at one probe and widening only as probes prove out.
		if !r.restraint.CanDisrupt(candidate) {
			continue
		}
		// A static NodePool has no room to grow, known without a simulation, so terminate-first directly (no replacement
		// to stage). The budget still paces it and the drain still honors PDBs/TGP.
		if terminateFirst(ctx, candidate, pscheduling.Results{}) {
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
		// With the candidate-gone simulation in hand, the shared terminate-first primitive decides replace-first vs
		// delete-only from the fleet's capacity posture (reserved/ODCR with no headroom → terminate-first).
		if terminateFirst(ctx, candidate, results) {
			return r.terminateFirstCommand(ctx, candidate, rr)
		}
		// Bound the drain per the matched policy before the queue terminates the candidate, so repair is never an
		// unbounded hang and a forceful (0) policy skips the drain for conditions the kubelet can't evict through.
		if err := r.stampDrainBound(ctx, candidate, rr.merged); err != nil {
			return []Command{}, err
		}
		// Pinned variant: force each replacement into the candidate's own zone (no escape). Intersecting an In[zone]
		// requirement onto the scheduled replacement makes the cloud provider launch it in the failed domain, so a
		// domain-scoped fault re-inherits on the replacement and repair churns in place rather than escaping to a
		// healthy zone. (Unpinned leaves placement free, which is how repair escapes a bad zone today.)
		// On single-topo cells the pool spans all zones, so the intersection always keeps candidate.zone (non-empty). If
		// a replacement already carried a conflicting zone constraint (e.g. a per-AZ pool that escaped to another zone),
		// the intersection is empty and that NodeClaim never launches — which still yields the intended pinned outcome
		// (repair can't produce a working in-zone replacement → churns → no recovery), and is safe because replace-then-
		// terminate leaves the original in place (no outage), the stalled probe just resolves as RepairFailed.
		if r.pinned && candidate.zone != "" {
			for _, nc := range results.NewNodeClaims {
				nc.Requirements.Add(scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, candidate.zone))
			}
		}
		// Share the same *Replacement objects with the probe: the queue fills in each Replacement.Name on launch, so the
		// probe can later find the replacement NodeClaim(s) and verify they stay healthy through the dwell.
		replacements := replacementsFromNodeClaims(results.NewNodeClaims...)
		r.trackProbe(candidate, replacements...)
		return []Command{{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacements,
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
func (r *Repair) trackProbe(c *Candidate, replacements ...*Replacement) {
	r.probes[c.ProviderID()] = &repairProbe{candidate: c, nodeClaimName: c.NodeClaim.Name, replacements: replacements}
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
			// The original was terminated, so the queue proceeded — the replacement reached Initialized. But a durable
			// success (invariant: Ready AND healthy for a while) requires the replacement to STAY healthy through the
			// dwell. A systematic false positive re-flags the replacement too, so we must verify the replacement's health,
			// not merely time out from the original's termination — otherwise a re-flagged replacement counts as a success.
			health, replNode := r.replacementHealth(ctx, p)
			switch health {
			case replUnhealthy:
				// The replacement is itself repair-eligible (fault re-inherited/re-flagged) or was reaped: not a durable
				// success. Record a failure so the domain backs off; the unhealthy replacement is handled as its own candidate.
				r.restraint.Record(p.candidate, replNode, RepairFailed)
				delete(r.probes, providerID)
			case replPending:
				// The replacement isn't observable yet (not launched, or its Node hasn't registered): don't start the dwell
				// clock until we've confirmed a healthy replacement.
			case replHealthy:
				if p.succeededAt.IsZero() {
					p.succeededAt = now // first observation of a healthy replacement — measure the dwell from here
					p.dwell = r.probeDwell(r.dwellBase)
				}
				if !now.Before(p.succeededAt.Add(p.dwell)) {
					// Attribute the success to where the replacement actually came up (replNode), so a replacement in a
					// different zone doesn't credit the candidate's zone (learn-from-replacement).
					r.restraint.Record(p.candidate, replNode, RepairProven)
					delete(r.probes, providerID)
					delete(r.acted, providerID) // outcome settled; the original is gone, nothing left to watch for clearance
					// Claw-back variant: the credit is provisional. Keep watching the replacement for a re-break inside the
					// window (observeClawback below finalizes or reverses it). Only optimistic (dwell 0) sets clawback>0.
					if r.clawback > 0 {
						p.creditedAt = now
						r.watching[providerID] = p
					}
				}
			}
		case err == nil:
			// The original still exists → the replacement never came up healthy enough to terminate it (bad-AMI loop,
			// partitioned zone, command timeout). No replacement Node to attribute to, so the failure falls on the launch
			// target (all the candidate's domains except zone, which learn-from-replacement can't confirm).
			r.restraint.Record(p.candidate, nil, RepairFailed)
			delete(r.probes, providerID)
			// keep in acted: a failed repair leaves the original unhealthy; watch for it to clear on its own.
		default:
			// transient get error; leave the probe to be re-observed next pass
		}
	}

	// Claw-back watch: an optimistically-credited repair is provisional until its replacement survives the claw-back
	// window. Re-check each watched replacement — a re-break inside the window reverses the credit (RepairClawedBack, so
	// the domain undoes its widen and backs off); surviving the window finalizes the credit. Empty unless the claw-back
	// variant is active. The re-flagged replacement is separately handled as its own repair candidate on a later pass.
	for providerID, p := range r.watching {
		health, replNode := r.replacementHealth(ctx, p)
		switch {
		case health == replUnhealthy && replNode != nil:
			// A POSITIVELY-observed re-break: the credited replacement is itself repair-eligible again inside the window.
			// Claw the credit back so the domain undoes its widen and backs off.
			r.restraint.Record(p.candidate, replNode, RepairClawedBack)
			delete(r.watching, providerID)
		case health == replUnhealthy:
			// replUnhealthy with no node (the replacement NodeClaim is simply gone/reaped): we cannot attribute a
			// re-break to it, and in this post-credit context a disappearance is usually unrelated (an unrelated delete,
			// not a fault re-inheriting). Let the credit stand rather than claw back on an ambiguous deletion.
			delete(r.watching, providerID)
		default:
			// replHealthy or replPending: keep watching until the window closes, then the credit is final.
			if !now.Before(p.creditedAt.Add(r.clawback)) {
				delete(r.watching, providerID)
			}
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
			// Node exists and is healthy again: the episode cleared without a repair (no replacement involved).
			r.restraint.Record(c, nil, RepairCleared)
			delete(r.acted, providerID)
		}
	}
}

// replHealth is the observed health of a probe's pre-spun replacement(s) during the success dwell.
type replHealth int

const (
	replHealthy   replHealth = iota // every replacement is up and not itself exhibiting a repair-worthy fault
	replUnhealthy                   // a replacement is repair-eligible (fault re-inherited/re-flagged) or was reaped
	replPending                     // a replacement is not yet observable (not launched, or its Node not registered yet)
)

// replacementHealth inspects a probe's pre-spun replacement(s) so the dwell judges a DURABLE success — Ready AND healthy
// for a while (invariant) — rather than a bare timer from the original's termination. A systematic false positive
// re-flags the replacement, and a bad AMI / partitioned zone produces a replacement that never comes up healthy; both
// must be caught here as failures. A probe with no replacements (terminate-first / delete-only) has nothing to watch, so
// the dwell timer alone governs it. It also returns the resolved replacement Node (nil when there is none / it isn't
// observable) so the caller can attribute the outcome to the zone the replacement actually came up in.
func (r *Repair) replacementHealth(ctx context.Context, p *repairProbe) (replHealth, *corev1.Node) {
	if len(p.replacements) == 0 {
		return replHealthy, nil // terminate-first: no pre-spun replacement to watch; the dwell timer is the only signal
	}
	policies := r.cloudProvider.RepairPolicies()
	var lastNode *corev1.Node
	for _, repl := range p.replacements {
		if repl.Name == "" {
			return replPending, nil // not launched yet (queue hasn't assigned a NodeClaim name)
		}
		nc := &v1.NodeClaim{}
		if err := r.kubeClient.Get(ctx, types.NamespacedName{Name: repl.Name}, nc); err != nil {
			if apierrors.IsNotFound(err) {
				return replUnhealthy, nil // the replacement NodeClaim was reaped (ICE, init-TTL, or its own repair): not durable
			}
			return replPending, nil // transient get error; re-observe next pass
		}
		if nc.Status.NodeName == "" {
			return replPending, nil // the replacement's Node hasn't registered yet
		}
		// Resolve the Node by name (not a providerID field-selector list), so this works without a registered index.
		node := &corev1.Node{}
		if err := r.kubeClient.Get(ctx, types.NamespacedName{Name: nc.Status.NodeName}, node); err != nil {
			return replPending, nil // Node not readable yet
		}
		lastNode = node
		if _, cond := matchingPolicy(policies, node); cond != nil {
			return replUnhealthy, node // the replacement is itself repair-eligible: the fault was re-inherited/re-flagged
		}
	}
	return replHealthy, lastNode
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
