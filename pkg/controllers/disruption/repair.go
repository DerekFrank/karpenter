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
	"k8s.io/apimachinery/pkg/api/equality"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// agingConstant (τ) is the time a node must wait past its toleration to earn one rank tier of standing. It sets the
// starvation bound: a node overtakes a steadily-refreshed rival Δrank tiers up after Δrank·τ. See resiliency §3.1.2.
const agingConstant = 30 * time.Minute

// Repair is a voluntary disruption method that remediates unhealthy nodes. It replaces the standalone node.health
// controller: repair rides the shared disruption budget (reason "Unhealthy"), pre-spins a replacement before
// terminating (replace-then-terminate), orders candidates by rank + age/τ − backoff, and is vetoed by do-not-repair.
type Repair struct {
	consolidation
}

func NewRepair(c consolidation) *Repair {
	return &Repair{consolidation: c}
}

// ShouldDisrupt is a predicate that filters candidates to nodes that have an unhealthy condition matching a
// RepairPolicy, have waited past that policy's toleration, and are not vetoed by the do-not-repair annotation.
func (r *Repair) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	// Repair is behind the NodeRepair feature gate, matching the old node.health controller's gating.
	if !options.FromContext(ctx).FeatureGates.NodeRepair || c.Node == nil {
		return false
	}
	// do-not-repair is the operator's escape hatch: it blocks all repair on this node, whatever the drain bound.
	// TODO: revisit whether do-not-disrupt should also imply do-not-repair (kubernetes-sigs/karpenter#2424).
	if c.Annotations()[v1.DoNotRepairAnnotationKey] == "true" {
		return false
	}
	policy, cond := r.matchingPolicy(c.Node)
	if policy == nil {
		return false
	}
	// Eligibility is delayed by the policy's toleration — a confidence window before repair acts.
	return !r.clock.Now().Before(cond.LastTransitionTime.Add(policy.TolerationDuration))
}

// ComputeCommands orders eligible candidates by the repair score and returns one replace-then-terminate command for the
// highest-scoring candidate whose NodePool has budget. Only one command per pass, mirroring drift.
func (r *Repair) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	ranks := r.denseRanks()
	sort.SliceStable(candidates, func(i, j int) bool {
		si, sj := r.score(candidates[i], ranks), r.score(candidates[j], ranks)
		if si != sj {
			return si > sj // higher score repairs first
		}
		// Deterministic tie-break: lower disruption cost, then node name, so the same set always yields the same order.
		if candidates[i].DisruptionCost != candidates[j].DisruptionCost {
			return candidates[i].DisruptionCost < candidates[j].DisruptionCost
		}
		return candidates[i].Name() < candidates[j].Name()
	})

	for _, candidate := range candidates {
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Pre-spin the replacement; the queue terminates the original only once the replacement is healthy.
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
		if err := r.stampDrainBound(ctx, candidate); err != nil {
			return []Command{}, err
		}
		return []Command{{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:             results,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		}}, nil
	}
	return []Command{}, nil
}

// score computes E = rank + age/τ − backoff. Age is time past toleration (post-eligibility), so a flakier signal's
// longer toleration never leaks into its standing. Backoff pulls back a whole NodePool whose launches keep failing.
func (r *Repair) score(c *Candidate, ranks map[int]int) float64 {
	policy, cond := r.matchingPolicy(c.Node)
	if policy == nil {
		return 0
	}
	rank := float64(ranks[policy.Priority])
	eligibleAt := cond.LastTransitionTime.Add(policy.TolerationDuration)
	age := r.clock.Now().Sub(eligibleAt)
	if age < 0 {
		age = 0
	}
	return rank + age.Minutes()/agingConstant.Minutes() - r.backoff(c.NodePool)
}

// denseRanks compresses the set of configured policy priorities into contiguous tiers (adjacent tiers one apart),
// so arbitrary priority magnitudes can't change what τ means — only the ordering of priorities matters.
func (r *Repair) denseRanks() map[int]int {
	priorities := lo.Uniq(lo.Map(r.cloudProvider.RepairPolicies(), func(p cloudprovider.RepairPolicy, _ int) int { return p.Priority }))
	sort.Ints(priorities)
	ranks := make(map[int]int, len(priorities))
	for i, p := range priorities {
		ranks[p] = i // lowest priority -> rank 0, ascending
	}
	return ranks
}

// backoff pulls a NodePool behind healthier pools when its replacements keep failing. A failed launch is almost never
// node-specific (the AMI/capacity/config is bad for the whole pool), so it is keyed to the NodePool. The existing
// NodeRegistrationHealthy signal is that per-pool "launches are failing" bit; when it is False, apply a one-tier pull.
func (r *Repair) backoff(np *v1.NodePool) float64 {
	if np != nil && np.StatusConditions().Get(v1.ConditionTypeNodeRegistrationHealthy).IsFalse() {
		return 1
	}
	return 0
}

// matchingPolicy returns the highest-priority RepairPolicy whose (type,status) matches an unhealthy condition on the
// node, plus the matched condition. When a node trips multiple policies, the highest priority wins (ties broken by the
// earlier toleration deadline) so a node with several unhealthy reasons is ordered by its most urgent one.
func (r *Repair) matchingPolicy(node *corev1.Node) (*cloudprovider.RepairPolicy, *corev1.NodeCondition) {
	var best *cloudprovider.RepairPolicy
	var bestCond *corev1.NodeCondition
	deadline := time.Time{}
	for i := range r.cloudProvider.RepairPolicies() {
		policy := r.cloudProvider.RepairPolicies()[i]
		cond := nodeutils.GetCondition(node, policy.ConditionType)
		if cond.Status != policy.ConditionStatus {
			continue
		}
		terminationTime := cond.LastTransitionTime.Add(policy.TolerationDuration)
		if best == nil || policy.Priority > best.Priority ||
			(policy.Priority == best.Priority && terminationTime.Before(deadline)) {
			p := policy
			c := cond
			best, bestCond, deadline = &p, &c, terminationTime
		}
	}
	return best, bestCond
}

// stampDrainBound sets the NodeClaim termination-timestamp annotation the termination controller reads as the hard
// drain deadline: now + effective TGP (min of the policy bound and the NodeClaim's own TGP), or now for a forceful
// (0) policy. A nil policy bound leaves the NodeClaim's TGP untouched (inherit).
func (r *Repair) stampDrainBound(ctx context.Context, c *Candidate) error {
	policy, _ := r.matchingPolicy(c.Node)
	if policy == nil || policy.TerminationGracePeriod == nil {
		return nil // inherit the NodeClaim's own TerminationGracePeriod
	}
	effective := *policy.TerminationGracePeriod
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

func (r *Repair) Reason() v1.DisruptionReason { return v1.DisruptionReasonUnhealthy }

func (r *Repair) Class() string { return RepairDisruptionClass }

func (r *Repair) ConsolidationType() string { return "" }
