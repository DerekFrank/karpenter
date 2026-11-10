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
	"fmt"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// Replay paths and refusal reasons a ReplayResult reports.
const (
	ReplayPathReplay   = "replay"
	ReplayPathFallback = "fallback"

	ReplayReasonCandidate        = "candidate"
	ReplayReasonNominated        = "nominated"
	ReplayReasonBudget           = "budget"
	ReplayReasonPinnedFailed     = "pinned-failed"
	ReplayReasonUnschedulable    = "unschedulable"
	ReplayReasonReplacementCount = "replacement-count"
	ReplayReasonInstanceTypes    = "instance-types"
	ReplayReasonPrice            = "price"
	ReplayReasonError            = "error"
)

// ReplayResult is what ReplayValidator.Check decided about a command, and how.
type ReplayResult struct {
	Accepted bool
	// Path is how an accepted command was accepted: by replaying its placement, or by searching again for the pods
	// whose pinned placement failed.
	Path string
	// Reason is why a refused command was refused.
	Reason string
	Err    error
	// Command is the accepted command, with its replacement's instance types narrowed to the ones that still fit.
	Command  Command
	Duration time.Duration
	// failedPins are the pinned pods that couldn't schedule where the command placed them.
	failedPins []types.UID
}

// ReplayValidator validates a consolidation command by replaying the placement its simulation found (Command.Results)
// against current state, with the scheduler's own feasibility checks and no search: each pod moved off a candidate
// may schedule only where the command placed it. Pending pods and pods on deleting nodes schedule as resimulation
// schedules them. If only pinned pods fail, it unpins those and replays once more.
type ReplayValidator struct {
	validation
	filter         CandidateFilter
	validationType string
}

func NewSingleReplayValidator(c consolidation) *ReplayValidator {
	s := &SingleNodeConsolidation{consolidation: c}
	return newReplayValidator(c, s.ShouldDisrupt, s.ConsolidationType())
}

func NewMultiReplayValidator(c consolidation) *ReplayValidator {
	m := &MultiNodeConsolidation{consolidation: c}
	return newReplayValidator(c, m.ShouldDisrupt, m.ConsolidationType())
}

func newReplayValidator(c consolidation, filter CandidateFilter, validationType string) *ReplayValidator {
	return &ReplayValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonUnderutilized,
		},
		filter:         filter,
		validationType: validationType,
	}
}

func (r *ReplayValidator) Validate(ctx context.Context, cmd Command, validationPeriod time.Duration) (Command, error) {
	if validationPeriod > 0 {
		select {
		case <-ctx.Done():
			return Command{}, errors.New("context canceled")
		case <-r.clock.After(validationPeriod):
		}
	}
	result := r.Check(ctx, cmd)
	if !result.Accepted {
		return Command{}, result.Err
	}
	return result.Command, nil
}

// Check decides whether cmd is still valid against current state.
func (r *ReplayValidator) Check(ctx context.Context, cmd Command) (result ReplayResult) {
	start := time.Now()
	defer func() { result.Duration = time.Since(start) }()

	candidates, reason, err := r.currentCandidates(ctx, cmd.Candidates)
	if err != nil {
		return refused(reason, err)
	}
	pins := witness(cmd, candidates)
	result = r.replay(ctx, cmd, candidates, pins)
	if !result.Accepted && result.Reason == ReplayReasonPinnedFailed {
		for _, uid := range result.failedPins {
			delete(pins, uid)
		}
		result = r.replay(ctx, cmd, candidates, pins)
		if result.Accepted {
			result.Path = ReplayPathFallback
		}
	}
	if !result.Accepted {
		return result
	}
	// Revalidate the candidates after replaying, as resimulation does (https://github.com/kubernetes-sigs/karpenter/issues/1167).
	if _, reason, err := r.currentCandidates(ctx, cmd.Candidates); err != nil {
		return refused(reason, err)
	}
	return result
}

func refused(reason string, err error) ReplayResult {
	return ReplayResult{Reason: reason, Err: err}
}

// currentCandidates returns the current representation of the command's candidates, checking only them: each must
// still pass the method's filter, have no nomination, and fit its NodePool's budget.
func (r *ReplayValidator) currentCandidates(ctx context.Context, proposed []*Candidate) ([]*Candidate, string, error) {
	names := sets.New(lo.Map(proposed, func(c *Candidate, _ int) string { return c.Name() })...)
	nodePoolMap, nodePoolToInstanceTypesMap, err := BuildNodePoolMap(ctx, r.kubeClient, r.cloudProvider)
	if err != nil {
		return nil, ReplayReasonError, err
	}
	pdbs, err := pdb.NewLimits(ctx, r.kubeClient)
	if err != nil {
		return nil, ReplayReasonError, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	fixedNowClk := fixedNowClock{Clock: r.clock, now: r.clock.Now()}
	stateNodes := lo.Filter(r.cluster.DeepCopyNodes(), func(n *karpentercluster.StateNode, _ int) bool { return names.Has(n.Name()) })
	candidates := lo.FilterMap(stateNodes, func(n *karpentercluster.StateNode, _ int) (*Candidate, bool) {
		cn, err := NewCandidate(ctx, r.kubeClient, r.recorder, fixedNowClk, n, pdbs, nodePoolMap, nodePoolToInstanceTypesMap, r.queue, GracefulDisruptionClass)
		return cn, err == nil && r.filter(ctx, cn)
	})
	if len(candidates) != len(proposed) {
		FailedValidationsTotal.Add(float64(len(proposed)), map[string]string{ConsolidationTypeLabel: r.validationType})
		return nil, ReplayReasonCandidate, NewChurnValidationError(fmt.Errorf("%d candidates are no longer valid", len(proposed)-len(candidates)))
	}
	budgets, err := BuildDisruptionBudgetMapping(ctx, r.cluster, r.clock, r.kubeClient, r.cloudProvider, r.recorder, r.reason)
	if err != nil {
		return nil, ReplayReasonError, fmt.Errorf("building disruption budgets, %w", err)
	}
	for _, c := range candidates {
		if r.cluster.IsNodeNominated(c.NodeClaim.Name) {
			FailedValidationsTotal.Add(float64(len(proposed)), map[string]string{ConsolidationTypeLabel: r.validationType})
			return nil, ReplayReasonNominated, NewBudgetValidationError(fmt.Errorf("a candidate was nominated during validation"))
		}
		if budgets[c.NodePool.Name] == 0 {
			FailedValidationsTotal.Add(float64(len(proposed)), map[string]string{ConsolidationTypeLabel: r.validationType})
			return nil, ReplayReasonBudget, NewBudgetValidationError(fmt.Errorf("a candidate can no longer be disrupted without violating budgets"))
		}
		budgets[c.NodePool.Name]--
	}
	return candidates, "", nil
}

// witness pins each pod the command moves off its candidates to where the command's simulation placed it. A pod that
// is no longer on a candidate, such as one recreated under a new UID, isn't pinned and schedules as usual.
func witness(cmd Command, candidates []*Candidate) map[types.UID]pscheduling.PinTarget {
	moved := sets.New[types.UID]()
	for _, c := range candidates {
		for _, p := range c.reschedulablePods {
			moved.Insert(p.UID)
		}
	}
	pins := map[types.UID]pscheduling.PinTarget{}
	for _, n := range cmd.Results.ExistingNodes {
		for _, p := range n.Pods {
			if moved.Has(p.UID) {
				pins[p.UID] = pscheduling.PinTarget{Node: n.Name()}
			}
		}
	}
	for _, n := range cmd.Results.NewNodeClaims {
		for _, p := range n.Pods {
			if moved.Has(p.UID) {
				pins[p.UID] = pscheduling.PinTarget{}
			}
		}
	}
	return pins
}

// replay simulates scheduling with the pods in pins pinned, and accepts the command if every pod that must move
// schedules, onto no more new NodeClaims than the command replaces its candidates with, and a replacement still has
// instance types the command chose that are cheaper than the candidates.
//
//nolint:gocyclo
func (r *ReplayValidator) replay(ctx context.Context, cmd Command, candidates []*Candidate, pins map[types.UID]pscheduling.PinTarget) ReplayResult {
	results, err := SimulateScheduling(ctx, r.kubeClient, r.cluster, r.provisioner, r.clock, r.recorder,
		[]pscheduling.Options{pscheduling.IsConsolidationSimulation, pscheduling.Pin(pins)}, SimulationOptions{}, candidates...)
	if err != nil {
		return refused(ReplayReasonError, fmt.Errorf("simulating scheduling, %w", err))
	}
	if !results.AllNonPendingPodsScheduled() {
		failed := lo.Keys(lo.OmitBy(results.PodErrors, func(p *corev1.Pod, _ error) bool { return podutils.IsProvisionable(p) }))
		pinnedOnly := lo.EveryBy(failed, func(p *corev1.Pod) bool { _, ok := pins[p.UID]; return ok })
		res := refused(lo.Ternary(pinnedOnly, ReplayReasonPinnedFailed, ReplayReasonUnschedulable),
			NewSchedulingValidationError(errors.New(results.NonPendingPodSchedulingErrors())))
		if pinnedOnly {
			res.failedPins = lo.Map(failed, func(p *corev1.Pod, _ int) types.UID { return p.UID })
		}
		return res
	}
	accepted := cmd
	accepted.Candidates = candidates
	accepted.Results = results
	switch {
	case len(results.NewNodeClaims) == 0:
		if len(cmd.Replacements) != 0 {
			// Every pod fits without the replacement, so the command would launch a node nothing needs.
			return refused(ReplayReasonReplacementCount, NewSchedulingValidationError(fmt.Errorf("pods no longer need a replacement")))
		}
		return ReplayResult{Accepted: true, Path: ReplayPathReplay, Command: accepted}
	case len(results.NewNodeClaims) > 1 || len(cmd.Replacements) == 0:
		return refused(ReplayReasonReplacementCount, NewSchedulingValidationError(fmt.Errorf("replay needs %d new nodeclaims, command has %d", len(results.NewNodeClaims), len(cmd.Replacements))))
	}
	// The replacement keeps only the instance types the command chose that still fit its pods, and that are cheaper
	// than the candidates at their current prices.
	if results.NewNodeClaims[0].NodePoolName != cmd.Replacements[0].NodePoolName {
		return refused(ReplayReasonInstanceTypes, NewSchedulingValidationError(fmt.Errorf("the replacement's pods now need NodePool %q, not %q", results.NewNodeClaims[0].NodePoolName, cmd.Replacements[0].NodePoolName)))
	}
	chosen := sets.New(lo.Map(cmd.Replacements[0].InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) string { return it.Name })...)
	replacement := results.NewNodeClaims[0]
	replacement.InstanceTypeOptions = lo.Filter(replacement.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) bool { return chosen.Has(it.Name) })
	if len(replacement.InstanceTypeOptions) == 0 {
		return refused(ReplayReasonInstanceTypes, NewSchedulingValidationError(fmt.Errorf("no instance type the command chose fits the replacement's pods")))
	}
	replacement.InstanceTypeOptions = replacement.InstanceTypeOptions.OrderByPrice(replacement.Requirements)
	replacement, err = replacement.RemoveInstanceTypeOptionsByPriceAndMinValues(replacement.Requirements, sumCandidatePrices(candidates))
	if err != nil || len(replacement.InstanceTypeOptions) == 0 {
		return refused(ReplayReasonPrice, NewSchedulingValidationError(fmt.Errorf("the replacement is no longer cheaper than the candidates")))
	}
	// As computeConsolidation does, a replacement priced as spot launches only as spot.
	if ct := replacement.Requirements.Get(v1.CapacityTypeLabelKey); cloudprovider.HasCapacityTypeClass(ct, cloudprovider.CapacityTypeClassSpot) &&
		cloudprovider.HasCapacityTypeClass(ct, cloudprovider.CapacityTypeClassOnDemand) {
		replacement.Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	}
	results.NewNodeClaims[0] = replacement
	accepted.Replacements = replacementsFromNodeClaims(replacement)
	return ReplayResult{Accepted: true, Path: ReplayPathReplay, Command: accepted}
}
