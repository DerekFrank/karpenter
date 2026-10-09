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
	"fmt"
	"math"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	"k8s.io/klog/v2"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	pkgscheduling "sigs.k8s.io/karpenter/pkg/scheduling"

	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// StaticDrift is a subreconciler that deletes drifted static candidates.
type StaticDrift struct {
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
	cloudprovider cloudprovider.CloudProvider
	recorder      events.Recorder
}

func NewStaticDrift(cluster *state.Cluster, provisioner *provisioning.Provisioner, cloudprovider cloudprovider.CloudProvider, recorder events.Recorder) *StaticDrift {
	return &StaticDrift{
		cluster:       cluster,
		provisioner:   provisioner,
		cloudprovider: cloudprovider,
		recorder:      recorder,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *StaticDrift) ShouldDisrupt(_ context.Context, c *Candidate) bool {
	return c.OwnedByStaticNodePool() && c.NodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).IsTrue()
}

func (d *StaticDrift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	// Group candidates by nodepool name
	candidatesByNodePool := lo.GroupBy(candidates, func(candidate *Candidate) string {
		return candidate.NodePool.Name
	})

	var cmds []Command
	for npName, npCandidates := range candidatesByNodePool {
		np := npCandidates[0].NodePool

		if disruptionBudgetMapping[npName] == 0 {
			continue
		}

		limit, ok := np.Spec.Limits[resources.Node]
		nodeLimit := lo.Ternary(ok, limit.Value(), int64(math.MaxInt64))
		// Current nodes (includes in‑flight per your cluster state)
		runningNodes, _, nodesPendingDisruptionCount := d.cluster.NodePoolState.GetNodeCount(npName)

		// We dont want to disrupt nodes until scale down is complete
		if int64(runningNodes+nodesPendingDisruptionCount) > lo.FromPtr(np.Spec.Replicas) {
			continue
		}

		maxDrifts := lo.Min([]int64{
			int64(disruptionBudgetMapping[np.Name]),
			int64(len(npCandidates)),
		})

		// Full reservations are checked before reserving limits.nodes so the delete-only path never takes a slot.
		nct := scheduling.NewNodeClaimTemplate(np)
		reserved, unreserved := lo.FilterReject(npCandidates, func(c *Candidate, _ int) bool { return c.capacityType == v1.CapacityTypeReserved })
		if len(reserved) > 0 {
			reservationsFull, err := staticReservationsFull(ctx, d.cloudprovider, np, nct)
			if err != nil {
				return []Command{}, err
			}
			if reservationsFull {
				// Only a reserved candidate frees a slot the refill can use, so it goes first and is the only one that can
				// terminate first; the rest can't be replaced either way.
				for _, c := range append(reserved, unreserved...)[:maxDrifts] {
					if options.FromContext(ctx).FeatureGates.TerminateFirstDrift && c.capacityType == v1.CapacityTypeReserved {
						cmds = append(cmds, Command{
							Candidates:          []*Candidate{c},
							PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{c}),
							TerminateFirst:      true,
						})
						continue
					}
					d.recorder.Publish(disruptionevents.Blocked(c.Node, c.NodeClaim, "static NodePool's capacity reservations are full and cannot stage a replacement")...)
				}
				continue
			}
		}

		// Acquire limits from cluster state without bursting over. maxAllowedDrifts is how many candidates we can drift
		// while staging a replacement for each without exceeding the NodePool's node limit; 0 means the pool is at its
		// limit and can't stage any replacement.
		maxAllowedDrifts := d.cluster.NodePoolState.ReserveNodeCount(npName, nodeLimit, maxDrifts)

		// Terminate-first (RFC #3203): when the NodePool is at its node limit it can't stage a replacement first — a
		// pre-spun replacement would be an (N+1)th node the operator capped out. Issue budget-paced delete-only commands;
		// once the freed slot is released the static.provisioning controller refills the pool back to Spec.Replicas. The
		// drain still honors PDBs and is bounded by TGP. When the pool has room under its limit, fall through to the
		// normal replace-first path below. No replacement is reserved for terminate-first, so the reservation above is a
		// no-op in that case (it reserved nothing).
		if options.FromContext(ctx).FeatureGates.TerminateFirstDrift && maxAllowedDrifts == 0 {
			for _, c := range npCandidates[:maxDrifts] {
				cmds = append(cmds, Command{
					Candidates:          []*Candidate{c},
					PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{c}),
					TerminateFirst:      true,
				})
			}
			continue
		}

		// We will not get a negative value here
		if maxAllowedDrifts == 0 {
			for _, c := range npCandidates[:maxDrifts] {
				d.recorder.Publish(disruptionevents.Blocked(c.Node, c.NodeClaim, "static NodePool is at its node limit and cannot stage a replacement")...)
			}
			continue
		}

		// Select candidates up to maxAllowedDrifts
		for _, c := range npCandidates[:maxAllowedDrifts] {
			result := scheduling.Results{
				NewNodeClaims: []*scheduling.NodeClaim{{NodeClaimTemplate: *nct}},
			}
			cmds = append(cmds, Command{
				Candidates:          []*Candidate{c},
				Replacements:        replacementsFromNodeClaims(result.NewNodeClaims...),
				Results:             result,
				PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{c}),
			})
		}
	}
	return cmds, nil
}

func (d *StaticDrift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *StaticDrift) Class() string {
	return EventualDisruptionClass
}

func (d *StaticDrift) ConsolidationType() string {
	return ""
}

// staticReservationsFull reports whether a static NodePool's replacement can only land in full capacity reservations, so
// pre-spinning it would fail to launch until a reserved node frees its slot. A static replacement is the bare NodePool
// template, so this is an offering lookup rather than a scheduling simulation. GetInstanceTypes is NodeClass-scoped, so
// offerings are filtered by the template's requirements.
func staticReservationsFull(ctx context.Context, cloudProvider cloudprovider.CloudProvider, np *v1.NodePool, nct *scheduling.NodeClaimTemplate) (bool, error) {
	its, err := cloudProvider.GetInstanceTypes(ctx, np)
	if err != nil {
		return false, serrors.Wrap(fmt.Errorf("getting instance types, %w", err), "NodePool", klog.KObj(np))
	}
	return !lo.SomeBy(its, func(it *cloudprovider.InstanceType) bool {
		return nct.Requirements.IsCompatible(it.Requirements, pkgscheduling.AllowUndefinedWellKnownLabels) &&
			len(it.Offerings.Compatible(nct.Requirements).Launchable()) > 0
	}), nil
}
