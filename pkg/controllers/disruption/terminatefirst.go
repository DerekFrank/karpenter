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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// Terminate-First Disruption (RFC kubernetes-sigs/karpenter#3203). A voluntary disruption normally replace-firsts:
// launch the replacement, then remove the original. A fleet with no room to grow — a reserved/ODCR pool whose
// reservation is full, or a static NodePool at its replica count — can't stage a replacement first, so it must
// terminate first and let reactive provisioning refill the freed slot.
//
// This is a shared primitive, not a per-method bolt-on: any voluntary disruption method decides it the same way, from
// the candidate's capacity posture and the candidate-gone scheduling simulation it already runs — there is no new API.
// It is gated by the TerminateFirst feature flag.
//
// A launch *failure* must never select terminate-first: this reads structural capacity (static replicas, a full
// reservation with no fallback), never launch outcomes, so a failed launch keeps replacing-first under the existing
// per-NodePool launch backoff.

// terminateFirst reports whether a voluntary disruption of a reserved candidate must delete it before a replacement can
// be provisioned, because its NodePool has no room to grow. It is decided from the single candidate-gone simulation the
// caller already ran (no extra pass): terminate-first exactly when the simulated replacement can only launch as
// reserved capacity (no on-demand/spot fallback) AND the candidate's own reservation is full — so the only way to place
// that replacement is to free the candidate's slot first. A reservation with a spare slot, or any non-reserved
// fallback, replaces-first as usual.
func terminateFirst(ctx context.Context, c *Candidate, results pscheduling.Results) bool {
	if !options.FromContext(ctx).FeatureGates.TerminateFirst {
		return false
	}
	if c.capacityType != v1.CapacityTypeReserved {
		return false
	}
	return onlyReservedReplacements(results.NewNodeClaims) && reservationFull(c)
}

// onlyReservedReplacements reports whether every simulated replacement can launch only as reserved capacity — i.e. the
// pool has no on-demand/spot fallback to grow into. An empty set (no replacement needed) is not "reserved-only": that
// is an ordinary empty-node delete, handled on the normal path. A replacement that permits on-demand or spot is not
// reserved-only: it can replace-first by growing into that fallback.
func onlyReservedReplacements(newNodeClaims []*pscheduling.NodeClaim) bool {
	if len(newNodeClaims) == 0 {
		return false
	}
	for _, nc := range newNodeClaims {
		ct := nc.Requirements.Get(v1.CapacityTypeLabelKey)
		if !ct.Has(v1.CapacityTypeReserved) || ct.Has(v1.CapacityTypeOnDemand) || ct.Has(v1.CapacityTypeSpot) {
			return false
		}
	}
	return true
}

// reservationFull reports whether the candidate's own capacity reservation has no spare slot. The candidate's reserved
// offering carries the cloud provider's real remaining ReservationCapacity, which already excludes the candidate's
// live instance — so 0 means the candidate holds the last slot and a replacement can only be placed by freeing it
// first (terminate-first). A spare slot (>0) means the pool can grow a replacement in place (replace-first).
func reservationFull(c *Candidate) bool {
	if c.instanceType == nil {
		return false
	}
	reservationID := c.Labels()[cloudprovider.ReservationIDLabel]
	for _, o := range c.instanceType.Offerings {
		if o.CapacityType() == v1.CapacityTypeReserved && o.ReservationID() == reservationID {
			return o.ReservationCapacity == 0
		}
	}
	return false
}
