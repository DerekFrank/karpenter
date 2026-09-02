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
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// Terminate-First Disruption (RFC kubernetes-sigs/karpenter#3203). A voluntary disruption normally replace-firsts:
// launch the replacement, then remove the original. A fleet with no room to grow — a reserved/ODCR pool whose
// reservation is full, or a static NodePool at its replica count — can't stage a replacement first, so it must
// terminate first and let reactive provisioning refill the freed slot.
//
// This is a shared primitive, not a per-method bolt-on: any voluntary disruption method decides it the same way, from
// the candidate's capacity posture and the candidate-gone scheduling simulation it already runs — there is no new API.
// Callers gate it on the TerminateFirst feature flag.
//
// A launch *failure* must never select terminate-first: this reads structural capacity (a full reservation the
// candidate itself holds), never launch outcomes, so a failed launch keeps replacing-first under the existing
// per-NodePool launch backoff.

// terminateFirst reports whether a voluntary disruption of a reserved candidate must delete it before a replacement can
// be provisioned. It is decided from the single candidate-gone simulation the caller already ran (no extra pass):
// terminate-first exactly when every simulated replacement can launch ONLY into the candidate's own reservation
// (reserved-only, no on-demand/spot fallback, and the candidate's reservation its only reserved option) AND that
// reservation is full — so the only way to place the replacement is to free the candidate's slot first. A spare
// reservation slot, a different reservation with capacity, or any non-reserved fallback all replace-first as usual.
func terminateFirst(c *Candidate, results pscheduling.Results) bool {
	if c.capacityType != v1.CapacityTypeReserved {
		return false
	}
	return replacementsReuseCandidateReservation(c, results.NewNodeClaims) && reservationFull(c)
}

// replacementsReuseCandidateReservation reports whether every simulated replacement can launch only into the
// candidate's own capacity reservation: reserved-only (no on-demand/spot fallback) and the candidate's reservation is
// the only reserved offering the replacement could use. If any replacement permits a non-reserved fallback, or could
// use a different reservation (which carries its own capacity), the pool can grow elsewhere and replaces-first. An
// empty set (no replacement needed) is an ordinary empty-node delete, handled on the normal path.
func replacementsReuseCandidateReservation(c *Candidate, newNodeClaims []*pscheduling.NodeClaim) bool {
	reservationID := c.Labels()[cloudprovider.ReservationIDLabel]
	if reservationID == "" || len(newNodeClaims) == 0 {
		return false
	}
	for _, nc := range newNodeClaims {
		ct := nc.Requirements.Get(v1.CapacityTypeLabelKey)
		if !ct.Has(v1.CapacityTypeReserved) || ct.Has(v1.CapacityTypeOnDemand) || ct.Has(v1.CapacityTypeSpot) {
			return false
		}
		if !reservedOfferingsOnlyForReservation(nc, reservationID) {
			return false
		}
	}
	return true
}

// reservedOfferingsOnlyForReservation reports whether the reserved offerings the replacement could launch into are all
// the given reservation — the candidate's own. A different reservation among the options means the pool can grow into
// that reservation's capacity without freeing the candidate, so it is not terminate-first.
func reservedOfferingsOnlyForReservation(nc *pscheduling.NodeClaim, reservationID string) bool {
	sawReserved := false
	for _, it := range nc.InstanceTypeOptions {
		for _, o := range it.Offerings {
			if o.CapacityType() != v1.CapacityTypeReserved || !o.Available {
				continue
			}
			if !nc.Requirements.IsCompatible(o.Requirements, scheduling.AllowUndefinedWellKnownLabels) {
				continue
			}
			sawReserved = true
			if o.ReservationID() != reservationID {
				return false
			}
		}
	}
	return sawReserved
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
