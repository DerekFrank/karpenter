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
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// Terminate-First Disruption (RFC kubernetes-sigs/karpenter#3203). A voluntary disruption normally replace-firsts:
// launch the replacement, then remove the original. A fleet with no room to grow (reserved/ODCR at capacity, or a
// static NodePool at its replica count) can't stage a replacement first, so it must terminate first and let reactive
// provisioning refill the freed slot. This is a shared primitive: both drift and repair select it the same way, from
// the candidate's capacity posture + the scheduling simulation — there is no new API. It is gated by the TerminateFirst
// feature flag (on by default; it only ever activates where replace-first cannot run, so it never touches a healthy
// fleet, and the disruption budget / do-not-disrupt remain the operator's levers).

// terminateFirst reports whether a candidate's NodePool has no room to grow a replacement, so a voluntary disruption
// must delete it before provisioning can refill the slot. results is the candidate-gone scheduling simulation. A launch
// *failure* must never land here — this reads structural capacity (static replicas, reserved-only placement), never
// launch outcomes — so a failed launch keeps replacing-first under the existing per-NodePool launch backoff.
func terminateFirst(ctx context.Context, c *Candidate, results pscheduling.Results) bool {
	if !options.FromContext(ctx).FeatureGates.TerminateFirst {
		return false
	}
	// Static: a NodePool pinned to Spec.Replicas runs a fixed node count — a pre-spun replacement would be an (N+1)th
	// node the operator capped out. Known before any simulation.
	if c.OwnedByStaticNodePool() {
		return true
	}
	// Dynamic/reserved: the simulation ran as if the candidate were already gone. If it can only place the replacement
	// back into the same reservation (every new NodeClaim is reserved capacity), the pool can't grow; any other option
	// (incl. on-demand) means replace-first as usual.
	return c.capacityType == v1.CapacityTypeReserved && onlyReservedReplacements(results.NewNodeClaims)
}

// onlyReservedReplacements reports whether every simulated replacement resolved to reserved capacity — i.e. the pool
// can only grow back into the same reservation, so there is no headroom. An empty set (no replacement needed) is not
// "reserved-only": that is an ordinary empty-node delete, handled on the normal path.
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
