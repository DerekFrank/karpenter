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

package cost

import (
	"testing"

	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

func TestNodeClaimOfferingKeepsTheContributionLaws(t *testing.T) {
	spot := nodeClaimOffering{offering: OfferingKey{Zone: "z", CapacityType: "spot", InstanceName: "a"}, n: 1}
	onDemand := nodeClaimOffering{offering: OfferingKey{Zone: "z", CapacityType: "on-demand", InstanceName: "a"}, n: 1}
	fixed := nodeClaimOffering{fixed: true, price: 0.75, n: 1}
	otherFixed := nodeClaimOffering{fixed: true, price: 1.25, n: 1}
	// A NodeClaim with every label, each empty.
	empty := nodeClaimOffering{n: 1}
	for _, base := range []NodePoolOfferings{
		{},
		{Offerings: map[OfferingKey]int{spot.offering: 2, onDemand.offering: 1}, Fixed: map[float64]int{0.75: 1}},
	} {
		for _, c := range [][2]nodeClaimOffering{{spot, onDemand}, {spot, spot}, {spot, fixed}, {fixed, onDemand}, {fixed, otherFixed}, {empty, spot}, {spot, empty}} {
			statetest.ExpectContribution(t, base, c[0], c[1])
		}
	}
}

func TestNodeClaimOfferingLeavesTheFieldsItGetsIntact(t *testing.T) {
	spot := nodeClaimOffering{offering: OfferingKey{Zone: "z", CapacityType: "spot", InstanceName: "a"}, n: 1}
	f := NodePoolOfferings{Offerings: map[OfferingKey]int{spot.offering: 1}}
	spot.Update(f, nodeClaimOffering{})
	spot.Delete(f)
	if f.Offerings[spot.offering] != 1 {
		t.Errorf("fields changed to %+v", f)
	}
	if got := spot.Delete(f); got.Offerings != nil || got.Fixed != nil {
		t.Errorf("Delete of the last contribution = %+v, want the zero fields", got)
	}
}
