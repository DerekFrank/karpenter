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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

var _ = Describe("nodeClaimOffering", func() {
	spot := nodeClaimOffering{offering: OfferingKey{Zone: "z", CapacityType: "spot", InstanceName: "a"}, n: 1}
	onDemand := nodeClaimOffering{offering: OfferingKey{Zone: "z", CapacityType: "on-demand", InstanceName: "a"}, n: 1}
	fixed := nodeClaimOffering{fixed: true, price: 0.75, n: 1}
	otherFixed := nodeClaimOffering{fixed: true, price: 1.25, n: 1}
	// A NodeClaim with every label, each empty.
	empty := nodeClaimOffering{n: 1}

	DescribeTable("should keep the contribution laws",
		func(prev, next nodeClaimOffering) {
			for _, base := range []NodePoolOfferings{
				{},
				{Offerings: map[OfferingKey]int{spot.offering: 2, onDemand.offering: 1}, Fixed: map[float64]int{0.75: 1}},
			} {
				statetest.ExpectContribution(GinkgoT(), base, prev, next)
			}
		},
		Entry("spot to on-demand", spot, onDemand),
		Entry("spot to spot", spot, spot),
		Entry("spot to fixed", spot, fixed),
		Entry("fixed to on-demand", fixed, onDemand),
		Entry("fixed to another fixed", fixed, otherFixed),
		Entry("empty to spot", empty, spot),
		Entry("spot to empty", spot, empty),
	)
	It("should leave the fields it gets intact", func() {
		f := NodePoolOfferings{Offerings: map[OfferingKey]int{spot.offering: 1}}
		spot.Update(f, nodeClaimOffering{})
		spot.Delete(f)
		Expect(f.Offerings).To(HaveKeyWithValue(spot.offering, 1), "fields changed to %+v", f)
		got := spot.Delete(f)
		Expect(got.Offerings).To(BeNil(), "Delete of the last contribution should give the zero fields")
		Expect(got.Fixed).To(BeNil(), "Delete of the last contribution should give the zero fields")
	})
})
