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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// White-box specs for the pure terminate-first helper (onlyReservedReplacements), isolated from the disruption
// pipeline. The end-to-end behavior (reservationFull, the feature-flag gate, delete-only vs replace commands) is
// covered by the integration specs in terminatefirst_test.go.

// replacementWith builds a simulated replacement NodeClaim whose capacity-type requirement permits exactly the given
// capacity types (mirroring what FinalizeScheduling leaves on a NodeClaim).
func replacementWith(capacityTypes ...string) *pscheduling.NodeClaim {
	nc := &pscheduling.NodeClaim{}
	nc.Requirements = scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityTypes...))
	return nc
}

var _ = Describe("TerminateFirst/onlyReservedReplacements", func() {
	It("is true when every replacement can launch only as reserved", func() {
		Expect(onlyReservedReplacements([]*pscheduling.NodeClaim{replacementWith(v1.CapacityTypeReserved)})).To(BeTrue())
	})
	It("is false for an empty set (an empty-node delete, not terminate-first)", func() {
		Expect(onlyReservedReplacements(nil)).To(BeFalse())
	})
	It("is false when a replacement permits on-demand fallback", func() {
		Expect(onlyReservedReplacements([]*pscheduling.NodeClaim{replacementWith(v1.CapacityTypeReserved, v1.CapacityTypeOnDemand)})).To(BeFalse())
	})
	It("is false when a replacement permits spot fallback", func() {
		Expect(onlyReservedReplacements([]*pscheduling.NodeClaim{replacementWith(v1.CapacityTypeReserved, v1.CapacityTypeSpot)})).To(BeFalse())
	})
	It("is false when a replacement is not reserved at all", func() {
		Expect(onlyReservedReplacements([]*pscheduling.NodeClaim{replacementWith(v1.CapacityTypeOnDemand)})).To(BeFalse())
	})
	It("is false when any one of several replacements has a fallback", func() {
		Expect(onlyReservedReplacements([]*pscheduling.NodeClaim{
			replacementWith(v1.CapacityTypeReserved),
			replacementWith(v1.CapacityTypeReserved, v1.CapacityTypeOnDemand),
		})).To(BeFalse())
	})
})
