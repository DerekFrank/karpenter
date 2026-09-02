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
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// White-box specs for the pure offering-matching helper reservedOfferingsOnlyForReservation, isolated from the
// disruption pipeline. The end-to-end behavior (capacity-type fallback, reservationFull, the feature-flag gate, and
// delete-only vs replace commands) is covered by the integration specs in terminatefirst_test.go.

// reservedNodeClaimWith builds a simulated replacement NodeClaim, reserved-only, whose instance-type options expose one
// reserved offering per given reservation id.
func reservedNodeClaimWith(reservationIDs ...string) *pscheduling.NodeClaim {
	nc := &pscheduling.NodeClaim{}
	nc.Requirements = scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeReserved))
	var offerings cloudprovider.Offerings
	for _, id := range reservationIDs {
		offerings = append(offerings, &cloudprovider.Offering{
			Available: true,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:          v1.CapacityTypeReserved,
				cloudprovider.ReservationIDLabel: id,
			}),
		})
	}
	nc.InstanceTypeOptions = cloudprovider.InstanceTypes{{Offerings: offerings}}
	return nc
}

var _ = Describe("TerminateFirst/reservedOfferingsOnlyForReservation", func() {
	It("is true when the candidate's reservation is the only reserved option", func() {
		Expect(reservedOfferingsOnlyForReservation(reservedNodeClaimWith("r-a"), "r-a")).To(BeTrue())
	})
	It("is false when a different reservation is also an option (the pool can grow there)", func() {
		Expect(reservedOfferingsOnlyForReservation(reservedNodeClaimWith("r-a", "r-b"), "r-a")).To(BeFalse())
	})
	It("is false when the only reserved option is a different reservation", func() {
		Expect(reservedOfferingsOnlyForReservation(reservedNodeClaimWith("r-b"), "r-a")).To(BeFalse())
	})
	It("is false when the replacement has no reserved offerings at all", func() {
		Expect(reservedOfferingsOnlyForReservation(reservedNodeClaimWith(), "r-a")).To(BeFalse())
	})
})
