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
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

// replacementOf builds a simulated replacement NodeClaim resolved to the given capacity type.
func replacementOf(capacityType string) *pscheduling.NodeClaim {
	nc := &pscheduling.NodeClaim{}
	nc.Requirements = scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: capacityType})
	return nc
}

// White-box specs for the shared terminate-first primitive (drift + repair): the pure capacity-posture decision,
// isolated from the disruption pipeline. The feature-flag gate is verified here rather than end-to-end because for a
// static pool the empty-replacement path also yields a delete-only command, so the flag's effect is only cleanly
// observable at this level.

func staticCandidate() *Candidate {
	return &Candidate{NodePool: &v1.NodePool{Spec: v1.NodePoolSpec{Replicas: lo.ToPtr(int64(1))}}}
}

// reservedResults is a candidate-gone simulation whose only replacement is reserved capacity (no headroom).
func reservedResults() pscheduling.Results {
	return pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{replacementOf(v1.CapacityTypeReserved)}}
}

var _ = Describe("Repair/TerminateFirst (primitive)", func() {
	It("selects terminate-first for a static NodePool", func() {
		ctx := options.ToContext(TestContextWithLogger(nil), test.Options())
		Expect(terminateFirst(ctx, staticCandidate(), pscheduling.Results{})).To(BeTrue())
	})

	It("selects terminate-first for a reserved candidate that can only re-place into a reservation", func() {
		ctx := options.ToContext(TestContextWithLogger(nil), test.Options())
		c := &Candidate{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np"}}, capacityType: v1.CapacityTypeReserved}
		Expect(terminateFirst(ctx, c, reservedResults())).To(BeTrue())
	})

	It("replaces-first for a reserved candidate that has an on-demand fallback", func() {
		ctx := options.ToContext(TestContextWithLogger(nil), test.Options())
		c := &Candidate{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np"}}, capacityType: v1.CapacityTypeReserved}
		results := pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{replacementOf(v1.CapacityTypeOnDemand)}}
		Expect(terminateFirst(ctx, c, results)).To(BeFalse())
	})

	It("is gated off by the TerminateFirst feature flag", func() {
		ctx := options.ToContext(TestContextWithLogger(nil), test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirst: lo.ToPtr(false)}}))
		Expect(terminateFirst(ctx, staticCandidate(), pscheduling.Results{})).To(BeFalse())
		c := &Candidate{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np"}}, capacityType: v1.CapacityTypeReserved}
		Expect(terminateFirst(ctx, c, reservedResults())).To(BeFalse())
	})
})
