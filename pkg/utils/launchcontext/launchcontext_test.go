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

package launchcontext_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/launchcontext"
)

func TestLaunchContext(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LaunchContext Utils")
}

var _ = Describe("LaunchContext", func() {
	It("round-trips a repair replacement through the annotation", func() {
		nc := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "gpu-a1b2c"}}
		want := launchcontext.Context{Cause: launchcontext.CauseUnhealthy, Replaces: "gpu-x9y8z"}
		want.StampOn(nc)

		Expect(nc.Annotations[v1.LaunchContextAnnotationKey]).To(Equal(`{"cause":"Unhealthy","replaces":"gpu-x9y8z"}`))
		got, ok := launchcontext.Get(nc)
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal(want))
	})

	It("omits replaces for a reactively provisioned NodeClaim", func() {
		nc := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nc"}}
		launchcontext.Context{Cause: launchcontext.CauseProvisioned}.StampOn(nc)
		Expect(nc.Annotations[v1.LaunchContextAnnotationKey]).To(Equal(`{"cause":"Provisioned"}`))
	})

	It("treats an absent, garbled, or cause-less annotation as no provenance", func() {
		_, ok := launchcontext.Get(&v1.NodeClaim{})
		Expect(ok).To(BeFalse())

		garbled := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{v1.LaunchContextAnnotationKey: "not json"}}}
		_, ok = launchcontext.Get(garbled)
		Expect(ok).To(BeFalse())

		noCause := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{v1.LaunchContextAnnotationKey: `{"replaces":"x"}`}}}
		_, ok = launchcontext.Get(noCause)
		Expect(ok).To(BeFalse())
	})

	It("maps a DisruptionReason to the coincident Cause", func() {
		Expect(launchcontext.ForReason(v1.DisruptionReasonUnhealthy)).To(Equal(launchcontext.CauseUnhealthy))
		Expect(launchcontext.ForReason(v1.DisruptionReasonDrifted)).To(Equal(launchcontext.CauseDrifted))
	})
})
