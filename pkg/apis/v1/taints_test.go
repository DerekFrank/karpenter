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

package v1_test

import (
	"github.com/awslabs/operatorpkg/docs"
	"github.com/awslabs/operatorpkg/wellknown"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

var _ = Describe("WellKnownTaints", func() {
	taints := v1.KarpenterTaints

	It("should document every taint", func() {
		for _, taint := range taints {
			Expect(taint.Taint.Key).ToNot(BeEmpty())
			Expect(taint.Taint.Effect).ToNot(BeEmpty(), taint.Taint.Key)
			Expect(taint.Help).ToNot(BeEmpty(), taint.Taint.Key)
			Expect(taint.UsedOn).ToNot(BeEmpty(), taint.Taint.Key)
			Expect(taint.Stage).To(BeElementOf(docs.Alpha, docs.Beta, docs.GA), taint.Taint.Key)
		}
	})
	It("should mark every internal only taint as alpha", func() {
		for _, taint := range taints {
			if taint.InternalOnly {
				Expect(taint.Stage).To(Equal(docs.Alpha), taint.Taint.Key)
			}
		}
	})
	It("should not document a taint twice", func() {
		Expect(lo.FindDuplicatesBy(taints, func(t wellknown.Taint) string { return t.Taint.ToString() })).To(BeEmpty())
	})
})
