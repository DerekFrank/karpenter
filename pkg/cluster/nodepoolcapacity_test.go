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

package cluster

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

var _ = Describe("NodePool Capacity", func() {
	It("should keep the contribution laws for NodePool capacity", func() {
		for _, b := range []NodePoolFields{{}, contributionBase()} {
			statetest.ExpectContribution(GinkgoT(), b, nodePoolCapacity(nodeResources("2")), nodePoolCapacity(nodeResources("8")))
			statetest.ExpectContribution(GinkgoT(), b, nodePoolCapacity(nodeResources("8")), nodePoolCapacity(nil))
		}
		// A NodeClaim's whole capacity leaving drops the resources it zeroes.
		c := nodePoolCapacity(nodeResources("2"))
		Expect(c.Delete(c.Update(NodePoolFields{}, nil)).Capacity).To(BeNil(), "capacity is non-nil once its only NodeClaim left")
	})
})
