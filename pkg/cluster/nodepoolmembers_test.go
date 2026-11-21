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
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("NodePool Members", func() {
	// The members are checked against a scan of every node and NodeClaim as its NodeClaims launch, move between
	// NodePools and go away.
	It("should index each NodePool's Nodes and unlaunched NodeClaims as its members", func() {
		cs := newSuiteState(testContext())
		nodePools := cs.NodePoolState
		nodeClaim := func(name, nodePool, providerID string) *v1.NodeClaim {
			nc := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1.NodePoolLabelKey: nodePool}}})
			nc.Status.ProviderID = providerID
			return nc
		}
		expect := func() {
			GinkgoHelper()
			for _, np := range []string{"a", "b"} {
				expectNodePoolMembers(cs, np)
			}
		}
		apply(nodeClaim("nc-1", "a", ""), nodeClaim("nc-2", "a", ""), nodeClaim("nc-3", "b", "fake://p3"))
		expect()
		apply(nodeClaim("nc-1", "a", "fake://p1"))
		expect()
		apply(nodeClaim("nc-3", "a", "fake://p3"))
		expect()
		markDeleting(nodeClaim("nc-2", "a", ""))
		expect()
		// A NodeClaim's NodePool is the NodeClaim's label, whatever its Node's says.
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{
				v1.NodePoolLabelKey: "b", v1.NodeRegisteredLabelKey: "true", corev1.LabelInstanceTypeStable: "x",
			}, OwnerReferences: []metav1.OwnerReference{{APIVersion: apis.Group + "/v1", Kind: "NodeClaim", Name: "nc-1", UID: "nc-1"}}},
			Spec: corev1.NodeSpec{ProviderID: "fake://p1"},
		}
		apply(node)
		expect()
		// LaunchedNodes(a) is the launched NodeClaims, the first linked to n1 by its owner reference.
		launched := nodePools.LaunchedNodes("a")
		Expect(launched).To(HaveLen(2))
		Expect(launched[0].Derived.Node).ToNot(BeNil())
		Expect(launched[0].Derived.Node.Name).To(Equal("n1"))
		remove(node)
		expect()
		remove(nodeClaim("nc-1", "a", ""), nodeClaim("nc-3", "a", ""))
		expect()
	})
})

// expectNodePoolMembers checks the NodePool's members read from its index against a scan of every node and NodeClaim.
func expectNodePoolMembers(s State, np string) {
	GinkgoHelper()
	var want, got []string
	for _, e := range s.NodeClaims.List() {
		if Launched(e) && e.Object.Labels[v1.NodePoolLabelKey] == np {
			want = append(want, ViewOfNodeClaim(e).ProviderID())
		}
	}
	for _, n := range s.NodePoolState.LaunchedNodes(np) {
		got = append(got, ViewOfNodeClaim(n).ProviderID())
	}
	// Both reads return their members sorted.
	slices.Sort(want)
	Expect(slices.Equal(got, want)).To(BeTrue(), "LaunchedNodes(%q) = %v, want %v", np, got, want)
	want, got = nil, nil
	for _, f := range s.NodeClaims.List() {
		if nc := f.Object; nc.Labels[v1.NodePoolLabelKey] == np && nc.Status.ProviderID == "" && nc.DeletionTimestamp.IsZero() {
			want = append(want, nc.Name)
		}
	}
	for _, nc := range s.NodePoolState.UnlaunchedNodeClaims(np) {
		got = append(got, nc.Name)
	}
	slices.Sort(want)
	Expect(slices.Equal(got, want)).To(BeTrue(), "UnlaunchedNodeClaims(%q) = %v, want %v", np, got, want)
}
