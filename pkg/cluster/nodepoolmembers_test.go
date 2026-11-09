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
	"context"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("NodePool Members", func() {
	// The members are checked against a scan of every node and NodeClaim as its NodeClaims launch, move between
	// NodePools and go away.
	It("should index each NodePool's Nodes and unlaunched NodeClaims as its members", func() {
		cs := New(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, nil, nil, fake.NewCloudProvider())
		s, nodePools := cs.Nodes, cs.NodePoolState
		nodeClaim := func(name, nodePool, providerID, rv string) *v1.NodeClaim {
			return &v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: rv, Labels: map[string]string{v1.NodePoolLabelKey: nodePool}},
				Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
				Status:     v1.NodeClaimStatus{ProviderID: providerID},
			}
		}
		expect := func() {
			GinkgoHelper()
			for _, np := range []string{"a", "b"} {
				expectNodePoolMembers(cs, np)
			}
		}
		s.NodeClaimStore.Observe(nodeClaim("1", "a", "", "1"))
		s.NodeClaimStore.Observe(nodeClaim("2", "a", "", "1"))
		s.NodeClaimStore.Observe(nodeClaim("3", "b", "p3", "1"))
		expect()
		s.NodeClaimStore.Observe(nodeClaim("1", "a", "p1", "2"))
		expect()
		s.NodeClaimStore.Observe(nodeClaim("3", "a", "p3", "2"))
		expect()
		deleting := nodeClaim("2", "a", "", "2")
		deleting.DeletionTimestamp = &metav1.Time{Time: time.Unix(1, 0)}
		deleting.Finalizers = []string{"test"}
		s.NodeClaimStore.Observe(deleting)
		expect()
		// A NodeClaim's NodePool is the NodeClaim's label, whatever its Node's says.
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "n1", UID: "n1", ResourceVersion: "1", Labels: map[string]string{
				v1.NodePoolLabelKey: "b", v1.NodeRegisteredLabelKey: "true", corev1.LabelInstanceTypeStable: "x",
			}, OwnerReferences: []metav1.OwnerReference{{APIVersion: apis.Group + "/v1", Kind: "NodeClaim", Name: "1", UID: "1"}}},
			Spec: corev1.NodeSpec{ProviderID: "p1"},
		}
		s.NodeStore.Observe(node)
		expect()
		// LaunchedNodes(a) is the launched NodeClaims, the first linked to n1 by its owner reference.
		launched := nodePools.LaunchedNodes("a")
		Expect(launched).To(HaveLen(2))
		Expect(launched[0].Node).ToNot(BeNil())
		Expect(launched[0].Node.Name).To(Equal("n1"))
		s.NodeStore.Forget(node.Name)
		expect()
		s.NodeClaimStore.Forget("1")
		s.NodeClaimStore.Forget("3")
		expect()
	})
})

// expectNodePoolMembers checks the NodePool's members read from its index against a scan of every node and NodeClaim.
//
//nolint:gocyclo
func expectNodePoolMembers(s State, np string) {
	GinkgoHelper()
	var want, got []string
	for n := range s.Nodes.Nodes() {
		if n.NodeClaim != nil && n.NodeClaim.Labels[v1.NodePoolLabelKey] == np {
			want = append(want, n.ProviderID())
		}
	}
	for _, n := range s.NodePoolState.LaunchedNodes(np) {
		got = append(got, n.ProviderID())
	}
	// Both reads return their members sorted.
	slices.Sort(want)
	Expect(slices.Equal(got, want)).To(BeTrue(), "LaunchedNodes(%q) = %v, want %v", np, got, want)
	want, got = nil, nil
	for _, f := range s.Nodes.NodeClaimStore.List() {
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
