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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// TestNodePoolMembersIndexEachNodePoolsNodesAndUnlaunchedNodeClaims checks the index against a scan of every node and
// NodeClaim as its NodeClaims launch, move between NodePools and go away.
func TestNodePoolMembersIndexEachNodePoolsNodesAndUnlaunchedNodeClaims(t *testing.T) {
	s := NewNodeState(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, nil)
	nodeClaim := func(name, nodePool, providerID, rv string) *v1.NodeClaim {
		return &v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: rv, Labels: map[string]string{v1.NodePoolLabelKey: nodePool}},
			Status:     v1.NodeClaimStatus{ProviderID: providerID},
		}
	}
	expect := func() {
		t.Helper()
		for _, np := range []string{"a", "b"} {
			expectNodePoolMembers(t, s, np)
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
		}},
		Spec: corev1.NodeSpec{ProviderID: "p1"},
	}
	s.NodeStore.Observe(node)
	expect()
	if got := s.NodePoolNodes("a"); len(got) != 2 || got[0].Node == nil || got[0].Node.Name != "n1" {
		t.Errorf("NodePoolNodes(a) = %v, want the launched NodeClaims, the first linked to n1", got)
	}
	s.NodeStore.Forget(node)
	expect()
	s.NodeClaimStore.Forget(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "1"}})
	s.NodeClaimStore.Forget(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "3"}})
	expect()
	if members, ok := s.nodePoolMembers.Get("a"); ok && (members.Launched > 0 || members.Unlaunched > 0) {
		t.Errorf("NodePool a keeps members %+v once nothing contributes", members)
	}
}

// expectNodePoolMembers checks the NodePool's members read from its index against a scan of every node and NodeClaim.
//
//nolint:gocyclo
func expectNodePoolMembers(t *testing.T, s *NodeState, np string) {
	t.Helper()
	var want, got []string
	for n := range s.Nodes() {
		if n.NodeClaim != nil && n.NodeClaim.Labels[v1.NodePoolLabelKey] == np {
			want = append(want, n.ProviderID())
		}
	}
	for _, n := range s.NodePoolNodes(np) {
		got = append(got, n.ProviderID())
	}
	// Both reads return their members sorted.
	if slices.Sort(want); !slices.Equal(got, want) {
		t.Errorf("NodePoolNodes(%q) = %v, want %v", np, got, want)
	}
	want, got = nil, nil
	for _, f := range s.NodeClaimStore.List() {
		if nc := f.NodeClaim; nc != nil && nc.Labels[v1.NodePoolLabelKey] == np && nc.Status.ProviderID == "" && nc.DeletionTimestamp.IsZero() {
			want = append(want, nc.Name)
		}
	}
	for _, nc := range s.UnlaunchedNodeClaims(np) {
		got = append(got, nc.Name)
	}
	if slices.Sort(want); !slices.Equal(got, want) {
		t.Errorf("UnlaunchedNodeClaims(%q) = %v, want %v", np, got, want)
	}
}
