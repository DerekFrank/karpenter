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

package state

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
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
)

func TestNodePoolMembersKeepTheContributionLaws(t *testing.T) {
	for _, base := range []nodePoolMembers{{}, {Nodes: 3, Unlaunched: 2}} {
		statetest.ExpectContribution(t, base, nodePoolNode(1), nodePoolNode(1))
		statetest.ExpectContribution(t, base, unlaunchedNodeClaim(1), unlaunchedNodeClaim(1))
	}
}

// TestNodePoolMembersIndexEachNodePoolsNodesAndUnlaunchedNodeClaims checks the index against a scan of every node and
// NodeClaim as its NodeClaims launch, move between NodePools and go away.
func TestNodePoolMembersIndexEachNodePoolsNodesAndUnlaunchedNodeClaims(t *testing.T) {
	c := NewCluster(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, nil, nil)
	nodeClaim := func(name, nodePool, providerID string) *v1.NodeClaim {
		return &v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: "1", Labels: map[string]string{v1.NodePoolLabelKey: nodePool}},
			Status:     v1.NodeClaimStatus{ProviderID: providerID},
		}
	}
	expect := func() {
		t.Helper()
		for _, np := range []string{"a", "b"} {
			expectNodePoolMembers(t, c, np)
		}
	}
	c.UpdateNodeClaim(nodeClaim("1", "a", ""))
	c.UpdateNodeClaim(nodeClaim("2", "a", ""))
	c.UpdateNodeClaim(nodeClaim("3", "b", "p3"))
	expect()
	launched := nodeClaim("1", "a", "p1")
	launched.ResourceVersion = "2"
	c.UpdateNodeClaim(launched)
	expect()
	moved := nodeClaim("3", "a", "p3")
	moved.ResourceVersion = "2"
	c.UpdateNodeClaim(moved)
	expect()
	deleting := nodeClaim("2", "a", "")
	deleting.ResourceVersion = "2"
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Unix(1, 0)}
	deleting.Finalizers = []string{"test"}
	c.UpdateNodeClaim(deleting)
	expect()
	// A Node registered for a launched NodeClaim takes its NodePool from the Node's labels.
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1", UID: "n1", ResourceVersion: "1", Labels: map[string]string{
			v1.NodePoolLabelKey: "b", v1.NodeRegisteredLabelKey: "true", corev1.LabelInstanceTypeStable: "x",
		}},
		Spec: corev1.NodeSpec{ProviderID: "p1"},
	}
	c.nodes.Observe(node)
	expect()
	c.nodes.Forget(node)
	expect()
	c.DeleteNodeClaim("1")
	c.DeleteNodeClaim("3")
	expect()
	if members, ok := c.nodePoolMembers.Get("a"); ok && (members.Nodes > 0 || members.Unlaunched > 0) {
		t.Errorf("NodePool a keeps members %+v once nothing contributes", members)
	}
}

// expectNodePoolMembers checks the NodePool's members read from its index against a scan of every node and NodeClaim.
func expectNodePoolMembers(t *testing.T, c *Cluster, np string) {
	t.Helper()
	var want, got []string
	for n := range c.Nodes() {
		if n.NodeClaim != nil && n.Labels()[v1.NodePoolLabelKey] == np {
			want = append(want, n.ProviderID())
		}
	}
	for _, n := range c.NodePoolNodes(np) {
		got = append(got, n.ProviderID())
	}
	// Both reads return their members sorted.
	if slices.Sort(want); !slices.Equal(got, want) {
		t.Errorf("NodePoolNodes(%q) = %v, want %v", np, got, want)
	}
	expectUnlaunchedNodeClaims(t, c, np)
}

func expectUnlaunchedNodeClaims(t *testing.T, c *Cluster, np string) {
	t.Helper()
	var want, got []string
	for _, nc := range c.nodeClaims.List() {
		if nc.Labels[v1.NodePoolLabelKey] == np && nc.Status.ProviderID == "" && nc.DeletionTimestamp.IsZero() {
			want = append(want, nc.Name)
		}
	}
	for _, nc := range c.UnlaunchedNodeClaims(np) {
		got = append(got, nc.Name)
	}
	if slices.Sort(want); !slices.Equal(got, want) {
		t.Errorf("UnlaunchedNodeClaims(%q) = %v, want %v", np, got, want)
	}
}
