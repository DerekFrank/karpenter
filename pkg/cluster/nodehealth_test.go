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
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
)

func newNodeHealthCluster(t *testing.T) *NodeState {
	t.Helper()
	return NewNodeState(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, testMatcher(t))
}

// healthNode is a Node cluster state tracks, labeled with nodePool unless it's empty.
func healthNode(name, nodePool string, bad bool) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: k8stypes.UID("uid-" + name), ResourceVersion: "1", Labels: map[string]string{corev1.LabelInstanceTypeStable: "small"}},
		Spec:       corev1.NodeSpec{ProviderID: "fake://" + name},
	}
	if nodePool != "" {
		node.Labels[v1.NodePoolLabelKey] = nodePool
	}
	if bad {
		node.Status.Conditions = []corev1.NodeCondition{{Type: "BadNode", Status: corev1.ConditionFalse}}
	}
	return node
}

func expectCounts(t *testing.T, got, want NodeCounts) {
	t.Helper()
	if got != want {
		t.Errorf("counts = %+v, want %+v", got, want)
	}
}

func TestNodeHealthCountsNodesPerNodePoolAndAcrossTheCluster(t *testing.T) {
	c := newNodeHealthCluster(t)
	for _, node := range []*corev1.Node{
		healthNode("a-1", "a", true), healthNode("a-2", "a", false), healthNode("a-3", "a", false),
		healthNode("b-1", "b", true),
		// An unmanaged Node counts across the cluster only.
		healthNode("unmanaged", "", true),
	} {
		c.NodeStore.Observe(node)
	}
	// A NodeClaim without a Node isn't a node yet.
	c.NodeClaimStore.Observe(&v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "launching", UID: "launching", Labels: map[string]string{v1.NodePoolLabelKey: "a"}},
		Status:     v1.NodeClaimStatus{ProviderID: "fake://launching"},
	})

	h := c.NodeHealth()
	expectCounts(t, h.NodePool("a"), NodeCounts{Nodes: 3, Unhealthy: 1})
	expectCounts(t, h.NodePool("b"), NodeCounts{Nodes: 1, Unhealthy: 1})
	expectCounts(t, h.NodePool("missing"), NodeCounts{})
	expectCounts(t, h.Cluster(), NodeCounts{Nodes: 5, Unhealthy: 3})
	if got, want := mapOf(h), map[nodeHealthKey]NodeCounts{
		{NodePool: "a"}: {Nodes: 3, Unhealthy: 1},
		{NodePool: "b"}: {Nodes: 1, Unhealthy: 1},
		{Cluster: true}: {Nodes: 5, Unhealthy: 3},
	}; !reflect.DeepEqual(got, want) {
		t.Errorf("node health = %v, want %v", got, want)
	}

	// A Node that recovers stops counting as unhealthy.
	c.NodeStore.Observe(withResourceVersion(healthNode("a-1", "a", false), "2"))
	expectCounts(t, h.NodePool("a"), NodeCounts{Nodes: 3, Unhealthy: 0})
	// A Node that moves between NodePools moves its count.
	c.NodeStore.Observe(withResourceVersion(healthNode("b-1", "a", true), "2"))
	expectCounts(t, h.NodePool("a"), NodeCounts{Nodes: 4, Unhealthy: 1})
	expectCounts(t, h.NodePool("b"), NodeCounts{})
	expectCounts(t, h.Cluster(), NodeCounts{Nodes: 5, Unhealthy: 2})

	// A Node that's gone stops counting, and a NodePool without Nodes leaves nothing behind.
	c.NodeStore.Forget(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "b-1"}})
	if _, ok := h.store.Get(nodeHealthKey{NodePool: "b"}); ok {
		t.Errorf("NodePool b has counts after its last Node went")
	}
	expectCounts(t, h.NodePool("a"), NodeCounts{Nodes: 3, Unhealthy: 0})
	expectCounts(t, h.Cluster(), NodeCounts{Nodes: 4, Unhealthy: 1})

	c.Clear()
	expectCounts(t, h.Cluster(), NodeCounts{})
	if n := len(lo.Keys(mapOf(h))); n != 0 {
		t.Errorf("node health holds %d keys after Reset, want 0", n)
	}
}

func withResourceVersion(node *corev1.Node, rv string) *corev1.Node {
	node.ResourceVersion = rv
	return node
}

func mapOf(h *NodeHealth) map[nodeHealthKey]NodeCounts {
	m := maps.Collect(h.store.List())
	return m
}

func TestDeriveNodeHealthCountsWithItsOwnPredicateAndTheNodesAlreadyHeld(t *testing.T) {
	c := newNodeHealthCluster(t)
	c.NodeStore.Observe(healthNode("a-1", "a", true))
	c.NodeStore.Observe(healthNode("a-2", "a", false))
	// Unhealthy here is a Node without the BadNode condition, the opposite of the matcher's.
	h := c.DeriveNodeHealth(func(node *corev1.Node) bool { return len(node.Status.Conditions) == 0 })
	expectCounts(t, h.NodePool("a"), NodeCounts{Nodes: 2, Unhealthy: 1})
	c.NodeStore.Observe(healthNode("a-3", "a", false))
	expectCounts(t, h.NodePool("a"), NodeCounts{Nodes: 3, Unhealthy: 2})
	expectCounts(t, c.NodeHealth().NodePool("a"), NodeCounts{Nodes: 3, Unhealthy: 1})
}

func TestNodeHealthConvergesAfterConcurrentChanges(t *testing.T) {
	c := newNodeHealthCluster(t)
	pools := []string{"", "a", "b", "c"}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 1)) //nolint:gosec // a seeded source makes failures reproducible
			for i := range 500 {
				name := fmt.Sprintf("node-%d", r.IntN(40))
				if r.IntN(4) == 0 {
					c.NodeStore.Forget(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
					continue
				}
				c.NodeStore.Observe(withResourceVersion(healthNode(name, pools[r.IntN(len(pools))], r.IntN(2) == 0), fmt.Sprint(w*1000+i)))
			}
		})
	}
	wg.Wait()
	want := map[nodeHealthKey]NodeCounts{}
	for _, node := range c.NodeStore.List() {
		bad := len(c.matcher.Match(node)) > 0
		for _, key := range []nodeHealthKey{{Cluster: true}, {NodePool: node.Labels[v1.NodePoolLabelKey]}} {
			if key == (nodeHealthKey{}) {
				continue
			}
			counts := want[key]
			counts.Nodes++
			counts.Unhealthy += lo.Ternary(bad, 1, 0)
			want[key] = counts
		}
	}
	if got := mapOf(c.NodeHealth()); !reflect.DeepEqual(got, want) {
		t.Errorf("node health = %v, want the recount %v", got, want)
	}
}

func TestNodeCountsKeepTheContributionLaws(t *testing.T) {
	for _, base := range []NodeCounts{{}, {Nodes: 3, Unhealthy: 1}} {
		statetest.ExpectContribution(t, base, newNodeCount(false), newNodeCount(true))
		statetest.ExpectContribution(t, base, newNodeCount(true), newNodeCount(false))
	}
}
