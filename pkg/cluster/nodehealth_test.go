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
	"math/rand/v2"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
)

func newNodeHealthCluster() State {
	GinkgoHelper()
	return New(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, nil, nil, fake.NewCloudProvider(), WithRepairPolicyMatcher(testMatcher()))
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

func withResourceVersion(node *corev1.Node, rv string) *corev1.Node {
	node.ResourceVersion = rv
	return node
}

func mapOf(h *NodeHealth) map[nodeHealthKey]NodeCounts {
	m := map[nodeHealthKey]NodeCounts{}
	for k, e := range h.store.List() {
		m[k] = e.Derived
	}
	return m
}

var _ = Describe("NodeHealth", func() {
	var s State
	BeforeEach(func() {
		s = newNodeHealthCluster()
	})

	It("should count Nodes per NodePool and across the cluster", func() {
		c, nodePools := s.Nodes, s.NodePoolState
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
			Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
			Status:     v1.NodeClaimStatus{ProviderID: "fake://launching"},
		})

		Expect(nodePools.NodeHealth("a")).To(Equal(NodeCounts{Nodes: 3, Unhealthy: 1}))
		Expect(nodePools.NodeHealth("b")).To(Equal(NodeCounts{Nodes: 1, Unhealthy: 1}))
		Expect(nodePools.NodeHealth("missing")).To(Equal(NodeCounts{}))
		Expect(c.ClusterHealth()).To(Equal(NodeCounts{Nodes: 5, Unhealthy: 3}))

		// A Node that recovers stops counting as unhealthy.
		c.NodeStore.Observe(withResourceVersion(healthNode("a-1", "a", false), "2"))
		Expect(nodePools.NodeHealth("a")).To(Equal(NodeCounts{Nodes: 3, Unhealthy: 0}))
		// A Node that moves between NodePools moves its count.
		c.NodeStore.Observe(withResourceVersion(healthNode("b-1", "a", true), "2"))
		Expect(nodePools.NodeHealth("a")).To(Equal(NodeCounts{Nodes: 4, Unhealthy: 1}))
		Expect(nodePools.NodeHealth("b")).To(Equal(NodeCounts{}))
		Expect(c.ClusterHealth()).To(Equal(NodeCounts{Nodes: 5, Unhealthy: 2}))

		// A Node that's gone stops counting, and a NodePool without Nodes leaves nothing behind.
		c.NodeStore.Forget("b-1")
		e, _ := nodePools.store.Get("b")
		Expect(e.Derived).To(BeZero(), "NodePool b has fields after its last Node went")
		Expect(nodePools.NodeHealth("a")).To(Equal(NodeCounts{Nodes: 3, Unhealthy: 0}))
		Expect(c.ClusterHealth()).To(Equal(NodeCounts{Nodes: 4, Unhealthy: 1}))

		c.Clear()
		Expect(c.ClusterHealth()).To(Equal(NodeCounts{}))
		Expect(nodePools.NodeHealth("a")).To(Equal(NodeCounts{}))
		_, ok := c.Cluster.Get(TheCluster)
		Expect(ok).To(BeFalse(), "the cluster's entry is still there after Reset")
	})
	It("should derive node health that counts with its own predicate and the Nodes already held", func() {
		c := s.Nodes
		c.NodeStore.Observe(healthNode("a-1", "a", true))
		c.NodeStore.Observe(healthNode("a-2", "a", false))
		// Unhealthy here is a Node without the BadNode condition, the opposite of the matcher's.
		h := c.DeriveNodeHealth(func(node *corev1.Node) bool { return len(node.Status.Conditions) == 0 })
		Expect(h.NodePool("a")).To(Equal(NodeCounts{Nodes: 2, Unhealthy: 1}))
		c.NodeStore.Observe(healthNode("a-3", "a", false))
		Expect(h.NodePool("a")).To(Equal(NodeCounts{Nodes: 3, Unhealthy: 2}))
		Expect(s.NodePoolState.NodeHealth("a")).To(Equal(NodeCounts{Nodes: 3, Unhealthy: 1}))
		Expect(h.Cluster()).To(Equal(NodeCounts{Nodes: 3, Unhealthy: 2}))
	})
	It("should converge after concurrent changes", func() {
		c := s.Nodes
		pools := []string{"", "a", "b", "c"}
		var wg sync.WaitGroup
		for w := range 8 {
			wg.Go(func() {
				r := rand.New(rand.NewPCG(uint64(w), 1)) //nolint:gosec // a seeded source makes failures reproducible
				for i := range 500 {
					name := fmt.Sprintf("node-%d", r.IntN(40))
					if r.IntN(4) == 0 {
						c.NodeStore.Forget(name)
						continue
					}
					c.NodeStore.Observe(withResourceVersion(healthNode(name, pools[r.IntN(len(pools))], r.IntN(2) == 0), fmt.Sprint(w*1000+i)))
				}
			})
		}
		wg.Wait()
		want := map[nodeHealthKey]NodeCounts{}
		for _, e := range c.NodeStore.List() {
			node := e.Object
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
		got := map[nodeHealthKey]NodeCounts{}
		if health := c.ClusterHealth(); health != (NodeCounts{}) {
			got[nodeHealthKey{Cluster: true}] = health
		}
		for _, name := range pools[1:] {
			if health := s.NodePoolState.NodeHealth(name); health != (NodeCounts{}) {
				got[nodeHealthKey{NodePool: name}] = health
			}
		}
		Expect(got).To(Equal(want), "node health, want the recount")
	})
	It("should keep the contribution laws for node counts", func() {
		for _, base := range []NodeCounts{{}, {Nodes: 3, Unhealthy: 1}} {
			statetest.ExpectContribution(GinkgoT(), base, newNodeCount(false), newNodeCount(true))
			statetest.ExpectContribution(GinkgoT(), base, newNodeCount(true), newNodeCount(false))
		}
		// A Node's contribution to its NodePool's fields touches only NodeHealth.
		for _, base := range []NodePoolFields{{}, {NodeHealth: NodeCounts{Nodes: 3, Unhealthy: 1}, NodeClaims: NodeClaimCounts{Active: 2}, Reserved: 1}} {
			statetest.ExpectContribution(GinkgoT(), base, nodePoolNode(newNodeCount(false)), nodePoolNode(newNodeCount(true)))
			statetest.ExpectContribution(GinkgoT(), base, nodePoolNode(newNodeCount(true)), nodePoolNode(newNodeCount(false)))
		}
	})
})
