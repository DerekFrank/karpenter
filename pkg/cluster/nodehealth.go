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
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state"
)

// NodeCounts count the Nodes cluster state tracks, with or without a NodeClaim, and the unhealthy ones among them.
type NodeCounts struct {
	Nodes     int
	Unhealthy int
}

// nodeHealthKey is what a Node's count goes to: its NodePool's counts, or the cluster's.
type nodeHealthKey struct {
	// NodePool is the Node's NodePool label, for the NodePool's counts.
	NodePool string
	// Cluster is set for the counts of every Node in the cluster.
	Cluster bool
}

// NodeHealth keeps NodeCounts per NodePool and across the cluster, derived from the Node store. A Node counts once
// cluster state tracks it (Tracked), and stops counting once it's gone.
type NodeHealth struct {
	store *state.Store[nodeHealthKey, NodeCounts]
}

// NodePool returns the counts of the Nodes labeled with the NodePool.
func (h *NodeHealth) NodePool(name string) NodeCounts {
	counts, _ := h.store.Get(nodeHealthKey{NodePool: name})
	return counts
}

// Cluster returns the counts of every Node in the cluster, managed or not.
func (h *NodeHealth) Cluster() NodeCounts {
	counts, _ := h.store.Get(nodeHealthKey{Cluster: true})
	return counts
}

// nodeCount is a Node's contribution to its counts: one node, and one unhealthy node if it's unhealthy. The zero
// nodeCount contributes nothing.
type nodeCount struct {
	nodes     int
	unhealthy int
}

func newNodeCount(unhealthy bool) nodeCount {
	return nodeCount{nodes: 1, unhealthy: lo.Ternary(unhealthy, 1, 0)}
}

func (n nodeCount) Update(f NodeCounts, prev nodeCount) NodeCounts {
	f.Nodes += n.nodes - prev.nodes
	f.Unhealthy += n.unhealthy - prev.unhealthy
	return f
}

func (n nodeCount) Delete(f NodeCounts) NodeCounts {
	f.Nodes -= n.nodes
	f.Unhealthy -= n.unhealthy
	return f
}

// newNodeHealth derives NodeHealth from the tracked Nodes, counting a Node as unhealthy when unhealthy accepts it.
// unhealthy must be pure, as a Derive's value must.
func (s *NodeState) newNodeHealth(unhealthy func(*corev1.Node) bool) *NodeHealth {
	h := &NodeHealth{store: state.NewStore[nodeHealthKey, NodeCounts]()}
	value := func(_ string, node *corev1.Node) nodeCount { return newNodeCount(unhealthy(node)) }
	state.Derive(s.NodeStore, h.store, func(_ string, node *corev1.Node) nodeHealthKey {
		if !Tracked(node) || node.Labels[v1.NodePoolLabelKey] == "" {
			return nodeHealthKey{}
		}
		return nodeHealthKey{NodePool: node.Labels[v1.NodePoolLabelKey]}
	}, value)
	state.Derive(s.NodeStore, h.store, func(_ string, node *corev1.Node) nodeHealthKey {
		return nodeHealthKey{Cluster: Tracked(node)}
	}, value)
	return h
}

// NodeHealth returns the counts of the tracked Nodes, and of those with a condition that matches a repair policy of
// the matcher cluster state was built with, regardless of toleration. Without a matcher, no Node is unhealthy.
func (s *NodeState) NodeHealth() *NodeHealth { return s.nodeHealth }

// DeriveNodeHealth returns counts of the tracked Nodes, and of those that unhealthy accepts, kept from now on, for a
// caller whose idea of unhealthy isn't the matcher's. unhealthy must be pure: it runs on every change of a Node.
func (s *NodeState) DeriveNodeHealth(unhealthy func(*corev1.Node) bool) *NodeHealth {
	return s.newNodeHealth(unhealthy)
}
