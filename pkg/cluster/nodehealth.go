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
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

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

// NodeHealth keeps NodeCounts per NodePool and across the cluster for a caller's own idea of unhealthy
// (DeriveNodeHealth), derived from the Node store. A Node counts once cluster state tracks it (Tracked), and stops
// counting once it's gone. The counts per NodePool by the repair policy matcher are NodePoolFields.NodeHealth.
type NodeHealth struct {
	store     *state.Store[nodeHealthKey, nodeHealthKey, NodeCounts, struct{}]
	nodes     *Nodes
	unhealthy func(*corev1.Node) bool
}

// NodePool returns the counts of the Nodes labeled with the NodePool.
func (h *NodeHealth) NodePool(name string) NodeCounts {
	e, _ := h.store.Get(nodeHealthKey{NodePool: name})
	return e.Derived
}

// Cluster returns the counts of every Node in the cluster, managed or not.
func (h *NodeHealth) Cluster() NodeCounts {
	e, _ := h.store.Get(nodeHealthKey{Cluster: true})
	return e.Derived
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

// DeriveNodeHealth returns counts of the tracked Nodes, and of those that unhealthy accepts, per NodePool and across
// the cluster, kept from now on, for a caller whose idea of unhealthy isn't the matcher's. It builds a store, so a
// controller calls it from its Register. unhealthy must be pure: it runs on every change of a Node.
func (n *Nodes) DeriveNodeHealth(unhealthy func(*corev1.Node) bool) *NodeHealth {
	h := &NodeHealth{
		store:     state.NewSyntheticStore[nodeHealthKey, nodeHealthKey, NodeCounts, struct{}](func(k nodeHealthKey) nodeHealthKey { return k }),
		nodes:     n,
		unhealthy: unhealthy,
	}
	value := func(_ string, node *corev1.Node) nodeCount { return newNodeCount(unhealthy(node)) }
	state.DeriveObject(n.Reader, h.store, func(_ string, node *corev1.Node) nodeHealthKey {
		if np := nodePoolOfNode("", node); np != "" {
			return nodeHealthKey{NodePool: np}
		}
		return nodeHealthKey{}
	}, value)
	state.DeriveObject(n.Reader, h.store, func(_ string, node *corev1.Node) nodeHealthKey {
		return nodeHealthKey{Cluster: Tracked(node)}
	}, value)
	return h
}

// Source enqueues, by name, the unhealthy Nodes counted under a NodePool, or across the cluster, whenever those counts
// change: the Nodes a threshold over the counts may hold back from repair, or release.
func (h *NodeHealth) Source() source.SyncingSource { return nodeHealthSource{h} }

type nodeHealthSource struct{ h *NodeHealth }

func (src nodeHealthSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	h := src.h
	context.AfterFunc(ctx, h.store.Subscribe(func(c state.Change[nodeHealthKey, nodeHealthKey, NodeCounts, struct{}]) {
		if c.Existed == c.Exists && c.Old.Derived == c.New.Derived {
			return
		}
		for name, e := range h.nodes.List(client.UnsafeDisableDeepCopy) {
			if !h.unhealthy(e.Object) {
				continue
			}
			if np := nodePoolOfNode("", e.Object); (c.Key.Cluster && Tracked(e.Object)) || (np != "" && c.Key.NodePool == np) {
				queue.Add(reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
			}
		}
	}))
	return nil
}

func (src nodeHealthSource) WaitForSync(ctx context.Context) error {
	return wait.PollUntilContextCancel(ctx, 10*time.Millisecond, true, func(context.Context) (bool, error) {
		return src.h.store.Synced(), nil
	})
}

// NodeHealth returns the counts of the tracked Nodes labeled with the NodePool, and of those with a condition that
// matches a repair policy of the matcher cluster state was built with, regardless of toleration.
func (n *NodePoolState) NodeHealth(nodePoolName string) NodeCounts {
	return n.Fields(nodePoolName).NodeHealth
}

// nodePoolNode is a tracked Node's contribution to its NodePool's NodeHealth.
type nodePoolNode nodeCount

func (n nodePoolNode) Update(f NodePoolFields, prev nodePoolNode) NodePoolFields {
	f.NodeHealth = nodeCount(n).Update(f.NodeHealth, nodeCount(prev))
	return f
}

func (n nodePoolNode) Delete(f NodePoolFields) NodePoolFields {
	f.NodeHealth = nodeCount(n).Delete(f.NodeHealth)
	return f
}
