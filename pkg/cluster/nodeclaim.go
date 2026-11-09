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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state"
)

// NodeClaimDerived are the fields derived about a NodeClaim from other stores.
type NodeClaimDerived struct {
	// Node is the name of the tracked Node whose owner reference names the NodeClaim, contributed by the Node store.
	// It's empty before registration sets the owner reference, and once the Node is gone.
	Node string
}

// NodeClaimIntent is what Karpenter decided about a NodeClaim's node. It drops with the NodeClaim.
type NodeClaimIntent struct {
	// Deletion marks the node while a disruption command disrupts it. Disruption holds its handle.
	Deletion state.Intended[bool]
	// Nomination marks the node as the target of a pending pod in a recent scheduling pass, until it lapses.
	// Provisioning holds its handle.
	Nomination state.Intended[bool]
	// BufferPods counts the virtual CapacityBuffer pods the provisioner's last scheduling pass placed on the node.
	// Provisioning holds its handle.
	BufferPods state.Intended[int]
}

// NodeClaimEntry is a NodeClaim's entry: the NodeClaim, its linked Node's name and the intent kept about its node.
type NodeClaimEntry = state.Entry[*v1.NodeClaim, NodeClaimDerived, NodeClaimIntent]

type nodeClaimStore = state.Store[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent]

// NodeClaims are the NodeClaims of the cloud provider's node classes, by name: an object store. The NodeClaim is the
// source of truth for Karpenter's view of the nodes it manages, so the intent about a node lives on its NodeClaim's
// entry and drops with it.
type NodeClaims struct {
	state.Reader[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent]
	store *nodeClaimStore
}

func newNodeClaims(kubeClient client.Client, informers cache.Informers, managed []schema.GroupKind) *NodeClaims {
	store := state.NewObjectStoreBy[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent](kubeClient, informers, func(nodeClaim *v1.NodeClaim) string {
		if nodeClaim.Spec.NodeClassRef == nil || !slices.Contains(managed, nodeClaim.Spec.NodeClassRef.GroupKind()) {
			return ""
		}
		return nodeClaim.Name
	})
	return &NodeClaims{Reader: store.Reader, store: store}
}

// Observe applies a version of a NodeClaim a caller just wrote or saw, before the informer delivers it.
func (n *NodeClaims) Observe(nodeClaim *v1.NodeClaim) { n.store.Observe(nodeClaim) }

// Forget drops the NodeClaim named name, for a caller that learns it's gone by its name alone.
func (n *NodeClaims) Forget(name string) { state.Forget(n.store, types.NamespacedName{Name: name}) }

// Exists returns whether cluster state holds the NodeClaim.
func (n *NodeClaims) Exists(name string) bool {
	_, ok := n.Get(name)
	return ok
}

// Unlaunched returns whether cluster state holds the NodeClaim, and it hasn't launched.
func (n *NodeClaims) Unlaunched(name string) bool {
	e, ok := n.Get(name)
	return ok && !launched(e)
}

// UnlaunchedSource is the NodeClaims as a source that wakes on a NodeClaim appearing, going, or launching, for a
// decision that waits until none is launching.
func (n *NodeClaims) UnlaunchedSource() source.SyncingSource {
	return n.SourceOn(func(_ string, e NodeClaimEntry) any { return launched(e) })
}

// launched returns whether the NodeClaim has a providerID: its instance exists, and it's a node in Nodes.
func launched(e NodeClaimEntry) bool { return e.Object != nil && e.Object.Status.ProviderID != "" }

// linkedNode is a tracked Node's contribution to the NodeClaim its owner reference names: its name. A NodeClaim has
// one such Node; were two to name it, the last to change would be its Node until either left.
type linkedNode string

func (n linkedNode) Update(f NodeClaimDerived, _ linkedNode) NodeClaimDerived {
	f.Node = string(n)
	return f
}

func (n linkedNode) Delete(f NodeClaimDerived) NodeClaimDerived {
	f.Node = ""
	return f
}

// linkedNodeKey is the NodeClaim a tracked Node links to: the one its owner reference names.
func linkedNodeKey(_ string, node *corev1.Node) string {
	if !Tracked(node) {
		return ""
	}
	return ownerNodeClaim(node)
}

func linkedNodeOf(name string, _ *corev1.Node) linkedNode { return linkedNode(name) }

// deleted returns whether the NodeClaim is being deleted or its instance is terminating.
func deleted(nodeClaim *v1.NodeClaim) bool {
	return !nodeClaim.DeletionTimestamp.IsZero() || conditionTrue(nodeClaim, v1.ConditionTypeInstanceTerminating)
}

func rebooting(nodeClaim *v1.NodeClaim) bool {
	return conditionTrue(nodeClaim, v1.ConditionTypeRebooting)
}

// conditionTrue returns whether the NodeClaim's condition is true. It reads the conditions without
// StatusConditions, which sets the NodeClaim's missing conditions Unknown, so that it never writes the NodeClaim that
// cluster state shares.
func conditionTrue(nodeClaim *v1.NodeClaim, conditionType string) bool {
	for i := range nodeClaim.Status.Conditions {
		if nodeClaim.Status.Conditions[i].Type == conditionType {
			return nodeClaim.Status.Conditions[i].Status == metav1.ConditionTrue
		}
	}
	return false
}

type nodeClaimIntent[V any] = state.Intent[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent, V]

// Deletion is the handle of the deletion mark on a NodeClaim's node, which the disruption queue holds while a command
// disrupts the node.
type Deletion struct{ intent *nodeClaimIntent[bool] }

func newDeletion(n *NodeClaims) *Deletion {
	return &Deletion{intent: state.NewIntent(n.store, func(i *NodeClaimIntent) *state.Intended[bool] { return &i.Deletion })}
}

// Mark marks the NodeClaims' nodes for deletion.
func (d *Deletion) Mark(nodeClaimNames ...string) {
	for _, name := range nodeClaimNames {
		d.intent.Set(name, true)
	}
}

// Unmark clears the NodeClaims' deletion marks.
func (d *Deletion) Unmark(nodeClaimNames ...string) {
	for _, name := range nodeClaimNames {
		d.intent.Clear(name)
	}
}

// Nomination is the handle of the nomination of a NodeClaim's node for a pending pod, which the provisioner holds. A
// nomination lapses NominationWindow after it's set, and the entry notifies the lapse.
type Nomination struct {
	intent *nodeClaimIntent[bool]
	clock  clock.Clock
}

// minNominationWindow is the shortest a nomination holds.
const minNominationWindow = 10 * time.Second

// NominationWindow is how long a nomination holds: twice the batch's longest duration, and at least 10s.
func NominationWindow(ctx context.Context) time.Duration {
	return max(2*options.FromContext(ctx).BatchMaxDuration, minNominationWindow)
}

func newNomination(clk clock.Clock, n *NodeClaims) *Nomination {
	return &Nomination{
		intent: state.NewIntent(n.store, func(i *NodeClaimIntent) *state.Intended[bool] { return &i.Nomination }).WithExpiry(clk, minNominationWindow),
		clock:  clk,
	}
}

// Nominate records that the NodeClaim's node was the target of a pending pod in a scheduling pass, for the
// NominationWindow of ctx's options, extending a nomination that holds.
func (n *Nomination) Nominate(ctx context.Context, nodeClaimName string) {
	n.intent.SetUntil(nodeClaimName, true, n.clock.Now().Add(NominationWindow(ctx)))
}

// BufferPods is the handle of the buffer pod counts on NodeClaims' nodes, which the provisioner holds.
type BufferPods struct {
	intent     *nodeClaimIntent[int]
	nodeClaims *NodeClaims
}

func newBufferPods(n *NodeClaims) *BufferPods {
	return &BufferPods{
		intent:     state.NewIntent(n.store, func(i *NodeClaimIntent) *state.Intended[int] { return &i.BufferPods }),
		nodeClaims: n,
	}
}

// Replace replaces the buffer pod counts with counts, by NodeClaim name. The provisioner calls it after each
// scheduling pass with the virtual buffer pods it placed on each node; a node missing from counts no longer hosts any.
func (b *BufferPods) Replace(counts map[string]int) {
	for name, e := range b.nodeClaims.List() {
		if _, ok := counts[name]; !ok && e.Intent.BufferPods.Value != 0 {
			b.intent.Clear(name)
		}
	}
	for name, count := range counts {
		b.intent.Set(name, count)
	}
}

// nodeOwner returns the NodeClaim whose entry a Node's change Touches: the one its owner reference names, while
// cluster state tracks it, so that the Derives from the NodeClaim store that read its Node recompute.
func nodeOwner(node *corev1.Node) string {
	if node == nil || !Tracked(node) {
		return ""
	}
	return ownerNodeClaim(node)
}
