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
	"reflect"
	"slices"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state"
)

// NodeClaimDerived are the fields derived about a NodeClaim from other stores: its node's Node and what that Node's
// entry carries, contributed by the Node store keyed by each tracked Node's owner reference. A pod event writes the
// Node's entry, then this one. A managed node reads only its NodeClaim's entry.
type NodeClaimDerived struct {
	// Node is the tracked Node whose owner reference names the NodeClaim. It's nil while the NodeClaim is in flight,
	// before registration sets the owner reference, and once the Node is gone. It's the Node store's object, read-only.
	Node *corev1.Node
	// Usage is the usage on Node's entry: the pods bound to it (requests, limits, host ports, volumes, DaemonSet usage,
	// pod counts) and its CSINode's drivers. It's empty while Node is nil.
	Usage NodeUsage
	// RepairPolicyMatches are the repair policies Node's conditions match, from Node's entry, carried here until repair
	// derives its own, so that disruption's repair reads one store.
	RepairPolicyMatches []repair.RepairPolicyMatch // bounded: repair policies
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

// NodeClaimEntry is a NodeClaim's entry, a managed node's: the NodeClaim, the Node it links with that Node's usage, and
// the intent kept about the node. ViewOfNodeClaim reads its facts; Registered, InFlight, Deleting, Nominated and
// HasBufferPods read what only a managed node has.
type NodeClaimEntry = state.Entry[*v1.NodeClaim, NodeClaimDerived, NodeClaimIntent]

type nodeClaimStore = state.Store[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent]

// NodeClaimIndex is an Index of the NodeClaims by values of type V, such as NodeClaimsByProviderID.
type NodeClaimIndex[V comparable] = state.Index[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent, V]

// NodeClaims are the NodeClaims of the cloud provider's node classes, by name: an object store over the newest version
// of each NodeClaim, and the store of managed nodes, launched or in flight. The NodeClaim is the source of truth for
// Karpenter's view of the nodes it manages, so the intent about a node lives on its NodeClaim's entry and drops with
// it. Disruption, static provisioning, limits and nominations read only this store.
//
// It's the one object store with a filter: a NodeClaim of another provider's node class is no node of Karpenter's, and
// every reader, a Derive's key, a List, a Get and a controller's source, would otherwise have to leave it out.
type NodeClaims struct {
	state.Reader[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent]
	store *nodeClaimStore
	// byProviderID and byNodePool are NodeClaimsByProviderID and NodeClaimsByNodePool.
	byProviderID *NodeClaimIndex[string]
	byNodePool   *NodeClaimIndex[string]
}

func newNodeClaims(from state.Newest, managed []schema.GroupKind) *NodeClaims {
	store := state.NewObjectStore[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent](from, state.WithKey(state.ByName),
		state.WithFilter(func(nodeClaim *v1.NodeClaim) bool {
			return nodeClaim.Spec.NodeClassRef != nil && slices.Contains(managed, nodeClaim.Spec.NodeClassRef.GroupKind())
		}))
	return &NodeClaims{
		Reader: store.Reader,
		store:  store,
		byProviderID: state.NewIndex(store, func(_ string, e NodeClaimEntry) []string {
			return lo.Ternary(e.Object.Status.ProviderID != "", []string{e.Object.Status.ProviderID}, nil)
		}),
		byNodePool: state.NewIndex(store, func(_ string, e NodeClaimEntry) []string {
			return lo.Ternary(e.Object.Labels[v1.NodePoolLabelKey] != "", []string{e.Object.Labels[v1.NodePoolLabelKey]}, nil)
		}),
	}
}

// Exists returns whether cluster state holds the NodeClaim.
func (n *NodeClaims) Exists(name string) bool {
	_, ok := n.Get(name, client.UnsafeDisableDeepCopy)
	return ok
}

// deriveNodes carries each tracked Node, its usage and its repair policy matches to the entry of the NodeClaim its
// owner reference names. It reads the Node's whole entry, so a pod bound to the Node changes the NodeClaim's entry too.
func (n *NodeClaims) deriveNodes(nodes *Nodes) {
	state.Derive(nodes.Reader, n.store, linkedNodeKey, linkedNodeOf)
}

// UnlaunchedSource is the NodeClaims as a source that wakes on a NodeClaim appearing, going, or launching, for a
// decision that waits until none is launching.
func (n *NodeClaims) UnlaunchedSource() source.SyncingSource {
	return n.SourceOn(func(_ string, e NodeClaimEntry) any { return Launched(e) })
}

// Launched returns whether the NodeClaim has a providerID: its instance exists.
func Launched(e NodeClaimEntry) bool { return e.Object != nil && e.Object.Status.ProviderID != "" }

// Registered returns whether the managed node's Node is linked and carries the registered label.
func Registered(e NodeClaimEntry) bool { return registered(e.Object, e.Derived.Node) }

// InFlight returns whether the managed node has no Node yet: it's launching, or its Node is registering.
func InFlight(e NodeClaimEntry) bool { return e.Derived.Node == nil }

// Deleting returns whether the managed node is going away: the disruption queue marked it for deletion, or its
// NodeClaim is being deleted or its instance is terminating. Its pods are reschedulable.
func Deleting(e NodeClaimEntry) bool { return e.Intent.Deletion.Value || deleted(e.Object) }

// Nominated returns whether the managed node was the target of a pending pod in a recent scheduling pass, at now.
func Nominated(e NodeClaimEntry, now time.Time) bool {
	nominated, holds := e.Intent.Nomination.At(now)
	return nominated && holds
}

// HasBufferPods returns whether the provisioner's last scheduling pass placed a virtual buffer pod on the managed node.
func HasBufferPods(e NodeClaimEntry) bool { return e.Intent.BufferPods.Value > 0 }

// linkedNode is a tracked Node's contribution to the NodeClaim its owner reference names: the Node, its usage and its
// repair policy matches. A NodeClaim has one such Node; were two to name it, the last to change would be its Node
// until either left.
type linkedNode struct {
	node    *corev1.Node
	usage   NodeUsage
	matches []repair.RepairPolicyMatch
}

func (n linkedNode) Update(f NodeClaimDerived, _ linkedNode) NodeClaimDerived {
	f.Node, f.Usage, f.RepairPolicyMatches = n.node, n.usage, n.matches
	return f
}

func (n linkedNode) Delete(f NodeClaimDerived) NodeClaimDerived {
	f.Node, f.Usage, f.RepairPolicyMatches = nil, NodeUsage{}, nil
	return f
}

// Equal reports whether both carry the same Node entry's fields. A Node entry's fields never change once stored, so the
// same Node and the same usage maps are the same fields; equal fields in new maps compare unequal, which costs a write.
func (n linkedNode) Equal(o linkedNode) bool {
	return n.node == o.node && sameUsage(n.usage, o.usage) && slices.EqualFunc(n.matches, o.matches, repair.RepairPolicyMatch.Equal)
}

// sameUsage returns whether a and b are the same usage: equal counts, and the same maps and slices.
func sameUsage(a, b NodeUsage) bool {
	return a.Pods == b.Pods && a.Terminating == b.Terminating && a.DoNotDisrupt == b.DoNotDisrupt &&
		a.DisruptionCost == b.DisruptionCost && a.HostPorts == b.HostPorts &&
		same(a.Requests, b.Requests) && same(a.Limits, b.Limits) && same(a.DaemonSetRequests, b.DaemonSetRequests) &&
		same(a.DaemonSetLimits, b.DaemonSetLimits) && same(a.Volumes, b.Volumes) && same(a.AntiAffinityPods, b.AntiAffinityPods) &&
		len(a.CSIDrivers) == len(b.CSIDrivers) && same(a.CSIDrivers, b.CSIDrivers)
}

// same returns whether two maps or two slices are the same one, by reference.
func same(a, b any) bool { return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() }

// linkedNodeKey is the NodeClaim a tracked Node links to: the one its owner reference names.
func linkedNodeKey(_ string, e NodeEntry) string {
	if !Tracked(e.Object) {
		return ""
	}
	return OwnerNodeClaim(e.Object)
}

func linkedNodeOf(_ string, e NodeEntry) linkedNode {
	return linkedNode{node: e.Object, usage: e.Derived.Usage, matches: e.Derived.RepairPolicyMatches}
}

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
	for name, e := range b.nodeClaims.List(client.UnsafeDisableDeepCopy) {
		if _, ok := counts[name]; !ok && e.Intent.BufferPods.Value != 0 {
			b.intent.Clear(name)
		}
	}
	for name, count := range counts {
		b.intent.Set(name, count)
	}
}
