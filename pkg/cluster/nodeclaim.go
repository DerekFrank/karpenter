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
	"maps"
	"slices"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// NodeClaimFields are the fields kept for each NodeClaim, keyed by its name: the NodeClaim, the Node its owner reference
// names, and the intent Karpenter keeps about its node. The NodeClaim is the source of truth for Karpenter's view of
// the nodes it manages, so the intent lives with it and survives its launch, when it gains a providerID, and its
// registration, when its Node gains the owner reference. The intent goes with the NodeClaim.
//
// The NodeClaim store's join part sets NodeClaim, and a Derive from the Node store sets Node and RepairPolicyMatches,
// so an entry can briefly hold a Node without a NodeClaim; readers skip it.
type NodeClaimFields struct {
	NodeClaim *v1.NodeClaim
	// Node is the tracked Node whose NodeClaim owner reference names the NodeClaim, and RepairPolicyMatches the repair
	// policies its conditions match, fed by the Node store. They go once the Node does, or its owner reference changes.
	Node                *corev1.Node
	RepairPolicyMatches []repair.RepairPolicyMatch
	// MarkedForDeletion is set by the disruption queue while a command disrupts the node.
	MarkedForDeletion bool
	// NominatedUntil is when the provisioner's last nomination of the node for a pending pod lapses.
	NominatedUntil metav1.Time
	// BufferPods counts the virtual CapacityBuffer pods the provisioner's last scheduling pass placed on the node. It
	// replaces them all after each pass (UpdateBufferPodCounts).
	BufferPods int
}

// orNothing drops the intent kept about a NodeClaim once the NodeClaim is gone. It keeps the Node, which the Derive
// from the Node store set and clears.
func (f NodeClaimFields) orNothing() NodeClaimFields {
	if f.NodeClaim == nil {
		return NodeClaimFields{Node: f.Node, RepairPolicyMatches: f.RepairPolicyMatches}
	}
	return f
}

// ownedNode is a tracked Node's contribution to the NodeClaim its owner reference names: the Node, and the repair
// policies its conditions match. Each NodeClaim normally has one such Node; were two to name it, the last to change
// would be its Node until either left. Delete clears only what Update set.
type ownedNode struct {
	node    *corev1.Node
	matches []repair.RepairPolicyMatch
}

func (n ownedNode) Update(f NodeClaimFields, _ ownedNode) NodeClaimFields {
	f.Node, f.RepairPolicyMatches = n.node, n.matches
	return f
}

func (n ownedNode) Delete(f NodeClaimFields) NodeClaimFields {
	f.Node, f.RepairPolicyMatches = nil, nil
	return f
}

func (n ownedNode) Equal(o ownedNode) bool {
	return n.node == o.node && slices.EqualFunc(n.matches, o.matches, repair.RepairPolicyMatch.Equal)
}

// launched returns whether the NodeClaim has a providerID: its instance exists, and it's a node in Nodes.
func (f NodeClaimFields) launched() bool {
	return f.NodeClaim != nil && f.NodeClaim.Status.ProviderID != ""
}

// newNodeClaimStore returns the NodeClaim store: a single-part join, so that each entry holds the NodeClaim with the
// intent kept about it.
func newNodeClaimStore() *state.Store[string, NodeClaimFields] {
	return state.NewJoinStore(nil, nil, state.Join(func(nodeClaim *v1.NodeClaim) string { return nodeClaim.Name },
		func(f NodeClaimFields, nodeClaim *v1.NodeClaim) NodeClaimFields {
			f.NodeClaim = nodeClaim
			return f.orNothing()
		}))
}

// ProviderIDFields are the NodeClaims and the Nodes that carry a providerID, keyed by it, sorted by name. A providerID
// is one machine's, so each holds one name, or more only while a conflict lasts, in which case readers take the first.
// The NodeClaims are the providerID index; the Nodes are the fallback link of a NodeClaim to its Node before its
// registration adds the owner reference.
type ProviderIDFields struct {
	// NodeClaims are the NodeClaims with the providerID, fed by the NodeClaim store.
	NodeClaims []string
	// Nodes are the Nodes cluster state tracks with the providerID, fed by the Node store.
	Nodes []string
	// BareNodes counts the Nodes among Nodes with neither a NodeClaim owner reference nor the NodePool label. Such a
	// Node is unmanaged unless a NodeClaim has its providerID.
	BareNodes int
}

// nodeClaimAtProviderID is a launched NodeClaim's contribution to its providerID: its name.
type nodeClaimAtProviderID string

func (n nodeClaimAtProviderID) Update(f ProviderIDFields, prev nodeClaimAtProviderID) ProviderIDFields {
	f.NodeClaims = insertName(removeName(slices.Clone(f.NodeClaims), string(prev)), string(n))
	return f
}

func (n nodeClaimAtProviderID) Delete(f ProviderIDFields) ProviderIDFields {
	f.NodeClaims = removeName(slices.Clone(f.NodeClaims), string(n))
	return f
}

func (n nodeClaimAtProviderID) Equal(o nodeClaimAtProviderID) bool { return n == o }

// insertName inserts name into the sorted names, which it modifies; the empty name is no name.
func insertName(names []string, name string) []string {
	if name == "" {
		return lo.Ternary(len(names) == 0, nil, names)
	}
	i, _ := slices.BinarySearch(names, name)
	return slices.Insert(names, i, name)
}

// removeName removes one occurrence of name from the sorted names, which it modifies, and returns nil for no names, so
// that a providerID nothing carries leaves the store.
func removeName(names []string, name string) []string {
	if i, found := slices.BinarySearch(names, name); found && name != "" {
		names = slices.Delete(names, i, i+1)
	}
	return lo.Ternary(len(names) == 0, nil, names)
}

// nodePoolCapacity is a NodeClaim's contribution to its NodePool's resources, counted against the NodePool's limits:
// its node's capacity, as StateNode.Capacity reads it from the NodeClaim and the Node on its entry, and one
// resources.Node.
type nodePoolCapacity corev1.ResourceList

func (n nodePoolCapacity) Update(total corev1.ResourceList, prev nodePoolCapacity) corev1.ResourceList {
	sum := resources.Merge(total, corev1.ResourceList(n))
	resources.SubtractFrom(sum, corev1.ResourceList(prev))
	return nonzero(sum)
}

func (n nodePoolCapacity) Delete(total corev1.ResourceList) corev1.ResourceList {
	sum := resources.Merge(total)
	resources.SubtractFrom(sum, corev1.ResourceList(n))
	return nonzero(sum)
}

func (n nodePoolCapacity) Equal(o nodePoolCapacity) bool {
	return resourcesEqual(corev1.ResourceList(n), corev1.ResourceList(o))
}

func resourcesEqual(a, b corev1.ResourceList) bool {
	return maps.EqualFunc(a, b, func(x, y resource.Quantity) bool { return x.Cmp(y) == 0 })
}

// nodePoolOfCapacity is the NodePool a NodeClaim's capacity counts against: its node's NodePool label while it's
// launched, until it's marked for deletion, deleting or its instance is terminating.
func nodePoolOfCapacity(_ string, f NodeClaimFields) string {
	if !f.launched() || f.MarkedForDeletion || deleted(f.NodeClaim) {
		return ""
	}
	return (&StateNode{Node: f.Node, NodeClaim: f.NodeClaim}).Labels()[v1.NodePoolLabelKey]
}

// capacityOf is the NodeClaim's node's capacity: the Node's once it's initialized, and the NodeClaim's before, with the
// Node's where it has one, as StateNode.Capacity reads it. Capacity always includes one resources.Node.
func capacityOf(_ string, f NodeClaimFields) nodePoolCapacity {
	return nodePoolCapacity((&StateNode{Node: f.Node, NodeClaim: f.NodeClaim}).Capacity())
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

// NodeClaimForProviderID returns the name of the NodeClaim with the providerID, for a caller that only has the
// providerID. A caller holding the NodeClaim should use its name.
func (s *NodeState) NodeClaimForProviderID(providerID string) (string, bool) {
	f, ok := s.providerIDs.Get(providerID)
	if !ok || len(f.NodeClaims) == 0 {
		return "", false
	}
	return f.NodeClaims[0], true
}

func (s *NodeState) NodeClaimExists(nodeClaimName string) bool {
	f, _ := s.NodeClaimStore.Get(nodeClaimName)
	return f.NodeClaim != nil
}

func (s *NodeState) UnlaunchedNodeClaimExists(nodeClaimName string) bool {
	f, _ := s.NodeClaimStore.Get(nodeClaimName)
	return f.NodeClaim != nil && !f.launched()
}

// IsNodeNominated returns true if the NodeClaim's node was expected to have a pod bound to it during a recent
// scheduling batch.
func (s *NodeState) IsNodeNominated(nodeClaimName string) bool {
	f, ok := s.NodeClaimStore.Get(nodeClaimName)
	return ok && f.NominatedUntil.After(s.clock.Now())
}

// NominateNodeForPod records that the NodeClaim's node was the target of a pending pod during a scheduling batch.
func (s *NodeState) NominateNodeForPod(ctx context.Context, nodeClaimName string) {
	until := metav1.Time{Time: s.clock.Now().Add(nominationWindow(ctx))}
	s.updateIntent(nodeClaimName, func(f NodeClaimFields) NodeClaimFields {
		f.NominatedUntil = until // extends nomination window if already nominated
		return f
	})
}

// MarkForDeletion marks the NodeClaims' nodes as pending deletion.
func (s *NodeState) MarkForDeletion(nodeClaimNames ...string) {
	for _, name := range nodeClaimNames {
		s.updateIntent(name, func(f NodeClaimFields) NodeClaimFields { f.MarkedForDeletion = true; return f })
	}
}

// UnmarkForDeletion removes the marking on the NodeClaims' nodes as nodes the controller intends to delete.
func (s *NodeState) UnmarkForDeletion(nodeClaimNames ...string) {
	for _, name := range nodeClaimNames {
		s.updateIntent(name, func(f NodeClaimFields) NodeClaimFields { f.MarkedForDeletion = false; return f })
	}
}

// updateIntent updates the intent kept about a NodeClaim cluster state holds; intent about a NodeClaim it doesn't hold
// would outlive it, so it's dropped.
func (s *NodeState) updateIntent(nodeClaimName string, update func(NodeClaimFields) NodeClaimFields) {
	s.NodeClaimStore.Update(nodeClaimName, func(f NodeClaimFields) NodeClaimFields {
		if f.NodeClaim == nil {
			return f
		}
		return update(f)
	})
}

// NodePoolResourcesFor returns the capacity counted against the NodePool's limits: the capacity of its launched
// NodeClaims' nodes that aren't marked for deletion or deleting.
func (s *NodeState) NodePoolResourcesFor(nodePoolName string) corev1.ResourceList {
	resources, _ := s.nodePoolCapacity.Get(nodePoolName)
	return maps.Clone(resources)
}
