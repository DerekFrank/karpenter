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
	"cmp"
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
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
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

// nodePoolMembers count each NodePool's launched and unlaunched NodeClaims, keyed by the NodePool's name. The members
// themselves are the sources of the Derives that feed it (NodeState.poolLaunched and NodeState.poolUnlaunched), which
// index them in place: a set of every member in the fields would be copied on every change, so a NodePool's fields
// would cost as much as its members to update.
type nodePoolMembers struct {
	// Launched counts the NodePool's NodeClaims with a providerID. A NodeClaim stops counting once it's gone or its
	// label changes.
	Launched int
	// Unlaunched counts the NodePool's NodeClaims without a providerID. A NodeClaim stops counting once it has a
	// providerID or a deletionTimestamp.
	Unlaunched int
}

// launchedNodeClaim is a launched NodeClaim's contribution to its NodePool's members: one NodeClaim.
type launchedNodeClaim int

func (n launchedNodeClaim) Update(f nodePoolMembers, prev launchedNodeClaim) nodePoolMembers {
	f.Launched += int(n - prev)
	return f
}

func (n launchedNodeClaim) Delete(f nodePoolMembers) nodePoolMembers {
	f.Launched -= int(n)
	return f
}

// unlaunchedNodeClaim is an unlaunched NodeClaim's contribution to its NodePool's members: one NodeClaim.
type unlaunchedNodeClaim int

func (u unlaunchedNodeClaim) Update(f nodePoolMembers, prev unlaunchedNodeClaim) nodePoolMembers {
	f.Unlaunched += int(u - prev)
	return f
}

func (u unlaunchedNodeClaim) Delete(f nodePoolMembers) nodePoolMembers {
	f.Unlaunched -= int(u)
	return f
}

// nodePoolOfLaunched is the key a NodeClaim contributes to in nodePoolMembers while it's launched: its NodePool.
func nodePoolOfLaunched(_ string, f NodeClaimFields) string {
	if !f.launched() {
		return ""
	}
	return f.NodeClaim.Labels[v1.NodePoolLabelKey]
}

// nodePoolOfUnlaunched is the key a NodeClaim contributes to in nodePoolMembers: its NodePool, while it has neither a
// providerID nor a deletionTimestamp.
func nodePoolOfUnlaunched(_ string, f NodeClaimFields) string {
	if f.NodeClaim == nil || f.NodeClaim.Status.ProviderID != "" || !f.NodeClaim.DeletionTimestamp.IsZero() {
		return ""
	}
	return f.NodeClaim.Labels[v1.NodePoolLabelKey]
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

// NodePoolDisruption counts a NodePool's nodes for its disruption budgets, from the NodeClaim store: each NodeClaim's
// entry holds the Node its owner reference links, so one source decides all three counts.
type NodePoolDisruption struct {
	// Nodes counts the NodePool's launched NodeClaims that are initialized (their Initialized condition is true) or
	// rebooting, until the instance is terminating. A NodeClaim still initializing has no pods scheduled to it yet and
	// counts toward no budget, so that a replacement doesn't round a percentage budget up.
	Nodes int
	// Disrupting counts the NodeClaims among Nodes that are marked for deletion, deleting or rebooting, which every
	// disruption budget subtracts.
	Disrupting int
	// NotReady counts the NodeClaims among Nodes that aren't Disrupting and whose linked Node isn't Ready or is missing,
	// which every disruption budget but repair's subtracts.
	NotReady int
}

func (d NodePoolDisruption) Update(f NodePoolDisruption, prev NodePoolDisruption) NodePoolDisruption {
	f.Nodes += d.Nodes - prev.Nodes
	f.Disrupting += d.Disrupting - prev.Disrupting
	f.NotReady += d.NotReady - prev.NotReady
	return f
}

func (d NodePoolDisruption) Delete(f NodePoolDisruption) NodePoolDisruption {
	f.Nodes -= d.Nodes
	f.Disrupting -= d.Disrupting
	f.NotReady -= d.NotReady
	return f
}

func (d NodePoolDisruption) Equal(o NodePoolDisruption) bool { return d == o }

// nodePoolDisruptionOfNodeClaim returns the NodePool a NodeClaim counts toward for disruption budgets, or "" if it
// counts toward none. A rebooting NodeClaim counts until its reboot completes, though it's uninitialized meanwhile. A
// NodeClaim's instance is terminating only once the node is drained and the cloud provider deleted it.
func nodePoolDisruptionOfNodeClaim(_ string, f NodeClaimFields) string {
	if !f.launched() || conditionTrue(f.NodeClaim, v1.ConditionTypeInstanceTerminating) {
		return ""
	}
	if !conditionTrue(f.NodeClaim, v1.ConditionTypeInitialized) && !rebooting(f.NodeClaim) {
		return ""
	}
	return f.NodeClaim.Labels[v1.NodePoolLabelKey]
}

// nodeClaimDisruption returns a counted NodeClaim's contribution to its NodePool's disruption counts. A NodeClaim
// without a linked Node reads as not Ready: an initialized NodeClaim's Node has registered, so it has the owner
// reference that links it, and a missing one is gone.
func nodeClaimDisruption(_ string, f NodeClaimFields) NodePoolDisruption {
	if f.MarkedForDeletion || deleted(f.NodeClaim) || rebooting(f.NodeClaim) {
		return NodePoolDisruption{Nodes: 1, Disrupting: 1}
	}
	if f.Node == nil || nodeutils.GetCondition(f.Node, corev1.NodeReady).Status != corev1.ConditionTrue {
		return NodePoolDisruption{Nodes: 1, NotReady: 1}
	}
	return NodePoolDisruption{Nodes: 1}
}

// NodePoolNodes returns the StateNodes of the NodePool's launched NodeClaims, sorted by providerID, read from the
// NodePool's index rather than a scan of every node. They share their Node, NodeClaim and usage with cluster state, so
// they must not be modified.
func (s *NodeState) NodePoolNodes(nodePoolName string) StateNodes {
	names := s.poolLaunched.Sources(nodePoolName)
	nodes := make(StateNodes, 0, len(names))
	for _, name := range names {
		// The index can lag the NodeClaims, so a NodeClaim it lists may have left or moved.
		if f, ok := s.NodeClaimStore.Get(name); ok && nodePoolOfLaunched(name, f) == nodePoolName {
			nodes = append(nodes, s.nodeClaimNode(f, false))
		}
	}
	slices.SortFunc(nodes, func(a, b *StateNode) int { return cmp.Compare(a.ProviderID(), b.ProviderID()) })
	return nodes
}

// UnlaunchedNodeClaims returns the NodePool's NodeClaims that have neither a providerID nor a deletionTimestamp, sorted
// by name. They are shared with cluster state, so they must not be modified.
func (s *NodeState) UnlaunchedNodeClaims(nodePoolName string) []*v1.NodeClaim {
	names := s.poolUnlaunched.Sources(nodePoolName)
	slices.Sort(names)
	nodeClaims := make([]*v1.NodeClaim, 0, len(names))
	for _, name := range names {
		if f, ok := s.NodeClaimStore.Get(name); ok && nodePoolOfUnlaunched(name, f) == nodePoolName {
			nodeClaims = append(nodeClaims, f.NodeClaim)
		}
	}
	return nodeClaims
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

// UpdateBufferPodCounts replaces the buffer pod counts with counts, by NodeClaim name. The provisioner calls it after
// each scheduling pass with the virtual buffer pods it placed on each node, counted from Results.ExistingNodes; a node
// missing from counts no longer hosts any. When real pods consume buffer capacity, the virtual pods move to other
// nodes or new NodeClaims, and the next pass's counts follow them.
func (s *NodeState) UpdateBufferPodCounts(counts map[string]int) {
	for name, f := range s.NodeClaimStore.List() {
		if _, ok := counts[name]; !ok && f.BufferPods != 0 {
			s.updateIntent(name, func(f NodeClaimFields) NodeClaimFields { f.BufferPods = 0; return f })
		}
	}
	for name, count := range counts {
		s.updateIntent(name, func(f NodeClaimFields) NodeClaimFields { f.BufferPods = count; return f })
	}
}

// HasBufferPods returns true if the NodeClaim's node has at least one virtual buffer pod placed on it during the last
// provisioning pass.
func (s *NodeState) HasBufferPods(nodeClaimName string) bool {
	return s.BufferPodCount(nodeClaimName) > 0
}

// BufferPodCount returns the number of virtual buffer pods on the NodeClaim's node.
func (s *NodeState) BufferPodCount(nodeClaimName string) int {
	f, _ := s.NodeClaimStore.Get(nodeClaimName)
	return f.BufferPods
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

// NodePoolDisruption returns the NodePool's nodes as its disruption budgets count them.
func (s *NodeState) NodePoolDisruption(nodePoolName string) NodePoolDisruption {
	d, _ := s.nodePoolDisruption.Get(nodePoolName)
	return d
}
