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
	"iter"

	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state"
)

// NodeState holds the stores behind the nodes Karpenter schedules onto and disrupts, NodeClaim first:
//
//   - NodeClaimStore holds each NodeClaim with the intent kept about its node and the Node its owner reference links,
//     by NodeClaim name (nodeclaim.go). Derived from it: each NodePool's members, capacity and disruption counts, and
//     the providerID index.
//   - NodeStore holds each Node, by name (node.go). Derived from it: the Node on its NodeClaim's entry, by owner
//     reference; the providerID fallback link, for the window before registration; each Node with its repair policy
//     matches, by name, with its usage; the candidate unmanaged Nodes; and the Node health counts.
//   - PodStore holds each pod, by namespaced name. Derived from it: the usage of each Node, by Node name, and each
//     DaemonSet's newest pod.
//   - NodePoolState counts each NodePool's NodeClaims, with the reservations against its node limit, and
//     PodSchedulingStore keeps what the provisioner decides about each pod (podscheduling.go).
//
// A StateNode is built at read time from a NodeClaim, its linked Node and that Node's usage, or from an unmanaged Node.
// Built with a kubeClient and informers, the object stores follow the API server once State.Register starts them;
// without, a caller feeds them (Observe, Forget, Resync).
type NodeState struct {
	// ctx carries the options derived fields read. Derived fields are computed on whatever goroutine changes their
	// source, so they read configuration fixed at construction rather than a request's.
	ctx     context.Context
	clock   clock.Clock
	matcher *repair.RepairPolicyMatcher // nil when node repair is disabled

	// NodeClaimStore holds each NodeClaim and the intent kept about it, by NodeClaim name.
	NodeClaimStore *state.Store[string, NodeClaimFields]
	// UnlaunchedNodeClaimStore holds the name of each NodeClaim without a providerID, fed by the NodeClaim store, until
	// it launches or goes.
	UnlaunchedNodeClaimStore *state.Store[string, struct{}]
	// providerIDs holds the NodeClaims and Nodes with each providerID, fed by both object stores.
	providerIDs *state.Store[string, ProviderIDFields]
	// nodePoolMembers counts each NodePool's launched and unlaunched NodeClaims; poolLaunched and poolUnlaunched index
	// them.
	nodePoolMembers *state.Store[string, nodePoolMembers]
	poolLaunched    *state.Derivation[string, string, NodeClaimFields, nodePoolMembers, launchedNodeClaim]
	poolUnlaunched  *state.Derivation[string, string, NodeClaimFields, nodePoolMembers, unlaunchedNodeClaim]
	// nodePoolCapacity sums the capacity of each NodePool's NodeClaims, counted against its limits.
	nodePoolCapacity *state.Store[string, corev1.ResourceList]
	// nodePoolDisruption counts each NodePool's nodes for its disruption budgets, fed by the NodeClaim store.
	nodePoolDisruption *state.Store[string, NodePoolDisruption]

	// NodeStore holds each Node, by name.
	NodeStore  *state.Store[string, *corev1.Node]
	nodeHealth *NodeHealth
	// nodeCounts counts the nodes in Nodes for ClusterStateNodesCount. bareNodes indexes the bare Nodes it counts,
	// which are the candidates for unmanaged Nodes.
	nodeCounts *state.Store[nodeCountKey, int]
	bare       *state.Derivation[string, nodeCountKey, *corev1.Node, int, counted]

	// PodStore holds each pod, by namespaced name.
	PodStore *state.Store[types.NamespacedName, *corev1.Pod]
	// nodeUsage keeps each tracked Node with its repair policy matches and the fields derived from its pods, by Node
	// name, so a read of a Node's name takes one lookup. PodsOnNodes is the Derive from PodStore, which knows the pods
	// bound to each Node.
	nodeUsage   *state.Store[string, podFields]
	PodsOnNodes *state.Derivation[types.NamespacedName, string, *corev1.Pod, podFields, podContribution]
	// pvcDrivers keeps the CSI driver of each PersistentVolumeClaim, by namespaced name, which counts its volume against
	// the driver's limit on the node a pod mounts it on.
	pvcDrivers *state.Store[types.NamespacedName, pvcFields]
	// daemonSetPods keeps the newest pod of each DaemonSet, by DaemonSet, fed by the pod store.
	daemonSetPods *state.Store[types.NamespacedName, daemonSetFields]

	// NodePoolState counts each NodePool's NodeClaims and holds the nodes reserved against its node limit.
	NodePoolState *NodePoolState
	// PodSchedulingStore keeps what the provisioner decides about each pod as it schedules it, by namespaced name, for
	// the pod metrics. NodeClaimPodsStore keeps the pods last simulated to schedule against each NodeClaim, fed by it.
	PodSchedulingStore *state.Store[types.NamespacedName, PodSchedulingFields]
	NodeClaimPodsStore *state.Store[string, map[types.NamespacedName]struct{}]
}

// nodeCountKey is what a source counts toward in nodeCounts.
type nodeCountKey int

const (
	_ nodeCountKey = iota
	// launchedNodeClaims counts the NodeClaims with a providerID.
	launchedNodeClaims
	// bareNodes counts the Nodes with neither a NodeClaim owner reference nor the NodePool label.
	bareNodes
	// claimedBareNodes counts the bare Nodes whose providerID a NodeClaim has, which are that NodeClaim's node.
	claimedBareNodes
)

// counted is a source's contribution to a count.
type counted int

func (n counted) Update(f int, prev counted) int { return f + int(n-prev) }
func (n counted) Delete(f int) int               { return f - int(n) }

// NewNodeState returns empty node state. With a kubeClient and informers, its NodeClaim, Node and pod stores follow
// the API server once they start, as State.Register starts them; with nil, a caller feeds them. It holds the
// NodeClaims of cloudProvider's node classes. ctx carries the options its derived fields read, such as the feature
// gates that decide a pod's disruption cost. A matcher makes it match each Node against the repair policies as it
// changes; node repair reads the same matcher back through RepairPolicyMatcher.
func NewNodeState(ctx context.Context, clk clock.Clock, kubeClient client.Client, informers cache.Informers, cloudProvider cloudprovider.CloudProvider, matcher *repair.RepairPolicyMatcher) *NodeState {
	managed := lo.Map(cloudProvider.GetSupportedNodeClasses(), func(nodeClass status.Object, _ int) schema.GroupKind {
		return object.GVK(nodeClass).GroupKind()
	})
	s := &NodeState{
		ctx:                      ctx,
		clock:                    clk,
		matcher:                  matcher,
		NodeClaimStore:           newNodeClaimStore(kubeClient, informers, managed),
		UnlaunchedNodeClaimStore: state.NewStore[string, struct{}](),
		providerIDs:              state.NewStore[string, ProviderIDFields](),
		nodePoolMembers:          state.NewStore[string, nodePoolMembers](),
		nodePoolCapacity:         state.NewStore[string, corev1.ResourceList](),
		nodePoolDisruption:       state.NewStore[string, NodePoolDisruption](),
		NodeStore:                newNodeStore(kubeClient, informers),
		nodeCounts:               state.NewStore[nodeCountKey, int](),
		PodStore:                 state.NewObjectStore[*corev1.Pod](kubeClient, informers),
		nodeUsage:                state.NewStore[string, podFields](),
		pvcDrivers:               state.NewStore[types.NamespacedName, pvcFields](),
		daemonSetPods:            state.NewStore[types.NamespacedName, daemonSetFields](),
	}
	s.PodSchedulingStore, s.NodeClaimPodsStore = newPodSchedulingStores()
	s.NodePoolState = newNodePoolState(clk, s.NodeClaimStore)
	// The NodeClaim side.
	state.Derive(s.NodeClaimStore, s.UnlaunchedNodeClaimStore, unlaunchedKey, func(string, NodeClaimFields) awaitingLaunch {
		return awaitingLaunch{}
	})
	state.Derive(s.NodeClaimStore, s.providerIDs,
		func(_ string, f NodeClaimFields) string {
			if !f.launched() {
				return ""
			}
			return f.NodeClaim.Status.ProviderID
		},
		func(name string, _ NodeClaimFields) nodeClaimAtProviderID { return nodeClaimAtProviderID(name) })
	s.poolLaunched = state.Derive(s.NodeClaimStore, s.nodePoolMembers, nodePoolOfLaunched, func(string, NodeClaimFields) launchedNodeClaim { return 1 })
	s.poolUnlaunched = state.Derive(s.NodeClaimStore, s.nodePoolMembers, nodePoolOfUnlaunched, func(string, NodeClaimFields) unlaunchedNodeClaim { return 1 })
	state.Derive(s.NodeClaimStore, s.nodePoolCapacity, nodePoolOfCapacity, capacityOf)
	state.Derive(s.NodeClaimStore, s.nodePoolDisruption, nodePoolDisruptionOfNodeClaim, nodeClaimDisruption)
	state.Derive(s.NodeClaimStore, s.nodeCounts,
		func(_ string, f NodeClaimFields) nodeCountKey { return lo.Ternary(f.launched(), launchedNodeClaims, 0) },
		func(string, NodeClaimFields) counted { return 1 })

	// The Node side.
	// The Node an owner reference links goes on its NodeClaim's entry, so a registered NodeClaim reads with its Node.
	state.Derive(s.NodeStore, s.NodeClaimStore, nodeOwnerKey, func(_ string, node *corev1.Node) ownedNode {
		return ownedNode{node: node, matches: s.repairMatchesOf(node)}
	})
	state.Derive(s.NodeStore, s.providerIDs, nodeProviderIDKey, nodeAtProviderIDOf)
	s.bare = state.Derive(s.NodeStore, s.nodeCounts, bareNodeKey, func(string, *corev1.Node) counted { return 1 })
	state.Derive(s.providerIDs, s.nodeCounts,
		func(_ string, f ProviderIDFields) nodeCountKey {
			return lo.Ternary(len(f.NodeClaims) > 0 && f.BareNodes > 0, claimedBareNodes, 0)
		},
		func(_ string, f ProviderIDFields) counted { return counted(f.BareNodes) })
	state.Derive(s.NodeStore, s.nodeUsage, nodeOnNodeKey, func(_ string, node *corev1.Node) nodeOnNode {
		return nodeOnNode{node: node, matches: s.repairMatchesOf(node)}
	})
	s.nodeHealth = s.newNodeHealth(func(node *corev1.Node) bool { return matcher != nil && matcher.MatchesAny(node) })
	s.nodeCounts.Subscribe(func(state.Change[nodeCountKey, int]) {
		ClusterStateNodesCount.Set(float64(s.nodeCount()), nil)
	})

	// The pods.
	s.PodsOnNodes = state.Derive(s.PodStore, s.nodeUsage, podNodeKey, s.newPodContribution)
	state.Derive(s.PodStore, s.daemonSetPods, daemonSetKey, func(_ types.NamespacedName, pod *corev1.Pod) newestPod { return newestPod{pod} })
	return s
}

// nodeCount is how many nodes Nodes returns: the launched NodeClaims, and the unmanaged Nodes.
func (s *NodeState) nodeCount() int {
	launched, _ := s.nodeCounts.Get(launchedNodeClaims)
	bare, _ := s.nodeCounts.Get(bareNodes)
	claimed, _ := s.nodeCounts.Get(claimedBareNodes)
	return launched + bare - claimed
}

// stores returns every store of the node state.
func (s *NodeState) stores() []interface {
	Clear()
	Synced() bool
} {
	return []interface {
		Clear()
		Synced() bool
	}{
		s.NodeClaimStore, s.UnlaunchedNodeClaimStore, s.NodeStore, s.PodStore, s.providerIDs, s.nodePoolMembers,
		s.nodePoolCapacity, s.nodePoolDisruption, s.nodeCounts, s.nodeUsage, s.pvcDrivers, s.daemonSetPods,
		s.NodePoolState.nodePools, s.PodSchedulingStore, s.NodeClaimPodsStore,
	}
}

// Clear empties the stores in place, so that their subscribers stay subscribed.
func (s *NodeState) Clear() {
	for _, store := range s.stores() {
		store.Clear()
	}
}

// Synced returns whether every store holds what the API server held when its object stores started: each object
// store has applied its first List, and each derived store everything derived from them. A controller that reads node
// state declares it in Register, with WatchesRawSource(state.Requires(...)).
func (s *NodeState) Synced() bool {
	for _, store := range s.stores() {
		if !store.Synced() {
			return false
		}
	}
	return true
}

// Nodes returns an iterator over the state nodes in the cluster: each launched NodeClaim, with its linked Node if any,
// and each unmanaged Node. The StateNodes share their Node, NodeClaim and usage with cluster state, so they must not be
// modified; use DeepCopyNodes for StateNodes to modify.
func (s *NodeState) Nodes() iter.Seq[*StateNode] {
	return func(yield func(*StateNode) bool) {
		s.nodes(false, yield)
	}
}

// DeepCopyNodes returns the state nodes in the cluster, with their own copies of the usage that the scheduler adds
// simulated pods to. They share their Node and NodeClaim with cluster state, which must not be modified.
func (s *NodeState) DeepCopyNodes() StateNodes {
	var nodes StateNodes
	s.nodes(true, func(n *StateNode) bool {
		nodes = append(nodes, n)
		return true
	})
	return nodes
}

func (s *NodeState) nodes(copyUsage bool, yield func(*StateNode) bool) {
	for _, f := range s.NodeClaimStore.List() {
		if f.launched() && !yield(s.nodeClaimNode(f, copyUsage)) {
			return
		}
	}
	// Only bare Nodes can be unmanaged; the derive from the Node store indexes them.
	for _, name := range s.bare.Sources(bareNodes) {
		if f, _ := s.nodeUsage.Get(name); f.Node != nil && s.unmanaged(f.Node) && !yield(s.withNode(&StateNode{}, f, copyUsage)) {
			return
		}
	}
}

// nodeClaimNode builds the StateNode of a NodeClaim and its linked Node.
func (s *NodeState) nodeClaimNode(f NodeClaimFields, copyUsage bool) *StateNode {
	n := &StateNode{
		NodeClaim:         f.NodeClaim,
		markedForDeletion: f.MarkedForDeletion,
		nominatedUntil:    f.NominatedUntil,
	}
	return s.withNode(n, s.linkedNode(f.NodeClaim.Name, f), copyUsage)
}

// repairMatchesOf returns the repair policies the Node's conditions match, or none without a matcher. Matching only
// runs for a Node with a condition a policy covers.
func (s *NodeState) repairMatchesOf(node *corev1.Node) []repair.RepairPolicyMatch {
	if s.matcher == nil || !s.matcher.MatchesAny(node) {
		return nil
	}
	return s.matcher.Match(node)
}

// withNode sets the StateNode's Node from the fields kept by its name, which has a nil Node for a NodeClaim without
// one, with its repair policy matches and its usage joined with its PVCs' drivers. copyUsage copies the host port
// usage, which the scheduler adds simulated pods to; the volume usage is always the StateNode's own.
func (s *NodeState) withNode(n *StateNode, pods podFields, copyUsage bool) *StateNode {
	n.Node, n.repairPolicyMatches = pods.Node, pods.RepairPolicyMatches
	n.Derived.PodRequests, n.Derived.PodLimits = pods.Requests, pods.Limits
	n.Derived.DaemonSetRequests, n.Derived.DaemonSetLimits = pods.DaemonSetRequests, pods.DaemonSetLimits
	n.Derived.DisruptionCost = pods.DisruptionCost
	n.Derived.HostPorts = lo.TernaryF(pods.HostPorts == nil, scheduling.NewHostPortUsage, func() *scheduling.HostPortUsage {
		return lo.Ternary(copyUsage, pods.HostPorts.DeepCopy(), pods.HostPorts)
	})
	n.Derived.Volumes = scheduling.NewVolumeUsage()
	if len(pods.PVCs) > 0 {
		volumes := scheduling.Volumes{}
		for pvc := range pods.PVCs {
			if driver, _ := s.pvcDrivers.Get(pvc); driver.Driver != "" {
				volumes.Add(driver.Driver, pvc.String())
			}
		}
		n.Derived.Volumes.Insert(volumes)
	}
	for _, driver := range pods.CSIDrivers {
		if driver.Allocatable == nil || driver.Allocatable.Count == nil {
			n.Derived.Volumes.AddUnbounded(driver.Name)
			continue
		}
		n.Derived.Volumes.AddLimit(driver.Name, int(*driver.Allocatable.Count))
	}
	return n
}

// TrackedNodeNames returns the names of the Nodes cluster state tracks.
func (s *NodeState) TrackedNodeNames() iter.Seq[string] {
	return func(yield func(string) bool) {
		for name, node := range s.NodeStore.List() {
			if Tracked(node) && !yield(name) {
				return
			}
		}
	}
}

// Subscribe calls fn after each change to a node that may make consolidation possible: a node added or removed, or
// one whose initialization, deletion mark or pod count changes. fn must not block.
func (s *NodeState) Subscribe(fn func()) {
	s.NodeClaimStore.Subscribe(func(c state.Change[string, NodeClaimFields]) {
		if c.Old.launched() != c.New.launched() || c.Old.MarkedForDeletion != c.New.MarkedForDeletion ||
			(c.Old.NodeClaim != nil && c.New.NodeClaim != nil && deleted(c.Old.NodeClaim) != deleted(c.New.NodeClaim)) {
			fn()
		}
	})
	s.NodeStore.Subscribe(func(c state.Change[string, *corev1.Node]) {
		if c.Existed != c.Exists || initialized(c.Old) != initialized(c.New) {
			fn()
		}
	})
	s.nodeUsage.Subscribe(func(c state.Change[string, podFields]) {
		if c.Old.Pods != c.New.Pods {
			fn()
		}
	})
}

func initialized(node *corev1.Node) bool {
	return node != nil && node.Labels[v1.NodeInitializedLabelKey] == "true"
}
