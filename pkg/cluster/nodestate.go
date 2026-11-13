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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state"
)

// NodeState is the node accessor: it reads the nodes Karpenter schedules onto and disrupts from the stores of each
// kind, one file each, NodeClaim first:
//
//   - NodeClaimStore holds each NodeClaim, by name, with its linked Node's name and the intent kept about its node
//     (nodeclaim.go).
//   - NodeStore holds each Node, by name, with the usage of the pods bound to it, its CSINode's drivers and its repair
//     policy matches (node.go, nodeusage.go).
//   - PodStore holds each pod, by namespaced name (pod.go); PVCStore each PVC's resolved driver (pvc.go), and
//     CSINodeStore each CSINode (node.go).
//   - DaemonSets holds each DaemonSet its pods name (daemonset.go), and Cluster the cluster-wide counts (cluster.go).
//
// A StateNode is built at read time from a launched NodeClaim's entry and the entry of the Node it names, one lookup
// per store, or from an unmanaged Node's entry. The object stores follow each kind's newest version through the
// overlay.
type NodeState struct {
	clock   clock.Clock
	matcher *repair.RepairPolicyMatcher // nil when node repair is disabled

	NodeClaimStore *NodeClaims
	NodeStore      *Nodes
	PodStore       *Pods
	CSINodeStore   *CSINodes
	PVCStore       *PVCs
	DaemonSets     *DaemonSets
	Cluster        *Clusters
	// PodsOnNodes is the Derive from the pod store, which knows the pods bound to each Node.
	PodsOnNodes *state.Derivation[types.NamespacedName, string, NodeDerived, podContribution]
}

// NewNodeState returns the node state, with the Derives that keep it. Its object stores follow the newest versions
// sources returns of each kind. It holds the NodeClaims of cloudProvider's node classes. ctx carries the options its derived fields read, such as the feature
// gates that decide a pod's disruption cost. A matcher makes it match each Node against the repair policies as it
// changes.
func NewNodeState(ctx context.Context, clk clock.Clock, sources Sources, cloudProvider cloudprovider.CloudProvider, matcher *repair.RepairPolicyMatcher) *NodeState {
	managed := lo.Map(cloudProvider.GetSupportedNodeClasses(), func(nodeClass status.Object, _ int) schema.GroupKind {
		return object.GVK(nodeClass).GroupKind()
	})
	pods := newPods(sources(&corev1.Pod{}))
	s := &NodeState{
		clock:          clk,
		matcher:        matcher,
		NodeClaimStore: newNodeClaims(sources(&v1.NodeClaim{}), managed),
		NodeStore:      newNodes(sources(&corev1.Node{})),
		PodStore:       pods,
		CSINodeStore:   newCSINodes(sources(&storagev1.CSINode{})),
		PVCStore:       newPVCs(),
		DaemonSets:     newDaemonSets(pods),
		Cluster:        newClusters(),
	}
	// The join: each tracked Node's name on the entry of the NodeClaim its owner reference names. A Node's change
	// Touches that NodeClaim, so the Derives from the NodeClaim store that read its Node recompute.
	state.DeriveObject(s.NodeStore.Reader, s.NodeClaimStore.store, linkedNodeKey, linkedNodeOf)
	state.TouchOn(s.NodeStore.Reader, s.NodeClaimStore.store, func(c state.Change[string, *corev1.Node, NodeDerived, struct{}]) []string {
		if c.Existed && c.Exists && c.Old.Object == c.New.Object {
			return nil
		}
		var owners []string
		if owner := nodeOwner(c.Old.Object); owner != "" {
			owners = append(owners, owner)
		}
		if owner := nodeOwner(c.New.Object); owner != "" && owner != nodeOwner(c.Old.Object) {
			owners = append(owners, owner)
		}
		return owners
	})
	if matcher != nil {
		s.NodeStore.deriveRepairMatches(matcher)
	}
	s.CSINodeStore.deriveDrivers(s.NodeStore)
	s.PVCStore.index(pods)
	s.PodsOnNodes = state.Derive(pods.Reader, s.NodeStore.store, podNodeKey, podContributionOf(ctx, s.PVCStore))
	s.Cluster.derive(s.NodeClaimStore, s.NodeStore)
	return s
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
		s.NodeClaimStore.store, s.NodeStore.store, s.PodStore.store, s.CSINodeStore.store, s.PVCStore.store,
		s.DaemonSets.store, s.Cluster.store,
	}
}

// Clear empties the stores in place, so that their subscribers stay subscribed. It's for tests, between specs; storelint reports a Clear outside a test.
func (s *NodeState) Clear() {
	for _, store := range s.stores() {
		store.Clear()
	}
}

// Synced returns whether every store holds what the API server held when its object stores started: each object
// store has applied what its source held once it synced, and each store everything derived from them. A controller that reads node state
// declares it in Register, with WatchesRawSource(state.Requires(...)).
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
	for _, e := range s.NodeClaimStore.List() {
		if Launched(e) && !yield(s.nodeClaimNode(e, copyUsage)) {
			return
		}
	}
	for _, name := range s.Cluster.UnmanagedNodes() {
		// The Derive's sources can lag the Node store, so a Node it lists may have gone or become managed.
		if e, ok := s.NodeStore.Get(name); ok && Unmanaged(e.Object) && !yield(s.withNode(&StateNode{}, e, copyUsage)) {
			return
		}
	}
}

// nodeClaimNode builds the StateNode of a NodeClaim and the Node its entry names.
func (s *NodeState) nodeClaimNode(e NodeClaimEntry, copyUsage bool) *StateNode {
	n := &StateNode{NodeClaim: e.Object, intent: e.Intent}
	var node NodeEntry
	if e.Derived.Node != "" {
		node, _ = s.NodeStore.Get(e.Derived.Node)
	}
	return s.withNode(n, node, copyUsage)
}

// withNode sets the StateNode's Node and its usage from the Node's entry, which has a nil Node for a NodeClaim without
// one. copyUsage copies the host port usage, which the scheduler adds simulated pods to; the volume usage is always the
// StateNode's own.
func (s *NodeState) withNode(n *StateNode, e NodeEntry, copyUsage bool) *StateNode {
	n.Node, n.repairPolicyMatches = e.Object, e.Derived.RepairPolicyMatches
	u := e.Derived.Usage
	if e.Object == nil {
		u = NodeUsage{}
	}
	n.Derived.PodRequests, n.Derived.PodLimits = u.Requests, u.Limits
	n.Derived.DaemonSetRequests, n.Derived.DaemonSetLimits = u.DaemonSetRequests, u.DaemonSetLimits
	n.Derived.DisruptionCost = u.DisruptionCost
	n.Derived.HostPorts = lo.TernaryF(u.HostPorts == nil, scheduling.NewHostPortUsage, func() *scheduling.HostPortUsage {
		return lo.Ternary(copyUsage, u.HostPorts.DeepCopy(), u.HostPorts)
	})
	n.Derived.Volumes = u.volumeUsage(e.Derived)
	return n
}

// NodeClaimExists returns whether cluster state holds the NodeClaim.
func (s *NodeState) NodeClaimExists(nodeClaimName string) bool {
	return s.NodeClaimStore.Exists(nodeClaimName)
}

// HasUnlaunchedNodeClaims returns whether any NodeClaim is waiting on its launch to resolve its instance. A decision
// that counts the nodes in the cluster waits for none, since an unlaunched NodeClaim isn't a node in Nodes yet; watch
// UnlaunchedSource to run again once one launches.
func (s *NodeState) HasUnlaunchedNodeClaims() bool {
	return s.Cluster.Counts().UnlaunchedNodeClaims > 0
}

// IsNodeNominated returns whether the NodeClaim's node was the target of a pending pod in a recent scheduling pass.
func (s *NodeState) IsNodeNominated(nodeClaimName string) bool {
	e, ok := s.NodeClaimStore.Get(nodeClaimName)
	if !ok {
		return false
	}
	nominated, holds := e.Intent.Nomination.At(s.clock.Now())
	return nominated && holds
}

// HasBufferPods returns whether the NodeClaim's node has at least one virtual buffer pod placed on it during the last
// provisioning pass.
func (s *NodeState) HasBufferPods(nodeClaimName string) bool {
	e, _ := s.NodeClaimStore.Get(nodeClaimName)
	return e.Intent.BufferPods.Value > 0
}

// PVCDriverResolved returns whether cluster state holds the CSI driver of the PVC.
func (s *NodeState) PVCDriverResolved(key types.NamespacedName) bool { return s.PVCStore.Resolved(key) }

// FeedPVCDriver reports the CSI driver of the PVC: ResolvedPVCDriver("") for a PVC whose volume no driver limits, and
// the zero PVCDriver for a PVC that's gone, which drops it.
func (s *NodeState) FeedPVCDriver(key types.NamespacedName, driver PVCDriver) {
	s.PVCStore.Feed(key, driver)
}

// GetDaemonSetPod returns a copy of the DaemonSet's newest pod, or nil if cluster state holds none.
func (s *NodeState) GetDaemonSetPod(daemonset *appsv1.DaemonSet) *corev1.Pod {
	return s.DaemonSets.GetDaemonSetPod(daemonset)
}

// ForPodsWithAntiAffinity calls the supplied function once for each pod with required anti affinity terms that is
// currently bound to a tracked Node. The pod returned may not be up-to-date with respect to status, however since the
// anti-affinity terms can't be modified, they will be correct.
func (s *NodeState) ForPodsWithAntiAffinity(fn func(p *corev1.Pod, n *corev1.Node) bool) {
	for _, e := range s.NodeStore.List() {
		if len(e.Derived.Usage.AntiAffinityPods) == 0 || !Tracked(e.Object) {
			continue
		}
		for _, pod := range e.Derived.Usage.AntiAffinityPods {
			if !fn(pod, e.Object) {
				return
			}
		}
	}
}

// RepairPolicyMatcher returns the matcher cluster state matches Nodes with, or nil when node repair is disabled.
func (s *NodeState) RepairPolicyMatcher() *repair.RepairPolicyMatcher {
	return s.matcher
}

// Subscribe calls fn after each change to a node that may make consolidation possible: a node added or removed, or
// one whose initialization, deletion mark or pod count changes. fn must not block.
func (s *NodeState) Subscribe(fn func()) {
	s.NodeClaimStore.Subscribe(func(c state.Change[string, *v1.NodeClaim, NodeClaimDerived, NodeClaimIntent]) {
		if c.Existed != c.Exists || Launched(c.Old) != Launched(c.New) || c.Old.Intent.Deletion.Value != c.New.Intent.Deletion.Value ||
			(c.Existed && c.Exists && deleted(c.Old.Object) != deleted(c.New.Object)) {
			fn()
		}
	})
	s.NodeStore.Subscribe(func(c state.Change[string, *corev1.Node, NodeDerived, struct{}]) {
		if c.Existed != c.Exists || initialized(c.Old.Object) != initialized(c.New.Object) || c.Old.Derived.Usage.Pods != c.New.Derived.Usage.Pods {
			fn()
		}
	})
}

func initialized(node *corev1.Node) bool {
	return node != nil && node.Labels[v1.NodeInitializedLabelKey] == "true"
}
