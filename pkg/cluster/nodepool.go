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
	"maps"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// NodePool is a NodePool with the fields Karpenter derives and the intent it keeps about it.
type NodePool = state.View[*v1.NodePool, NodePoolFields, NodePoolIntent]

// NodePoolEntry is a NodePool's entry.
type NodePoolEntry = state.Entry[*v1.NodePool, NodePoolFields, NodePoolIntent]

// NodePoolIntent is what Karpenter decided about a NodePool. It drops with the NodePool.
type NodePoolIntent struct {
	// Registration is the NodePool's recent launch outcomes, recorded by the NodeClaim lifecycle controller through the
	// Registrations handle.
	Registration state.Intended[Registration]
}

type nodePoolStore = state.Store[string, *v1.NodePool, NodePoolFields, NodePoolIntent]

// NodePoolFields are the fields derived about each NodePool, on its entry, so that a reservation against the NodePool's
// node limit is admitted atomically with the counts it checks.
//
// The NodeClaim side comes from the NodeClaim store, through one Derive keyed by each NodeClaim's NodePool label
// (nodeClaimContribution), and one for Capacity keyed by its node's (nodePoolCapacity). The Node side comes from the
// Node store, keyed by each Node's NodePool label. The NodeClaim-side fields that read the NodeClaim's Node (Capacity,
// Disruption.NotReady) read it from the Node store, which Touches the NodeClaim when its Node changes, so they see a
// Node change one hop after NodeHealth does: for a moment they can disagree, and each converges.
type NodePoolFields struct {
	// Usage is the NodePool's status.resources: the status.capacity of each of its NodeClaims, launched or not, and
	// one resources.Node each. A NodeClaim stops counting once it has a deletionTimestamp or its DisruptionReason
	// condition is true, so that a NodePool's published usage drops as soon as a NodeClaim is on its way out.
	Usage corev1.ResourceList // bounded: resource names
	// Capacity is what counts against the NodePool's spec.limits: the capacity of each launched NodeClaim's node, as
	// StateNode.Capacity reads it (the Node's once it's initialized, the NodeClaim's before), and one resources.Node
	// each. It differs from Usage on purpose: it counts only launched NodeClaims, whose capacity is known; it keeps a
	// NodeClaim whose DisruptionReason is set but that isn't marked yet, since its node still runs; and it drops a node
	// as soon as the disruption queue marks it for deletion or its instance is terminating, so that a replacement for
	// it fits under the limit. It counts toward its node's NodePool label, which is the Node's once it registers, so a
	// registered node relabeled into another NodePool keeps counting where its Node says it runs.
	Capacity corev1.ResourceList // bounded: resource names
	// NodeClaims counts the NodePool's NodeClaims, for its node limit and replicas. A NodeClaim is deleting once it has
	// a deletionTimestamp or its DisruptionReason condition is true, and active otherwise. It counts until it's gone.
	NodeClaims NodeClaimCounts
	// Members counts the NodePool's launched and unlaunched NodeClaims. The members themselves are the sources of the
	// NodeClaim Derive, read with LaunchedNodes and UnlaunchedNodeClaims.
	Members NodePoolMembers
	// Disruption counts the NodePool's nodes for its disruption budgets.
	Disruption NodePoolDisruption
	// NodeHealth counts the NodePool's tracked Nodes by their NodePool label, and those with a condition that matches
	// a repair policy, fed by the Node store. A Node counts once cluster state tracks it, and until it's gone.
	NodeHealth NodeCounts
	// Reserved is the number of nodes held against the NodePool's node limit for NodeClaims a controller is creating
	// and NodeClaims doesn't count yet, held by claims.
	Reserved int64
	// ReservedResources is what is held against the NodePool's resource limits for NodeClaims a controller is creating
	// and whose launched capacity Usage doesn't count yet: the most each could launch with, and one resources.Node,
	// held by claims.
	ReservedResources corev1.ResourceList // bounded: resource names
}

// NodeClaimCounts are a NodePool's NodeClaims by state.
type NodeClaimCounts struct {
	Active   int
	Deleting int
}

// NodePools is the state accessor for NodePools: read NodePools only through it.
type NodePools = state.Accessor[*v1.NodePool, *v1.NodePoolList, NodePoolFields, NodePoolIntent]

// NewNodePools returns the state accessor for NodePools, reading them through kubeClient with their entries' fields.
// As a source, it reconciles a NodePool whenever its entry changes, once it's synced.
func NewNodePools(kubeClient client.Reader, nodePools *NodePoolState) *NodePools {
	return state.NewAccessor(kubeClient,
		func() *v1.NodePool { return &v1.NodePool{} },
		func() *v1.NodePoolList { return &v1.NodePoolList{} },
		func(l *v1.NodePoolList) []*v1.NodePool {
			items := make([]*v1.NodePool, len(l.Items))
			for i := range l.Items {
				items[i] = &l.Items[i]
			}
			return items
		},
		func(np *v1.NodePool) (NodePoolFields, NodePoolIntent) {
			e, _ := nodePools.store.Get(np.Name)
			return e.Derived, e.Intent
		},
		nodePools.store.Source())
}

// NodePoolState holds each NodePool's entry, by name: the NodePool, its NodePoolFields and its NodePoolIntent, and the
// reservations against each NodePool's node and resource limits.
type NodePoolState struct {
	// store is the NodePool object store, which the NodeClaim and Node Derives feed.
	store *nodePoolStore
	// members is the Derive from the NodeClaim store, which knows each NodePool's NodeClaims.
	members      *state.Derivation[string, string, NodePoolFields, nodeClaimContribution]
	reservations *state.Claims[string, string, NodePoolFields, nodeClaimContribution, reservedNodes]
	// resourceReservations hold against each NodePool's resource limits, admitted against Usage.
	resourceReservations *state.Claims[string, string, NodePoolFields, nodeClaimContribution, reservedResources]
	nodes                *NodeState
}

// NewNodePoolState returns the NodePools' entries, derived from the node state's NodeClaim and Node stores. With a
// kubeClient and informers, its NodePool store follows the API server once State.Register starts it; with nil, a
// caller feeds it (Observe, Forget).
func NewNodePoolState(clk clock.Clock, kubeClient client.Client, informers cache.Informers, nodes *NodeState) *NodePoolState {
	store := state.NewObjectStoreBy[string, *v1.NodePool, NodePoolFields, NodePoolIntent](kubeClient, informers, func(np *v1.NodePool) string { return np.Name })
	members := state.Derive(nodes.NodeClaimStore.Reader, store, nodePoolOfNodeClaim, nodeClaimContributionOf(nodes.NodeStore))
	state.Derive(nodes.NodeClaimStore.Reader, store, nodePoolOfCapacity(nodes.NodeStore), capacityOf(nodes.NodeStore))
	state.DeriveObject(nodes.NodeStore.Reader, store, nodePoolOfNode, func(_ string, node *corev1.Node) nodePoolNode {
		return nodePoolNode(newNodeCount(nodes.matcher != nil && nodes.matcher.MatchesAny(node)))
	})
	return &NodePoolState{
		store:   store,
		members: members,
		// A NodeClaim counts against the node limit as soon as it exists, so the first time the counts see it settles
		// its reservation.
		reservations: state.NewClaims[string, string, NodePoolFields, nodeClaimContribution, reservedNodes](members,
			func(nodeClaimContribution) bool { return true },
			ReservationTTL, clk,
		).OnExpire(countExpiry[string, reservedNodes](metrics.NodePoolClaimStore)),
		// A NodeClaim's Usage counts what a resource reservation held for it once it counts the NodeClaim's launched
		// capacity, or never will once the NodeClaim is deleting. Until then the reservation's resources.Node counts
		// twice, held and counted, which only refuses a later admission.
		resourceReservations: state.NewClaims[string, string, NodePoolFields, nodeClaimContribution, reservedResources](members,
			func(c nodeClaimContribution) bool { return c.capacityCounted || c.counts.Deleting > 0 },
			ReservationTTL, clk,
		).OnExpire(countExpiry[string, reservedResources](metrics.NodePoolClaimStore)),
		nodes: nodes,
	}
}

// nodePoolOfNodeClaim is the NodePool a NodeClaim counts toward: its NodePool label, for as long as it exists. Each
// field of its contribution decides whether it counts there.
func nodePoolOfNodeClaim(_ string, e NodeClaimEntry) string {
	return e.Object.Labels[v1.NodePoolLabelKey]
}

// nodePoolOfNode is the NodePool a tracked Node with a NodePool label counts toward.
func nodePoolOfNode(_ string, node *corev1.Node) string {
	if !Tracked(node) {
		return ""
	}
	return node.Labels[v1.NodePoolLabelKey]
}

// Get returns the NodePool's derived fields. They count what counts toward the NodePool's name whether or not cluster
// state has seen the NodePool yet.
func (n *NodePoolState) Get(nodePoolName string) NodePoolFields {
	e, _ := n.store.Get(nodePoolName)
	return e.Derived
}

// Source is the NodePools' entries as a source, which enqueues each NodePool whose entry changes.
func (n *NodePoolState) Source() source.SyncingSource { return n.store.Source() }

// SourceOn is the NodePools' entries as a source narrowed to what on returns, which enqueues each NodePool that
// appears, goes, or whose on changes, for a controller that acts on part of the entry.
func (n *NodePoolState) SourceOn(on func(string, NodePoolEntry) any) source.SyncingSource {
	return n.store.SourceOn(on)
}

// Subscribe calls fn with each change of a NodePool's entry, and returns a func that stops it. fn must not block.
func (n *NodePoolState) Subscribe(fn func(state.Change[string, *v1.NodePool, NodePoolFields, NodePoolIntent])) func() {
	return n.store.Subscribe(fn)
}

// Synced returns whether the fields count what the NodeClaim and Node stores held when they synced, and the NodePool
// store has applied its first List.
func (n *NodePoolState) Synced() bool { return n.store.Synced() }

// Clear empties the store in place, ending every reservation, so that its subscribers stay subscribed.
func (n *NodePoolState) Clear() { n.store.Clear() }

// Observe applies a version of a NodePool, for a caller that feeds the NodePool store.
func (n *NodePoolState) Observe(nodePool *v1.NodePool) { n.store.Observe(nodePool) }

// Forget drops the NodePool named nodePoolName, for a caller that feeds the NodePool store.
func (n *NodePoolState) Forget(nodePoolName string) {
	state.Forget(n.store, types.NamespacedName{Name: nodePoolName})
}

// nodeClaimContribution is a NodeClaim's contribution to its NodePool's fields but Capacity. Each part is zero where
// the NodeClaim doesn't count toward the field it feeds.
type nodeClaimContribution struct {
	usage corev1.ResourceList
	// capacityCounted is whether usage counts the NodeClaim's launched capacity.
	capacityCounted bool
	counts          NodeClaimCounts
	members         NodePoolMembers
	disruption      NodePoolDisruption
}

// nodeClaimContributionOf returns a NodeClaim's contribution to its NodePool, which reads its linked Node from nodes.
func nodeClaimContributionOf(nodes *Nodes) func(string, NodeClaimEntry) nodeClaimContribution {
	return func(_ string, e NodeClaimEntry) nodeClaimContribution {
		nodeClaim := e.Object
		var c nodeClaimContribution
		if !nodeClaim.DeletionTimestamp.IsZero() || conditionTrue(nodeClaim, v1.ConditionTypeDisruptionReason) {
			c.counts.Deleting = 1
		} else {
			c.counts.Active = 1
			c.usage = lo.Assign(nodeClaim.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")})
			c.capacityCounted = len(nodeClaim.Status.Capacity) > 0
		}
		switch {
		case launched(e):
			c.members.Launched = 1
		case unlaunched(e):
			c.members.Unlaunched = 1
		}
		if countsForDisruption(e) {
			c.disruption = nodeClaimDisruption(e, nodes.linked(e))
		}
		return c
	}
}

func (c nodeClaimContribution) Update(f NodePoolFields, prev nodeClaimContribution) NodePoolFields {
	if !resourcesEqual(c.usage, prev.usage) {
		f.Usage = replaceResources(f.Usage, c.usage, prev.usage)
	}
	f.NodeClaims.Active += c.counts.Active - prev.counts.Active
	f.NodeClaims.Deleting += c.counts.Deleting - prev.counts.Deleting
	f.Members.Launched += c.members.Launched - prev.members.Launched
	f.Members.Unlaunched += c.members.Unlaunched - prev.members.Unlaunched
	f.Disruption.Nodes += c.disruption.Nodes - prev.disruption.Nodes
	f.Disruption.Disrupting += c.disruption.Disrupting - prev.disruption.Disrupting
	f.Disruption.NotReady += c.disruption.NotReady - prev.disruption.NotReady
	return f
}

func (c nodeClaimContribution) Delete(f NodePoolFields) NodePoolFields {
	return nodeClaimContribution{}.Update(f, c)
}

func (c nodeClaimContribution) Equal(o nodeClaimContribution) bool {
	return c.counts == o.counts && c.capacityCounted == o.capacityCounted && c.members == o.members && c.disruption == o.disruption &&
		resourcesEqual(c.usage, o.usage)
}

// linked returns the Node the NodeClaim's entry names, or nil for none.
func (n *Nodes) linked(e NodeClaimEntry) *corev1.Node {
	if e.Derived.Node == "" {
		return nil
	}
	node, _ := n.Get(e.Derived.Node)
	return node.Object
}

// replaceResources returns total with add in place of sub, without the resources it zeroes, in a new list.
func replaceResources(total, add, sub corev1.ResourceList) corev1.ResourceList {
	sum := resources.Merge(total, add)
	resources.SubtractFrom(sum, sub)
	return nonzero(sum)
}

// nonzero drops usage's zero quantities, and returns nil for usage that is all zero.
func nonzero(usage corev1.ResourceList) corev1.ResourceList {
	usage = lo.OmitBy(usage, func(_ corev1.ResourceName, q resource.Quantity) bool { return q.IsZero() })
	if len(usage) == 0 {
		return nil
	}
	return usage
}

func resourcesEqual(a, b corev1.ResourceList) bool {
	return maps.EqualFunc(a, b, func(x, y resource.Quantity) bool { return x.Cmp(y) == 0 })
}
