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
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

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
// (nodeClaimContribution). The Node side comes from the Node store, keyed by each Node's NodePool label. The
// NodeClaim-side field that reads the NodeClaim's Node (Disruption.NotReady) reads it from the NodeClaim's own entry,
// whose Derived.Node the Node store contributes, so it sees a Node change one hop after NodeHealth does: for a moment
// they can disagree, and each converges.
type NodePoolFields struct {
	// Usage is the NodePool's status.resources: the status.capacity of each of its NodeClaims, launched or not, and
	// one resources.Node each. A NodeClaim stops counting once it has a deletionTimestamp or its DisruptionReason
	// condition is true, so that a NodePool's published usage drops as soon as a NodeClaim is on its way out.
	Usage corev1.ResourceList // bounded: resource names
	// NodeClaims counts the NodePool's NodeClaims, for its node limit and replicas. A NodeClaim is deleting once it has
	// a deletionTimestamp or its DisruptionReason condition is true, and active otherwise. It counts until it's gone,
	// launched or not, since each holds or will hold an instance against the limit. It isn't Disruption, which counts
	// only the nodes a budget protects.
	NodeClaims NodeClaimCounts
	// Disruption counts the NodePool's nodes for its disruption budgets: only launched NodeClaims that are initialized
	// or rebooting, until their instance terminates, disrupting by the deletion mark rather than DisruptionReason. It
	// isn't NodeClaims, which counts every NodeClaim against the node limit.
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

// NodePoolState holds each NodePool's entry, by name: the NodePool, its NodePoolFields and its NodePoolIntent, and the
// reservations against each NodePool's node and resource limits. Read NodePools through its Reader: Get and List
// return the NodePool, a deep copy unless client.UnsafeDisableDeepCopy, with its fields. Its fields count what counts
// toward a NodePool's name whether or not cluster state has seen the NodePool yet: Get returns them with false.
type NodePoolState struct {
	state.Reader[string, *v1.NodePool, NodePoolFields, NodePoolIntent]
	// store is the NodePool object store, which the NodeClaim and Node Derives feed.
	store *nodePoolStore
	// members is the Derive from the NodeClaim store, which the claims settle against.
	members      *state.Derivation[string, string, NodePoolFields, nodeClaimContribution]
	reservations *state.Claims[string, string, NodePoolFields, nodeClaimContribution, reservedNodes]
	// resourceReservations hold against each NodePool's resource limits, admitted against Usage.
	resourceReservations *state.Claims[string, string, NodePoolFields, nodeClaimContribution, reservedResources]
	nodeClaims           *NodeClaims
}

// NewNodePoolState returns the NodePools' entries, derived from the NodeClaim and Node stores. Its NodePool store
// follows from, the newest version of each NodePool. A matcher counts the Nodes whose conditions match a repair policy.
func NewNodePoolState(clk clock.Clock, from state.Newest, nodeClaims *NodeClaims, nodes *Nodes, matcher *repair.RepairPolicyMatcher) *NodePoolState {
	store := state.NewObjectStore[string, *v1.NodePool, NodePoolFields, NodePoolIntent](from, state.WithKey(state.ByName))
	members := state.Derive(nodeClaims.Reader, store, nodePoolOfNodeClaim, nodeClaimContributionOf)
	state.DeriveObject(nodes.Reader, store, nodePoolOfNode, func(_ string, node *corev1.Node) nodePoolNode {
		return nodePoolNode(newNodeCount(matcher != nil && matcher.MatchesAny(node)))
	})
	return &NodePoolState{
		Reader:  store.Reader,
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
		nodeClaims: nodeClaims,
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

// Fields returns the NodePool's derived fields, for a read of them alone.
func (n *NodePoolState) Fields(nodePoolName string) NodePoolFields {
	e, _ := n.Get(nodePoolName, client.UnsafeDisableDeepCopy)
	return e.Derived
}

// Clear empties the store in place, ending every reservation, so that its subscribers stay subscribed. It's for tests, between specs; storelint reports a Clear outside a test.
func (n *NodePoolState) Clear() { n.store.Clear() }

// nodeClaimContribution is a NodeClaim's contribution to its NodePool's fields. Each part is zero where
// the NodeClaim doesn't count toward the field it feeds.
type nodeClaimContribution struct {
	usage corev1.ResourceList
	// capacityCounted is whether usage counts the NodeClaim's launched capacity.
	capacityCounted bool
	counts          NodeClaimCounts
	disruption      NodePoolDisruption
}

// nodeClaimContributionOf returns a NodeClaim's contribution to its NodePool, which reads its linked Node from its
// own entry.
func nodeClaimContributionOf(_ string, e NodeClaimEntry) nodeClaimContribution {
	nodeClaim := e.Object
	var c nodeClaimContribution
	if !nodeClaim.DeletionTimestamp.IsZero() || conditionTrue(nodeClaim, v1.ConditionTypeDisruptionReason) {
		c.counts.Deleting = 1
	} else {
		c.counts.Active = 1
		c.usage = lo.Assign(nodeClaim.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")})
		c.capacityCounted = len(nodeClaim.Status.Capacity) > 0
	}
	if countsForDisruption(e) {
		c.disruption = nodeClaimDisruption(e, e.Derived.Node)
	}
	return c
}

func (c nodeClaimContribution) Update(f NodePoolFields, prev nodeClaimContribution) NodePoolFields {
	if !resourcesEqual(c.usage, prev.usage) {
		f.Usage = replaceResources(f.Usage, c.usage, prev.usage)
	}
	f.NodeClaims.Active += c.counts.Active - prev.counts.Active
	f.NodeClaims.Deleting += c.counts.Deleting - prev.counts.Deleting
	f.Disruption.Nodes += c.disruption.Nodes - prev.disruption.Nodes
	f.Disruption.Disrupting += c.disruption.Disrupting - prev.disruption.Disrupting
	f.Disruption.NotReady += c.disruption.NotReady - prev.disruption.NotReady
	return f
}

func (c nodeClaimContribution) Delete(f NodePoolFields) NodePoolFields {
	return nodeClaimContribution{}.Update(f, c)
}

func (c nodeClaimContribution) Equal(o nodeClaimContribution) bool {
	return c.counts == o.counts && c.capacityCounted == o.capacityCounted && c.disruption == o.disruption &&
		resourcesEqual(c.usage, o.usage)
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
