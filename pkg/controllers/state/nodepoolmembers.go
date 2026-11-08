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

package state

import (
	"cmp"
	"slices"

	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// nodePoolMembers count each NodePool's nodes and unlaunched NodeClaims, keyed by the NodePool's name. The members
// themselves are the sources of the Derives that feed it (Cluster.poolNodes and Cluster.poolUnlaunched), which index them
// in place: a set of every member in the fields would be copied on every change, so a NodePool's fields would cost as
// much as its members to update.
type nodePoolMembers struct {
	// Nodes counts the nodes with a NodeClaim and the NodePool's label, fed by nodes. A node stops counting once its
	// NodeClaim leaves it or its label changes.
	Nodes int
	// Unlaunched counts the NodePool's NodeClaims without a providerID, fed by nodeClaims. A NodeClaim stops counting once
	// it has a providerID or a deletionTimestamp.
	Unlaunched int
}

// nodePoolNode is a node's contribution to its NodePool's members: one node.
type nodePoolNode int

func (n nodePoolNode) Update(f nodePoolMembers, prev nodePoolNode) nodePoolMembers {
	f.Nodes += int(n - prev)
	return f
}

func (n nodePoolNode) Delete(f nodePoolMembers) nodePoolMembers {
	f.Nodes -= int(n)
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

// nodePoolOfNode is the key a node's fields contribute to in nodePoolMembers: its NodePool, if it has a NodeClaim.
func nodePoolOfNode(_ string, f nodeFields) string {
	if f.NodeClaim == nil {
		return ""
	}
	return (&StateNode{Node: f.Node, NodeClaim: f.NodeClaim}).Labels()[v1.NodePoolLabelKey]
}

// nodePoolOfUnlaunched is the key a NodeClaim contributes to in nodePoolMembers: its NodePool, while it has neither a
// providerID nor a deletionTimestamp.
func nodePoolOfUnlaunched(_ types.NamespacedName, nodeClaim *v1.NodeClaim) string {
	if nodeClaim.Status.ProviderID != "" || !nodeClaim.DeletionTimestamp.IsZero() {
		return ""
	}
	return nodeClaim.Labels[v1.NodePoolLabelKey]
}

// NodePoolNodes returns the StateNodes in the NodePool that have a NodeClaim, sorted by providerID, read from the
// NodePool's index rather than a scan of every node. They share their Node, NodeClaim and usage with cluster state, so
// they must not be modified.
func (c *Cluster) NodePoolNodes(nodePoolName string) StateNodes {
	providerIDs := c.poolNodes.Sources(nodePoolName)
	slices.Sort(providerIDs)
	nodes := make(StateNodes, 0, len(providerIDs))
	for _, providerID := range providerIDs {
		// The index can lag nodes, so a node it lists may have left or moved.
		if f, ok := c.nodes.Get(providerID); ok && nodePoolOfNode(providerID, f) == nodePoolName {
			nodes = append(nodes, c.stateNode(f, false))
		}
	}
	return nodes
}

// UnlaunchedNodeClaims returns the NodePool's NodeClaims that have neither a providerID nor a deletionTimestamp, sorted
// by name. They are shared with cluster state, so they must not be modified.
func (c *Cluster) UnlaunchedNodeClaims(nodePoolName string) []*v1.NodeClaim {
	names := c.poolUnlaunched.Sources(nodePoolName)
	nodeClaims := make([]*v1.NodeClaim, 0, len(names))
	slices.SortFunc(names, func(a, b types.NamespacedName) int { return cmp.Compare(a.String(), b.String()) })
	for _, name := range names {
		if nodeClaim, ok := c.nodeClaims.Get(name); ok && nodePoolOfUnlaunched(name, nodeClaim) == nodePoolName {
			nodeClaims = append(nodeClaims, nodeClaim)
		}
	}
	return nodeClaims
}
