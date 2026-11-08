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
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// nodePoolMembers index each NodePool's nodes and unlaunched NodeClaims, keyed by the NodePool's name, so a reader of
// one NodePool looks up its members instead of scanning every node.
type nodePoolMembers struct {
	// ProviderIDs are the keys in nodes of the nodes with a NodeClaim and the NodePool's label, fed by nodes. A node
	// stops counting once its NodeClaim leaves it or its label changes.
	ProviderIDs sets.Set[string]
	// Unlaunched are the NodePool's NodeClaims without a providerID, fed by nodeClaims. A NodeClaim stops counting once
	// it has a providerID or a deletionTimestamp.
	Unlaunched sets.Set[types.NamespacedName]
}

// nodePoolNode is a node's contribution to its NodePool's members: its providerID.
type nodePoolNode string

func (n nodePoolNode) Update(f nodePoolMembers, prev nodePoolNode) nodePoolMembers {
	f.ProviderIDs = f.ProviderIDs.Clone()
	if prev != "" {
		f.ProviderIDs.Delete(string(prev))
	}
	f.ProviderIDs.Insert(string(n))
	return f
}

func (n nodePoolNode) Delete(f nodePoolMembers) nodePoolMembers {
	f.ProviderIDs = f.ProviderIDs.Clone().Delete(string(n))
	if f.ProviderIDs.Len() == 0 {
		f.ProviderIDs = nil
	}
	return f
}

// unlaunchedNodeClaim is an unlaunched NodeClaim's contribution to its NodePool's members: its name.
type unlaunchedNodeClaim types.NamespacedName

func (u unlaunchedNodeClaim) Update(f nodePoolMembers, prev unlaunchedNodeClaim) nodePoolMembers {
	f.Unlaunched = f.Unlaunched.Clone()
	if prev != (unlaunchedNodeClaim{}) {
		f.Unlaunched.Delete(types.NamespacedName(prev))
	}
	f.Unlaunched.Insert(types.NamespacedName(u))
	return f
}

func (u unlaunchedNodeClaim) Delete(f nodePoolMembers) nodePoolMembers {
	f.Unlaunched = f.Unlaunched.Clone().Delete(types.NamespacedName(u))
	if f.Unlaunched.Len() == 0 {
		f.Unlaunched = nil
	}
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
	members, _ := c.nodePoolMembers.Get(nodePoolName)
	nodes := make(StateNodes, 0, members.ProviderIDs.Len())
	for _, providerID := range sets.List(members.ProviderIDs) {
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
	members, _ := c.nodePoolMembers.Get(nodePoolName)
	nodeClaims := make([]*v1.NodeClaim, 0, members.Unlaunched.Len())
	names := members.Unlaunched.UnsortedList()
	slices.SortFunc(names, func(a, b types.NamespacedName) int { return cmp.Compare(a.String(), b.String()) })
	for _, name := range names {
		if nodeClaim, ok := c.nodeClaims.Get(name); ok && nodePoolOfUnlaunched(name, nodeClaim) == nodePoolName {
			nodeClaims = append(nodeClaims, nodeClaim)
		}
	}
	return nodeClaims
}
