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
	"slices"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// LaunchedNodes returns the StateNodes of the NodePool's launched NodeClaims, sorted by providerID, read from the
// NodeClaim Derive's Sources rather than a scan of every node. They share their Node, NodeClaim and usage with cluster state,
// so they must not be modified.
func (n *NodePoolState) LaunchedNodes(nodePoolName string) StateNodes {
	names := n.members.Sources(nodePoolName)
	nodes := make(StateNodes, 0, len(names))
	for _, name := range names {
		// The Sources can lag the NodeClaims, so a NodeClaim they list may have left, moved or not launched.
		if e, ok := n.nodes.NodeClaimStore.Get(name); ok && Launched(e) && nodePoolOfNodeClaim(name, e) == nodePoolName {
			nodes = append(nodes, n.nodes.nodeClaimNode(e, false))
		}
	}
	slices.SortFunc(nodes, func(a, b *StateNode) int { return cmp.Compare(a.ProviderID(), b.ProviderID()) })
	return nodes
}

// UnlaunchedNodeClaims returns the NodePool's NodeClaims that have neither a providerID nor a deletionTimestamp, sorted
// by name. They are shared with cluster state, so they must not be modified.
func (n *NodePoolState) UnlaunchedNodeClaims(nodePoolName string) []*v1.NodeClaim {
	names := n.members.Sources(nodePoolName)
	slices.Sort(names)
	var nodeClaims []*v1.NodeClaim
	for _, name := range names {
		if e, ok := n.nodes.NodeClaimStore.Get(name); ok && unlaunched(e) && nodePoolOfNodeClaim(name, e) == nodePoolName {
			nodeClaims = append(nodeClaims, e.Object)
		}
	}
	return nodeClaims
}

func unlaunched(e NodeClaimEntry) bool {
	return e.Object != nil && e.Object.Status.ProviderID == "" && e.Object.DeletionTimestamp.IsZero()
}
