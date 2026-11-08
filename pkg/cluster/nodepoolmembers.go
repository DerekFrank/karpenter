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
