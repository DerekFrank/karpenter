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

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// LaunchedNodes returns the entries of the NodePool's launched NodeClaims, sorted by providerID, read from
// NodeClaimsByNodePool rather than a scan of every node. Their objects are the store's, so they must not be modified.
func (n *NodePoolState) LaunchedNodes(nodePoolName string) []NodeClaimEntry {
	var nodes []NodeClaimEntry
	for _, e := range n.nodeClaims.byNodePool.List(nodePoolName, client.UnsafeDisableDeepCopy) {
		// An entry that changed since the Lookup may have left the NodePool.
		if Launched(e) && e.Object.Labels[v1.NodePoolLabelKey] == nodePoolName {
			nodes = append(nodes, e)
		}
	}
	slices.SortFunc(nodes, func(a, b NodeClaimEntry) int {
		return cmp.Compare(ViewOfNodeClaim(a).ProviderID(), ViewOfNodeClaim(b).ProviderID())
	})
	return nodes
}

// UnlaunchedNodeClaims returns the NodePool's NodeClaims that have neither a providerID nor a deletionTimestamp, sorted
// by name, read from NodeClaimsByNodePool. They are shared with cluster state, so they must not be modified.
func (n *NodePoolState) UnlaunchedNodeClaims(nodePoolName string) []*v1.NodeClaim {
	var nodeClaims []*v1.NodeClaim
	for _, e := range n.nodeClaims.byNodePool.List(nodePoolName, client.UnsafeDisableDeepCopy) {
		if unlaunched(e) && e.Object.Labels[v1.NodePoolLabelKey] == nodePoolName {
			nodeClaims = append(nodeClaims, e.Object)
		}
	}
	slices.SortFunc(nodeClaims, func(a, b *v1.NodeClaim) int { return cmp.Compare(a.Name, b.Name) })
	return nodeClaims
}

func unlaunched(e NodeClaimEntry) bool {
	return e.Object != nil && e.Object.Status.ProviderID == "" && e.Object.DeletionTimestamp.IsZero()
}
