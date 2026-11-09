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
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/state"
)

// Cluster is the cluster as a whole: a synthetic singleton, built from its key while anything counts toward it. A
// Derive's zero key contributes to nothing, so the singleton's key is the one non-zero Cluster, TheCluster.
type Cluster struct{ name string }

// TheCluster is the key of the cluster's entry.
var TheCluster = Cluster{name: "cluster"}

// ClusterDerived are the cluster-wide counts, derived from the NodeClaim and Node stores.
type ClusterDerived struct {
	// LaunchedNodeClaims counts the NodeClaims with a providerID, which are nodes in Nodes.
	LaunchedNodeClaims int
	// UnlaunchedNodeClaims counts the NodeClaims without one, which a decision that counts the cluster's nodes waits
	// on.
	UnlaunchedNodeClaims int
	// UnmanagedNodes counts the Nodes with neither a NodeClaim owner reference nor the NodePool label. The Nodes
	// themselves are the unmanaged Derive's Sources.
	UnmanagedNodes int
	// Health counts the Nodes cluster state tracks, and those with a condition a repair policy matches.
	Health NodeCounts
}

// ClusterEntry is the cluster's entry.
type ClusterEntry = state.Entry[Cluster, ClusterDerived, struct{}]

// Clusters is the store of the cluster's one entry.
type Clusters struct {
	state.Reader[Cluster, Cluster, ClusterDerived, struct{}]
	store *state.Store[Cluster, Cluster, ClusterDerived, struct{}]
	// unmanaged is the Derive from the Node store, which knows the unmanaged Nodes.
	unmanaged *state.Derivation[string, Cluster, ClusterDerived, clusterCount[unmanagedNodes]]
}

func newClusters() *Clusters {
	store := state.NewSyntheticStore[Cluster, Cluster, ClusterDerived, struct{}](func(c Cluster) Cluster { return c })
	return &Clusters{Reader: store.Reader, store: store}
}

// Counts returns the cluster's counts.
func (c *Clusters) Counts() ClusterDerived {
	e, _ := c.Get(TheCluster)
	return e.Derived
}

// NodeCount is how many nodes Nodes returns: the launched NodeClaims, and the unmanaged Nodes.
func (c *Clusters) NodeCount() int {
	d := c.Counts()
	return d.LaunchedNodeClaims + d.UnmanagedNodes
}

// countField names the field of ClusterDerived a clusterCount adds to.
type countField interface {
	add(*ClusterDerived, int)
}

type (
	launchedNodeClaims   struct{}
	unlaunchedNodeClaims struct{}
	unmanagedNodes       struct{}
)

func (launchedNodeClaims) add(d *ClusterDerived, n int)   { d.LaunchedNodeClaims += n }
func (unlaunchedNodeClaims) add(d *ClusterDerived, n int) { d.UnlaunchedNodeClaims += n }
func (unmanagedNodes) add(d *ClusterDerived, n int)       { d.UnmanagedNodes += n }

// clusterCount is an object's contribution of one to a count of the cluster's.
type clusterCount[F countField] int

func (c clusterCount[F]) Update(d ClusterDerived, prev clusterCount[F]) ClusterDerived {
	var f F
	f.add(&d, int(c-prev))
	return d
}

func (c clusterCount[F]) Delete(d ClusterDerived) ClusterDerived {
	var f F
	f.add(&d, -int(c))
	return d
}

// clusterHealth is a tracked Node's contribution to the cluster's Health.
type clusterHealth nodeCount

func (n clusterHealth) Update(d ClusterDerived, prev clusterHealth) ClusterDerived {
	d.Health = nodeCount(n).Update(d.Health, nodeCount(prev))
	return d
}

func (n clusterHealth) Delete(d ClusterDerived) ClusterDerived {
	d.Health = nodeCount(n).Delete(d.Health)
	return d
}

// derive keeps the cluster's counts from the NodeClaim and Node stores.
func (c *Clusters) derive(nodeClaims *NodeClaims, nodes *Nodes, matcher *repair.RepairPolicyMatcher) {
	state.Derive(nodeClaims.Reader, c.store,
		func(_ string, e NodeClaimEntry) Cluster { return lo.Ternary(launched(e), TheCluster, Cluster{}) },
		func(string, NodeClaimEntry) clusterCount[launchedNodeClaims] { return 1 })
	state.Derive(nodeClaims.Reader, c.store,
		func(_ string, e NodeClaimEntry) Cluster { return lo.Ternary(!launched(e), TheCluster, Cluster{}) },
		func(string, NodeClaimEntry) clusterCount[unlaunchedNodeClaims] { return 1 })
	c.unmanaged = state.DeriveObject(nodes.Reader, c.store,
		func(_ string, node *corev1.Node) Cluster { return lo.Ternary(unmanaged(node), TheCluster, Cluster{}) },
		func(string, *corev1.Node) clusterCount[unmanagedNodes] { return 1 })
	state.DeriveObject(nodes.Reader, c.store,
		func(_ string, node *corev1.Node) Cluster { return lo.Ternary(Tracked(node), TheCluster, Cluster{}) },
		func(_ string, node *corev1.Node) clusterHealth {
			return clusterHealth(newNodeCount(matcher != nil && matcher.MatchesAny(node)))
		})
	c.Subscribe(func(state.Change[Cluster, Cluster, ClusterDerived, struct{}]) {
		ClusterStateNodesCount.Set(float64(c.NodeCount()), nil)
	})
}

// UnmanagedNodes returns the names of the unmanaged Nodes.
func (c *Clusters) UnmanagedNodes() []string { return c.unmanaged.Sources(TheCluster) }
