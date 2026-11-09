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

	"sigs.k8s.io/karpenter/pkg/state"
)

// Cluster is the cluster as a whole: a synthetic singleton, built from its key while anything counts toward it. A
// Derive's zero key contributes to nothing, so the singleton's key is the one non-zero Cluster, TheCluster.
type Cluster struct{ name string }

// TheCluster is the key of the cluster's entry.
var TheCluster = Cluster{name: "cluster"}

// ClusterDerived are the cluster-wide counts, derived from the NodeClaim and Node stores.
type ClusterDerived struct {
	// UnlaunchedNodeClaims counts the NodeClaims without a providerID, deleting or not, which a decision that counts
	// the cluster's nodes waits on (HasUnlaunchedNodeClaims). It isn't a NodePool's NodeClaims, which split by deletion
	// rather than launch, nor NodePoolState.UnlaunchedNodeClaims, which leaves out the deleting ones.
	UnlaunchedNodeClaims int
}

// ClusterEntry is the cluster's entry.
type ClusterEntry = state.Entry[Cluster, ClusterDerived, struct{}]

// Clusters is the store of the cluster's one entry.
type Clusters struct {
	state.Reader[Cluster, Cluster, ClusterDerived, struct{}]
	store *state.Store[Cluster, Cluster, ClusterDerived, struct{}]
	// unmanaged is the Derive from the Node store whose Sources are the unmanaged Nodes.
	unmanaged *state.Derivation[string, Cluster, ClusterDerived, unmanagedNode]
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

// unlaunchedNodeClaim is an unlaunched NodeClaim's contribution of one to the cluster's UnlaunchedNodeClaims.
type unlaunchedNodeClaim int

func (n unlaunchedNodeClaim) Update(d ClusterDerived, prev unlaunchedNodeClaim) ClusterDerived {
	d.UnlaunchedNodeClaims += int(n - prev)
	return d
}

func (n unlaunchedNodeClaim) Delete(d ClusterDerived) ClusterDerived {
	d.UnlaunchedNodeClaims -= int(n)
	return d
}

// unmanagedNode is an unmanaged Node's contribution to the cluster's entry: nothing but its place among the unmanaged
// Derive's Sources, which UnmanagedNodes reads.
type unmanagedNode struct{}

func (unmanagedNode) Update(d ClusterDerived, _ unmanagedNode) ClusterDerived { return d }

func (unmanagedNode) Delete(d ClusterDerived) ClusterDerived { return d }

// derive keeps the cluster's counts, and the index of unmanaged Nodes, from the NodeClaim and Node stores.
func (c *Clusters) derive(nodeClaims *NodeClaims, nodes *Nodes) {
	state.Derive(nodeClaims.Reader, c.store,
		func(_ string, e NodeClaimEntry) Cluster { return lo.Ternary(!Launched(e), TheCluster, Cluster{}) },
		func(string, NodeClaimEntry) unlaunchedNodeClaim { return 1 })
	c.unmanaged = state.DeriveObject(nodes.Reader, c.store,
		func(_ string, node *corev1.Node) Cluster { return lo.Ternary(Unmanaged(node), TheCluster, Cluster{}) },
		func(string, *corev1.Node) unmanagedNode { return unmanagedNode{} })
}

// UnmanagedNodes returns the names of the unmanaged Nodes.
func (c *Clusters) UnmanagedNodes() []string { return c.unmanaged.Sources(TheCluster) }
