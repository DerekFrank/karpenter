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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state"
)

// Cluster is the cluster as a whole: a synthetic singleton, built from its key while anything counts toward it. A
// Derive's zero key contributes to nothing, so the singleton's key is the one non-zero Cluster, TheCluster.
type Cluster struct{ name string }

// TheCluster is the key of the cluster's entry.
var TheCluster = Cluster{name: "cluster"}

// ClusterDerived are the cluster-wide counts, derived from the NodeClaim store.
type ClusterDerived struct {
	// UnlaunchedNodeClaims counts the NodeClaims without a providerID, deleting or not, which a decision that counts
	// the cluster's nodes waits on (NodeClaimsLaunched). It isn't a NodePool's NodeClaims, which split by deletion
	// rather than launch, nor NodePoolState.UnlaunchedNodeClaims, which leaves out the deleting ones.
	UnlaunchedNodeClaims int
}

// ClusterEntry is the cluster's entry.
type ClusterEntry = state.Entry[Cluster, ClusterDerived, struct{}]

// Clusters is the store of the cluster's one entry.
type Clusters struct {
	state.Reader[Cluster, Cluster, ClusterDerived, struct{}]
	store *state.Store[Cluster, Cluster, ClusterDerived, struct{}]
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

// derive keeps the cluster's counts from the NodeClaim store.
func (c *Clusters) derive(nodeClaims *NodeClaims) {
	state.DeriveObject(nodeClaims.Reader, c.store,
		func(_ string, nodeClaim *v1.NodeClaim) Cluster {
			return lo.Ternary(nodeClaim.Status.ProviderID == "", TheCluster, Cluster{})
		},
		func(string, *v1.NodeClaim) unlaunchedNodeClaim { return 1 })
}

// NodeClaimsLaunched returns whether every NodeClaim has launched, for a decision that counts the cluster's nodes,
// which waits until none is launching since an unlaunched NodeClaim has no capacity yet. A controller calling it
// declares the NodeClaim store with state.Requires and wakes on NodeClaims.UnlaunchedSource.
func (c *Clusters) NodeClaimsLaunched() bool { return c.Counts().UnlaunchedNodeClaims == 0 }
