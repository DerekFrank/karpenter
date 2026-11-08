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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state"
)

// ReservationTTL bounds how long a reservation holds a node for a NodeClaim that is never created or never counted, such
// as one whose caller dropped it, or whose create failed ambiguously.
const ReservationTTL = 2 * time.Minute

// NodePoolFields are the fields Karpenter keeps on each NodePool.
type NodePoolFields struct {
	// NodeClaims counts the NodePool's NodeClaims. A NodeClaim is deleting once it has a deletionTimestamp or its
	// DisruptionReason condition is true, and active otherwise.
	NodeClaims NodeClaimCounts
	// Reserved is the number of nodes held against the NodePool's node limit for NodeClaims a controller is creating
	// and the NodeClaims don't count yet.
	Reserved int64
}

// NodeClaimCounts are a NodePool's NodeClaims by state. A NodeClaim contributes a count of one to its NodePool.
type NodeClaimCounts struct {
	Active   int
	Deleting int
}

func (c NodeClaimCounts) Update(f NodePoolFields, prev NodeClaimCounts) NodePoolFields {
	f.NodeClaims.Active += c.Active - prev.Active
	f.NodeClaims.Deleting += c.Deleting - prev.Deleting
	return f
}

func (c NodeClaimCounts) Delete(f NodePoolFields) NodePoolFields {
	f.NodeClaims.Active -= c.Active
	f.NodeClaims.Deleting -= c.Deleting
	return f
}

// reservedNodes are nodes a Reservation holds against its NodePool's node limit.
type reservedNodes int64

func (r reservedNodes) Update(f NodePoolFields, prev reservedNodes) NodePoolFields {
	f.Reserved += int64(r - prev)
	return f
}

func (r reservedNodes) Delete(f NodePoolFields) NodePoolFields {
	f.Reserved -= int64(r)
	return f
}

// Reservation holds one node against a NodePool's node limit for a NodeClaim a controller is about to create, until the
// NodePool's counts include that NodeClaim. Bind it to the NodeClaim once created, or Release it if it won't be.
type Reservation = *state.Claim[string, string, reservedNodes]

// Reservations are the Reservations one ReserveNodeCount granted.
type Reservations []Reservation

// Release releases every reservation.
func (r Reservations) Release() {
	for _, reservation := range r {
		reservation.Release()
	}
}

// NodePoolState holds each NodePool with its NodePoolFields.
type NodePoolState struct {
	nodePools    *state.Store[string, NodePoolFields]
	reservations *state.Claims[string, string, NodeClaimFields, NodePoolFields, NodeClaimCounts, reservedNodes]
}

// newNodePoolState returns the NodePools' state, derived from the NodeClaim store, keyed by NodeClaim name.
func newNodePoolState(clk clock.Clock, nodeClaims *state.Store[string, NodeClaimFields]) *NodePoolState {
	nodePools := state.NewStore[string, NodePoolFields]()
	counts := state.Derive(nodeClaims, nodePools,
		func(_ string, f NodeClaimFields) string {
			if f.NodeClaim == nil {
				return ""
			}
			return f.NodeClaim.Labels[v1.NodePoolLabelKey]
		},
		func(_ string, f NodeClaimFields) NodeClaimCounts {
			nodeClaim := f.NodeClaim
			if !nodeClaim.DeletionTimestamp.IsZero() {
				return NodeClaimCounts{Deleting: 1}
			}
			for i := range nodeClaim.Status.Conditions {
				if nodeClaim.Status.Conditions[i].Type == v1.ConditionTypeDisruptionReason && nodeClaim.Status.Conditions[i].Status == metav1.ConditionTrue {
					return NodeClaimCounts{Deleting: 1}
				}
			}
			return NodeClaimCounts{Active: 1}
		},
	)
	return &NodePoolState{
		nodePools: nodePools,
		// A NodeClaim counts against the node limit as soon as it exists, so the first time the counts see it settles
		// its reservation.
		reservations: state.NewClaims[string, string, NodeClaimFields, NodePoolFields, NodeClaimCounts, reservedNodes](counts,
			func(NodeClaimCounts) bool { return true },
			ReservationTTL, clk,
		),
	}
}

// GetNodeCount returns the NodePool's active and deleting NodeClaims, and the nodes reserved for NodeClaims the counts
// don't include yet.
func (n *NodePoolState) GetNodeCount(npName string) (active, deleting, reserved int) {
	np, _ := n.nodePools.Get(npName)
	return np.NodeClaims.Active, np.NodeClaims.Deleting, int(np.Reserved)
}

// ReserveNodeCount reserves up to wantedLimit nodes for NodeClaims a caller is about to create, without the NodePool's
// active, deleting and reserved nodes exceeding limit, and returns a Reservation for each.
func (n *NodePoolState) ReserveNodeCount(np string, limit int64, wantedLimit int64) Reservations {
	return n.reservations.Reserve(np, int(max(0, wantedLimit)), 1, func(f NodePoolFields) bool {
		return int64(f.NodeClaims.Active+f.NodeClaims.Deleting)+f.Reserved <= limit
	})
}
