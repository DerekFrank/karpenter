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

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/state"
)

// ReservationTTL bounds how long a reservation holds against a NodePool for a NodeClaim that is never created or never counted, such
// as one whose caller dropped it, or whose create failed ambiguously.
const ReservationTTL = 2 * time.Minute

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

// GetNodeCount returns the NodePool's active and deleting NodeClaims, and the nodes reserved for NodeClaims the counts
// don't include yet.
func (n *NodePoolState) GetNodeCount(nodePoolName string) (active, deleting, reserved int) {
	f := n.Get(nodePoolName)
	return f.NodeClaims.Active, f.NodeClaims.Deleting, int(f.Reserved)
}

// ReserveNodeCount reserves up to wanted nodes for NodeClaims a caller is about to create, without the NodePool's
// active, deleting and reserved nodes exceeding limit, and returns a Reservation for each. A caller reserving against
// several NodePools reserves them in sorted name order and, on a refusal, releases every reservation it holds.
func (n *NodePoolState) ReserveNodeCount(nodePoolName string, limit int64, wanted int64) Reservations {
	return n.reservations.Reserve(nodePoolName, int(max(0, wanted)), 1, func(f NodePoolFields) bool {
		return int64(f.NodeClaims.Active+f.NodeClaims.Deleting)+f.Reserved <= limit
	})
}

// ReserveReplicas reserves nodes for a static NodePool's NodeClaims a caller is about to create, up to replicas less the
// NodePool's active and reserved nodes, and without its active, deleting and reserved nodes exceeding limit, and returns
// a Reservation for each. Each admit checks both bounds under the store's lock, so a node another controller reserves
// meanwhile, such as a disruption replacement, counts against replicas however the two interleave.
func (n *NodePoolState) ReserveReplicas(nodePoolName string, replicas, limit int64) Reservations {
	return n.reservations.Reserve(nodePoolName, int(max(0, replicas)), 1, func(f NodePoolFields) bool {
		return int64(f.NodeClaims.Active)+f.Reserved <= replicas && int64(f.NodeClaims.Active+f.NodeClaims.Deleting)+f.Reserved <= limit
	})
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

// ResourceReservation holds the most a NodeClaim could launch with, and one resources.Node, against its NodePool's
// resource limits for a NodeClaim a controller is about to create, until the NodePool's Usage counts the NodeClaim's
// launched capacity. Bind it to the NodeClaim once created, or Release it if it won't be.
type ResourceReservation = *state.Claim[string, string, reservedResources]

// ReserveResources holds value against the NodePool's resource limits for a NodeClaim a caller is about to create,
// without the NodePool's Usage and ReservedResources exceeding a limit, and returns its ResourceReservation, or nil if
// it doesn't fit. replacing names NodeClaims that count now and stop counting before the new one launches, such as the
// candidates a disruption command replaces, so what they count is credited. A caller reserving against several
// NodePools reserves them in sorted name order and, on a refusal, releases every reservation it holds.
func (n *NodePoolState) ReserveResources(nodePoolName string, limits, value corev1.ResourceList, replacing []string) ResourceReservation {
	claims := n.resourceReservations.ReserveReplacing(nodePoolName, 1, reservedResources(value), replacing, func(f NodePoolFields) bool {
		return withinLimits(limits, f.Usage, f.ReservedResources)
	})
	if len(claims) == 0 {
		return nil
	}
	return claims[0]
}

// ReservedResources returns what is held against the NodePool's resource limits for NodeClaims whose launched capacity
// Usage doesn't count yet (NodePoolFields.ReservedResources). It's shared; copy it to change it.
func (n *NodePoolState) ReservedResources(nodePoolName string) corev1.ResourceList {
	return n.Get(nodePoolName).ReservedResources
}

// withinLimits returns whether counted and held together are within every limit.
func withinLimits(limits, counted, held corev1.ResourceList) bool {
	for name, limit := range limits {
		used := counted[name].DeepCopy()
		used.Add(held[name])
		if used.Cmp(limit) > 0 {
			return false
		}
	}
	return true
}

// reservedResources are what a ResourceReservation holds against its NodePool's resource limits.
type reservedResources corev1.ResourceList

func (r reservedResources) Update(f NodePoolFields, prev reservedResources) NodePoolFields {
	f.ReservedResources = replaceResources(f.ReservedResources, corev1.ResourceList(r), corev1.ResourceList(prev))
	return f
}

func (r reservedResources) Delete(f NodePoolFields) NodePoolFields {
	f.ReservedResources = replaceResources(f.ReservedResources, nil, corev1.ResourceList(r))
	return f
}

func (r reservedResources) Equal(o reservedResources) bool {
	return resourcesEqual(corev1.ResourceList(r), corev1.ResourceList(o))
}
