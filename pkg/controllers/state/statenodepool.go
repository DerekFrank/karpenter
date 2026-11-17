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
	"context"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// ReservationTTL bounds how long a reservation holds a node for a NodeClaim that is never created or never counted,
// such as one whose create failed ambiguously.
const ReservationTTL = 2 * time.Minute

// goneTTL is how long a removed NodeClaim is remembered, so that a late or stale version can't bring it back.
const goneTTL = 2 * ReservationTTL

// NodePoolState counts each NodePool's NodeClaims and the nodes reserved for NodeClaims being created, under one lock,
// so that a reservation is admitted atomically with the counts it checks.
type NodePoolState struct {
	mu    sync.Mutex
	clock clock.Clock

	nodeClaims   map[string]nodeClaimRecord // NodeClaim name -> record
	gone         map[string]goneNodeClaim   // NodeClaim name -> removed NodeClaim
	counts       map[string]nodePoolCounts  // NodePool name -> counts
	reservations map[*Reservation]struct{}
	bound        map[string]*Reservation // NodeClaim name -> reservation bound to it
	subscribers  []subscriber
}

// nodePoolCounts counts a NodePool's NodeClaims until they're gone, launched or not. A NodeClaim is deleting once it has
// a deletionTimestamp or its DisruptionReason condition is true, and active otherwise.
type nodePoolCounts struct {
	active, deleting, reserved int
}

type nodeClaimRecord struct {
	nodePool        string
	uid             types.UID
	resourceVersion string
	deleting        bool
	// deleted is whether a version with a deletionTimestamp was applied, which no later version can undo.
	deleted bool
}

type goneNodeClaim struct {
	uid types.UID // empty when the NodeClaim was removed before it was applied
	at  time.Time
}

type subscriber struct {
	ctx context.Context
	q   workqueue.TypedRateLimitingInterface[reconcile.Request]
}

// Reservation holds one node against a NodePool for a NodeClaim a controller is about to create. Bind it to the
// NodeClaim once created, or Release it if it won't be. It ends exactly once: when its NodeClaim is counted or removed,
// when it's released, or when ReservationTTL passes. A nil Reservation does nothing.
type Reservation struct {
	state     *NodePoolState
	nodePool  string
	nodeClaim string
	expires   time.Time
	done      bool
}

// Reservations are the reservations one call granted.
type Reservations []*Reservation

func (r Reservations) Release() {
	for _, reservation := range r {
		reservation.Release()
	}
}

func NewNodePoolState(clk clock.Clock) *NodePoolState {
	n := &NodePoolState{clock: clk}
	n.Reset()
	return n
}

// Reset empties the state, ending every reservation.
func (n *NodePoolState) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for r := range n.reservations {
		r.done = true
	}
	n.nodeClaims = map[string]nodeClaimRecord{}
	n.gone = map[string]goneNodeClaim{}
	n.counts = map[string]nodePoolCounts{}
	n.reservations = map[*Reservation]struct{}{}
	n.bound = map[string]*Reservation{}
}

// GetNodeCount returns the NodePool's active and deleting NodeClaims, and its reserved nodes.
func (n *NodePoolState) GetNodeCount(nodePoolName string) (active, deleting, reserved int) {
	n.lock()
	defer n.mu.Unlock()
	c := n.counts[nodePoolName]
	return c.active, c.deleting, c.reserved
}

// ReserveNodeCount reserves up to wanted nodes without the NodePool's active, deleting and reserved nodes exceeding
// limit. A caller reserving against several NodePools reserves them in sorted name order and, on a refusal, releases
// every reservation it holds.
func (n *NodePoolState) ReserveNodeCount(nodePoolName string, limit int64, wanted int64) Reservations {
	return n.reserve(nodePoolName, wanted, func(c nodePoolCounts) bool {
		return int64(c.active+c.deleting+c.reserved) <= limit
	})
}

// ReserveReplicas reserves nodes up to replicas less the NodePool's active and reserved nodes, without its active,
// deleting and reserved nodes exceeding limit. A node another controller reserves meanwhile, such as a disruption
// replacement, counts against replicas.
func (n *NodePoolState) ReserveReplicas(nodePoolName string, replicas, limit int64) Reservations {
	return n.reserve(nodePoolName, replicas, func(c nodePoolCounts) bool {
		return int64(c.active+c.reserved) <= replicas && int64(c.active+c.deleting+c.reserved) <= limit
	})
}

// reserve grants reservations one at a time while admit accepts the counts with the reservation held.
func (n *NodePoolState) reserve(nodePoolName string, wanted int64, admit func(nodePoolCounts) bool) Reservations {
	n.lock()
	defer n.mu.Unlock()
	var reservations Reservations
	for range max(0, wanted) {
		c := n.counts[nodePoolName]
		c.reserved++
		if !admit(c) {
			break
		}
		n.set(nodePoolName, c)
		r := &Reservation{state: n, nodePool: nodePoolName, expires: n.clock.Now().Add(ReservationTTL)}
		n.reservations[r] = struct{}{}
		reservations = append(reservations, r)
	}
	return reservations
}

// Apply counts a version of a NodeClaim, and ends the reservation bound to it. A version older than the one applied,
// one that drops an applied deletionTimestamp, or one of a removed NodeClaim, is ignored.
func (n *NodePoolState) Apply(nodeClaim *v1.NodeClaim) {
	n.lock()
	defer n.mu.Unlock()
	if g, ok := n.gone[nodeClaim.Name]; ok && (g.uid == "" || g.uid == nodeClaim.UID) {
		return
	}
	prev, seen := n.nodeClaims[nodeClaim.Name]
	if seen && prev.uid == nodeClaim.UID {
		cmp, err := resourceversion.CompareResourceVersion(nodeClaim.ResourceVersion, prev.resourceVersion)
		if (err == nil && cmp < 0) || (prev.deleted && nodeClaim.DeletionTimestamp.IsZero()) {
			return
		}
	}
	if seen {
		n.count(prev, -1)
		delete(n.nodeClaims, nodeClaim.Name)
	}
	if nodePool := nodeClaim.Labels[v1.NodePoolLabelKey]; nodePool != "" {
		deleted := !nodeClaim.DeletionTimestamp.IsZero()
		record := nodeClaimRecord{nodePool: nodePool, uid: nodeClaim.UID, resourceVersion: nodeClaim.ResourceVersion,
			deleting: deleted || conditionTrue(nodeClaim, v1.ConditionTypeDisruptionReason), deleted: deleted}
		n.nodeClaims[nodeClaim.Name] = record
		n.count(record, 1)
	}
	if r, ok := n.bound[nodeClaim.Name]; ok {
		n.release(r)
	}
}

// Remove uncounts the named NodeClaim, ends the reservation bound to it, and remembers it as gone.
func (n *NodePoolState) Remove(nodeClaimName string) {
	n.lock()
	defer n.mu.Unlock()
	prev, ok := n.nodeClaims[nodeClaimName]
	if ok {
		n.count(prev, -1)
		delete(n.nodeClaims, nodeClaimName)
	}
	n.gone[nodeClaimName] = goneNodeClaim{uid: prev.uid, at: n.clock.Now()}
	if r, ok := n.bound[nodeClaimName]; ok {
		n.release(r)
	}
}

// Bind ties the reservation to the NodeClaim created for it. It ends when the NodePool counts that NodeClaim, at once
// if it already does, or when the NodeClaim is removed first.
func (r *Reservation) Bind(nodeClaimName string) {
	if r == nil {
		return
	}
	n := r.state
	n.lock()
	defer n.mu.Unlock()
	if r.done {
		return
	}
	_, counted := n.nodeClaims[nodeClaimName]
	if _, gone := n.gone[nodeClaimName]; counted || gone {
		n.release(r)
		return
	}
	if other, ok := n.bound[nodeClaimName]; ok && other != r {
		n.release(other)
	}
	r.nodeClaim = nodeClaimName
	n.bound[nodeClaimName] = r
}

// Release ends the reservation, if it hasn't ended.
func (r *Reservation) Release() {
	if r == nil {
		return
	}
	n := r.state
	n.lock()
	defer n.mu.Unlock()
	n.release(r)
}

// Source enqueues each NodePool, by name, whose counts change.
func (n *NodePoolState) Source() source.Source {
	return source.Func(func(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.subscribers = append(n.subscribers, subscriber{ctx: ctx, q: q})
		return nil
	})
}

func (n *NodePoolState) release(r *Reservation) {
	if r.done {
		return
	}
	r.done = true
	delete(n.reservations, r)
	if n.bound[r.nodeClaim] == r {
		delete(n.bound, r.nodeClaim)
	}
	c := n.counts[r.nodePool]
	c.reserved--
	n.set(r.nodePool, c)
}

func (n *NodePoolState) count(record nodeClaimRecord, delta int) {
	c := n.counts[record.nodePool]
	if record.deleting {
		c.deleting += delta
	} else {
		c.active += delta
	}
	n.set(record.nodePool, c)
}

// set stores the NodePool's counts and enqueues the NodePool for each subscriber. Enqueueing doesn't block.
func (n *NodePoolState) set(nodePoolName string, c nodePoolCounts) {
	if c == (nodePoolCounts{}) {
		delete(n.counts, nodePoolName)
	} else {
		n.counts[nodePoolName] = c
	}
	n.subscribers = slices.DeleteFunc(n.subscribers, func(s subscriber) bool { return s.ctx.Err() != nil })
	for _, s := range n.subscribers {
		s.q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Name: nodePoolName}})
	}
}

// lock takes the lock and expires reservations past their TTL and gone NodeClaims past goneTTL. The static
// controllers' periodic requeues call in at least every minute.
func (n *NodePoolState) lock() {
	n.mu.Lock()
	now := n.clock.Now()
	for r := range n.reservations {
		if !now.Before(r.expires) {
			n.release(r)
		}
	}
	for name, g := range n.gone {
		if now.Sub(g.at) >= goneTTL {
			delete(n.gone, name)
		}
	}
}

// conditionTrue reads a condition without StatusConditions(), which may mutate an object shared with the informer cache.
func conditionTrue(nodeClaim *v1.NodeClaim, conditionType string) bool {
	for i := range nodeClaim.Status.Conditions {
		if nodeClaim.Status.Conditions[i].Type == conditionType {
			return nodeClaim.Status.Conditions[i].IsTrue()
		}
	}
	return false
}
