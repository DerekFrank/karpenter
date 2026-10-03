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
	"sync"
	"sync/atomic"

	"github.com/samber/lo"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// nodeClaimClass is the accounting class of a NodeClaim. It is always derived from a nodeClaimRecord and never stored.
type nodeClaimClass int

const (
	classActive nodeClaimClass = iota
	classPendingDisruption
	classDeleting
)

// nodeClaimRecord holds what NodePoolState knows about one NodeClaim. Observed fields are written only by Observe;
// intents are written only by the imperative callers that own them. Each writer sets its own input and the class is
// computed from all of them together, so the order in which writers run doesn't change the result.
type nodeClaimRecord struct {
	nodePool string

	// deletionObserved is true when the observed NodeClaim has a DeletionTimestamp or is InstanceTerminating, the same
	// rule StateNode.Deleted uses. Unlike StateNode, it also applies to NodeClaims that haven't launched yet.
	deletionObserved bool
	// disruptingObserved is true when the observed NodeClaim has the DisruptionReason condition set to true.
	disruptingObserved bool

	// markedForDeletion is owned by Cluster.MarkForDeletion and Cluster.UnmarkForDeletion. It lasts for the lifetime
	// of the disruption command rather than until the cache catches up.
	markedForDeletion bool
	// deleteRequested is set by static deprovisioning before it deletes the NodeClaim. It bridges the gap until the
	// cache shows the deletion and is cleared once deletionObserved is true.
	deleteRequested bool
	// disruptRequested is set by the disruption queue after it patches the DisruptionReason condition. It bridges the
	// gap until the cache shows the condition and is cleared once disruptingObserved is true.
	disruptRequested bool
}

func (r *nodeClaimRecord) class() nodeClaimClass {
	switch {
	case r.deletionObserved || r.markedForDeletion || r.deleteRequested:
		return classDeleting
	case r.disruptingObserved || r.disruptRequested:
		return classPendingDisruption
	default:
		return classActive
	}
}

// nodeClaimCounts is the number of a NodePool's NodeClaim records in each class.
type nodeClaimCounts struct {
	active            int
	pendingDisruption int
	deleting          int
}

func (c *nodeClaimCounts) add(class nodeClaimClass, delta int) {
	switch class {
	case classActive:
		c.active += delta
	case classPendingDisruption:
		c.pendingDisruption += delta
	case classDeleting:
		c.deleting += delta
	}
}

func (c *nodeClaimCounts) total() int {
	return c.active + c.pendingDisruption + c.deleting
}

// NodePoolState is a derived index of NodeClaims per NodePool, along with the node count each NodePool has reserved
// against its node limit. Records are keyed by NodeClaim name. Only Observe creates a record and only Forget deletes
// one. Intent setters never create a record, so an intent can't outlive the NodeClaim it was set for.
type NodePoolState struct {
	mu sync.RWMutex

	nodeClaims                  map[string]*nodeClaimRecord // nodeclaim name -> record
	nodePoolNameToCounts        map[string]*nodeClaimCounts // nodepool name -> counts derived from nodeClaims
	nodePoolNameToNodePoolLimit map[string]*atomic.Int64    // nodepool name -> node count reserved against the nodepool limit
}

func NewNodePoolState() *NodePoolState {
	return &NodePoolState{
		nodeClaims:                  map[string]*nodeClaimRecord{},
		nodePoolNameToCounts:        map[string]*nodeClaimCounts{},
		nodePoolNameToNodePoolLimit: map[string]*atomic.Int64{},
	}
}

// Observe records the NodeClaim's observed state, creating its record if this is the first time it has been seen.
// It must be called with the NodeClaim object wherever cluster state is updated from one, so that the persisted
// DeletionTimestamp, InstanceTerminating and DisruptionReason drive the NodeClaim's class.
func (n *NodePoolState) Observe(nodeClaim *v1.NodeClaim) {
	npName := nodeClaim.Labels[v1.NodePoolLabelKey]
	if npName == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.nodeClaims[nodeClaim.Name]; !ok {
		r := &nodeClaimRecord{nodePool: npName}
		n.nodeClaims[nodeClaim.Name] = r
		n.count(npName, r.class(), 1)
	}
	n.update(nodeClaim.Name, func(r *nodeClaimRecord) {
		r.nodePool = npName
		r.deletionObserved = !nodeClaim.DeletionTimestamp.IsZero() || nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue()
		r.disruptingObserved = nodeClaim.StatusConditions().Get(v1.ConditionTypeDisruptionReason).IsTrue()
		// Once the cache shows what an intent was waiting for, the persisted field takes over. The cache never goes
		// back to an older version of an object, so a later stale read can't undo this.
		if r.deletionObserved {
			r.deleteRequested = false
		}
		if r.disruptingObserved {
			r.disruptRequested = false
		}
	})
}

// RequestDisrupt records that the disruption queue has set the DisruptionReason condition on the NodeClaim, so it
// counts as pending disruption until the cache shows the condition. Call it only after the status patch succeeds.
func (n *NodePoolState) RequestDisrupt(ctx context.Context, nodeClaimName string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.update(nodeClaimName, func(r *nodeClaimRecord) { r.disruptRequested = !r.disruptingObserved }) {
		logUnknownNodeClaim(ctx, nodeClaimName, "disrupt")
	}
}

// ClearDisruptRequests drops the disrupt intents of the nodes' NodeClaims once no disruption command owns them. This
// covers a command abandoned before the cache ever showed the DisruptionReason condition it set.
func (n *NodePoolState) ClearDisruptRequests(nodes ...*StateNode) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, node := range nodes {
		if node.NodeClaim != nil {
			n.update(node.NodeClaim.Name, func(r *nodeClaimRecord) { r.disruptRequested = false })
		}
	}
}

// RequestDelete records that static deprovisioning is about to delete the NodeClaim, so it counts as deleting until
// the cache shows the deletion. Call it before the Delete call so the intent is in place before the NotFound
// reconcile that Forgets the NodeClaim.
func (n *NodePoolState) RequestDelete(ctx context.Context, nodeClaimName string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.update(nodeClaimName, func(r *nodeClaimRecord) { r.deleteRequested = !r.deletionObserved }) {
		logUnknownNodeClaim(ctx, nodeClaimName, "delete")
	}
}

// ClearDeleteRequest drops the delete intent set by RequestDelete, for when the Delete call fails.
func (n *NodePoolState) ClearDeleteRequest(nodeClaimName string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.update(nodeClaimName, func(r *nodeClaimRecord) { r.deleteRequested = false })
}

// SetMarkedForDeletion records whether the NodeClaim is marked for deletion by a disruption command. Cluster calls it
// only for NodeClaims attached to a StateNode, which Observe has already recorded if they belong to a NodePool, so
// an unknown NodeClaim here is one NodePoolState doesn't track and is silently ignored.
func (n *NodePoolState) SetMarkedForDeletion(nodeClaimName string, marked bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.update(nodeClaimName, func(r *nodeClaimRecord) { r.markedForDeletion = marked })
}

// Forget drops the NodeClaim's record and all of its intents. Call it only when the NodeClaim no longer exists, not
// when its key in cluster state changes. The NodePool's reservation is kept.
func (n *NodePoolState) Forget(nodeClaimName string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	r, ok := n.nodeClaims[nodeClaimName]
	if !ok {
		return
	}
	delete(n.nodeClaims, nodeClaimName)
	n.count(r.nodePool, r.class(), -1)
	n.dropNodePoolIfUnused(r.nodePool)
}

// GetNodeCount returns the number of NodeClaims in a NodePool by class (active, deleting, pendingdisruption)
func (n *NodePoolState) GetNodeCount(npName string) (active, deleting, pendingdisruption int) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.nodeCounts(npName)
}

// ReserveNodeCount attempts to reserve nodes against a NodePool's limit.
// It ensures that the total of active nodes + deleting nodes + pending disruption nodes + reserved nodes doesn't exceed the limit.
func (n *NodePoolState) ReserveNodeCount(np string, limit int64, wantedLimit int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.ensureNodePoolLimitEntry(np)
	defer n.dropNodePoolIfUnused(np)
	active, deleting, pendingdisruption := n.nodeCounts(np)

	// We retry until CompareAndSwap is successful
	for {
		currentlyReserved := n.nodePoolNameToNodePoolLimit[np].Load()
		remainingLimit := limit - int64(active+deleting+pendingdisruption) - currentlyReserved
		if remainingLimit < 0 {
			return 0
		}
		grantedLimit := lo.Ternary(wantedLimit > remainingLimit,
			remainingLimit,
			wantedLimit,
		)

		if n.nodePoolNameToNodePoolLimit[np].CompareAndSwap(currentlyReserved, currentlyReserved+grantedLimit) {
			return grantedLimit
		}
	}
}

// ReleaseNodeCount releases the NodePoolTracker ReservedNodeLimit
func (n *NodePoolState) ReleaseNodeCount(npName string, count int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	defer n.dropNodePoolIfUnused(npName)
	// We retry until CompareAndSwap is successful
	for {
		reserved, ok := n.nodePoolNameToNodePoolLimit[npName]
		// The entry is only dropped while nothing is reserved, so a missing entry means there is nothing to release.
		if !ok {
			return
		}
		currentlyReserved := reserved.Load()
		if reserved.CompareAndSwap(
			currentlyReserved,
			lo.Ternary(currentlyReserved-count < 0, 0, currentlyReserved-count)) {
			return
		}
	}
}

func (n *NodePoolState) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodeClaims = map[string]*nodeClaimRecord{}
	n.nodePoolNameToCounts = map[string]*nodeClaimCounts{}
	n.nodePoolNameToNodePoolLimit = map[string]*atomic.Int64{}
}

// Methods that expect the caller to hold the lock

func (n *NodePoolState) nodeCounts(npName string) (active, deleting, pendingdisruption int) {
	if c, ok := n.nodePoolNameToCounts[npName]; ok {
		return c.active, c.deleting, c.pendingDisruption
	}
	return 0, 0, 0
}

// update applies f to an existing record and moves the record's count if its NodePool or class changed. It returns
// false, without calling f, if there is no record for the NodeClaim.
func (n *NodePoolState) update(nodeClaimName string, f func(*nodeClaimRecord)) bool {
	r, ok := n.nodeClaims[nodeClaimName]
	if !ok {
		return false
	}
	oldNodePool, oldClass := r.nodePool, r.class()
	f(r)
	if oldNodePool != r.nodePool || oldClass != r.class() {
		n.count(oldNodePool, oldClass, -1)
		n.count(r.nodePool, r.class(), 1)
		n.dropNodePoolIfUnused(oldNodePool)
	}
	return true
}

func (n *NodePoolState) count(npName string, class nodeClaimClass, delta int) {
	c, ok := n.nodePoolNameToCounts[npName]
	if !ok {
		c = &nodeClaimCounts{}
		n.nodePoolNameToCounts[npName] = c
	}
	c.add(class, delta)
}

func (n *NodePoolState) ensureNodePoolLimitEntry(npName string) {
	if _, ok := n.nodePoolNameToNodePoolLimit[npName]; !ok {
		n.nodePoolNameToNodePoolLimit[npName] = &atomic.Int64{}
	}
}

// dropNodePoolIfUnused drops a NodePool's entries once it has no NodeClaim records and nothing reserved. The
// reservation belongs to the NodePool rather than its NodeClaims, so it must survive the NodePool's last Forget.
func (n *NodePoolState) dropNodePoolIfUnused(npName string) {
	if c, ok := n.nodePoolNameToCounts[npName]; ok {
		if c.total() > 0 {
			return
		}
		delete(n.nodePoolNameToCounts, npName)
	}
	if reserved, ok := n.nodePoolNameToNodePoolLimit[npName]; ok && reserved.Load() == 0 {
		delete(n.nodePoolNameToNodePoolLimit, npName)
	}
}

func logUnknownNodeClaim(ctx context.Context, nodeClaimName, intent string) {
	log.FromContext(ctx).WithValues("NodeClaim", klog.KRef("", nodeClaimName), "intent", intent).V(1).Info("dropping NodePoolState intent for unknown nodeclaim")
}
