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
	"sync"

	"github.com/awslabs/operatorpkg/status"
	"k8s.io/apimachinery/pkg/util/resourceversion"
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

// nodeClaimRecord holds what NodePoolState knows about one NodeClaim. The observed fields come from the newest version
// of the NodeClaim that Observe has seen. markedForDeletion is the only decision held in memory.
type nodeClaimRecord struct {
	nodePool string
	// lastRV is the resourceVersion of the newest observation applied to the record
	lastRV string

	// deleting is true when the observed NodeClaim has a DeletionTimestamp or is InstanceTerminating, the same rule
	// StateNode.Deleted uses. Unlike StateNode, it also applies to NodeClaims that haven't launched yet.
	deleting bool
	// disrupting is true when the observed NodeClaim has the DisruptionReason condition set to true
	disrupting bool

	// markedForDeletion is set by whichever controller has committed to deleting the NodeClaim: the disruption queue
	// once a command's replacements have launched, or static deprovisioning before it deletes the NodeClaim. Cluster
	// sets it, together with the NodeClaim's StateNode if it has one.
	markedForDeletion bool
}

func (r *nodeClaimRecord) class() nodeClaimClass {
	switch {
	case r.deleting || r.markedForDeletion:
		return classDeleting
	case r.disrupting:
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

// NodePoolState is a derived index of the NodeClaims of static NodePools, along with the node count each NodePool has
// reserved against its node limit. NodeClaims of dynamic NodePools have no record. Records are keyed by NodeClaim name,
// so a NodeClaim keeps its record when it launches. Only Observe creates a record and only Forget deletes one.
type NodePoolState struct {
	mu sync.RWMutex

	nodeClaims           map[string]*nodeClaimRecord // nodeclaim name -> record
	nodePoolNameToCounts map[string]*nodeClaimCounts // nodepool name -> counts derived from nodeClaims
	// nodepool name -> node count reserved against the nodepool limit. It belongs to the nodepool rather than its
	// NodeClaims, so it survives the nodepool's last Forget.
	reserved map[string]int64

	logIncomparableResourceVersion sync.Once
}

func NewNodePoolState() *NodePoolState {
	return &NodePoolState{
		nodeClaims:           map[string]*nodeClaimRecord{},
		nodePoolNameToCounts: map[string]*nodeClaimCounts{},
		reserved:             map[string]int64{},
	}
}

// Observe records the NodeClaim's persisted state, creating its record if this is the first time it has been seen, and
// returns whether it did. Call it only for NodeClaims of static NodePools, with every version that the informer
// delivers or that a creating write returns, so that DeletionTimestamp, InstanceTerminating and DisruptionReason drive
// the NodeClaim's class. An observation older than one already applied is dropped, so a stale read can't undo a newer
// one.
func (n *NodePoolState) Observe(nodeClaim *v1.NodeClaim) (created bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.observeLocked(nodeClaim)
}

// ObserveIfTracked is Observe for a NodeClaim that already has a record, and returns whether it has one. Use it for the
// NodeClaim returned by a write. It never creates a record, so the response to a write that lands after the NodeClaim
// was forgotten can't bring it back.
func (n *NodePoolState) ObserveIfTracked(nodeClaim *v1.NodeClaim) (tracked bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.nodeClaims[nodeClaim.Name]; !ok {
		return false
	}
	n.observeLocked(nodeClaim)
	return true
}

func (n *NodePoolState) observeLocked(nodeClaim *v1.NodeClaim) (created bool) {
	npName := nodeClaim.Labels[v1.NodePoolLabelKey]
	if npName == "" {
		return false
	}
	r, ok := n.nodeClaims[nodeClaim.Name]
	switch {
	case !ok:
		r = &nodeClaimRecord{nodePool: npName}
		n.nodeClaims[nodeClaim.Name] = r
		n.count(npName, r.class(), 1)
		created = true
	case !n.isNotOlder(nodeClaim, r.lastRV):
		return false
	}
	n.update(r, func(r *nodeClaimRecord) {
		r.nodePool = npName
		r.lastRV = nodeClaim.ResourceVersion
		r.deleting = !nodeClaim.DeletionTimestamp.IsZero() || nodeClaim.StatusConditions(status.WithObservedOnly()).Get(v1.ConditionTypeInstanceTerminating).IsTrue()
		r.disrupting = nodeClaim.StatusConditions(status.WithObservedOnly()).Get(v1.ConditionTypeDisruptionReason).IsTrue()
	})
	return created
}

// Tracked returns whether the NodeClaim has a record
func (n *NodePoolState) Tracked(nodeClaimName string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	_, ok := n.nodeClaims[nodeClaimName]
	return ok
}

// isNotOlder reports whether the NodeClaim is the same version as, or newer than, lastRV. Comparable resourceVersions
// (KEP-5504) let observations from different sources be ordered. If they can't be compared, the observation is applied
// anyway, so accounting degrades to last-writer-wins rather than getting stuck.
func (n *NodePoolState) isNotOlder(nodeClaim *v1.NodeClaim, lastRV string) bool {
	if lastRV == "" {
		return true
	}
	cmp, err := resourceversion.CompareResourceVersion(nodeClaim.ResourceVersion, lastRV)
	if err != nil {
		n.logIncomparableResourceVersion.Do(func() {
			log.Log.WithValues("NodeClaim", klog.KObj(nodeClaim)).V(1).Info("failed comparing resource versions, applying nodeclaim observations in arrival order", "error", err)
		})
		return true
	}
	return cmp >= 0
}

// MarkedForDeletion returns whether the NodeClaim is marked for deletion
func (n *NodePoolState) MarkedForDeletion(nodeClaimName string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	r, ok := n.nodeClaims[nodeClaimName]
	return ok && r.markedForDeletion
}

// setMarkedForDeletion sets the NodeClaim's mark, and returns whether that changed it. It is compare-and-set, so a
// caller that needs to be the only one acting on the NodeClaim can skip it when it returns false. Marking a NodeClaim
// that has no record does nothing, so a mark can never outlive the NodeClaim it was set for. Only Cluster calls it, so
// that the NodeClaim's StateNode is marked with it.
func (n *NodePoolState) setMarkedForDeletion(nodeClaimName string, marked bool) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	r, ok := n.nodeClaims[nodeClaimName]
	if !ok {
		return false
	}
	if r.markedForDeletion == marked {
		return false
	}
	n.update(r, func(r *nodeClaimRecord) { r.markedForDeletion = marked })
	return true
}

// Forget drops the NodeClaim's record. Call it only when the NodeClaim no longer exists, not when its key in cluster
// state changes. The NodePool's reservation is kept.
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

	active, deleting, pendingdisruption := n.nodeCounts(np)
	granted := max(0, min(wantedLimit, limit-int64(active+deleting+pendingdisruption)-n.reserved[np]))
	if granted > 0 {
		n.reserved[np] += granted
	}
	return granted
}

// ReleaseNodeCount releases the NodePoolTracker ReservedNodeLimit
func (n *NodePoolState) ReleaseNodeCount(npName string, count int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reserved := n.reserved[npName] - count; reserved > 0 {
		n.reserved[npName] = reserved
	} else {
		delete(n.reserved, npName)
	}
}

func (n *NodePoolState) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodeClaims = map[string]*nodeClaimRecord{}
	n.nodePoolNameToCounts = map[string]*nodeClaimCounts{}
	n.reserved = map[string]int64{}
}

// Methods that expect the caller to hold the lock

func (n *NodePoolState) nodeCounts(npName string) (active, deleting, pendingdisruption int) {
	if c, ok := n.nodePoolNameToCounts[npName]; ok {
		return c.active, c.deleting, c.pendingDisruption
	}
	return 0, 0, 0
}

// update applies f to the record and moves the record's count if its NodePool or class changed
func (n *NodePoolState) update(r *nodeClaimRecord, f func(*nodeClaimRecord)) {
	oldNodePool, oldClass := r.nodePool, r.class()
	f(r)
	if oldNodePool != r.nodePool || oldClass != r.class() {
		n.count(oldNodePool, oldClass, -1)
		n.count(r.nodePool, r.class(), 1)
		n.dropNodePoolIfUnused(oldNodePool)
	}
}

func (n *NodePoolState) count(npName string, class nodeClaimClass, delta int) {
	c, ok := n.nodePoolNameToCounts[npName]
	if !ok {
		c = &nodeClaimCounts{}
		n.nodePoolNameToCounts[npName] = c
	}
	c.add(class, delta)
}

// dropNodePoolIfUnused drops a NodePool's counts once it has no NodeClaim records
func (n *NodePoolState) dropNodePoolIfUnused(npName string) {
	if c, ok := n.nodePoolNameToCounts[npName]; ok && c.total() == 0 {
		delete(n.nodePoolNameToCounts, npName)
	}
}
