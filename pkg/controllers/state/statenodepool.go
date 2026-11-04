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
	"strconv"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// NodeClaimCounts are a NodePool's NodeClaims by state. A NodeClaim is deleting once it has a deletionTimestamp or its
// DisruptionReason condition is true, and active otherwise.
type NodeClaimCounts struct {
	Active   int
	Deleting int
}

// NodePoolState counts each NodePool's NodeClaims, and holds the nodes controllers reserve against a NodePool's node
// limit for NodeClaims they're about to create.
//
// The counts are kept incrementally rather than computed from the cache: static provisioning reserves after every
// NodeClaim it creates, so a recount on each reservation would make a scale-up quadratic in the NodePool's size.
type NodePoolState struct {
	mu         sync.Mutex
	nodeClaims map[string]counted // NodeClaim name -> what it's counted as
	counts     map[string]NodeClaimCounts
	reserved   map[string]int64
}

type counted struct {
	nodePool        string
	deleting        bool
	resourceVersion uint64
}

func NewNodePoolState() *NodePoolState {
	return &NodePoolState{nodeClaims: map[string]counted{}, counts: map[string]NodeClaimCounts{}, reserved: map[string]int64{}}
}

// UpdateNodeClaim counts nodeClaim on its NodePool, unless a newer version of it is already counted.
func (n *NodePoolState) UpdateNodeClaim(nodeClaim *v1.NodeClaim) {
	rv, _ := strconv.ParseUint(nodeClaim.ResourceVersion, 10, 64)
	n.mu.Lock()
	defer n.mu.Unlock()
	old, ok := n.nodeClaims[nodeClaim.Name]
	if ok && rv != 0 && rv < old.resourceVersion {
		return
	}
	if ok {
		n.countLocked(old, -1)
	}
	next := counted{nodePool: nodeClaim.Labels[v1.NodePoolLabelKey], deleting: deleting(nodeClaim), resourceVersion: rv}
	n.nodeClaims[nodeClaim.Name] = next
	n.countLocked(next, 1)
}

// DeleteNodeClaim stops counting the NodeClaim.
func (n *NodePoolState) DeleteNodeClaim(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if old, ok := n.nodeClaims[name]; ok {
		n.countLocked(old, -1)
		delete(n.nodeClaims, name)
	}
}

func (n *NodePoolState) countLocked(c counted, delta int) {
	if c.nodePool == "" {
		return
	}
	counts := n.counts[c.nodePool]
	if c.deleting {
		counts.Deleting += delta
	} else {
		counts.Active += delta
	}
	if counts == (NodeClaimCounts{}) {
		delete(n.counts, c.nodePool)
		return
	}
	n.counts[c.nodePool] = counts
}

func deleting(nodeClaim *v1.NodeClaim) bool {
	if !nodeClaim.DeletionTimestamp.IsZero() {
		return true
	}
	// Read the condition directly, since StatusConditions() builds a condition set on every call.
	for i := range nodeClaim.Status.Conditions {
		if nodeClaim.Status.Conditions[i].Type == v1.ConditionTypeDisruptionReason && nodeClaim.Status.Conditions[i].Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// GetNodeCount returns the NodePool's active and deleting NodeClaims, and the nodes reserved for NodeClaims that
// haven't been created yet.
func (n *NodePoolState) GetNodeCount(nodePool string) (active, deleting, reserved int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	counts := n.counts[nodePool]
	return counts.Active, counts.Deleting, int(n.reserved[nodePool])
}

// ReserveNodeCount reserves up to wantedLimit nodes for NodeClaims a caller is about to create, without the NodePool's
// active, deleting and reserved nodes exceeding limit, and returns how many it reserved. The caller releases them with
// ReleaseNodeCount once it has created the NodeClaims, or failed to.
func (n *NodePoolState) ReserveNodeCount(nodePool string, limit int64, wantedLimit int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	counts := n.counts[nodePool]
	granted := max(0, min(wantedLimit, limit-int64(counts.Active+counts.Deleting)-n.reserved[nodePool]))
	n.reserved[nodePool] += granted
	return granted
}

// ReleaseNodeCount releases count of the NodePool's reserved nodes.
func (n *NodePoolState) ReleaseNodeCount(nodePool string, count int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.reserved[nodePool] -= count; n.reserved[nodePool] <= 0 {
		delete(n.reserved, nodePool)
	}
}

func (n *NodePoolState) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodeClaims, n.counts, n.reserved = map[string]counted{}, map[string]NodeClaimCounts{}, map[string]int64{}
}
