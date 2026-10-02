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
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

type nodePoolStateOp int

const (
	opObserveFresh nodePoolStateOp = iota
	opObserveStale
	opRequestDisrupt
	opRequestDelete
	opMarkForDeletion
	opForget
	numNodePoolStateOps
)

func (o nodePoolStateOp) String() string {
	return [...]string{"ObserveFresh", "ObserveStale", "RequestDisrupt", "RequestDelete", "MarkForDeletion", "Forget"}[o]
}

const (
	propertyNodePool  = "nodepool"
	propertyNodeClaim = "nodeclaim"
	// bystander is another NodeClaim in the same NodePool that no operation touches
	propertyBystander = "bystander"
)

// nodePoolStateWorld models one NodeClaim on the API server, the informer cache's view of it, and which intents its
// owners have set. RequestDisrupt and RequestDelete stand for the queue patching DisruptionReason and the deprovisioner
// deleting the NodeClaim, so each one also writes a new object version. The cache moves forward only on
// ObserveFresh; ObserveStale reconciles whatever the cache last held, which can be older than the API server.
type nodePoolStateWorld struct {
	disrupted, deleted             bool // latest object on the API server
	cachedDisrupted, cachedDeleted bool // object in the informer cache

	recorded                                  bool // NodePoolState has a record (alive since the last Observe after any Forget)
	disruptIntent, deleteIntent, markedIntent bool // intents set while a record existed, since it was last created
}

func (w *nodePoolStateWorld) object(disrupted, deleted bool) *v1.NodeClaim {
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: propertyNodeClaim, Labels: map[string]string{v1.NodePoolLabelKey: propertyNodePool}}}
	if disrupted {
		nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
	}
	if deleted {
		nodeClaim.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		nodeClaim.Finalizers = []string{v1.TerminationFinalizer}
	}
	return nodeClaim
}

func (w *nodePoolStateWorld) observe(n *NodePoolState, disrupted, deleted bool) {
	n.Observe(w.object(disrupted, deleted))
	if !w.recorded {
		w.recorded = true
		w.disruptIntent, w.deleteIntent, w.markedIntent = false, false, false
	}
}

func (w *nodePoolStateWorld) apply(ctx context.Context, n *NodePoolState, op nodePoolStateOp) {
	switch op {
	case opObserveFresh:
		w.cachedDisrupted, w.cachedDeleted = w.disrupted, w.deleted
		w.observe(n, w.cachedDisrupted, w.cachedDeleted)
	case opObserveStale:
		// Before the cache holds the object, cachedDisrupted and cachedDeleted are both false
		w.observe(n, w.cachedDisrupted, w.cachedDeleted)
	case opRequestDisrupt:
		w.disrupted = true
		n.RequestDisrupt(ctx, propertyNodeClaim)
		w.disruptIntent = w.disruptIntent || w.recorded
	case opRequestDelete:
		n.RequestDelete(ctx, propertyNodeClaim)
		w.deleted = true
		w.deleteIntent = w.deleteIntent || w.recorded
	case opMarkForDeletion:
		n.SetMarkedForDeletion(propertyNodeClaim, true)
		w.markedIntent = w.markedIntent || w.recorded
	case opForget:
		n.Forget(propertyNodeClaim)
		w.recorded = false
	}
}

// expectedClass is what the NodeClaim should count as: the union of what the cache has shown and the intents set
// while it was tracked. It doesn't depend on the order in which those happened.
func (w *nodePoolStateWorld) expectedClass() nodeClaimClass {
	switch {
	case w.cachedDeleted || w.deleteIntent || w.markedIntent:
		return classDeleting
	case w.cachedDisrupted || w.disruptIntent:
		return classPendingDisruption
	default:
		return classActive
	}
}

// checkNodePoolStateInvariants checks that the counts are exactly the classes of the records summed per NodePool
func checkNodePoolStateInvariants(n *NodePoolState) error {
	want := map[string]*nodeClaimCounts{}
	for _, r := range n.nodeClaims {
		if want[r.nodePool] == nil {
			want[r.nodePool] = &nodeClaimCounts{}
		}
		want[r.nodePool].add(r.class(), 1)
	}
	if len(want) != len(n.nodePoolNameToCounts) {
		return fmt.Errorf("counted nodepools %d, want %d", len(n.nodePoolNameToCounts), len(want))
	}
	for np, c := range want {
		if got := n.nodePoolNameToCounts[np]; got == nil || *got != *c {
			return fmt.Errorf("nodepool %q counts %+v, want %+v", np, got, *c)
		}
	}
	return nil
}

// checkNodePoolStateSequence applies ops to a NodePoolState that also tracks a bystander NodeClaim and holds a
// reservation for their NodePool, then checks the result against the model
func checkNodePoolStateSequence(ctx context.Context, ops []nodePoolStateOp, startTracked bool) error {
	n := NewNodePoolState()
	n.Observe(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: propertyBystander, Labels: map[string]string{v1.NodePoolLabelKey: propertyNodePool}}})
	if granted := n.ReserveNodeCount(propertyNodePool, 100, 1); granted != 1 {
		return fmt.Errorf("reserved %d, want 1", granted)
	}
	w := &nodePoolStateWorld{}
	if startTracked {
		w.apply(ctx, n, opObserveFresh)
	}
	for i, op := range ops {
		w.apply(ctx, n, op)
		if err := checkNodePoolStateInvariants(n); err != nil {
			return fmt.Errorf("after step %d: %w", i, err)
		}
	}
	return checkNodePoolStateOutcome(n, w)
}

// checkNodePoolStateOutcome checks the NodeClaim's record, the NodePool's counts and its reservation against the model
func checkNodePoolStateOutcome(n *NodePoolState, w *nodePoolStateWorld) error {
	r, ok := n.nodeClaims[propertyNodeClaim]
	if ok != w.recorded {
		return fmt.Errorf("record exists %v, want %v", ok, w.recorded)
	}
	want := nodeClaimCounts{active: 1} // bystander
	if ok {
		if got := r.class(); got != w.expectedClass() {
			return fmt.Errorf("class %d, want %d (record %+v)", got, w.expectedClass(), *r)
		}
		// An intent never outlives the cache showing what it was waiting for
		if (r.disruptRequested && r.disruptingObserved) || (r.deleteRequested && r.deletionObserved) {
			return fmt.Errorf("intent outlived its observation (record %+v)", *r)
		}
		want.add(w.expectedClass(), 1)
	}
	active, deleting, pending := n.GetNodeCount(propertyNodePool)
	if got := (nodeClaimCounts{active: active, deleting: deleting, pendingDisruption: pending}); got != want {
		return fmt.Errorf("counts %+v, want %+v", got, want)
	}
	// The reservation belongs to the NodePool and survives any Forget
	if reserved := n.nodePoolNameToNodePoolLimit[propertyNodePool]; reserved == nil || reserved.Load() != 1 {
		return fmt.Errorf("reservation lost")
	}
	return nil
}

// Checks every sequence of up to maxLen operations, with and without the NodeClaim tracked at the start
func TestNodePoolStateInterleavings(t *testing.T) {
	ctx := context.Background()
	const maxLen = 6
	sequences := 0
	var run func(ops []nodePoolStateOp)
	run = func(ops []nodePoolStateOp) {
		for _, startTracked := range []bool{true, false} {
			sequences++
			if err := checkNodePoolStateSequence(ctx, ops, startTracked); err != nil {
				t.Fatalf("tracked=%v %v: %v", startTracked, ops, err)
			}
		}
		if len(ops) == maxLen {
			return
		}
		for op := range numNodePoolStateOps {
			run(append(append([]nodePoolStateOp{}, ops...), op))
		}
	}
	run(nil)
	t.Logf("checked %d sequences", sequences)
}
