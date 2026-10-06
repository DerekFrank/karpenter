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
	"fmt"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Checks every sequence of up to maxLen operations on one NodeClaim against a model of what NodePoolState should know
// about it, with and without the NodeClaim tracked at the start and with and without a bystander NodeClaim keeping the
// NodePool's counts alive. Writes add a version of the NodeClaim to the API server without observing it. Observations
// read the latest version or an older one, as a lagging informer cache or a stale read would. ObserveLatestIfTracked
// is how cluster state observes the response to a write, which must never create a record.
//
//nolint:gocyclo
func TestNodePoolStateInterleavings(t *testing.T) {
	const (
		opWriteDisrupt = iota
		opWriteClear
		opWriteDelete
		opObserveLatest
		opObservePrevious
		opObserveOldest
		opObserveLatestIfTracked
		opMark
		opUnmark
		opForget
		numOps
	)
	opNames := [numOps]string{"WriteDisrupt", "WriteClear", "WriteDelete", "ObserveLatest", "ObservePrevious", "ObserveOldest", "ObserveLatestIfTracked", "Mark", "Unmark", "Forget"}
	const maxLen = 5
	const nodePoolName, nodeClaimName, bystanderName = "nodepool", "nodeclaim", "bystander"

	sequences := 0
	for length, total := 0, 1; length <= maxLen; length, total = length+1, total*numOps {
		for code := range total {
			ops := make([]int, length)
			for i, c := 0, code; i < length; i, c = i+1, c/numOps {
				ops[i] = c % numOps
			}
			for _, startTracked := range []bool{true, false} {
				for _, bystander := range []bool{true, false} {
					sequences++
					err := func() error {
						n := NewNodePoolState()
						if bystander {
							n.Observe(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: bystanderName, Labels: map[string]string{v1.NodePoolLabelKey: nodePoolName}, ResourceVersion: "1"}})
						}
						if granted := n.ReserveNodeCount(nodePoolName, 100, 1); granted != 1 {
							return fmt.Errorf("reserved %d, want 1", granted)
						}
						// The model: versions[i] is the version of the NodeClaim with resourceVersion i+1. recorded is whether
						// NodePoolState has a record, applied is the newest version observed since the record was created, and
						// marked is whether it was marked since then.
						versions := []struct{ disrupted, deleted bool }{{}}
						recorded, applied, marked := false, 0, false
						steps := ops
						if startTracked {
							steps = append([]int{opObserveOldest}, ops...)
						}
						for step, op := range steps {
							switch op {
							case opWriteDisrupt, opWriteClear, opWriteDelete:
								next := versions[len(versions)-1]
								next.disrupted = op == opWriteDisrupt || (next.disrupted && op != opWriteClear)
								next.deleted = next.deleted || op == opWriteDelete
								versions = append(versions, next)
							case opObserveLatest, opObservePrevious, opObserveOldest, opObserveLatestIfTracked:
								i := 0
								switch latest := len(versions) - 1; op {
								case opObserveLatest, opObserveLatestIfTracked:
									i = latest
								case opObservePrevious:
									i = max(latest-1, 0)
								}
								nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
									Name:            nodeClaimName,
									Labels:          map[string]string{v1.NodePoolLabelKey: nodePoolName},
									ResourceVersion: strconv.Itoa(i + 1),
								}}
								if versions[i].disrupted {
									nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
								}
								if versions[i].deleted {
									nodeClaim.DeletionTimestamp = &metav1.Time{Time: time.Now()}
									nodeClaim.Finalizers = []string{v1.TerminationFinalizer}
								}
								if op == opObserveLatestIfTracked {
									if tracked := n.ObserveIfTracked(nodeClaim); tracked != recorded {
										return fmt.Errorf("step %d: ObserveIfTracked returned %v, want %v", step, tracked, recorded)
									}
								} else if created := n.Observe(nodeClaim); created == recorded {
									return fmt.Errorf("step %d: Observe returned %v, want %v", step, created, !recorded)
								}
								switch {
								case op == opObserveLatestIfTracked && !recorded:
								case !recorded:
									recorded, applied, marked = true, i, false
								case i > applied:
									applied = i
								}
							case opMark:
								// The mark is compare-and-set, and only a tracked NodeClaim can be marked
								if got, want := n.setMarkedForDeletion(nodeClaimName, true), recorded && !marked; got != want {
									return fmt.Errorf("step %d: setMarkedForDeletion returned %v, want %v", step, got, want)
								}
								marked = recorded
							case opUnmark:
								n.setMarkedForDeletion(nodeClaimName, false)
								marked = false
							case opForget:
								n.Forget(nodeClaimName)
								recorded = false
							}

							// The counts are exactly the classes of the records summed per NodePool
							summed := map[string]*nodeClaimCounts{}
							for _, r := range n.nodeClaims {
								if summed[r.nodePool] == nil {
									summed[r.nodePool] = &nodeClaimCounts{}
								}
								summed[r.nodePool].add(r.class(), 1)
							}
							if len(summed) != len(n.nodePoolNameToCounts) {
								return fmt.Errorf("after step %d: counted nodepools %d, want %d", step, len(n.nodePoolNameToCounts), len(summed))
							}
							for np, c := range summed {
								if got := n.nodePoolNameToCounts[np]; got == nil || *got != *c {
									return fmt.Errorf("after step %d: nodepool %q counts %+v, want %+v", step, np, got, *c)
								}
							}

							// Only Observe creates a record, so none exists after Forget until the next Observe, whatever
							// ObserveIfTracked sees. The NodeClaim's class is the newest version observed, plus the mark.
							r, ok := n.nodeClaims[nodeClaimName]
							if ok != recorded {
								return fmt.Errorf("after step %d: record exists %v, want %v", step, ok, recorded)
							}
							want := nodeClaimCounts{}
							if bystander {
								want.active = 1
							}
							if ok {
								wantClass := classActive
								switch v := versions[applied]; {
								case v.deleted || marked:
									wantClass = classDeleting
								case v.disrupted:
									wantClass = classPendingDisruption
								}
								if got := r.class(); got != wantClass {
									return fmt.Errorf("after step %d: class %d, want %d (record %+v)", step, got, wantClass, *r)
								}
								want.add(wantClass, 1)
							}
							active, deleting, pending := n.GetNodeCount(nodePoolName)
							if got := (nodeClaimCounts{active: active, deleting: deleting, pendingDisruption: pending}); got != want {
								return fmt.Errorf("after step %d: counts %+v, want %+v", step, got, want)
							}
							// The reservation belongs to the NodePool and survives any Forget
							if n.reserved[nodePoolName] != 1 {
								return fmt.Errorf("after step %d: reservation lost", step)
							}
						}
						return nil
					}()
					if err != nil {
						names := make([]string, len(ops))
						for i, op := range ops {
							names[i] = opNames[op]
						}
						t.Fatalf("tracked=%v bystander=%v %v: %v", startTracked, bystander, names, err)
					}
				}
			}
		}
	}
	t.Logf("checked %d sequences", sequences)
}
