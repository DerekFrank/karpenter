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

package state_test

import (
	"sync"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Static capacity races", func() {
	staticNodeClaim := func(providerID string) *v1.NodeClaim {
		nc := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels:     map[string]string{v1.NodePoolLabelKey: nodePool.Name},
			Finalizers: []string{v1.TerminationFinalizer},
		}})
		nc.Status.ProviderID = providerID
		return nc
	}
	reconcile := func(nodeClaims ...*v1.NodeClaim) {
		for _, nc := range nodeClaims {
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nc))
		}
	}
	applied := func(providerID string) *v1.NodeClaim {
		nc := staticNodeClaim(providerID)
		ExpectApplied(ctx, env.Client, nc)
		reconcile(nc)
		return nc
	}
	disrupt := func(nc *v1.NodeClaim) {
		nc.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
		ExpectApplied(ctx, env.Client, nc)
	}

	It("should keep a NodeClaim being disrupted counted as deleting when the informer re-reconciles it", func() {
		nc := applied(test.RandomProviderID())
		ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
		beforePatch := nc.DeepCopy()

		disrupt(nc)
		cluster.NodePoolState.Apply(nc)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)

		// The informer processes an event queued before the patch, then the patch.
		cluster.UpdateNodeClaim(beforePatch)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
		reconcile(nc)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
	})

	It("should count a deleting NodeClaim without a providerID as deleting", func() {
		nc := applied("")
		ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

		ExpectDeletionTimestampSet(ctx, env.Client, nc)
		reconcile(nc)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
	})

	It("should not lose reservations or pending NodeClaims when the last deleting NodeClaim is cleaned up", func() {
		deleting, candidate := applied(test.RandomProviderID()), applied(test.RandomProviderID())
		ExpectDeletionTimestampSet(ctx, env.Client, deleting)
		reconcile(deleting)

		Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 3, 1)).To(HaveLen(1))
		disrupt(candidate)
		reconcile(candidate)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 2, 1)

		ExpectFinalizersRemoved(ctx, env.Client, deleting)
		ExpectNotFound(ctx, env.Client, deleting)
		reconcile(deleting)

		Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(BeEmpty(), "reserved a slot past limits.nodes")
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 1)
	})

	It("should not drop an outstanding reservation when a NodeClaim launches", func() {
		nc := applied("")
		ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
		Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(HaveLen(1))

		nc.Status.ProviderID = test.RandomProviderID()
		ExpectApplied(ctx, env.Client, nc)
		reconcile(nc)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 1)
		Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(BeEmpty())
	})

	It("should never observe a launching NodeClaim disappear from the NodePool count", func() {
		applied(test.RandomProviderID())
		var dips atomic.Int64
		for range 2000 {
			nc := staticNodeClaim("")
			cluster.UpdateNodeClaim(nc)
			active, _, _ := cluster.NodePoolState.GetNodeCount(nodePool.Name)
			Expect(active).To(Equal(2))

			done := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				for {
					select {
					case <-done:
						return
					default:
					}
					if active, deleting, _ := cluster.NodePoolState.GetNodeCount(nodePool.Name); active+deleting < 2 {
						dips.Add(1)
					}
				}
			})
			launched := nc.DeepCopy()
			launched.Status.ProviderID = test.RandomProviderID()
			cluster.UpdateNodeClaim(launched)
			close(done)
			wg.Wait()
			cluster.DeleteNodeClaim(nc.Name)
		}
		Expect(dips.Load()).To(BeZero(), "observed the NodePool's NodeClaim count drop while a NodeClaim launched")
	})
})
