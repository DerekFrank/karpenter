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
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("NodePoolState", func() {
	var nodeClaim *v1.NodeClaim
	var nodePool *v1.NodePool

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "nodepool-2"}})
		nodeClaim = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
		ExpectApplied(ctx, env.Client, nodePool)
	})

	Context("ReserveNodeCount", func() {
		It("should reserve requested capacity when available", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 3)).To(HaveLen(3))

			// Should be able to reserve remaining capacity
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 2)).To(HaveLen(2))
		})

		It("should grant partial capacity when requested exceeds available", func() {
			// Reserve most capacity first
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 4)).To(HaveLen(4))

			// Request more than available, should get only what's left
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 5)).To(HaveLen(1))
		})

		It("should return zero when no capacity available", func() {
			// Reserve all capacity
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 5)).To(HaveLen(5))

			// No more capacity available
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 1)).To(BeEmpty())
		})

		It("should account for running NodeClaims", func() {
			// Add a running NodeClaim
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

			// Should have 4 slots available (5 limit - 1 running)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 4)).To(HaveLen(4))

			// No more capacity
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 1)).To(BeEmpty())
		})

		It("should account for deleting NodeClaims", func() {
			// Add and mark NodeClaim for deletion
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			cluster.MarkForDeletion(nodeClaim.Status.ProviderID)

			// Should have 4 slots available (5 limit - 1 deleting)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 4)).To(HaveLen(4))
		})

		It("should be thread-safe with concurrent reservations", func() {
			var wg sync.WaitGroup
			var totalGranted int64
			numGoroutines := 10
			requestPerGoroutine := int64(1)
			limit := int64(5)

			for range numGoroutines {
				wg.Go(func() {
					granted := int64(len(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, limit, requestPerGoroutine)))
					atomic.AddInt64(&totalGranted, granted)
				})
			}

			wg.Wait()
			// Should not exceed the limit
			Expect(totalGranted).To(BeNumerically("<=", limit))
			Expect(totalGranted).To(Equal(limit)) // Should grant exactly the limit
		})
	})

	Context("Reservations", func() {
		It("should release reserved capacity", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 3)
			Expect(reservations).To(HaveLen(3))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 3)).To(HaveLen(2))

			reservations.Release()
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 4)).To(HaveLen(3))
		})

		It("should not release a reservation twice", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 2)
			Expect(reservations).To(HaveLen(2))
			reservations[0].Release()
			reservations[0].Release()
			_, _, reserved := cluster.NodePoolState.GetNodeCount(nodePool.Name)
			Expect(reserved).To(Equal(1))
		})

		It("should hand a bound reservation off to the NodeClaim's count", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 1)
			Expect(reservations).To(HaveLen(1))
			ExpectApplied(ctx, env.Client, nodeClaim)
			reservations[0].Bind(nodeClaim.Name)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)

			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
		})

		It("should expire a reservation that is never counted", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 2)).To(HaveLen(2))
			env.Clock.Step(state.ReservationTTL)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		})

		It("should be thread-safe with concurrent releases", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 10)
			Expect(reservations).To(HaveLen(10))

			var wg sync.WaitGroup
			for range 50 {
				wg.Go(func() { reservations.Release() })
			}
			wg.Wait()

			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 100)).To(HaveLen(10))
		})
	})

	It("should ignore stale versions, versions that drop a deletionTimestamp, and versions of removed NodeClaims", func() {
		version := func(rv string, deleting bool) *v1.NodeClaim {
			nc := nodeClaim.DeepCopy()
			nc.ResourceVersion = rv
			if deleting {
				nc.DeletionTimestamp = &metav1.Time{Time: env.Clock.Now()}
			}
			return nc
		}
		cluster.NodePoolState.Apply(version("5", true))
		cluster.NodePoolState.Apply(version("4", false))
		cluster.NodePoolState.Apply(version("5", false))
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)

		cluster.NodePoolState.Remove(nodeClaim.Name)
		cluster.NodePoolState.Apply(version("6", true))
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
	})
})
