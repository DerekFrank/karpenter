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
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 2)).To(HaveLen(2))
		})

		It("should grant partial capacity when requested exceeds available", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 4)).To(HaveLen(4))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 5)).To(HaveLen(1))
		})

		It("should return no reservations when no capacity is available", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 5)).To(HaveLen(5))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 1)).To(BeEmpty())
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, -1)).To(BeEmpty())
		})

		It("should account for running NodeClaims", func() {
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 4)).To(HaveLen(4))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 1)).To(BeEmpty())
		})

		It("should account for deleting NodeClaims", func() {
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			cluster.MarkForDeletion(nodeClaim.Status.ProviderID)

			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 4)).To(HaveLen(4))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 1)).To(BeEmpty())
		})

		It("should never grant more than the limit to concurrent callers", func() {
			var wg sync.WaitGroup
			var granted atomic.Int64
			for range 10 {
				wg.Go(func() {
					granted.Add(int64(len(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 1))))
				})
			}
			wg.Wait()
			Expect(granted.Load()).To(BeEquivalentTo(5))
		})
	})

	Context("Reservations", func() {
		It("should free a released reservation", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 5)
			Expect(reservations).To(HaveLen(5))
			reservations[:2].Release()
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 3)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 5)).To(HaveLen(2))
		})

		It("should release a reservation once", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 2)
			reservations[0].Release()
			reservations[0].Release()
			reservations[0].Bind(client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)
		})

		It("should hold a bound reservation until the NodeClaim counts, and never count the NodeClaim zero times", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 1, 1)
			Expect(reservations).To(HaveLen(1))
			ExpectApplied(ctx, env.Client, nodeClaim)
			reservations[0].Bind(client.ObjectKeyFromObject(nodeClaim))

			// Created, but cluster state hasn't seen it yet: the reservation still holds its node.
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 1, 1)).To(BeEmpty())

			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 1, 1)).To(BeEmpty())
		})

		It("should release a reservation bound to a NodeClaim the counts already include", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 1)

			reservations[0].Bind(client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 2)).To(HaveLen(1))
		})

		It("should release a bound reservation whose NodeClaim is deleted before it counts", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)
			reservations[0].Bind(client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)

			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectDeleted(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		})

		It("should expire a reservation that is never counted", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(HaveLen(1))
			bound := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)
			bound[0].Bind(client.ObjectKeyFromObject(nodeClaim))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(BeEmpty())

			env.Clock.Step(state.ReservationTTL)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 2)).To(HaveLen(2))
		})

		It("should keep reservations while a NodePool has no NodeClaims", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 2)).To(HaveLen(2))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 2)
		})
	})
})
