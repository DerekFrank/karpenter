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
	var np *v1.NodePool

	BeforeEach(func() {
		np = test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "nodepool-2"}})
		nodeClaim = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey: np.Name,
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
		ExpectApplied(ctx, env.Client, np)
	})

	Context("ReserveNodeCount", func() {
		It("should reserve requested capacity when available", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 3)).To(HaveLen(3))
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 2)).To(HaveLen(2))
		})

		It("should grant partial capacity when requested exceeds available", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 4)).To(HaveLen(4))
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 5)).To(HaveLen(1))
		})

		It("should return no reservations when no capacity is available", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 5)).To(HaveLen(5))
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 1)).To(BeEmpty())
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, -1)).To(BeEmpty())
		})

		It("should account for running NodeClaims", func() {
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 4)).To(HaveLen(4))
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 1)).To(BeEmpty())
		})

		It("should account for deleting NodeClaims", func() {
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			cluster.MarkForDeletion(nodeClaim.Name)

			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 4)).To(HaveLen(4))
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 1)).To(BeEmpty())
		})

		It("should never grant more than the limit to concurrent callers", func() {
			var wg sync.WaitGroup
			var granted atomic.Int64
			for range 10 {
				wg.Go(func() {
					granted.Add(int64(len(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 1))))
				})
			}
			wg.Wait()
			Expect(granted.Load()).To(BeEquivalentTo(5))
		})
	})

	Context("Reservations", func() {
		It("should free a released reservation", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 5)
			Expect(reservations).To(HaveLen(5))
			reservations[:2].Release()
			ExpectStateNodePoolCount(cluster, np.Name, 0, 0, 3)
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 5)).To(HaveLen(2))
		})

		It("should release a reservation once", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 2)
			reservations[0].Release()
			reservations[0].Release()
			reservations[0].Bind(nodeClaim.Name)
			ExpectStateNodePoolCount(cluster, np.Name, 0, 0, 1)
		})

		It("should hold a bound reservation until the NodeClaim counts, and never count the NodeClaim zero times", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(np.Name, 1, 1)
			Expect(reservations).To(HaveLen(1))
			ExpectApplied(ctx, env.Client, nodeClaim)
			reservations[0].Bind(nodeClaim.Name)

			// Created, but cluster state hasn't seen it yet: the reservation still holds its node.
			ExpectStateNodePoolCount(cluster, np.Name, 0, 0, 1)
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 1, 1)).To(BeEmpty())

			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, np.Name, 1, 0, 0)
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 1, 1)).To(BeEmpty())
		})

		It("should release a reservation bound to a NodeClaim the counts already include", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(np.Name, 2, 1)
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, np.Name, 1, 0, 1)

			reservations[0].Bind(nodeClaim.Name)
			ExpectStateNodePoolCount(cluster, np.Name, 1, 0, 0)
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 2, 2)).To(HaveLen(1))
		})

		It("should release a bound reservation whose NodeClaim is deleted before it counts", func() {
			reservations := cluster.NodePoolState.ReserveNodeCount(np.Name, 2, 1)
			reservations[0].Bind(nodeClaim.Name)
			ExpectStateNodePoolCount(cluster, np.Name, 0, 0, 1)

			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectDeleted(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, np.Name, 0, 0, 0)
		})

		It("should expire a reservation that is never counted", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 2, 1)).To(HaveLen(1))
			bound := cluster.NodePoolState.ReserveNodeCount(np.Name, 2, 1)
			bound[0].Bind(nodeClaim.Name)
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 2, 1)).To(BeEmpty())

			env.Clock.Step(state.ReservationTTL)
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 2, 2)).To(HaveLen(2))
		})

		It("should keep reservations while a NodePool has no NodeClaims", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(np.Name, 5, 2)).To(HaveLen(2))
			ExpectStateNodePoolCount(cluster, np.Name, 0, 0, 2)
		})
	})
})

var _ = Describe("NodePoolState Tracking", func() {
	var nodeClaim *v1.NodeClaim
	var nodeClaim2 *v1.NodeClaim
	var nodePool2 *v1.NodePool

	BeforeEach(func() {
		nodePool2 = test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "nodepool2"}})
		ExpectApplied(ctx, env.Client, nodePool2)

		nodeClaim = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-nodeclaim",
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool.Name,
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
			},
		})

		nodeClaim2 = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-nodeclaim-2",
				Labels: map[string]string{
					v1.NodePoolLabelKey: nodePool2.Name,
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
	})

	Context("UpdateNodeClaim", func() {
		Context("New NodeClaim gets added", func() {
			It("should track NodeClaim in running state when created with ProviderID", func() {
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				Expect(cluster.NodeClaimExists(nodeClaim.Name)).To(BeTrue())
			})

			It("should track NodeClaim without ProviderID", func() {
				nodeClaimWithoutProvider := test.NodeClaim(v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-nodeclaim-no-provider",
						Labels: map[string]string{
							v1.NodePoolLabelKey: nodePool.Name,
						},
					},
					// No ProviderID set
				})

				ExpectApplied(ctx, env.Client, nodeClaimWithoutProvider)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaimWithoutProvider))

				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				Expect(cluster.NodeClaimExists(nodeClaimWithoutProvider.Name)).To(BeTrue())
			})

			It("should not track NodeClaim that has no nodepool", func() {
				nodeClaimWithoutNodePool := test.NodeClaim(v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-nodeclaim-no-provider",
					},
					Status: v1.NodeClaimStatus{
						ProviderID: test.RandomProviderID(),
					}})

				ExpectApplied(ctx, env.Client, nodeClaimWithoutNodePool)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaimWithoutNodePool))

				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount("")
				Expect(running).To(Equal(0))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				Expect(cluster.NodeClaimExists(nodeClaimWithoutNodePool.Name)).To(BeTrue())
			})

			It("should track multiple NodeClaims in the same NodePool", func() {
				nodeClaim3 := test.NodeClaim(v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-nodeclaim-2",
						Labels: map[string]string{
							v1.NodePoolLabelKey: nodePool.Name,
						},
					},
					Status: v1.NodeClaimStatus{
						ProviderID: test.RandomProviderID(),
					},
				})

				ExpectApplied(ctx, env.Client, nodeClaim, nodeClaim3)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim3))

				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(2))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))
			})

			It("should track NodeClaims across different NodePools", func() {
				ExpectApplied(ctx, env.Client, nodeClaim, nodeClaim2)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim2))

				running1, deleting1, pendingdisruption1 := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				running2, deleting2, pendingdisruption2 := cluster.NodePoolState.GetNodeCount(nodePool2.Name)

				Expect(running1).To(Equal(1))
				Expect(deleting1).To(Equal(0))
				Expect(pendingdisruption1).To(Equal(0))

				Expect(running2).To(Equal(1))
				Expect(deleting2).To(Equal(0))
				Expect(pendingdisruption2).To(Equal(0))

			})
		})

		Context("Updates to existing NodeClaim", func() {
			BeforeEach(func() {
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			})

			It("should be a no-op when NodeClaim is already tracked and no state change", func() {
				// Verify initial state
				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				// Update NodeClaim with annotation change (no state change)
				nodeClaim.Annotations = map[string]string{"test": "annotation"}
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

				// State should remain the same
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))
			})

			It("should handle NodeClaim ProviderID change", func() {
				// Verify initial state
				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				// Change ProviderID
				nodeClaim.Status.ProviderID = test.RandomProviderID()
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

				// Should still track the NodeClaim correctly
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))
			})

			It("should count a NodeClaim as deleting once it's disrupted, and as active once the disruption clears", func() {
				ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

				nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)

				nodeClaim.Annotations = map[string]string{"updated": "true"}
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)

				Expect(nodeClaim.StatusConditions().Clear(v1.ConditionTypeDisruptionReason)).To(Succeed())
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
			})

			It("should count a NodeClaim as deleting once it's being deleted", func() {
				nodeClaim.Finalizers = []string{v1.TerminationFinalizer}
				ExpectApplied(ctx, env.Client, nodeClaim)
				Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
			})

			It("should not change the counts when a node is marked for deletion", func() {
				cluster.MarkForDeletion(nodeClaim.Name)
				ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
			})

			It("should handle NodeClaim cleanup correctly", func() {
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				// Delete the NodeClaim
				ExpectDeleted(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(0))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))
			})
		})

		Context("DeleteNodeClaim", func() {
			BeforeEach(func() {
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			})

			It("should remove NodeClaim from nodepool state", func() {
				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))
				Expect(cluster.NodeClaimExists(nodeClaim.Name)).To(BeTrue())

				ExpectDeleted(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(0))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))
				Expect(cluster.NodeClaimExists(nodeClaim.Name)).To(BeFalse())
			})

		})

	})

	Context("ReserveNodeCount", func() {
		BeforeEach(func() {
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		})

		It("should reserve up to the limit counting active, deleting and reserved nodes", func() {
			disrupted := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
			disrupted.StatusConditions().SetTrue(v1.ConditionTypeDisruptionReason)
			ExpectApplied(ctx, env.Client, disrupted)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(disrupted))

			first := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 2)
			Expect(first).To(HaveLen(2))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 1, 2)
			second := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 10)
			Expect(second).To(HaveLen(1))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 1)).To(BeEmpty())

			first.Release()
			second.Release()
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 1, 0)
		})

		It("should keep reservations when a NodePool's last NodeClaim is deleted", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 5, 2)).To(HaveLen(2))
			ExpectDeleted(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 2)
		})

		It("should reserve atomically under concurrent callers", func() {
			var wg sync.WaitGroup
			var granted atomic.Int64
			for range 50 {
				wg.Go(func() { granted.Add(int64(len(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 1)))) })
			}
			wg.Wait()
			Expect(granted.Load()).To(BeEquivalentTo(9))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 9)
		})
	})
})
