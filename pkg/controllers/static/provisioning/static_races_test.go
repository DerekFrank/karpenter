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

package static_test

import (
	"context"
	"math"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	static "sigs.k8s.io/karpenter/pkg/controllers/static/provisioning"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// deletedRightAfterCreateClient deletes each NodeClaim it creates, and has cluster state observe the deletion, before
// the provisioner seeds cluster state with it.
type deletedRightAfterCreateClient struct {
	client.Client
}

func (d *deletedRightAfterCreateClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := d.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if nc, ok := obj.(*v1.NodeClaim); ok {
		gone := nc.DeepCopy()
		ExpectFinalizersRemoved(ctx, d.Client, gone)
		ExpectDeleted(ctx, d.Client, gone)
		cluster.DeleteNodeClaim(nc.Name)
	}
	return nil
}

var _ = Describe("Static capacity races", func() {
	var nodePool *v1.NodePool

	withReplicas := func(replicas int64, resolved int) []*v1.NodeClaim {
		nodePool.Spec.Replicas = new(replicas)
		ExpectApplied(ctx, env.Client, nodePool)
		nodeClaims, nodes := test.NodeClaimsAndNodes(resolved, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					v1.NodeInitializedLabelKey:     "true",
					corev1.LabelInstanceTypeStable: "stable.instance",
				},
				Finalizers: []string{v1.TerminationFinalizer},
			},
			Status: v1.NodeClaimStatus{Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10"),
				corev1.ResourceMemory: resource.MustParse("1000Mi"),
			}},
		})
		for i := range nodeClaims {
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims)
		return nodeClaims
	}
	nodeClaimCount := func(live bool) int {
		list := &v1.NodeClaimList{}
		Expect(env.Client.List(ctx, list)).To(Succeed())
		return len(lo.Filter(list.Items, func(nc v1.NodeClaim, _ int) bool { return !live || nc.DeletionTimestamp.IsZero() }))
	}

	BeforeEach(func() {
		nodePool = test.StaticNodePool()
	})

	It("should refill when a NodeClaim that never launched is deleting", func() {
		withReplicas(2, 1)
		failed := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels:     map[string]string{v1.NodePoolLabelKey: nodePool.Name},
			Finalizers: []string{v1.TerminationFinalizer},
		}})
		failed.Status.ProviderID = ""
		ExpectApplied(ctx, env.Client, failed)
		cluster.UpdateNodeClaim(failed)
		cluster.SetSynced(true)

		ExpectDeletionTimestampSet(ctx, env.Client, failed)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(failed))
		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(nodeClaimCount(true)).To(Equal(2), "static NodePool left below spec.replicas after a launch failure")
	})

	It("should not provision for a NodeClaim that is being disrupted", func() {
		nodeClaims := withReplicas(3, 2)
		// StaticDrift holds a reservation for the replacement, and markDisrupted marks the candidate.
		Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, math.MaxInt64, 1)).To(HaveLen(1))
		nodeClaims[1].StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
		ExpectApplied(ctx, env.Client, nodeClaims[1])
		cluster.NodePoolState.Apply(nodeClaims[1])

		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(nodeClaimCount(false)).To(Equal(3), "provisioned more than spec.replicas - (active + pending)")
	})

	It("should refill when reconciled on a NodeClaim deletion before cluster state observes it", func() {
		nodeClaims := withReplicas(2, 2)
		wakes := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
		defer wakes.ShutDown()
		sourceCtx, stop := context.WithCancel(ctx)
		defer stop()
		Expect(cluster.NodePoolState.Source().Start(sourceCtx, wakes)).To(Succeed())

		ExpectDeletionTimestampSet(ctx, env.Client, nodeClaims[0])
		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaims[0]))

		Eventually(wakes.Len).Should(BeNumerically(">=", 1))
		req, _ := wakes.Get()
		Expect(req.Name).To(Equal(nodePool.Name))
		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(nodeClaimCount(true)).To(Equal(2), "static NodePool left below spec.replicas after a NodeClaim started deleting")
	})

	It("should not track a NodeClaim that was deleted before the post-Create seed", func() {
		withReplicas(1, 0)
		cluster.SetSynced(true)
		provisioner := static.NewController(&deletedRightAfterCreateClient{Client: env.Client}, cluster, events.NewRecorder(&record.FakeRecorder{}),
			cloudProvider, prov, env.Clock, deviceallocation.NewController(env.Client), virtualpods.NewVirtualPodCache(env.Client))

		ExpectObjectReconciled(ctx, env.Client, provisioner, nodePool)
		Expect(nodeClaimCount(false)).To(Equal(0))
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
	})
})
