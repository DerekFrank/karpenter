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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	static "sigs.k8s.io/karpenter/pkg/controllers/static/deprovisioning"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// deletedUnderneathClient deletes the NodeClaim, and has cluster state observe the deletion, before the
// deprovisioner's own Delete, which then gets NotFound.
type deletedUnderneathClient struct {
	client.Client
}

func (d *deletedUnderneathClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if nc, ok := obj.(*v1.NodeClaim); ok {
		ExpectDeleted(ctx, d.Client, nc.DeepCopy())
		cluster.DeleteNodeClaim(nc.Name)
	}
	return d.Client.Delete(ctx, obj, opts...)
}

var _ = Describe("Static capacity races", func() {
	var nodePool *v1.NodePool

	withReplicas := func(replicas int64, resolved int) []*v1.NodeClaim {
		nodePool.Spec.Replicas = new(replicas)
		ExpectApplied(ctx, env.Client, nodePool)
		nodeClaims, nodes := test.NodeClaimsAndNodes(resolved, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				v1.NodeInitializedLabelKey:     "true",
				corev1.LabelInstanceTypeStable: "stable.instance",
			}},
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
	// unresolved creates a NodeClaim that hasn't launched and seeds cluster state with it, as Provisioner.Create does.
	unresolved := func() *v1.NodeClaim {
		nc := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels:     map[string]string{v1.NodePoolLabelKey: nodePool.Name},
			Finalizers: []string{v1.TerminationFinalizer},
		}})
		nc.Status.ProviderID = ""
		ExpectApplied(ctx, env.Client, nc)
		cluster.UpdateNodeClaim(nc)
		return nc
	}
	liveNodeClaims := func() []v1.NodeClaim {
		list := &v1.NodeClaimList{}
		Expect(env.Client.List(ctx, list)).To(Succeed())
		return lo.Filter(list.Items, func(nc v1.NodeClaim, _ int) bool { return nc.DeletionTimestamp.IsZero() })
	}
	reconcileState := func(nodeClaims ...*v1.NodeClaim) {
		for _, nc := range nodeClaims {
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
		}
	}

	BeforeEach(func() {
		nodePool = test.StaticNodePool()
	})

	It("should not deprovision while a drift replacement is staged for a pending candidate", func() {
		candidate := withReplicas(3, 3)[0]
		beforePatch := candidate.DeepCopy()
		candidate.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
		ExpectApplied(ctx, env.Client, candidate)
		cluster.NodePoolState.Apply(candidate)
		// The informer processes an event queued before the patch, then the patch.
		cluster.UpdateNodeClaim(beforePatch)
		reconcileState(candidate)
		unresolved() // the replacement

		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(liveNodeClaims()).To(HaveLen(4), "deprovisioner deleted a NodeClaim while a drift command was mid-flight")
	})

	It("should never scale below replicas after deleting unresolved NodeClaims", func() {
		withReplicas(2, 2)
		u1, u2 := unresolved(), unresolved()
		ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		Expect(liveNodeClaims()).To(HaveLen(2))
		reconcileState(u1, u2)
		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

		Expect(liveNodeClaims()).To(HaveLen(2), "static NodePool scaled below spec.replicas")
	})

	It("should keep a deprovisioned NodeClaim counted as deleting when the informer processes a stale version", func() {
		nodeClaims := withReplicas(2, 3)
		stale := lo.Map(nodeClaims, func(nc *v1.NodeClaim, _ int) *v1.NodeClaim { return nc.DeepCopy() })
		for _, nc := range nodeClaims {
			nc.Finalizers = append(nc.Finalizers, v1.TerminationFinalizer)
			ExpectApplied(ctx, env.Client, nc)
		}

		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 1, 0)
		live := lo.Map(liveNodeClaims(), func(nc v1.NodeClaim, _ int) string { return nc.Name })
		deleted, _ := lo.Find(stale, func(nc *v1.NodeClaim) bool { return !lo.Contains(live, nc.Name) })
		Expect(deleted).ToNot(BeNil())

		cluster.UpdateNodeClaim(deleted)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 1, 0)
	})

	It("should not leave a phantom deleting NodeClaim when the NodeClaim was already gone", func() {
		nodeClaims := withReplicas(1, 2)
		deprovisioner := static.NewController(&deletedUnderneathClient{Client: env.Client}, cluster, cloudProvider, env.Clock, recorder)

		ExpectObjectReconciled(ctx, env.Client, deprovisioner, nodePool)
		Expect(liveNodeClaims()).To(HaveLen(1))
		reconcileState(nodeClaims...)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
	})
})
