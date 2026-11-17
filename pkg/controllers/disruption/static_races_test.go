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

package disruption_test

import (
	"context"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	staticdeprovisioning "sigs.k8s.io/karpenter/pkg/controllers/static/deprovisioning"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// goneBeforeMarkClient deletes the named NodeClaim, and has cluster state observe the deletion, on its first Get, which
// then gets NotFound.
type goneBeforeMarkClient struct {
	client.Client
	name string
	done bool
}

func (g *goneBeforeMarkClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1.NodeClaim); ok && key.Name == g.name && !g.done {
		g.done = true
		nc := &v1.NodeClaim{}
		Expect(g.Client.Get(ctx, key, nc)).To(Succeed())
		ExpectFinalizersRemoved(ctx, g.Client, nc)
		ExpectDeleted(ctx, g.Client, nc)
		cluster.DeleteNodeClaim(key.Name)
	}
	return g.Client.Get(ctx, key, obj, opts...)
}

var _ = Describe("Static capacity races", func() {
	var nodePool *v1.NodePool

	staticNodeClaimsAndNodes := func(n int, drifted bool) ([]*v1.NodeClaim, []*corev1.Node) {
		nodeClaims, nodes := test.NodeClaimsAndNodes(n, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
				Finalizers: []string{v1.TerminationFinalizer},
			},
			Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:  resource.MustParse("32"),
				corev1.ResourcePods: resource.MustParse("100"),
			}},
		})
		for i := range nodeClaims {
			if drifted {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			}
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		return nodeClaims, nodes
	}
	staticDriftCommand := func(nc *v1.NodeClaim) *disruption.Command {
		nct := scheduling.NewNodeClaimTemplate(nodePool)
		nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
		return &disruption.Command{
			Method:            disruption.NewStaticDrift(cluster, prov, cloudProvider, recorder, nodePoolBackoff),
			CreationTimestamp: env.Clock.Now(),
			ID:                uuid.New(),
			Candidates:        []*disruption.Candidate{{StateNode: ExpectStateNodeExistsForNodeClaim(cluster, nc), NodePool: nodePool}},
			Replacements:      []*disruption.Replacement{{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}}},
		}
	}
	liveNodeClaims := func() int {
		list := &v1.NodeClaimList{}
		Expect(env.Client.List(ctx, list)).To(Succeed())
		return len(lo.Filter(list.Items, func(nc v1.NodeClaim, _ int) bool { return nc.DeletionTimestamp.IsZero() }))
	}
	expectDeleting := func(nc *v1.NodeClaim) {
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nc), nc)).To(Succeed())
		Expect(nc.DeletionTimestamp.IsZero()).To(BeFalse())
	}

	BeforeEach(func() {
		nodePool = test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{
			Replicas:   new(int64(1)),
			Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: "100%"}}},
		}})
		ExpectApplied(ctx, env.Client, nodePool)
	})

	It("should not leave a phantom pending-disruption NodeClaim when the candidate is already gone", func() {
		nodeClaims, _ := staticNodeClaimsAndNodes(1, true)
		q := disruption.NewQueue(&goneBeforeMarkClient{Client: env.Client, name: nodeClaims[0].Name}, recorder, cluster, env.Clock, prov, nodePoolBackoff)

		Expect(q.StartCommand(ctx, staticDriftCommand(nodeClaims[0]))).To(Succeed())

		Expect(liveNodeClaims()).To(Equal(1))
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaims[0]))
		ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
	})

	It("should not stage a drift replacement for a NodeClaim static deprovisioning is deleting", func() {
		nodePool.Spec.Replicas = new(int64(3))
		ExpectApplied(ctx, env.Client, nodePool)
		nodeClaims, nodes := staticNodeClaimsAndNodes(3, false)
		// Only the empty node is drifted, so it's the one deprovisioned.
		nodeClaims[0].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		ExpectApplied(ctx, env.Client, nodeClaims[0])
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaims[0]))
		for _, node := range nodes[1:] {
			pod := test.Pod()
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}

		nodePool.Spec.Replicas = new(int64(2))
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectObjectReconciled(ctx, env.Client, staticdeprovisioning.NewController(env.Client, cluster, cloudProvider, env.Clock, recorder), nodePool)
		expectDeleting(nodeClaims[0])

		ExpectSingletonReconciled(ctx, disruptionController)

		Expect(liveNodeClaims()).To(BeNumerically("<=", 2), "static NodePool exceeded spec.replicas")
	})

	It("should not mark a candidate Active after its deletion succeeded at the command timeout", func() {
		nodeClaims, _ := staticNodeClaimsAndNodes(1, true)
		cmd := staticDriftCommand(nodeClaims[0])
		Expect(queue.StartCommand(ctx, cmd)).To(Succeed())

		replacement := &v1.NodeClaim{}
		Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd.Replacements[0].Name}, replacement)).To(Succeed())
		replacement, replacementNode := ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cluster, cloudProvider, replacement)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController,
			[]*corev1.Node{replacementNode}, []*v1.NodeClaim{replacement})

		// The replacement initialized just as the command reached its timeout.
		env.Clock.Step(11 * time.Minute)
		ExpectObjectReconciled(ctx, env.Client, queue, nodeClaims[0])

		expectDeleting(nodeClaims[0])
		active, _, _ := cluster.NodePoolState.GetNodeCount(nodePool.Name)
		Expect(active).To(Equal(1), "deleted candidate re-counted as Active")
		Expect(cmd.Succeeded).To(BeTrue(), "command whose candidate was deleted was treated as failed")
	})

	It("should not leave the candidate pending disruption when the replacement launch fails", func() {
		nodeClaims, _ := staticNodeClaimsAndNodes(1, true)
		cmd := staticDriftCommand(nodeClaims[0])
		// Without the NodePool, the replacement Create fails.
		ExpectDeleted(ctx, env.Client, nodePool)

		Expect(queue.StartCommand(ctx, cmd)).ToNot(Succeed())
		ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
	})
})
