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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

var _ = Describe("NodeClaim-first Nodes", func() {
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	BeforeEach(func() {
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Status: v1.NodeClaimStatus{
				Capacity:    corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
			},
		})
	})
	// apply creates the objects through the API server and feeds them to cluster state through its informers.
	apply := func(objs ...client.Object) {
		GinkgoHelper()
		ExpectApplied(ctx, env.Client, objs...)
		for _, obj := range objs {
			switch obj.(type) {
			case *v1.NodeClaim:
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(obj))
			case *corev1.Node:
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(obj))
			case *corev1.Pod:
				ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(obj))
			}
		}
	}
	onlyNode := func() *karpentercluster.StateNode {
		GinkgoHelper()
		ExpectStateNodeCount("==", 1)
		for n := range cluster.Nodes() {
			return n
		}
		return nil
	}

	It("should link a Node to the NodeClaim its owner reference names, whatever its providerID", func() {
		apply(nodeClaim)
		node.Spec.ProviderID = test.RandomProviderID()
		node = nodeclaimutils.UpdateNodeOwnerReferences(ExpectExists(ctx, env.Client, nodeClaim), node)
		apply(node)

		n := onlyNode()
		Expect(n.NodeClaim.Name).To(Equal(nodeClaim.Name))
		Expect(n.Node).ToNot(BeNil())
		Expect(n.Node.Name).To(Equal(node.Name))
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, 1, nil)
	})
	It("should not read an orphaned Node, with the NodePool label and no NodeClaim, as a node or capacity", func() {
		apply(node)

		ExpectStateNodeCount("==", 0)
		Expect(cluster.NodePoolState.Capacity(nodePool.Name)).To(BeEmpty())
		Expect(cluster.NodePoolState.LaunchedNodes(nodePool.Name)).To(BeEmpty())
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, 0, nil)
		// The Node is still a Node cluster state tracks, so cluster state can sync on it.
		Expect(cluster.ClusterHealth().Nodes).To(Equal(1))
	})
	It("should read an unmanaged Node, with no owning NodeClaim and no NodePool label, as its own node", func() {
		unmanaged := test.Node(test.NodeOptions{
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			ProviderID:  test.RandomProviderID(),
		})
		apply(nodeClaim, node, unmanaged)

		ExpectStateNodeCount("==", 2)
		n := ExpectStateNodeExists(cluster, unmanaged)
		Expect(n.Managed()).To(BeFalse())
		Expect(n.NodeClaim).To(BeNil())
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, 2, nil)

		// Once a NodeClaim's owner reference names it, it's that NodeClaim's node. The providerID links nothing.
		claiming := test.NodeClaim(v1.NodeClaim{Status: v1.NodeClaimStatus{ProviderID: unmanaged.Spec.ProviderID}})
		apply(claiming)
		ExpectStateNodeCount("==", 3)
		apply(nodeclaimutils.UpdateNodeOwnerReferences(ExpectExists(ctx, env.Client, claiming), ExpectExists(ctx, env.Client, unmanaged)))
		ExpectStateNodeCount("==", 2)
		Expect(ExpectStateNodeExists(cluster, unmanaged).NodeClaim.Name).To(Equal(claiming.Name))
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, 2, nil)
	})
	It("should keep intent kept about a NodeClaim before its launch once it launches", func() {
		providerID := nodeClaim.Status.ProviderID
		nodeClaim.Status.ProviderID = ""
		apply(nodeClaim)
		ExpectStateNodeCount("==", 0)

		cs.Deletion.Mark(nodeClaim.Name)
		cs.Nomination.Nominate(ctx, nodeClaim.Name)
		cs.BufferPods.Replace(map[string]int{nodeClaim.Name: 2})

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		nodeClaim.Status.ProviderID = providerID
		apply(nodeClaim, node)

		n := onlyNode()
		Expect(n.MarkedForDeletion()).To(BeTrue())
		Expect(n.Nominated(env.Clock)).To(BeTrue())
		Expect(cluster.IsNodeNominated(nodeClaim.Name)).To(BeTrue())
		Expect(bufferPodCount(nodeClaim.Name)).To(Equal(2))
	})
	It("should count an initialized NodeClaim whose Node is missing as NotReady for disruption budgets", func() {
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cluster.NodePoolState.Disruption(nodePool.Name)).To(Equal(karpentercluster.NodePoolDisruption{Nodes: 1}))

		ExpectMakeNodesNotReady(ctx, env.Client, env.Clock, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		Expect(cluster.NodePoolState.Disruption(nodePool.Name)).To(Equal(karpentercluster.NodePoolDisruption{Nodes: 1, NotReady: 1}))

		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		Expect(cluster.NodePoolState.Disruption(nodePool.Name)).To(Equal(karpentercluster.NodePoolDisruption{Nodes: 1, NotReady: 1}))

		// A disrupting NodeClaim counts as Disrupting, not NotReady.
		cs.Deletion.Mark(nodeClaim.Name)
		Expect(cluster.NodePoolState.Disruption(nodePool.Name)).To(Equal(karpentercluster.NodePoolDisruption{Nodes: 1, Disrupting: 1}))
	})
	It("should count a registered node's Node capacity against its NodePool's limits once it's initialized", func() {
		node.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cluster.NodePoolState.Capacity(nodePool.Name)[corev1.ResourceCPU]).To(BeComparableTo(resource.MustParse("3")))

		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		Expect(cluster.NodePoolState.Capacity(nodePool.Name)[corev1.ResourceCPU]).To(BeComparableTo(resource.MustParse("4")))
	})
	It("should drop the intent kept about a NodeClaim once it's gone", func() {
		apply(nodeClaim, node)
		cs.Deletion.Mark(nodeClaim.Name)
		ExpectDeleted(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

		apply(test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: nodeClaim.Name}, Status: v1.NodeClaimStatus{ProviderID: test.RandomProviderID()}}))
		Expect(onlyNode().MarkedForDeletion()).To(BeFalse())
	})
})
