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

	It("should link a Node without an owner reference to its NodeClaim by providerID, and count it once", func() {
		Expect(node.OwnerReferences).To(BeEmpty())
		apply(nodeClaim, node)

		n := onlyNode()
		Expect(n.NodeClaim.Name).To(Equal(nodeClaim.Name))
		Expect(n.Node).ToNot(BeNil())
		Expect(n.Node.Name).To(Equal(node.Name))
		Expect(n.Managed()).To(BeTrue())
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, 1, nil)
		Expect(cluster.NodePoolNodes(nodePool.Name)).To(HaveLen(1))
	})
	It("should link a Node without a NodePool label and without an owner reference by providerID, never as unmanaged", func() {
		delete(node.Labels, v1.NodePoolLabelKey)
		apply(nodeClaim, node)

		n := onlyNode()
		Expect(n.NodeClaim).ToNot(BeNil())
		Expect(n.Node.Name).To(Equal(node.Name))
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, 1, nil)
	})
	It("should attribute pods bound before registration through the providerID link", func() {
		pod := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		}})
		apply(nodeClaim, node, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
		apply(pod)

		Expect(onlyNode().PodRequests()[corev1.ResourceCPU]).To(BeComparableTo(resource.MustParse("1")))
		// Registration adds the owner reference; the usage stays with the node.
		node = ExpectExists(ctx, env.Client, node)
		apply(nodeclaimutils.UpdateNodeOwnerReferences(ExpectExists(ctx, env.Client, nodeClaim), node))
		Expect(onlyNode().PodRequests()[corev1.ResourceCPU]).To(BeComparableTo(resource.MustParse("1")))
	})
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
		Expect(cluster.NodePoolResourcesFor(nodePool.Name)).To(BeEmpty())
		Expect(cluster.NodePoolNodes(nodePool.Name)).To(BeEmpty())
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, 0, nil)
		// The Node is still a Node cluster state tracks, so cluster state can sync on it.
		Expect(cluster.NodeHealth().Cluster().Nodes).To(Equal(1))
	})
	It("should read an unmanaged Node, with no NodeClaim, NodePool label or claimed providerID, as its own node", func() {
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

		// Once a NodeClaim claims its providerID, it's that NodeClaim's node.
		claiming := test.NodeClaim(v1.NodeClaim{Status: v1.NodeClaimStatus{ProviderID: unmanaged.Spec.ProviderID}})
		apply(claiming)
		ExpectStateNodeCount("==", 2)
		Expect(ExpectStateNodeExists(cluster, unmanaged).NodeClaim.Name).To(Equal(claiming.Name))
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, 2, nil)
	})
	It("should keep intent kept about a NodeClaim before its launch once it launches", func() {
		providerID := nodeClaim.Status.ProviderID
		nodeClaim.Status.ProviderID = ""
		apply(nodeClaim)
		ExpectStateNodeCount("==", 0)

		cluster.MarkForDeletion(nodeClaim.Name)
		cluster.NominateNodeForPod(ctx, nodeClaim.Name)
		cluster.UpdateBufferPodCounts(map[string]int{nodeClaim.Name: 2})

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		nodeClaim.Status.ProviderID = providerID
		apply(nodeClaim, node)

		n := onlyNode()
		Expect(n.MarkedForDeletion()).To(BeTrue())
		Expect(n.Nominated(env.Clock)).To(BeTrue())
		Expect(cluster.IsNodeNominated(nodeClaim.Name)).To(BeTrue())
		Expect(cluster.BufferPodCount(nodeClaim.Name)).To(Equal(2))
		name, ok := cluster.NodeClaimForProviderID(providerID)
		Expect(ok).To(BeTrue())
		Expect(name).To(Equal(nodeClaim.Name))
	})
	It("should count an initialized NodeClaim whose Node is missing as NotReady for disruption budgets", func() {
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cluster.NodePoolDisruption(nodePool.Name)).To(Equal(karpentercluster.NodePoolDisruption{Nodes: 1}))

		ExpectMakeNodesNotReady(ctx, env.Client, env.Clock, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		Expect(cluster.NodePoolDisruption(nodePool.Name)).To(Equal(karpentercluster.NodePoolDisruption{Nodes: 1, NotReady: 1}))

		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		Expect(cluster.NodePoolDisruption(nodePool.Name)).To(Equal(karpentercluster.NodePoolDisruption{Nodes: 1, NotReady: 1}))

		// A disrupting NodeClaim counts as Disrupting, not NotReady.
		cluster.MarkForDeletion(nodeClaim.Name)
		Expect(cluster.NodePoolDisruption(nodePool.Name)).To(Equal(karpentercluster.NodePoolDisruption{Nodes: 1, Disrupting: 1}))
	})
	It("should count a registered node's Node capacity against its NodePool's limits once it's initialized", func() {
		node.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cluster.NodePoolResourcesFor(nodePool.Name)[corev1.ResourceCPU]).To(BeComparableTo(resource.MustParse("3")))

		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		Expect(cluster.NodePoolResourcesFor(nodePool.Name)[corev1.ResourceCPU]).To(BeComparableTo(resource.MustParse("4")))
	})
	It("should drop the intent kept about a NodeClaim once it's gone", func() {
		apply(nodeClaim, node)
		cluster.MarkForDeletion(nodeClaim.Name)
		ExpectDeleted(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

		apply(test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: nodeClaim.Name}, Status: v1.NodeClaimStatus{ProviderID: test.RandomProviderID()}}))
		Expect(onlyNode().MarkedForDeletion()).To(BeFalse())
	})
})
