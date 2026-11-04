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

package counter_test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/nodepool/counter"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var nodePoolController *counter.Controller
var nodeClaimInformer *controllertest.FakeInformer
var ctx context.Context
var env *test.Environment
var cloudProvider *fake.CloudProvider

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Counter")
}

var _ = BeforeSuite(func() {
	cloudProvider = fake.NewCloudProvider()
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var nodePool *v1.NodePool
var nodeClaim, nodeClaim2 *v1.NodeClaim
var expected corev1.ResourceList

func observe(nodeClaims ...*v1.NodeClaim) {
	GinkgoHelper()
	for _, nc := range nodeClaims {
		nodeClaimInformer.Add(ExpectExists(ctx, env.Client, nc))
	}
}

func deleteNodeClaims(nodeClaims ...*v1.NodeClaim) {
	GinkgoHelper()
	for _, nc := range nodeClaims {
		ExpectDeleted(ctx, env.Client, nc)
		nodeClaimInformer.Delete(nc)
	}
}

var _ = Describe("Counter", func() {
	BeforeEach(func() {
		informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
		usage := cluster.NewNodePoolUsage(env.Client, informers)
		nodePoolController = counter.NewController(env.Client, cloudProvider, cluster.NewNodePools(env.Client, usage), usage)
		started, stop := context.WithCancel(ctx)
		DeferCleanup(stop)
		go func() { _ = usage.NodeClaims.Start(started) }()
		// The first List against the test environment can take about a second.
		Eventually(usage.Synced).WithTimeout(10 * time.Second).Should(BeTrue())
		nodeClaimInformer = lo.Must(informers.FakeInformerFor(ctx, &v1.NodeClaim{}))

		cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
		nodePool = test.NodePool()
		instanceType := cloudProvider.InstanceTypes[0]
		nodeClaim = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: instanceType.Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourcePods:   resource.MustParse("256"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		})
		nodeClaim2 = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: instanceType.Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourcePods:   resource.MustParse("1000"),
					corev1.ResourceMemory: resource.MustParse("5Gi"),
				},
			},
		})
		expected = counter.BaseResources.DeepCopy()
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)
	})
	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
	})
	It("should ignore NodePools which aren't managed by this instance of Karpenter", func() {
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
			NodeClassRef: &v1.NodeClassReference{
				Group: "karpenter.test.sh",
				Kind:  "UnmanagedNodeClass",
				Name:  "default",
			},
		}}}})
		nodeClaim = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey: nodePool.Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourcePods:   resource.MustParse("256"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
			Spec: v1.NodeClaimSpec{
				NodeClassRef: nodePool.Spec.Template.Spec.NodeClassRef,
			},
		})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim)
		observe(nodeClaim)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)
		Expect(nodePool.Status.Resources).To(BeNil())
	})
	It("should set well-known resource to zero when no nodes exist in the cluster", func() {
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)

		Expect(nodePool.Status.Resources).To(BeComparableTo(expected))
	})
	It("should count the NodeClaim's capacity, not its Node's", func() {
		node := test.NodeClaimLinkedNode(nodeClaim)
		node.Status.Capacity = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourcePods:   resource.MustParse("512"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		}
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		observe(nodeClaim)

		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)

		expected = resources.MergeInto(expected, nodeClaim.Status.Capacity)
		expected[resources.Node] = resource.MustParse("1")
		Expect(nodePool.Status.Resources).To(BeComparableTo(expected))
	})
	It("should decrease the counter when an existing NodeClaim is deleted", func() {
		ExpectApplied(ctx, env.Client, nodeClaim, nodeClaim2)
		observe(nodeClaim, nodeClaim2)

		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)

		expected = resources.MergeInto(expected, corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("600m"),
			corev1.ResourcePods:   resource.MustParse("1256"),
			corev1.ResourceMemory: resource.MustParse("6Gi"),
			resources.Node:        resource.MustParse("2"),
		})
		Expect(nodePool.Status.Resources).To(BeComparableTo(expected))

		deleteNodeClaims(nodeClaim)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)

		expected = counter.BaseResources.DeepCopy()
		expected = resources.MergeInto(expected, nodeClaim2.Status.Capacity)
		expected[resources.Node] = resource.MustParse("1")
		Expect(nodePool.Status.Resources).To(BeComparableTo(expected))
	})
	It("should zero out the counter when all NodeClaims are deleted", func() {
		ExpectApplied(ctx, env.Client, nodeClaim)
		observe(nodeClaim)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))

		deleteNodeClaims(nodeClaim)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)
		Expect(nodePool.Status.Resources).To(BeComparableTo(counter.BaseResources))
	})
	It("should stop counting a NodeClaim once it's disrupted", func() {
		ExpectApplied(ctx, env.Client, nodeClaim, nodeClaim2)
		nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
		ExpectApplied(ctx, env.Client, nodeClaim)
		observe(nodeClaim, nodeClaim2)

		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)
		Expect(*nodePool.Status.Nodes).To(Equal(int64(1)))
	})
	It("should stop counting a NodeClaim once it's being deleted", func() {
		nodeClaim.Finalizers = []string{v1.TerminationFinalizer}
		ExpectApplied(ctx, env.Client, nodeClaim, nodeClaim2)
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		observe(nodeClaim, nodeClaim2)

		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		nodePool = ExpectExists(ctx, env.Client, nodePool)
		Expect(*nodePool.Status.Nodes).To(Equal(int64(1)))
	})

	Context("Status.Nodes Field", func() {
		It("should set Status.Nodes to zero when no nodes exist", func() {
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
			nodePool = ExpectExists(ctx, env.Client, nodePool)

			Expect(nodePool.Status.Nodes).ToNot(BeNil())
			Expect(*nodePool.Status.Nodes).To(Equal(int64(0)))
		})
		It("should update Status.Nodes when NodeClaims are added and removed", func() {
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
			nodePool = ExpectExists(ctx, env.Client, nodePool)
			Expect(*nodePool.Status.Nodes).To(Equal(int64(0)))

			ExpectApplied(ctx, env.Client, nodeClaim)
			observe(nodeClaim)
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
			nodePool = ExpectExists(ctx, env.Client, nodePool)
			Expect(*nodePool.Status.Nodes).To(Equal(int64(1)))

			ExpectApplied(ctx, env.Client, nodeClaim2)
			observe(nodeClaim2)
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
			nodePool = ExpectExists(ctx, env.Client, nodePool)
			Expect(*nodePool.Status.Nodes).To(Equal(int64(2)))

			deleteNodeClaims(nodeClaim)
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
			nodePool = ExpectExists(ctx, env.Client, nodePool)
			Expect(*nodePool.Status.Nodes).To(Equal(int64(1)))

			deleteNodeClaims(nodeClaim2)
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
			nodePool = ExpectExists(ctx, env.Client, nodePool)
			Expect(*nodePool.Status.Nodes).To(Equal(int64(0)))
		})
		It("should count the NodeClaims of a static NodePool, not its replicas", func() {
			staticNodePool := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: new(int64(3))}})
			ExpectApplied(ctx, env.Client, staticNodePool)
			staticNodeClaims := []*v1.NodeClaim{}
			for range 2 {
				nc := test.NodeClaim(v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: staticNodePool.Name}},
					Status:     v1.NodeClaimStatus{Capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("300m")}},
				})
				ExpectApplied(ctx, env.Client, nc)
				staticNodeClaims = append(staticNodeClaims, nc)
			}
			ExpectApplied(ctx, env.Client, nodeClaim)
			observe(append(staticNodeClaims, nodeClaim)...)

			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(staticNodePool))
			nodePool = ExpectExists(ctx, env.Client, nodePool)
			staticNodePool = ExpectExists(ctx, env.Client, staticNodePool)
			Expect(*nodePool.Status.Nodes).To(Equal(int64(1)))
			Expect(*staticNodePool.Status.Nodes).To(Equal(int64(2)))
			Expect(*staticNodePool.Spec.Replicas).To(Equal(int64(3)))
		})
	})
})
