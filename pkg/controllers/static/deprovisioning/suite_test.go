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
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"

	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	static "sigs.k8s.io/karpenter/pkg/controllers/static/deprovisioning"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var (
	cs            karpentercluster.State
	ctx           context.Context
	cluster       *state.Cluster
	cloudProvider *fake.CloudProvider
	controller    *static.Controller
	env           *test.Environment
	recorder      *test.EventRecorder
)

type failingClient struct {
	client.Client
}

func (f *failingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*v1.NodeClaim); ok {
		return fmt.Errorf("simulated error deleting nodeclaims")
	}
	return f.Client.Delete(ctx, obj, opts...)
}

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controllers/Deprovisioning/Static")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	cs = karpentercluster.New(ctx, env.Clock, env.Overlay, cloudProvider)
	cluster = state.NewCluster(env.Clock, env.Overlay, cs.Nodes, cs.NodePoolState, cs.CapacityQuotaState, cs.PodScheduling)
	recorder = test.NewEventRecorder()
	controller = static.NewController(env.Overlay, cluster, cloudProvider, env.Clock, recorder)
})

var _ = BeforeEach(func() {
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider.Reset()
	cluster.Reset()

	// ensure any waiters on our clock are allowed to proceed before resetting our clock time
	for env.Clock.HasWaiters() {
		env.Clock.Step(1 * time.Minute)
	}
	env.Clock.SetTime(time.Now())
	recorder.Reset()
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
	cloudProvider.Reset()
	cluster.Reset()
})

var _ = Describe("Static Deprovisioning Controller", func() {
	Context("Reconcile", func() {
		It("should return early if nodepool is not managed by cloud provider", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(1))
			nodePool.Spec.Template.Spec.NodeClassRef = &v1.NodeClassReference{
				Group: "test.group",
				Kind:  "UnmanagedNodeClass",
				Name:  "test",
			}
			nodeClaims, nodes := test.NodeClaimsAndNodes(1, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						v1.NodeInitializedLabelKey:     "true",
						corev1.LabelInstanceTypeStable: "stable.instance",
					},
				},
				Status: v1.NodeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10"),
						corev1.ResourceMemory: resource.MustParse("1000Mi"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0], nodes[0])

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not delete any NodeClaims
			existingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, existingNodeClaims)).To(Succeed())
			Expect(existingNodeClaims.Items).To(HaveLen(1))
		})
		It("should return early if nodepool replicas is nil", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = nil
			ExpectApplied(ctx, env.Client, nodePool)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

			Expect(result.RequeueAfter).To(BeZero())
		})
		It("should return early if nodepool is being deleted", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(1))

			// Create 2 nodeclaims (more than desired replicas of 1)
			nodeClaims, nodes := test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						v1.NodeInitializedLabelKey:     "true",
						corev1.LabelInstanceTypeStable: "stable.instance",
					},
				},
				Status: v1.NodeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10"),
						corev1.ResourceMemory: resource.MustParse("1000Mi"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0], nodeClaims[1], nodes[0], nodes[1])
			ExpectDeletionTimestampSet(ctx, env.Client, nodePool)

			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})
			Expect(cluster.Nodes()).To(HaveLen(2))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not delete any NodeClaims since nodepool is being deleted
			existingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, existingNodeClaims)).To(Succeed())
			Expect(existingNodeClaims.Items).To(HaveLen(2))
		})
		It("should return early if current node count is less than or equal to desired replicas", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(3))

			// Create 2 nodes (less than desired 3)
			nodeClaims, nodes := test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						v1.NodeInitializedLabelKey:     "true",
						corev1.LabelInstanceTypeStable: "stable.instance",
					},
				},
				Status: v1.NodeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10"),
						corev1.ResourceMemory: resource.MustParse("1000Mi"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0], nodeClaims[1], nodes[0], nodes[1])

			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})
			Expect(cluster.Nodes()).To(HaveLen(2))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

			Expect(result.RequeueAfter).To(BeZero())

			// Should not delete any NodeClaims
			existingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, existingNodeClaims)).To(Succeed())
			Expect(existingNodeClaims.Items).To(HaveLen(2))
		})
		It("should only consider running nodeclaims and not deleting nodeclaims", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(1))

			nodeClaims, nodes := test.NodeClaimsAndNodes(4, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						v1.NodeInitializedLabelKey:     "true",
						corev1.LabelInstanceTypeStable: "stable.instance",
					},
				},
				Status: v1.NodeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10"),
						corev1.ResourceMemory: resource.MustParse("1000Mi"),
					},
				},
			})
			ExpectApplied(ctx, env.Client, nodePool)
			for i := range 4 {
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)
			Expect(cluster.Nodes()).To(HaveLen(4))

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

			// If 3 of the nodes are being disrupted then do not deprovision any
			for i := range 3 {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDisruptionReason)
				ExpectApplied(ctx, env.Client, nodeClaims[i])
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 3, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should terminate 0 NodeClaims as 3 NodeClaims are deleting
			remainingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
			Expect(remainingNodeClaims.Items).To(HaveLen(4))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 3, 0)
		})
		It("should terminate excess nodeclaims when current count exceeds desired replicas", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(2))

			nodeClaims, nodes := test.NodeClaimsAndNodes(4, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						v1.NodeInitializedLabelKey:     "true",
						corev1.LabelInstanceTypeStable: "stable.instance",
					},
				},
				Status: v1.NodeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10"),
						corev1.ResourceMemory: resource.MustParse("1000Mi"),
					},
				},
			})
			ExpectApplied(ctx, env.Client, nodePool)
			for i := range 4 {
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}
			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)
			Expect(cluster.Nodes()).To(HaveLen(4))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should terminate 2 NodeClaims (4 current - 2 desired = 2 to terminate)
			remainingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
			Expect(remainingNodeClaims.Items).To(HaveLen(2))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)
		})
		It("should handle zero replicas by terminating all nodeclaims", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(0))

			nodeClaims, nodes := test.NodeClaimsAndNodes(3, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						v1.NodeInitializedLabelKey:     "true",
						corev1.LabelInstanceTypeStable: "stable.instance",
					},
				},
				Status: v1.NodeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10"),
						corev1.ResourceMemory: resource.MustParse("1000Mi"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool)
			for i := range 3 {
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}
			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)
			Expect(cluster.Nodes()).To(HaveLen(3))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should terminate all NodeClaims
			remainingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
			Expect(remainingNodeClaims.Items).To(HaveLen(0))

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		})
		It("should handle no active nodeclaims gracefully", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(0))
			ExpectApplied(ctx, env.Client, nodePool)

			// Update cluster state with no nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{}, []*v1.NodeClaim{})

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			existingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, existingNodeClaims)).To(Succeed())
			Expect(existingNodeClaims.Items).To(HaveLen(0))
		})
		Context("Failing Scenarios", func() {
			It("should return error when nodeclaim deletion fails", func() {
				nodePool := test.StaticNodePool()
				nodePool.Spec.Replicas = new(int64(1))
				ExpectApplied(ctx, env.Client, nodePool)

				failingController := static.NewController(&failingClient{Client: env.Overlay}, cluster, cloudProvider, env.Clock, recorder)

				// Create 3 nodeclaims, so 2 need to be terminated
				nodeClaims, nodes := test.NodeClaimsAndNodes(3, v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							v1.NodePoolLabelKey:            nodePool.Name,
							v1.NodeInitializedLabelKey:     "true",
							corev1.LabelInstanceTypeStable: "stable.instance",
						},
					},
					Status: v1.NodeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10"),
							corev1.ResourceMemory: resource.MustParse("1000Mi"),
						},
					},
				})

				ExpectApplied(ctx, env.Client, nodePool)
				for i := range 3 {
					ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
				}

				// Update cluster state to track the nodes
				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)
				Expect(cluster.Nodes()).To(HaveLen(3))

				// Verify StateNodePool Has been updated
				ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)

				_, err := failingController.Reconcile(ctx, nodePool)
				Expect(err).To(HaveOccurred())

				// Verify that some nodeclaims still exist (deletion didn't complete as expected)
				remainingNodeClaims := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())

				// At least one nodeclaim should still be active due to the finalizer
				activeNodeClaims := lo.Filter(remainingNodeClaims.Items, func(nc v1.NodeClaim, _ int) bool {
					return nc.DeletionTimestamp.IsZero()
				})
				Expect(len(activeNodeClaims)).To(BeNumerically(">", 1)) // More than desired replicas (1)
				// Verify StateNodePool Has been updated
				ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)
			})
		})
		Context("Deprovision Candidate Selection", func() {
			It("should prioritize nodeclaims that have unresolved providerID", func() {
				nodePool := test.StaticNodePool()
				nodePool.Spec.Replicas = new(int64(2))

				nodeClaims, nodes := test.NodeClaimsAndNodes(2, v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							v1.NodePoolLabelKey:            nodePool.Name,
							v1.NodeInitializedLabelKey:     "true",
							corev1.LabelInstanceTypeStable: "stable.instance",
						},
					},
					Status: v1.NodeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10"),
							corev1.ResourceMemory: resource.MustParse("1000Mi"),
						},
					},
				})

				// Create 2 unresolved NodeClaims (no ProviderID, no nodes)
				unresolvedNodeClaim1 := test.NodeClaim(v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							v1.NodePoolLabelKey: nodePool.Name,
						},
					},
					Status: v1.NodeClaimStatus{},
				})
				unresolvedNodeClaim1.Status.ProviderID = "" // either createfleet failed or has not been called yet
				unresolvedNodeClaim2 := test.NodeClaim(v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							v1.NodePoolLabelKey: nodePool.Name,
						},
					},
					Status: v1.NodeClaimStatus{},
				})
				unresolvedNodeClaim2.Status.ProviderID = ""

				ExpectApplied(ctx, env.Client, nodePool)
				ExpectApplied(ctx, env.Client, nodes[0], nodes[1], nodeClaims[0], nodeClaims[1])
				ExpectApplied(ctx, env.Client, unresolvedNodeClaim1, unresolvedNodeClaim2)

				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)
				Expect(cluster.Nodes()).To(HaveLen(2))

				ncCount := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, ncCount)).To(Succeed())
				Expect(ncCount.Items).To(HaveLen(4))

				// Verify StateNodePool has been updated (resolved + unresolved)
				ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

				result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(result.RequeueAfter).To(BeZero())

				// Should terminate 3 NodeClaims (3 total - 1 desired = 2 to terminate)
				remainingNodeClaims := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
				Expect(remainingNodeClaims.Items).To(HaveLen(2))
				activeNodeClaims := lo.Filter(remainingNodeClaims.Items, func(nc v1.NodeClaim, _ int) bool {
					return nc.DeletionTimestamp.IsZero()
				})
				Expect(activeNodeClaims).To(HaveLen(2))
				Expect(activeNodeClaims[0].Status.ProviderID).NotTo(BeEmpty())
				Expect(activeNodeClaims[1].Status.ProviderID).NotTo(BeEmpty())

				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)
			})
			It("should prioritize empty nodes (with only daemonset pods) for termination", func() {
				nodePool := test.StaticNodePool()
				nodePool.Spec.Replicas = new(int64(2))

				nodeClaims, nodes := test.NodeClaimsAndNodes(4, v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							v1.NodePoolLabelKey:            nodePool.Name,
							v1.NodeInitializedLabelKey:     "true",
							corev1.LabelInstanceTypeStable: "stable.instance",
						},
					},
					Status: v1.NodeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10"),
							corev1.ResourceMemory: resource.MustParse("1000Mi"),
						},
					},
				})
				ExpectApplied(ctx, env.Client, nodePool)

				// Nodes 0 and 2: Add only DaemonSet pods (reschedulable)
				for i := range 4 {
					pod := test.Pod(test.PodOptions{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: lo.Ternary(i == 0 || i == 2, "kube-system", "default"),
							OwnerReferences: lo.Ternary(i == 0 || i == 2,
								[]metav1.OwnerReference{{
									APIVersion: "apps/v1",
									Kind:       "DaemonSet",
									Name:       "test-daemonset",
									UID:        "test-uid",
								}},
								nil),
						},
						NodeName: nodes[i].Name,
					})
					ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
					ExpectApplied(ctx, env.Client, pod)
				}

				// Update cluster state to track the nodes
				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)
				Expect(cluster.Nodes()).To(HaveLen(4))

				// Verify StateNodePool Has been updated
				ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

				result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(result.RequeueAfter).To(BeZero())

				// Should terminate 2 NodeClaims (4 current - 2 desired = 2 to terminate)
				remainingNodeClaims := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())

				activeNodeClaims := lo.Filter(remainingNodeClaims.Items, func(nc v1.NodeClaim, _ int) bool {
					return nc.DeletionTimestamp.IsZero()
				})
				activeNodeClaimNames := lo.Map(activeNodeClaims, func(nc v1.NodeClaim, _ int) string {
					return nc.Name
				})
				Expect(activeNodeClaimNames).To(HaveLen(2))
				Expect(activeNodeClaimNames).To(ContainElements(nodeClaims[1].Name, nodeClaims[3].Name))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

			})
			It("should terminate non-empty nodes when empty nodes are insufficient", func() {
				nodePool := test.StaticNodePool()
				nodePool.Spec.Replicas = new(int64(1))

				nodeClaims, nodes := test.NodeClaimsAndNodes(4, v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							v1.NodePoolLabelKey:            nodePool.Name,
							v1.NodeInitializedLabelKey:     "true",
							corev1.LabelInstanceTypeStable: "stable.instance",
						},
					},
					Status: v1.NodeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10"),
							corev1.ResourceMemory: resource.MustParse("1000Mi"),
						},
					},
				})
				ExpectApplied(ctx, env.Client, nodePool)
				for i := range 4 {
					ExpectApplied(ctx, env.Client, nodes[i], nodeClaims[i])
				}

				pod1 := test.Pod(test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{},
					NodeName:   nodes[0].Name,
				})
				pod2 := test.Pod(test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{},
					NodeName:   nodes[2].Name,
				})

				ExpectApplied(ctx, env.Client, pod1, pod2)

				// Update cluster state to track the nodes
				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)
				Expect(cluster.Nodes()).To(HaveLen(4))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

				result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(result.RequeueAfter).To(BeZero())

				// Should terminate 3 NodeClaims (4 current - 1 desired = 3 to terminate)
				remainingNodeClaims := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
				Expect(remainingNodeClaims.Items).To(HaveLen(1))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

			})
			Describe("candidates from the NodePool's index", func() {
				nodeClaimsAndNodesFor := func(nodePool *v1.NodePool, n int) ([]*v1.NodeClaim, []*corev1.Node) {
					return test.NodeClaimsAndNodes(n, v1.NodeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								v1.NodePoolLabelKey:            nodePool.Name,
								v1.NodeInitializedLabelKey:     "true",
								corev1.LabelInstanceTypeStable: "stable.instance",
							},
						},
						Status: v1.NodeClaimStatus{
							Capacity: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("10"),
								corev1.ResourceMemory: resource.MustParse("1000Mi"),
							},
						},
					})
				}
				unresolvedFor := func(nodePool *v1.NodePool) *v1.NodeClaim {
					nc := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
					nc.Status.ProviderID = ""
					return nc
				}
				daemonSetPod := func(node *corev1.Node, annotations map[string]string) *corev1.Pod {
					return test.Pod(test.PodOptions{
						ObjectMeta: metav1.ObjectMeta{
							Namespace:   "kube-system",
							Annotations: annotations,
							OwnerReferences: []metav1.OwnerReference{{
								APIVersion: "apps/v1",
								Kind:       "DaemonSet",
								Name:       "test-daemonset",
								UID:        "test-uid",
							}},
						},
						NodeName: node.Name,
					})
				}
				// remaining returns the NodeClaims the controller didn't delete.
				remaining := func() []string {
					var ncl v1.NodeClaimList
					Expect(env.Client.List(ctx, &ncl)).To(Succeed())
					return lo.FilterMap(ncl.Items, func(nc v1.NodeClaim, _ int) (string, bool) { return nc.Name, nc.DeletionTimestamp.IsZero() })
				}

				It("should only pick candidates from the reconciled NodePool", func() {
					nodePool, other := test.StaticNodePool(), test.StaticNodePool()
					nodePool.Spec.Replicas = new(int64(1))
					other.Spec.Replicas = new(int64(3))
					nodeClaims, nodes := nodeClaimsAndNodesFor(nodePool, 2)
					otherNodeClaims, otherNodes := nodeClaimsAndNodesFor(other, 2)
					unresolved, otherUnresolved := unresolvedFor(nodePool), unresolvedFor(other)
					ExpectApplied(ctx, env.Client, nodePool, other, unresolved, otherUnresolved)
					for i := range 2 {
						ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i], otherNodeClaims[i], otherNodes[i])
					}
					// nodes[1] runs an application pod; every other node is empty.
					ExpectApplied(ctx, env.Client, test.Pod(test.PodOptions{NodeName: nodes[1].Name}))
					ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock,
						append(nodes, otherNodes...), append(nodeClaims, otherNodeClaims...))
					ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)

					ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

					// 3 running - 1 desired: the unresolved NodeClaim first, then the empty node, all from nodePool.
					Expect(remaining()).To(ConsistOf(nodeClaims[1].Name, otherNodeClaims[0].Name, otherNodeClaims[1].Name, otherUnresolved.Name))
				})
				It("should not treat a node with only do-not-disrupt DaemonSet pods as empty", func() {
					nodePool := test.StaticNodePool()
					nodePool.Spec.Replicas = new(int64(1))
					nodeClaims, nodes := nodeClaimsAndNodesFor(nodePool, 2)
					ExpectApplied(ctx, env.Client, nodePool)
					for i := range 2 {
						ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
					}
					// Both nodes are non-empty; the do-not-disrupt DaemonSet pod ranks nodes[0] after nodes[1].
					ExpectApplied(ctx, env.Client,
						daemonSetPod(nodes[0], map[string]string{v1.DoNotDisruptAnnotationKey: "true"}),
						test.Pod(test.PodOptions{NodeName: nodes[1].Name}))
					ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)

					ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

					Expect(remaining()).To(ConsistOf(nodeClaims[0].Name))
				})
				It("should not pick a node marked for deletion", func() {
					nodePool := test.StaticNodePool()
					nodePool.Spec.Replicas = new(int64(2))
					nodeClaims, nodes := nodeClaimsAndNodesFor(nodePool, 3)
					ExpectApplied(ctx, env.Client, nodePool)
					for i := range 3 {
						ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
					}
					// nodes[0] and nodes[1] are empty, but nodes[0] is already marked for deletion.
					ExpectApplied(ctx, env.Client, test.Pod(test.PodOptions{NodeName: nodes[2].Name}))
					ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodes, nodeClaims)
					cs.Deletion.Mark(nodeClaims[0].Name)

					ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

					Expect(remaining()).To(ConsistOf(nodeClaims[0].Name, nodeClaims[2].Name))
				})
			})
			Describe("disruption cost ordering", func() {
				var (
					nodePool   *v1.NodePool
					nodes      []*corev1.Node
					nodeClaims []*v1.NodeClaim
					pods       []*corev1.Pod
				)

				remainingNames := func() []string {
					var ncl v1.NodeClaimList
					Expect(env.Client.List(ctx, &ncl, client.MatchingLabels{
						v1.NodePoolLabelKey: nodePool.Name,
					})).To(Succeed())
					out := make([]string, 0, len(ncl.Items))
					for i := range ncl.Items {
						out = append(out, ncl.Items[i].Name)
					}
					return out
				}
				name := func(i int) string { return nodeClaims[i].Name }

				BeforeEach(func() {
					nodePool = test.StaticNodePool()
					nodePool.Spec.Replicas = new(int64(8))
					ExpectApplied(ctx, env.Client, nodePool)

					nodes = nil
					nodeClaims = nil
					pods = nil

					// create two of each: low, high, dnd, ds (order matters only for indices)
					priority := []string{"low", "high", "dnd", "ds"}
					for i := range 2 {
						for _, p := range priority {
							nc, n := test.NodeClaimAndNode(v1.NodeClaim{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{
										v1.NodePoolLabelKey:        nodePool.Name,
										v1.NodeInitializedLabelKey: "true",
									},
								},
								Status: v1.NodeClaimStatus{
									ProviderID: test.RandomProviderID(),
									Capacity: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("10"),
										corev1.ResourceMemory: resource.MustParse("1000Mi"),
									},
								},
							})

							switch p {
							case "low":
								pods = append(pods, test.Pod(test.PodOptions{
									ObjectMeta: metav1.ObjectMeta{
										Name:      fmt.Sprintf("system-pod-low-%d", i),
										Namespace: "kube-system",
									},
									NodeName: n.Name,
								}))
							case "high":
								pods = append(pods,
									test.Pod(test.PodOptions{
										ObjectMeta: metav1.ObjectMeta{
											Name:      fmt.Sprintf("system-pod-high-%d", i),
											Namespace: "kube-system",
										},
										NodeName: n.Name,
									}),
									test.Pod(test.PodOptions{
										ObjectMeta: metav1.ObjectMeta{
											Name:      fmt.Sprintf("app-pod-high-%d", i),
											Namespace: "default",
										},
										NodeName: n.Name,
									}),
								)
							case "dnd":
								pods = append(pods, test.Pod(test.PodOptions{
									ObjectMeta: metav1.ObjectMeta{
										Name:      fmt.Sprintf("app-pod-dnd-%d", i),
										Namespace: "default",
										Annotations: map[string]string{
											v1.DoNotDisruptAnnotationKey: "true",
										},
									},
									NodeName: n.Name,
								}))
							case "ds":
								pods = append(pods, test.Pod(test.PodOptions{
									ObjectMeta: metav1.ObjectMeta{
										Name:      fmt.Sprintf("dmn-pod-%d", i),
										Namespace: "kube-system",
										OwnerReferences: []metav1.OwnerReference{{
											APIVersion: "apps/v1",
											Kind:       "DaemonSet",
											Name:       "test-daemonset",
											UID:        "test-uid",
										}},
									},
									NodeName: n.Name,
								}))
							}

							nodes = append(nodes, n)
							nodeClaims = append(nodeClaims, nc)
							ExpectApplied(ctx, env.Client, nc, n)
						}
					}
					for _, p := range pods {
						ExpectApplied(ctx, env.Client, p)
					}

					ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(
						ctx, env.Client, env.Clock, nodes, nodeClaims,
					)
					Expect(cluster.Nodes()).To(HaveLen(8))
				})
				DescribeTable("scales down in disruption cost order",
					func(replicas int64, expectIdx []int) {
						nodePool.Spec.Replicas = new(replicas)
						ExpectApplied(ctx, env.Client, nodePool)
						ExpectStateNodePoolCount(cluster, nodePool.Name, 8, 0, 0)

						res := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
						Expect(res.RequeueAfter).To(BeZero())
						ExpectStateNodePoolCount(cluster, nodePool.Name, int(replicas), 0, 0)

						want := make([]string, 0, len(expectIdx))
						for _, i := range expectIdx {
							want = append(want, name(i))
						}
						Eventually(remainingNames).Should(ConsistOf(want))
					},

					Entry("to 6 (drops DS-only first)", int64(6), []int{0, 1, 2, 4, 5, 6}),
					Entry("to 4 (drops low next)", int64(4), []int{1, 2, 5, 6}),
					Entry("to 2 (drops high next)", int64(2), []int{2, 6}),
					Entry("to 0 (drops do-not-disrupt last)", int64(0), []int{}),
				)
			})
		})
		Context("Helper Functions", func() {
			Describe("hasNodePoolReplicaOrStatusChanged", func() {
				It("should detect replica changes", func() {
					old := &v1.NodePool{Spec: v1.NodePoolSpec{Replicas: new(int64(5))}}
					new := &v1.NodePool{Spec: v1.NodePoolSpec{Replicas: new(int64(10))}}
					Expect(static.HasNodePoolReplicaCountChanged(old, new)).To(BeTrue())
				})
				It("should return false for identical replicas", func() {
					old := &v1.NodePool{Spec: v1.NodePoolSpec{Replicas: new(int64(5))}}
					new := old
					Expect(static.HasNodePoolReplicaCountChanged(old, new)).To(BeFalse())
				})
			})
		})
	})
})

// register registers c on a manager that never starts, which builds the stores c derives in Register as the operator
// does.
func register(c interface {
	Register(context.Context, manager.Manager) error
}) {
	GinkgoHelper()
	m, err := controllerruntime.NewManager(env.Config, controllerruntime.Options{
		Scheme:     scheme.Scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: new(true)},
	})
	Expect(err).ToNot(HaveOccurred())
	Expect(c.Register(ctx, m)).To(Succeed())
}

// drain returns the names queued, once each, in the order they were first queued.
func drain(queue workqueue.TypedRateLimitingInterface[reconcile.Request]) []string {
	var names []string
	for queue.Len() > 0 {
		req, _ := queue.Get()
		names = append(names, req.Name)
		queue.Done(req)
		queue.Forget(req)
	}
	return names
}

var _ = Describe("Static Deprovisioning Wakes", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var queue workqueue.TypedRateLimitingInterface[reconcile.Request]
	BeforeEach(func() {
		c := static.NewController(env.Overlay, cluster, cloudProvider, env.Clock, recorder)
		register(c)
		nodePool = test.StaticNodePool()
		nodePool.Spec.Replicas = new(int64(1))
		nodeClaim = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}},
			Status:     v1.NodeClaimStatus{ProviderID: test.RandomProviderID()},
		})
		ExpectApplied(ctx, env.Client, nodePool)
		queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
		DeferCleanup(queue.ShutDown)
		started, stop := context.WithCancel(ctx)
		DeferCleanup(stop)
		Expect(c.Active().Start(started, queue)).To(Succeed())
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want each NodePool cluster state holds on Start")
	})
	It("should wake for a NodePool whose active NodeClaims change", func() {
		ExpectApplied(ctx, env.Client, nodeClaim)
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want the NodePool for its new NodeClaim")

		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDisruptionReason)
		ExpectApplied(ctx, env.Client, nodeClaim)
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want the NodePool for its NodeClaim no longer active")
	})
	It("should not wake for a NodePool whose deleting NodeClaims or reservations change", func() {
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDisruptionReason)
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
		// The NodeClaim's create lands before its status, so it was active for that write, which wakes the NodePool.
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want the NodePool for the NodeClaim its create made active")

		ExpectDeleted(ctx, env.Client, nodeClaim)
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		Expect(drain(queue)).To(BeEmpty(), "want nothing for the NodePool's deleting NodeClaim going away")

		reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 1)
		Expect(reservations).To(HaveLen(1))
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)
		reservations.Release()
		Expect(drain(queue)).To(BeEmpty(), "want nothing for the NodePool's reservation")
	})
})
