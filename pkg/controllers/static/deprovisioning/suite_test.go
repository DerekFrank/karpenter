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

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	static "sigs.k8s.io/karpenter/pkg/controllers/static/deprovisioning"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var (
	ctx                      context.Context
	cluster                  *state.Cluster
	nodeController           *informer.NodeController
	daemonsetController      *informer.DaemonSetController
	cloudProvider            *fake.CloudProvider
	controller               *static.Controller
	env                      *test.Environment
	nodeClaimStateController *informer.NodeClaimController
	clusterCost              *cost.ClusterCost
	recorder                 *test.EventRecorder
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

// afterDeleteClient runs afterDelete after each successful NodeClaim Delete, before the caller's code after Delete runs
type afterDeleteClient struct {
	client.Client
	afterDelete func(*v1.NodeClaim)
}

func (a *afterDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if err := a.Client.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	if nodeClaim, ok := obj.(*v1.NodeClaim); ok && a.afterDelete != nil {
		a.afterDelete(nodeClaim)
	}
	return nil
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
	clusterCost = cost.NewClusterCost(ctx, cloudProvider, env.Client)
	cluster = state.NewCluster(env.Clock, env.Client, cloudProvider)
	nodeController = informer.NewNodeController(env.Client, cluster)
	daemonsetController = informer.NewDaemonSetController(env.Client, cluster)
	recorder = test.NewEventRecorder()
	controller = static.NewController(env.Client, cluster, cloudProvider, env.Clock, recorder)
	nodeClaimStateController = informer.NewNodeClaimController(env.Client, cloudProvider, cluster, clusterCost)
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
			cluster.UpdateNodePool(nodePool)

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
			cluster.UpdateNodePool(nodePool)

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
			cluster.UpdateNodePool(nodePool)
			ExpectDeletionTimestampSet(ctx, env.Client, nodePool)

			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})
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
			cluster.UpdateNodePool(nodePool)

			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})
			Expect(cluster.Nodes()).To(HaveLen(2))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

			Expect(result.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))

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
			cluster.UpdateNodePool(nodePool)
			for i := range 4 {
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims)
			Expect(cluster.Nodes()).To(HaveLen(4))

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

			// If 3 of the nodes are Marked for deletion then do not deprovision any
			for i := range 3 {
				cluster.MarkForDeletion(nodes[i].Spec.ProviderID)
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 3, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))

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
			cluster.UpdateNodePool(nodePool)
			for i := range 4 {
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}
			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims)
			Expect(cluster.Nodes()).To(HaveLen(4))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))

			// Should terminate 2 NodeClaims (4 current - 2 desired = 2 to terminate)
			remainingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
			Expect(remainingNodeClaims.Items).To(HaveLen(2))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 2, 0)
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
			cluster.UpdateNodePool(nodePool)
			for i := range 3 {
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}
			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims)
			Expect(cluster.Nodes()).To(HaveLen(3))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))

			// Should terminate all NodeClaims
			remainingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
			Expect(remainingNodeClaims.Items).To(HaveLen(0))
			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 3, 0)
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaims[0]))
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaims[1]))
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaims[2]))

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		})
		It("should handle no active nodeclaims gracefully", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(0))
			ExpectApplied(ctx, env.Client, nodePool)
			cluster.UpdateNodePool(nodePool)

			// Update cluster state with no nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, []*corev1.Node{}, []*v1.NodeClaim{})

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))

			existingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, existingNodeClaims)).To(Succeed())
			Expect(existingNodeClaims.Items).To(HaveLen(0))
		})
		Context("Failing Scenarios", func() {
			It("should return error when nodeclaim deletion fails", func() {
				nodePool := test.StaticNodePool()
				nodePool.Spec.Replicas = new(int64(1))
				ExpectApplied(ctx, env.Client, nodePool)
				cluster.UpdateNodePool(nodePool)

				failingController := static.NewController(&failingClient{Client: env.Client}, cluster, cloudProvider, env.Clock, recorder)

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
				cluster.UpdateNodePool(nodePool)
				for i := range 3 {
					ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
				}

				// Update cluster state to track the nodes
				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims)
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
		Context("NodePoolState accounting", func() {
			var nodePool *v1.NodePool
			var launched []*v1.NodeClaim
			var unlaunched *v1.NodeClaim
			var hookClient *afterDeleteClient
			var hookController *static.Controller

			BeforeEach(func() {
				nodePool = test.StaticNodePool()
				nodePool.Spec.Replicas = new(int64(2))
				nodePool.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("3")}
				var nodes []*corev1.Node
				launched, nodes = test.NodeClaimsAndNodes(2, v1.NodeClaim{
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
				unlaunched = test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
				unlaunched.Status.ProviderID = ""
				ExpectApplied(ctx, env.Client, nodePool, launched[0], launched[1], nodes[0], nodes[1])
				cluster.UpdateNodePool(nodePool)
				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, launched)

				hookClient = &afterDeleteClient{Client: env.Client}
				hookController = static.NewController(hookClient, cluster, cloudProvider, env.Clock, recorder)
			})

			It("should not leave a deleting NodeClaim behind when the NotFound is processed before Delete returns", func() {
				ExpectApplied(ctx, env.Client, unlaunched)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(unlaunched))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)

				hookClient.afterDelete = func(nodeClaim *v1.NodeClaim) {
					defer GinkgoRecover()
					// The NodeClaim has no finalizer, so it is already gone and the informer Forgets it
					ExpectNotFound(ctx, env.Client, nodeClaim)
					ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))
				}
				ExpectObjectReconciled(ctx, env.Client, hookController, nodePool)

				ExpectNotFound(ctx, env.Client, unlaunched)
				Expect(cluster.NodePoolState.MarkedForDeletion(unlaunched.Name)).To(BeFalse())
				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)
				// The freed slot under limits.nodes can be reserved again
				Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 3, 1)).To(BeEquivalentTo(1))
			})
			It("should mark the NodeClaim for deletion before Delete is called", func() {
				ExpectApplied(ctx, env.Client, unlaunched)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(unlaunched))

				nested := false
				hookClient.afterDelete = func(nodeClaim *v1.NodeClaim) {
					defer GinkgoRecover()
					if nested {
						return
					}
					nested = true
					Expect(cluster.NodePoolState.MarkedForDeletion(nodeClaim.Name)).To(BeTrue())
					// A concurrent reconcile that runs before the informer sees the deletion must not delete another NodeClaim
					ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 1, 0)
					ExpectObjectReconciled(ctx, env.Client, hookController, nodePool)
				}
				ExpectObjectReconciled(ctx, env.Client, hookController, nodePool)

				ExpectNotFound(ctx, env.Client, unlaunched)
				for _, nc := range launched {
					Expect(ExpectExists(ctx, env.Client, nc).DeletionTimestamp.IsZero()).To(BeTrue())
				}
			})
			It("should skip a NodeClaim that is already marked for deletion", func() {
				nodePool.Spec.Replicas = new(int64(1))
				ExpectApplied(ctx, env.Client, nodePool, unlaunched)
				cluster.UpdateNodePool(nodePool)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(unlaunched))
				Expect(cluster.MarkNodeClaimForDeletion(unlaunched.Name)).To(BeTrue())
				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 1, 0)

				// Unresolved NodeClaims are deleted first, so the marked one is picked. Another controller owns its
				// deletion, so this reconcile leaves it alone.
				ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(ExpectExists(ctx, env.Client, unlaunched).DeletionTimestamp.IsZero()).To(BeTrue())
				Expect(cluster.NodePoolState.MarkedForDeletion(unlaunched.Name)).To(BeTrue())
				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 1, 0)
			})
			It("should keep counting an unlaunched NodeClaim as deleting once the informer sees its DeletionTimestamp", func() {
				unlaunched.Finalizers = []string{"karpenter.sh/test-finalizer"}
				ExpectApplied(ctx, env.Client, unlaunched)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(unlaunched))

				ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(ExpectExists(ctx, env.Client, unlaunched).DeletionTimestamp.IsZero()).To(BeFalse())
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(unlaunched))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 1, 0)

				// The next pass must not pick a launched NodeClaim to make up for the one already deleting
				ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				for _, nc := range launched {
					Expect(ExpectExists(ctx, env.Client, nc).DeletionTimestamp.IsZero()).To(BeTrue())
				}
			})
			It("should not delete a NodeClaim that cluster state hasn't observed", func() {
				nodePool.Spec.Replicas = new(int64(1))
				ExpectApplied(ctx, env.Client, nodePool, unlaunched)
				cluster.UpdateNodePool(nodePool)

				// Unresolved NodeClaims are deleted first, so the unobserved one is picked. It isn't counted, so deleting
				// it wouldn't bring the NodePool down to its replicas, and it can't be marked, so it is skipped.
				ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(ExpectExists(ctx, env.Client, unlaunched).DeletionTimestamp.IsZero()).To(BeTrue())
				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

				// Once observed, it is counted and deleted
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(unlaunched))
				ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				ExpectNotFound(ctx, env.Client, unlaunched)
			})
			It("should unmark the NodeClaim when Delete fails", func() {
				failingController := static.NewController(&failingClient{Client: env.Client}, cluster, cloudProvider, env.Clock, recorder)
				ExpectApplied(ctx, env.Client, unlaunched)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(unlaunched))

				_, err := failingController.Reconcile(ctx, nodePool)
				Expect(err).To(HaveOccurred())
				ExpectExists(ctx, env.Client, unlaunched)
				Expect(cluster.NodePoolState.MarkedForDeletion(unlaunched.Name)).To(BeFalse())
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
				cluster.UpdateNodePool(nodePool)
				ExpectApplied(ctx, env.Client, nodes[0], nodes[1], nodeClaims[0], nodeClaims[1])
				ExpectApplied(ctx, env.Client, unresolvedNodeClaim1, unresolvedNodeClaim2)

				// Force Cluster State Update as provisioning controller updates after creating the NodeClaim
				cluster.UpdateNodeClaim(unresolvedNodeClaim1)
				cluster.UpdateNodeClaim(unresolvedNodeClaim2)

				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims)
				Expect(cluster.Nodes()).To(HaveLen(2))

				ncCount := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, ncCount)).To(Succeed())
				Expect(ncCount.Items).To(HaveLen(4))

				// Verify StateNodePool has been updated (resolved + unresolved)
				ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

				result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(result.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))

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

				// Post deletion ensure nodeclaims are marked as Deleting
				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 2, 0)
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
				cluster.UpdateNodePool(nodePool)

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
				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims)
				Expect(cluster.Nodes()).To(HaveLen(4))

				// Verify StateNodePool Has been updated
				ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

				result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(result.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))

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
				ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 2, 0)

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
				cluster.UpdateNodePool(nodePool)
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
				ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims)
				Expect(cluster.Nodes()).To(HaveLen(4))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

				result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				Expect(result.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))

				// Should terminate 3 NodeClaims (4 current - 1 desired = 3 to terminate)
				remainingNodeClaims := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
				Expect(remainingNodeClaims.Items).To(HaveLen(1))
				ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 3, 0)

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
					cluster.UpdateNodePool(nodePool)

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
						ctx, env.Client, env.Clock, nodeController, nodeClaimStateController, nodes, nodeClaims,
					)
					Expect(cluster.Nodes()).To(HaveLen(8))
				})
				DescribeTable("scales down in disruption cost order",
					func(replicas int64, expectIdx []int) {
						nodePool.Spec.Replicas = new(replicas)
						ExpectApplied(ctx, env.Client, nodePool)
						cluster.UpdateNodePool(nodePool)
						ExpectStateNodePoolCount(cluster, nodePool.Name, 8, 0, 0)

						res := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
						Expect(res.RequeueAfter).To(BeNumerically("~", time.Minute*1, time.Second))
						ExpectStateNodePoolCount(cluster, nodePool.Name, int(replicas), int(8-replicas), 0)

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
