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
	"math"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"

	"sigs.k8s.io/karpenter/pkg/state/virtualpods"

	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	static "sigs.k8s.io/karpenter/pkg/controllers/static/provisioning"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

type failingClient struct {
	client.Client
}

func (f *failingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*v1.NodeClaim); ok {
		return fmt.Errorf("simulated error creating nodeclaims")
	}
	return f.Client.Create(ctx, obj, opts...)
}

// timingOutClient times out creating NodeClaims, a failure after which the NodeClaim may exist anyway.
type timingOutClient struct {
	client.Client
}

func (f *timingOutClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*v1.NodeClaim); ok {
		return apierrors.NewTimeoutError("simulated timeout creating nodeclaims", 1)
	}
	return f.Client.Create(ctx, obj, opts...)
}

// timingOutNodePoolClient times out reading NodePools, which the provisioner does before it creates a NodeClaim.
type timingOutNodePoolClient struct {
	client.Client
}

func (f *timingOutNodePoolClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1.NodePool); ok {
		return apierrors.NewTimeoutError("simulated timeout getting nodepools", 1)
	}
	return f.Client.Get(ctx, key, obj, opts...)
}

var (
	cs            karpentercluster.State
	ctx           context.Context
	cluster       *state.Cluster
	cloudProvider *fake.CloudProvider
	controller    *static.Controller
	env           *test.Environment
	prov          *provisioning.Provisioner
)

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controllers/Provisioning/Static")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	virtualPodCache := virtualpods.NewVirtualPodCache(env.Overlay)
	cs = karpentercluster.New(ctx, env.Clock, env.Overlay, cloudProvider)
	cluster = state.NewCluster(env.Clock, env.Overlay, cs.Nodes, cs.NodePoolState, cs.CapacityQuotaState, cs.PodScheduling)
	prov = provisioning.NewProvisioner(env.Overlay, events.NewRecorder(&record.FakeRecorder{}), cloudProvider, cluster, env.Clock, deviceallocation.NewController(env.Overlay), virtualPodCache, nil, cs.Nomination, cs.BufferPods)
	controller = static.NewController(env.Overlay, cluster, events.NewRecorder(&record.FakeRecorder{}), cloudProvider, prov, env.Clock, deviceallocation.NewController(env.Overlay), virtualPodCache)
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
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
	cloudProvider.Reset()
	cluster.Reset()
})

var _ = Describe("Static Provisioning Controller", func() {
	Context("Reconcile", func() {
		It("should handle CreateNodeClaims errors gracefully", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(1))
			ExpectApplied(ctx, env.Client, nodePool)

			// Create controller with failing client
			failingController := static.NewController(&failingClient{Client: env.Overlay}, cluster, events.NewRecorder(&record.FakeRecorder{}), cloudProvider, prov, env.Clock, deviceallocation.NewController(env.Overlay), virtualpods.NewVirtualPodCache(env.Overlay))

			result, err := failingController.Reconcile(ctx, nodePool)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("creating nodeclaims"))
			Expect(result.RequeueAfter).To(BeZero())

			// Should not create any NodeClaims due to error
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(0))
			// The failed create released its reservation, so the next reconcile doesn't count it as reserved.
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		})
		DescribeTable("should release the reservations of NodeClaims it didn't create",
			func(newClient func(client.Client) client.Client, canceled bool, wantReserved int) {
				nodePool := test.StaticNodePool()
				nodePool.Spec.Replicas = new(int64(2))
				ExpectApplied(ctx, env.Client, nodePool)
				reconciler := static.NewController(newClient(env.Overlay), cluster, events.NewRecorder(&record.FakeRecorder{}), cloudProvider, prov, env.Clock, deviceallocation.NewController(env.Overlay), virtualpods.NewVirtualPodCache(env.Overlay))
				reconcileCtx := ctx
				if canceled {
					var cancel context.CancelFunc
					reconcileCtx, cancel = context.WithCancel(ctx)
					cancel()
				}

				_, err := reconciler.Reconcile(reconcileCtx, nodePool)
				Expect(err).To(HaveOccurred())
				nodeClaims := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
				Expect(nodeClaims.Items).To(BeEmpty())
				ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, wantReserved)
			},
			Entry("when the create fails", func(c client.Client) client.Client { return &failingClient{Client: c} }, false, 0),
			// A create that times out may have created the NodeClaim, so its reservation holds until it expires.
			Entry("except when the create times out", func(c client.Client) client.Client { return &timingOutClient{Client: c} }, false, 2),
			// A timeout before the create never created a NodeClaim.
			Entry("when reading the NodePool times out", func(c client.Client) client.Client { return &timingOutNodePoolClient{Client: c} }, false, 0),
			// A reconcile whose context is done creates none of them.
			Entry("when the context is done", func(c client.Client) client.Client { return c }, true, 0),
		)
		It("should never reserve past replicas while another controller reserves replacements", func() {
			// A disruption replacement may hold a node above replicas, but this controller's own reservations never may:
			// each admit it makes holds the NodePool's active and reserved nodes within replicas. A replacement reserved
			// between this controller reading the counts and reserving would otherwise be counted by neither.
			const replicas = 3
			for range 50 {
				nodePool := test.StaticNodePool()
				nodePool.Spec.Replicas = new(int64(replicas))
				ExpectApplied(ctx, env.Client, nodePool)

				// The controller's reservation is the only change that adds more than one node at once.
				var mu sync.Mutex
				var overshoots []string
				unsubscribe := cluster.NodePoolState.Subscribe(func(c karpenterstate.Change[string, *v1.NodePool, karpentercluster.NodePoolFields, karpentercluster.NodePoolIntent]) {
					old, next := c.Old.Derived, c.New.Derived
					if c.Key != nodePool.Name || next.Reserved-old.Reserved < 2 {
						return
					}
					if held := int64(next.NodeClaims.Active) + next.Reserved; held > replicas {
						mu.Lock()
						overshoots = append(overshoots, fmt.Sprintf("reserved %d with %d already reserved, holding %d of %d replicas", next.Reserved-old.Reserved, old.Reserved, held, replicas))
						mu.Unlock()
					}
				})
				done := make(chan struct{})
				replacer := make(chan struct{})
				started := make(chan struct{})
				go func() {
					defer close(replacer)
					for i := 0; ; i++ {
						select {
						case <-done:
							return
						default:
							cluster.NodePoolState.ReserveNodeCount(nodePool.Name, math.MaxInt64, 1).Release()
						}
						if i == 0 {
							close(started)
						}
					}
				}()
				<-started
				_, err := controller.Reconcile(ctx, nodePool)
				close(done)
				<-replacer
				unsubscribe()
				Expect(err).ToNot(HaveOccurred())
				Expect(overshoots).To(BeEmpty())
				ExpectCleanedUp(ctx, env.Client)
			}
		})
		It("should return early if nodepool is not managed by cloud provider", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(1))
			nodePool.Spec.Template.Spec.NodeClassRef = &v1.NodeClassReference{
				Group: "test.group",
				Kind:  "UnmanagedNodeClass",
				Name:  "test",
			}
			ExpectApplied(ctx, env.Client, nodePool)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not create any NodeClaims
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(0))
		})
		It("should return early if nodepool root condition is not true", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(1))
			nodePool.StatusConditions().SetFalse(v1.ConditionTypeValidationSucceeded, "ValidationFailed", "Validation failed")
			ExpectApplied(ctx, env.Client, nodePool)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not create any NodeClaims
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(0))
		})
		It("should return early if nodepool is being deleted", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(1))
			ExpectApplied(ctx, env.Client, nodePool)
			ExpectDeletionTimestampSet(ctx, env.Client, nodePool)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not create any NodeClaims
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(0))
		})
		It("should return early if nodepool replicas is nil", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = nil
			ExpectApplied(ctx, env.Client, nodePool)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not create any NodeClaims
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(0))
		})
		It("should return early if current node count exceeds desired replicas", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(1))
			// Create 2 nodes and nodeclaims that belong to this nodepool (exceeds desired replicas of 1)
			nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
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
			nodeClaim2, node2 := test.NodeClaimAndNode(v1.NodeClaim{
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
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim1, nodeClaim2, node1, node2)

			// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{node1, node2}, []*v1.NodeClaim{nodeClaim1, nodeClaim2})
			Expect(cluster.Nodes()).To(HaveLen(2))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not create any additional NodeClaims
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(2))
		})
		It("should create nodeclaims when current node count is less than desired replicas", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(2))
			ExpectApplied(ctx, env.Client, nodePool)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should create 2 NodeClaims
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(2))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

			// Verify NodeClaims have correct nodepool reference
			for _, nc := range nodeClaims.Items {
				Expect(nc.Spec.NodeClassRef).To(Equal(nodePool.Spec.Template.Spec.NodeClassRef))
				Expect(nc.Labels).To(HaveKeyWithValue(v1.NodePoolLabelKey, nodePool.Name))
			}
		})
		It("should create additional nodeclaims to reach desired replicas", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(3))
			// Create 2 nodes and nodeclaims that belong to this nodepool (exceeds desired replicas of 1)
			nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
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
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim1, node1)

			// 	// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			Expect(cluster.Nodes()).To(HaveLen(1))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)

			// Should create 2 additional NodeClaims (3 desired - 1 existing = 2 new)
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(3))
		})
		It("should not create additional nodeclaims", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(3))

			nodeClaimOpts := []v1.NodeClaim{{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey: nodePool.Name,
					},
				},
				Spec: v1.NodeClaimSpec{
					Resources: v1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("50Mi"),
						},
					},
				},
				Status: v1.NodeClaimStatus{
					ProviderID: test.RandomProviderID(),
				},
			}}
			nodeClaim1 := test.NodeClaim(nodeClaimOpts...) // has no node
			nodeClaim2 := test.NodeClaim(nodeClaimOpts...) // has node unregistered
			nodeClaim3 := test.NodeClaim(nodeClaimOpts...) // has just providerId
			node2 := test.Node(test.NodeOptions{
				ProviderID: nodeClaim2.Status.ProviderID,
				Taints:     []corev1.Taint{v1.UnregisteredNoExecuteTaint},
			})
			node3 := test.Node(test.NodeOptions{
				ProviderID: nodeClaim3.Status.ProviderID,
			})

			ExpectApplied(ctx, env.Client, nodeClaim1)
			ExpectApplied(ctx, env.Client, nodeClaim2, node2)
			ExpectApplied(ctx, env.Client, nodeClaim3, node3)

			// 	// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{node3, node2}, []*v1.NodeClaim{nodeClaim1, nodeClaim2, nodeClaim3})

			// Reconcile multiple times
			for range 10 {
				_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			}

			// Should have just 3 NodeClaims
			existingNodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, existingNodeClaims)).To(Succeed())
			Expect(existingNodeClaims.Items).To(HaveLen(3))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)
		})
		It("should not create additional nodeclaims when node limits are reached", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(3))
			nodePool.Spec.Limits = v1.Limits{
				corev1.ResourceName("nodes"): resource.MustParse("1"),
			}
			// Create 2 nodes and nodeclaims that belong to this nodepool (exceeds desired replicas of 1)
			nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
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
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim1, node1)

			// 	// Update cluster state to track the nodes
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			Expect(cluster.Nodes()).To(HaveLen(1))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not create additional NodeClaims (3 desired - 1 existing = 2 new but limit is 1)
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(1))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
		})
		It("should reserve nodepool nodecount during provisioning and release after", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(3))
			nodePool.Spec.Limits = v1.Limits{
				corev1.ResourceName("nodes"): resource.MustParse("10"),
			}

			ExpectApplied(ctx, env.Client, nodePool)
			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should create 3 NodeClaims
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(3))

			// Should be tracking running nodeclaims in nodepool state node
			ExpectStateNodePoolCount(cluster, nodePool.Name, 3, 0, 0)

			// Should be able to reserve remaining 7 NodeCounts
			reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 10)
			Expect(reservations).To(HaveLen(7))
			// Release the 7 NodeCount we held
			reservations.Release()

			// Size up the replicas to 15 with limit 10
			nodePool.Spec.Replicas = new(int64(15))
			ExpectApplied(ctx, env.Client, nodePool)

			// Update the state with Created NodeClaims
			for _, nodeClaim := range nodeClaims.Items {
				ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cloudProvider, &nodeClaim)
			}

			result = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should have 10 NodeClaims and not go over limits
			nodeClaims = &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(10))

			// Should not be able to Reserve more, before or after cluster state counts the NodeClaims it created
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 100)).To(BeEmpty())
			for _, nodeClaim := range nodeClaims.Items {
				ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cloudProvider, &nodeClaim)
			}
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 100)).To(BeEmpty())

			ExpectStateNodePoolCount(cluster, nodePool.Name, 10, 0, 0)
		})
		It("should handle zero replicas", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Replicas = new(int64(0))
			ExpectApplied(ctx, env.Client, nodePool)

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should not create any NodeClaims
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(0))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)

		})
		It("should respect nodepool template specifications", func() {
			// Input uses GT 2, but output will be canonicalized to GTE 3
			inputRequirements := []v1.NodeSelectorRequirementWithMinValues{
				{Key: "karpenter.k8s.aws/instance-category", Operator: corev1.NodeSelectorOpIn, Values: []string{"c", "r"}, MinValues: new(int(2))},
				{Key: "karpenter.k8s.aws/instance-family", Operator: corev1.NodeSelectorOpIn, Values: []string{"c4", "r4"}, MinValues: new(int(2))},
				{Key: "karpenter.k8s.aws/instance-cpu", Operator: corev1.NodeSelectorOpIn, Values: []string{"32"}},
				{Key: "karpenter.k8s.aws/instance-hypervisor", Operator: corev1.NodeSelectorOpIn, Values: []string{"nitro"}},
				{Key: "karpenter.k8s.aws/instance-generation", Operator: corev1.NodeSelectorOpGt, Values: []string{"2"}},
				{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"us-west-2a", "us-west-2b"}},
				{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64", "arm64"}},
				{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{"on-demand", "reserved", "spot"}},
			}
			expectedRequirements := []v1.NodeSelectorRequirementWithMinValues{
				{Key: "karpenter.k8s.aws/instance-category", Operator: corev1.NodeSelectorOpIn, Values: []string{"c", "r"}, MinValues: new(int(2))},
				{Key: "karpenter.k8s.aws/instance-family", Operator: corev1.NodeSelectorOpIn, Values: []string{"c4", "r4"}, MinValues: new(int(2))},
				{Key: "karpenter.k8s.aws/instance-cpu", Operator: corev1.NodeSelectorOpIn, Values: []string{"32"}},
				{Key: "karpenter.k8s.aws/instance-hypervisor", Operator: corev1.NodeSelectorOpIn, Values: []string{"nitro"}},
				{Key: "karpenter.k8s.aws/instance-generation", Operator: v1.NodeSelectorOpGte, Values: []string{"3"}}, // GT 2 canonicalized to GTE 3
				{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"us-west-2a", "us-west-2b"}},
				{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64", "arm64"}},
				{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{"on-demand", "reserved", "spot"}},
			}
			nodePool := test.StaticNodePool(v1.NodePool{
				Spec: v1.NodePoolSpec{
					Replicas: new(int64(4)),
					Template: v1.NodeClaimTemplate{
						ObjectMeta: v1.ObjectMeta{
							Labels: map[string]string{
								"custom-label": "custom-value",
							},
							Annotations: map[string]string{
								"custom-annotation": "custom-value",
							},
						},
					},
				},
			})
			nodePool.Spec.Template.Spec.Requirements = inputRequirements
			ExpectApplied(ctx, env.Client, nodePool)

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{}, []*v1.NodeClaim{})

			result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			Expect(result.RequeueAfter).To(BeZero())

			// Should create 4 NodeClaim with template specifications
			nodeClaims := &v1.NodeClaimList{}
			Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
			Expect(nodeClaims.Items).To(HaveLen(4))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 4, 0, 0)

			nc := nodeClaims.Items[0]
			Expect(nc.Labels).To(HaveKeyWithValue("custom-label", "custom-value"))
			Expect(nc.Annotations).To(HaveKeyWithValue("custom-annotation", "custom-value"))
			Expect(nc.Spec.Requirements).To(ContainElements(expectedRequirements))
		})
		It("should handle large replica counts", func() {
			nodePool := test.StaticNodePool()
			numNodeClaims := 1000
			nodePool.Spec.Replicas = new(int64(numNodeClaims))
			ExpectApplied(ctx, env.Client, nodePool)

			Eventually(func(g Gomega) int {
				// TODO: remove Eventually when 1.31 is deprecated https://github.com/kubernetes/enhancements/issues/4420
				_, _ = reconcile.AsReconciler(env.Client, controller).Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(nodePool)})
				nodeClaims := &v1.NodeClaimList{}
				Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
				return len(nodeClaims.Items)
			}).WithTimeout(40 * time.Second).Should(Equal(numNodeClaims))
			ExpectStateNodePoolCount(cluster, nodePool.Name, numNodeClaims, 0, 0)
		})
		It("handles concurrent reconciliation without exceeding NodePool limits", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Limits = v1.Limits{
				corev1.ResourceName("nodes"): resource.MustParse("10"),
			}
			nodePool.Spec.Replicas = new(int64(5))
			ExpectApplied(ctx, env.Client, nodePool)

			// Run many reconciles in parallel
			n := 50
			errs := make(chan error, n)
			for range n {
				go func() {
					defer GinkgoRecover()
					_, e := controller.Reconcile(ctx, nodePool)
					errs <- e
				}()
			}
			for range n {
				Expect(<-errs).ToNot(HaveOccurred())
			}

			// we should never observe > limit NodeClaims.
			Eventually(func() {
				ExpectStateNodePoolCount(cluster, nodePool.Name, 10, 0, 0)
			}, 5*time.Second)
		})
		It("should not over provision across many reconciles", func() {
			nodePool := test.StaticNodePool()
			nodePool.Spec.Limits = v1.Limits{
				corev1.ResourceName("nodes"): resource.MustParse("10"),
			}
			nodePool.Spec.Replicas = new(int64(5))
			ExpectApplied(ctx, env.Client, nodePool)

			// Run many reconciles in parallel
			n := 50
			lo.ForEach(lo.Range(n), func(i int, _ int) {
				defer GinkgoRecover()
				ExpectObjectReconciled(ctx, env.Client, controller, nodePool)

				// we should never observe > limit NodeClaims.
				var list v1.NodeClaimList
				Expect(env.Client.List(ctx, &list)).To(Succeed())
				Expect(len(list.Items)).To(BeNumerically("<=", 10))
			})

			Eventually(func() {
				ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
				// at the end we should have right counts in StateNodePool
				ExpectStateNodePoolCount(cluster, nodePool.Name, 10, 0, 0)
			}, 10*time.Second)
		})

	})
	Context("Helper Functions", func() {
		DescribeTable("should detect replica or status changes",
			func(oldReplicas, newReplicas *int64, oldReady, newReady bool, expected bool) {
				old := &v1.NodePool{Spec: v1.NodePoolSpec{Replicas: oldReplicas}}
				new := &v1.NodePool{Spec: v1.NodePoolSpec{Replicas: newReplicas}}

				if oldReady {
					old.StatusConditions().SetTrue(v1.ConditionTypeValidationSucceeded)
					old.StatusConditions().SetTrue(v1.ConditionTypeNodeClassReady)
				} else {
					old.StatusConditions().SetFalse(v1.ConditionTypeValidationSucceeded, "reason", "old not ready")
				}

				if newReady {
					new.StatusConditions().SetTrue(v1.ConditionTypeValidationSucceeded)
					new.StatusConditions().SetTrue(v1.ConditionTypeNodeClassReady)
				} else {
					new.StatusConditions().SetFalse(v1.ConditionTypeValidationSucceeded, "reason", "new not ready")
				}

				Expect(static.HasNodePoolReplicaOrStatusChanged(old, new)).To(Equal(expected))
			},
			Entry("replica changed", new(int64(5)), new(int64(3)), false, false, true),
			Entry("replica same, false → true", new(int64(5)), new(int64(5)), false, true, true),
			Entry("replica same, true → false", new(int64(5)), new(int64(5)), true, false, false),
			Entry("replica same, both true", new(int64(5)), new(int64(5)), true, true, false),
			Entry("replica same, both false", new(int64(5)), new(int64(5)), false, false, false),
		)
		It("should detect node limit changes", func() {
			before := test.StaticNodePool()
			after := before.DeepCopy()
			after.Spec.Limits = v1.Limits{resources.Node: resource.MustParse("3")}
			Expect(static.HasNodePoolReplicaOrStatusChanged(before, after)).To(BeTrue())
			Expect(static.HasNodePoolReplicaOrStatusChanged(after, after.DeepCopy())).To(BeFalse())
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

var _ = Describe("Static Provisioning Wakes", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var queue workqueue.TypedRateLimitingInterface[reconcile.Request]
	BeforeEach(func() {
		c := static.NewController(env.Overlay, cluster, events.NewRecorder(&record.FakeRecorder{}), cloudProvider, prov, env.Clock, deviceallocation.NewController(env.Overlay), virtualpods.NewVirtualPodCache(env.Overlay))
		register(c)
		nodePool = test.StaticNodePool()
		nodePool.Spec.Replicas = new(int64(2))
		nodeClaim = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Capacity:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			},
		})
		ExpectApplied(ctx, env.Client, nodePool)
		queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
		DeferCleanup(queue.ShutDown)
		started, stop := context.WithCancel(ctx)
		DeferCleanup(stop)
		Expect(c.Counts().Start(started, queue)).To(Succeed())
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want each NodePool cluster state holds on Start")
	})
	It("should wake for a NodePool whose NodeClaims come, start deleting or go", func() {
		ExpectApplied(ctx, env.Client, nodeClaim)
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want the NodePool for its new NodeClaim")

		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDisruptionReason)
		ExpectApplied(ctx, env.Client, nodeClaim)
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want the NodePool for its NodeClaim deleting")

		ExpectDeleted(ctx, env.Client, nodeClaim)
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want the NodePool for its NodeClaim going away")
	})
	It("should wake for a NodePool whose reservations change", func() {
		reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 1)
		Expect(reservations).To(HaveLen(1))
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want the NodePool for its reservation")
		reservations.Release()
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}), "want the NodePool for its released reservation")
	})
	It("should not wake for a NodePool whose usage or node health change", func() {
		ExpectApplied(ctx, env.Client, nodeClaim)
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}))

		nodeClaim.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
		ExpectApplied(ctx, env.Client, nodeClaim)
		Expect(new(cluster.NodePoolState.Get(nodePool.Name).Usage[corev1.ResourceCPU]).Value()).To(Equal(int64(2)))

		node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			corev1.LabelInstanceTypeStable: "instance-type",
		}}, ProviderID: nodeClaim.Status.ProviderID})
		ExpectApplied(ctx, env.Client, node)
		Expect(cluster.NodePoolState.Get(nodePool.Name).NodeHealth.Nodes).To(Equal(1))
		Expect(drain(queue)).To(BeEmpty(), "want nothing for the NodePool's usage and node health")
	})
	It("should wake once reservations that make up the shortfall expire, without a requeue, and provision in their place", func() {
		reservations := cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 10, 2)
		Expect(reservations).To(HaveLen(2))
		result := ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		Expect(result.RequeueAfter).To(BeZero())
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 2)
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}))

		// They expire at their TTL with nothing reading them, which changes the counts that wake the controller.
		env.Clock.Step(karpentercluster.ReservationTTL)
		Eventually(func() int { _, _, reserved := cluster.NodePoolState.GetNodeCount(nodePool.Name); return reserved }).Should(BeZero())
		Expect(drain(queue)).To(Equal([]string{nodePool.Name}))
		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		nodeClaims := &v1.NodeClaimList{}
		Expect(env.Client.List(ctx, nodeClaims)).To(Succeed())
		Expect(nodeClaims.Items).To(HaveLen(2))
	})
})
