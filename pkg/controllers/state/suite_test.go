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
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	cloudproviderapi "k8s.io/cloud-provider/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeoverlay"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *test.Environment
var cluster *state.Cluster
var clusterCost *cost.ClusterCost
var nodeClaimController *informer.NodeClaimController
var nodeController *informer.NodeController
var podController *informer.PodController
var nodePoolController *informer.NodePoolController
var daemonsetController *informer.DaemonSetController
var nodeOverlayStore *nodeoverlay.InstanceTypeStore
var nodeOverlayController *nodeoverlay.Controller
var pricingController *informer.PricingController
var cloudProvider *fake.CloudProvider
var nodePool *v1.NodePool

const csiProvider = "fake.csi.provider"

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controllers/State")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...), test.WithConfigOptions(func(config *rest.Config) {
		config.QPS = -1
	}))
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	cluster = state.NewCluster(env.Clock, env.Client, cloudProvider)
	clusterCost = cost.NewClusterCost(ctx, cloudProvider, env.Client)
	nodeClaimController = informer.NewNodeClaimController(env.Client, cloudProvider, cluster, clusterCost)
	nodeController = informer.NewNodeController(env.Client, cluster)
	podController = informer.NewPodController(env.Client, cluster)
	nodePoolController = informer.NewNodePoolController(env.Client, cloudProvider, cluster, clusterCost)
	nodeOverlayStore = nodeoverlay.NewInstanceTypeStore()
	nodeOverlayController = nodeoverlay.NewController(env.Clock, env.Client, cloudProvider, nodeOverlayStore, cluster)
	daemonsetController = informer.NewDaemonSetController(env.Client, cluster)
	pricingController = informer.NewPricingController(env.Client, cloudProvider, clusterCost)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	env.Clock.SetTime(time.Now())
	state.ClusterStateUnsyncedTimeSeconds.Reset()
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	nodePool = test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	ExpectApplied(ctx, env.Client, nodePool)
})
var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
	cluster.Reset()
	cloudProvider.Reset()
})
var _ = Describe("Pod Healthy NodePool", func() {
	It("should not store pod schedulable time if the nodePool that pod is scheduled to does not have NodeRegistrationHealthy=true", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod, nodePool)
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{nodePool.Name: {pod}}, nil)
		setTime := cluster.PodSchedulingSuccessTimeRegistrationHealthyCheck(client.ObjectKeyFromObject(pod))
		Expect(setTime.IsZero()).To(BeTrue())
	})
	It("should store pod schedulable time if the nodePool that pod is scheduled to has NodeRegistrationHealthy=true", func() {
		pod := test.Pod()
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		ExpectApplied(ctx, env.Client, pod, nodePool)

		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{nodePool.Name: {pod}}, nil)
		setTime := cluster.PodSchedulingSuccessTimeRegistrationHealthyCheck(client.ObjectKeyFromObject(pod))
		Expect(setTime.IsZero()).To(BeFalse())
	})
	It("should not update the pod schedulable time if it is already stored for a pod", func() {
		pod := test.Pod()
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		ExpectApplied(ctx, env.Client, pod, nodePool)

		// This will store the pod schedulable time
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{nodePool.Name: {pod}}, nil)
		setTime := cluster.PodSchedulingSuccessTimeRegistrationHealthyCheck(client.ObjectKeyFromObject(pod))
		Expect(setTime.IsZero()).To(BeFalse())

		env.Clock.Step(time.Minute)
		// We try to update pod schedulable time, but it should not change as we have already stored it
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{nodePool.Name: {pod}}, nil)
		Expect(cluster.PodSchedulingSuccessTimeRegistrationHealthyCheck(client.ObjectKeyFromObject(pod))).To(Equal(setTime))
	})
	It("should delete the pod schedulable time if the pod is deleted", func() {
		pod := test.Pod()
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		ExpectApplied(ctx, env.Client, pod, nodePool)

		// This will store the pod schedulable time
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{nodePool.Name: {pod}}, nil)
		setTime := cluster.PodSchedulingSuccessTimeRegistrationHealthyCheck(client.ObjectKeyFromObject(pod))
		Expect(setTime.IsZero()).To(BeFalse())

		// Delete the pod
		cluster.DeletePod(client.ObjectKeyFromObject(pod))
		Expect(cluster.PodSchedulingSuccessTimeRegistrationHealthyCheck(client.ObjectKeyFromObject(pod)).IsZero()).To(BeTrue())
	})
	It("should not store scheduling times for pods that are already bound to a node", func() {
		pod := test.Pod(test.PodOptions{NodeName: "test-node"})
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		ExpectApplied(ctx, env.Client, pod, nodePool)
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{nodePool.Name: {pod}}, nil)
		nn := client.ObjectKeyFromObject(pod)
		Expect(cluster.PodSchedulingSuccessTimeRegistrationHealthyCheck(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeTrue())
	})
})

var _ = Describe("Pod Ack", func() {
	It("should only mark pods as schedulable once", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		nn := client.ObjectKeyFromObject(pod)

		setTime := cluster.PodSchedulingSuccessTime(nn)
		Expect(setTime.IsZero()).To(BeTrue())

		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"n1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})
		setTime = cluster.PodSchedulingSuccessTime(nn)
		Expect(setTime.IsZero()).To(BeFalse())

		newTime := cluster.PodSchedulingSuccessTime(nn)
		Expect(newTime.Compare(setTime)).To(Equal(0))
		Expect(cluster.PodNodeClaimMapping(nn)).To(BeEquivalentTo("nc1"))
	})
	It("should delete pod schedulable time and pod to nodeClaim mapping if we get error for the pod", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		nn := client.ObjectKeyFromObject(pod)

		setTime := cluster.PodSchedulingSuccessTime(nn)
		Expect(setTime.IsZero()).To(BeTrue())
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"n1": {pod}}, nil)
		setTime = cluster.PodSchedulingSuccessTime(nn)
		Expect(setTime.IsZero()).To(BeFalse())

		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{
			pod: fmt.Errorf("ignoring pod"),
		}, nil, nil)
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodNodeClaimMapping(nn)).To(BeEquivalentTo(""))
	})
	It("should delete the pod mappings from memory when the pod is deleted", func() {
		pod := test.Pod()
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		ExpectApplied(ctx, env.Client, pod, nodePool)

		nn := client.ObjectKeyFromObject(pod)
		// This will store the pod mappings
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"np1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeFalse())
		Expect(cluster.PodSchedulingDecisionTime(nn).IsZero()).To(BeFalse())
		Expect(cluster.PodNodeClaimMapping(nn)).To(BeEquivalentTo("nc1"))

		// Delete the pod
		cluster.DeletePod(client.ObjectKeyFromObject(pod))
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodSchedulingDecisionTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodNodeClaimMapping(nn)).To(BeEquivalentTo(""))
	})
})

var _ = Describe("Volume Usage/Limits", func() {
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	var csiNode *storagev1.CSINode
	var sc *storagev1.StorageClass
	BeforeEach(func() {
		instanceType := cloudProvider.InstanceTypes[0]
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: instanceType.Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
		sc = test.StorageClass(test.StorageClassOptions{
			ObjectMeta:  metav1.ObjectMeta{Name: "my-storage-class"},
			Provisioner: new(csiProvider),
			Zones:       []string{"test-zone-1"},
		})
		csiNode = &storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{
				Name: node.Name,
			},
			Spec: storagev1.CSINodeSpec{
				Drivers: []storagev1.CSINodeDriver{
					{
						Name:   csiProvider,
						NodeID: "fake-node-id",
						Allocatable: &storagev1.VolumeNodeResources{
							Count: new(int32(10)),
						},
					},
				},
			},
		}
	})
	It("should hydrate the volume usage on a Node update", func() {
		ExpectApplied(ctx, env.Client, sc, node, csiNode)
		for range 10 {
			pvc := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
				StorageClassName: new(sc.Name),
			})
			pod := test.Pod(test.PodOptions{
				PersistentVolumeClaims: []string{pvc.Name},
			})
			ExpectApplied(ctx, env.Client, pvc, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		stateNode := ExpectStateNodeExists(cluster, node)

		// Adding more volumes should cause an error since we are at the volume limits
		Expect(stateNode.VolumeUsage().ExceedsLimits(scheduling.Volumes{
			csiProvider: sets.New("test"),
		})).ToNot(BeNil())
	})
	DescribeTable("should treat a published driver without an attachment limit as unbounded",
		func(allocatable *storagev1.VolumeNodeResources) {
			csiNode.Spec.Drivers[0].Allocatable = allocatable
			ExpectApplied(ctx, env.Client, node, csiNode)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			volumeUsage := ExpectStateNodeExists(cluster, node).VolumeUsage()
			volumeUsage.AddFallbackLimit(csiProvider, 1)
			Expect(volumeUsage.ExceedsLimits(scheduling.Volumes{
				csiProvider: sets.New("volume-a", "volume-b"),
			})).To(Succeed())
		},
		Entry("without allocatable data", nil),
		Entry("without an allocatable count", &storagev1.VolumeNodeResources{}),
	)
	It("should maintain the volume usage state when receiving NodeClaim updates", func() {
		ExpectApplied(ctx, env.Client, sc, nodeClaim, node, csiNode)
		for range 10 {
			pvc := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
				StorageClassName: new(sc.Name),
			})
			pod := test.Pod(test.PodOptions{
				PersistentVolumeClaims: []string{pvc.Name},
			})
			ExpectApplied(ctx, env.Client, pvc, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		stateNode := ExpectStateNodeExists(cluster, node)

		// Adding more volumes should cause an error since we are at the volume limits
		Expect(stateNode.VolumeUsage().ExceedsLimits(scheduling.Volumes{
			csiProvider: sets.New("test"),
		})).ToNot(BeNil())

		// Reconcile the nodeclaim one more time to ensure that we maintain our volume usage state
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

		// Ensure that we still consider adding another volume to the node breaching our volume limits
		Expect(stateNode.VolumeUsage().ExceedsLimits(scheduling.Volumes{
			csiProvider: sets.New("test"),
		})).ToNot(BeNil())
	})
	It("should ignore the volume usage limits breach if the pod update is for an already tracked pod", func() {
		ExpectApplied(ctx, env.Client, sc, nodeClaim, node, csiNode)
		var pvcs []*corev1.PersistentVolumeClaim
		for range 10 {
			pvc := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
				StorageClassName: new(sc.Name),
			})
			pod := test.Pod(test.PodOptions{
				PersistentVolumeClaims: []string{pvc.Name},
			})
			pvcs = append(pvcs, pvc)
			ExpectApplied(ctx, env.Client, pvc, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		stateNode := ExpectStateNodeExists(cluster, node)

		// Adding more volumes should not cause an error since this PVC volume is already tracked
		Expect(stateNode.VolumeUsage().ExceedsLimits(scheduling.Volumes{
			csiProvider: sets.New(client.ObjectKeyFromObject(pvcs[5]).String()),
		})).To(BeNil())
	})
})

var _ = Describe("HostPort Usage", func() {
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	BeforeEach(func() {
		instanceType := cloudProvider.InstanceTypes[0]
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: instanceType.Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
	})
	It("should hydrate the HostPort usage on a Node update", func() {
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		for i := range 10 {
			pod := test.Pod(test.PodOptions{
				HostPorts: []int32{int32(i)},
			})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		stateNode := ExpectStateNodeExists(cluster, node)

		// Adding a conflicting host port should cause an error
		Expect(stateNode.HostPortUsage().Conflicts(test.Pod(), []scheduling.HostPort{
			{
				IP:       net.IP("0.0.0.0"),
				Port:     int32(5),
				Protocol: corev1.ProtocolTCP,
			},
		})).ToNot(BeNil())
	})
	It("should maintain the host port usage state when receiving NodeClaim updates", func() {
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		for i := range 10 {
			pod := test.Pod(test.PodOptions{
				HostPorts: []int32{int32(i)},
			})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		stateNode := ExpectStateNodeExists(cluster, node)

		// Adding a conflicting host port should cause an error
		Expect(stateNode.HostPortUsage().Conflicts(test.Pod(), []scheduling.HostPort{
			{
				IP:       net.IP("0.0.0.0"),
				Port:     int32(5),
				Protocol: corev1.ProtocolTCP,
			},
		})).ToNot(BeNil())

		// Reconcile the nodeclaim one more time to ensure that we maintain our volume usage state
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

		// Ensure that we still consider the host port usage addition an error
		Expect(stateNode.HostPortUsage().Conflicts(test.Pod(), []scheduling.HostPort{
			{
				IP:       net.IP("0.0.0.0"),
				Port:     int32(5),
				Protocol: corev1.ProtocolTCP,
			},
		})).ToNot(BeNil())
	})
	It("should ignore the host port usage conflict if the pod update is for an already tracked pod", func() {
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		var pods []*corev1.Pod
		for i := range 10 {
			pod := test.Pod(test.PodOptions{
				HostPorts: []int32{int32(i)},
			})
			pods = append(pods, pod)
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		stateNode := ExpectStateNodeExists(cluster, node)

		// Adding a conflicting host port should not cause an error since this port is already tracked for the pod
		Expect(stateNode.HostPortUsage().Conflicts(pods[5], []scheduling.HostPort{
			{
				IP:       net.IP("0.0.0.0"),
				Port:     int32(5),
				Protocol: corev1.ProtocolTCP,
			},
		})).To(BeNil())
	})
})

var _ = Describe("Node Deletion", func() {
	It("should not leak a state node when the NodeClaim and Node names match", func() {
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
				},
			},
		})
		node.Name = nodeClaim.Name

		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectStateNodeCount("==", 1)

		// Expect that the node isn't leaked due to names matching
		ExpectDeleted(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectStateNodeCount("==", 1)
		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 0)
	})
})

var _ = Describe("Node Resource Level", func() {
	It("should not count pods not bound to nodes", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("2"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		// two pods, but neither is bound to the node so the node's CPU requests should be zero
		ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0.0")}, ExpectStateNodeExists(cluster, node).PodRequests())
	})
	It("should count new pods bound to nodes", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("2"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1.5")}, ExpectStateNodeExists(cluster, node).PodRequests())

		ExpectManualBinding(ctx, env.Client, pod2, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))
		ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3.5")}, ExpectStateNodeExists(cluster, node).PodRequests())
	})
	It("should count existing pods bound to nodes", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("2"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})

		// simulate a node that already exists in our cluster
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectManualBinding(ctx, env.Client, pod2, node)

		// that we just noticed
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3.5")}, ExpectStateNodeExists(cluster, node).PodRequests())
	})
	It("should subtract requests if the pod is deleted", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("2"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectManualBinding(ctx, env.Client, pod2, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3.5")}, ExpectStateNodeExists(cluster, node).PodRequests())

		// delete the pods and the CPU usage should go down
		ExpectDeleted(ctx, env.Client, pod2)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))
		ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1.5")}, ExpectStateNodeExists(cluster, node).PodRequests())

		ExpectDeleted(ctx, env.Client, pod1)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}, ExpectStateNodeExists(cluster, node).PodRequests())
	})
	It("should not add requests if the pod is terminal", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
			Phase: corev1.PodFailed,
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("2"),
				}},
			Phase: corev1.PodSucceeded,
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectManualBinding(ctx, env.Client, pod2, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}, ExpectStateNodeExists(cluster, node).PodRequests())
	})
	It("should stop tracking nodes that are deleted", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, pod1)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		for n := range cluster.Nodes() {
			ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2.5")}, n.Available())
			ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1.5")}, n.PodRequests())
		}

		// delete the node and the internal state should disappear as well
		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		for range cluster.Nodes() {
			Fail("shouldn't be called as the node was deleted")
		}
	})
	It("should track pods correctly if we miss events or they are consolidated", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Name: "stateful-set-pod"},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})

		node1 := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, pod1, node1)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		ExpectManualBinding(ctx, env.Client, pod1, node1)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		for n := range cluster.Nodes() {
			ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2.5")}, n.Available())
			ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1.5")}, n.PodRequests())
		}

		ExpectDeleted(ctx, env.Client, pod1)

		// second node has more capacity
		node2 := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("8"),
			},
			ProviderID: test.RandomProviderID(),
		})

		// and the pod can only bind to node2 due to the resource request
		pod2 := test.UnschedulablePod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Name: "stateful-set-pod"},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("5.0"),
				}},
		})

		ExpectApplied(ctx, env.Client, pod2, node2)
		ExpectManualBinding(ctx, env.Client, pod2, node2)
		// deleted the pod and then recreated it, but simulated only receiving an event on the new pod after it has
		// bound and not getting the new node event entirely
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node2))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		for n := range cluster.Nodes() {
			if n.Node.Name == node1.Name {
				// not on node1 any longer, so it should be fully free
				ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}, n.Available())
				ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}, n.PodRequests())
			} else {
				ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}, n.Available())
				ExpectResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("5")}, n.PodRequests())
			}
		}

	})
	// nolint:gosec
	It("should maintain a correct count of resource usage as pods are deleted/added", func() {
		var pods []*corev1.Pod
		for range 100 {
			pods = append(pods, test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU: resource.MustParse(fmt.Sprintf("%1.1f", rand.Float64()*2)),
					}},
			}))
		}
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU:  resource.MustParse("200"),
				corev1.ResourcePods: resource.MustParse("500"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:  resource.MustParse("0"),
			corev1.ResourcePods: resource.MustParse("0"),
		}, ExpectStateNodeExists(cluster, node).PodRequests())
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		sum := 0.0
		podCount := 0
		for _, pod := range pods {
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
			podCount++

			// extra reconciles shouldn't cause it to be multiply counted
			nReconciles := rand.Intn(3) + 1 // 1 to 3 reconciles
			for range nReconciles {
				ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
			}
			sum += pod.Spec.Containers[0].Resources.Requests.Cpu().AsApproximateFloat64()
			ExpectResources(corev1.ResourceList{
				corev1.ResourceCPU:  resource.MustParse(fmt.Sprintf("%1.1f", sum)),
				corev1.ResourcePods: resource.MustParse(fmt.Sprintf("%d", podCount)),
			}, ExpectStateNodeExists(cluster, node).PodRequests())
		}

		for _, pod := range pods {
			ExpectDeleted(ctx, env.Client, pod)
			nReconciles := rand.Intn(3) + 1
			// or multiply removed
			for range nReconciles {
				ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
			}
			sum -= pod.Spec.Containers[0].Resources.Requests.Cpu().AsApproximateFloat64()
			podCount--
			ExpectResources(corev1.ResourceList{
				corev1.ResourceCPU:  resource.MustParse(fmt.Sprintf("%1.1f", sum)),
				corev1.ResourcePods: resource.MustParse(fmt.Sprintf("%d", podCount)),
			}, ExpectStateNodeExists(cluster, node).PodRequests())
		}
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:  resource.MustParse("0"),
			corev1.ResourcePods: resource.MustParse("0"),
		}, ExpectStateNodeExists(cluster, node).PodRequests())
	})
	It("should track daemonset requested resources separately", func() {
		ds := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("2Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, ds)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(ds), ds)).To(Succeed())

		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})

		dsPod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				}},
		})
		dsPod.OwnerReferences = append(dsPod.OwnerReferences, metav1.OwnerReference{
			APIVersion:         "apps/v1",
			Kind:               "DaemonSet",
			Name:               ds.Name,
			UID:                ds.UID,
			Controller:         new(true),
			BlockOwnerDeletion: new(true),
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("8Gi"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, pod1, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		// daemonset pod isn't bound yet
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("0"),
			corev1.ResourceMemory: resource.MustParse("0"),
		}, ExpectStateNodeExists(cluster, node).DaemonSetRequests())
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1.5"),
		}, ExpectStateNodeExists(cluster, node).PodRequests())

		ExpectApplied(ctx, env.Client, dsPod)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(dsPod))
		ExpectManualBinding(ctx, env.Client, dsPod, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(dsPod))

		// just the DS request portion
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		}, ExpectStateNodeExists(cluster, node).DaemonSetRequests())
		// total request
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2.5"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		}, ExpectStateNodeExists(cluster, node).PodRequests())
	})
	It("should mark node for deletion when node is deleted", func() {
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
				},
				Finalizers: []string{v1.TerminationFinalizer},
			},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)

		Expect(env.Client.Delete(ctx, node)).To(Succeed())

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectNodeExists(ctx, env.Client, node.Name)
		Expect(ExpectStateNodeExists(cluster, node).MarkedForDeletion()).To(BeTrue())
	})
	It("should mark node for deletion when nodeclaim is deleted", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Finalizers: []string{v1.TerminationFinalizer},
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
				},
			},
			Spec: v1.NodeClaimSpec{
				Requirements: []v1.NodeSelectorRequirementWithMinValues{
					{
						Key:      corev1.LabelInstanceTypeStable,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{cloudProvider.InstanceTypes[0].Name},
					},
					{
						Key:      corev1.LabelTopologyZone,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"test-zone-1"},
					},
				},
				NodeClassRef: &v1.NodeClassReference{
					Group: "karpenter.test.sh",
					Kind:  "TestNodeClass",
					Name:  "default",
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("32Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
				},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("1"),
					corev1.ResourceMemory:           resource.MustParse("30Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
				},
			},
		})
		node := test.NodeClaimLinkedNode(nodeClaim)
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)

		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectExists(ctx, env.Client, nodeClaim)

		Expect(ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim).MarkedForDeletion()).To(BeTrue())
		Expect(ExpectStateNodeExists(cluster, node).MarkedForDeletion()).To(BeTrue())
	})
	It("should nominate the node until the nomination time passes", func() {
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
				},
				Finalizers: []string{v1.TerminationFinalizer},
			},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		cluster.NominateNodeForPod(ctx, node.Spec.ProviderID)

		// Expect that the node is now nominated
		Expect(ExpectStateNodeExists(cluster, node).Nominated(env.Clock)).To(BeTrue())
		env.Clock.Step(time.Second * 10) // nomination window is 20s so it should still be nominated
		Expect(ExpectStateNodeExists(cluster, node).Nominated(env.Clock)).To(BeTrue())
		env.Clock.Step(time.Second * 11) // past 20s, node should no longer be nominated
		Expect(ExpectStateNodeExists(cluster, node).Nominated(env.Clock)).To(BeFalse())
	})
	It("should handle a node changing from no providerID to registering a providerID", func() {
		node := test.Node()
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectStateNodeCount("==", 1)
		ExpectStateNodeExists(cluster, node)

		// Change the providerID; this mocks CCM adding the providerID onto the node after registration
		node.Spec.ProviderID = fmt.Sprintf("fake://%s", node.Name)
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectStateNodeCount("==", 1)
		ExpectStateNodeExists(cluster, node)
	})
})

var _ = Describe("Pod Anti-Affinity", func() {
	It("should track pods with required anti-affinity", func() {
		pod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
			PodAntiRequirements: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"foo": "bar"},
					},
					TopologyKey: corev1.LabelTopologyZone,
				},
			},
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})

		ExpectApplied(ctx, env.Client, pod)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
		foundPodCount := 0
		cluster.ForPodsWithAntiAffinity(func(p *corev1.Pod, n *corev1.Node) bool {
			foundPodCount++
			Expect(p.Name).To(Equal(pod.Name))
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 1))
	})
	It("should not track pods with preferred anti-affinity", func() {
		pod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
			PodAntiPreferences: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 15,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"foo": "bar"},
						},
						TopologyKey: corev1.LabelTopologyZone,
					},
				},
			},
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})

		ExpectApplied(ctx, env.Client, pod)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
		foundPodCount := 0
		cluster.ForPodsWithAntiAffinity(func(p *corev1.Pod, n *corev1.Node) bool {
			foundPodCount++
			Fail("shouldn't track pods with preferred anti-affinity")
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 0))
	})
	It("should stop tracking pods with required anti-affinity if the pod is deleted", func() {
		pod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
			PodAntiRequirements: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"foo": "bar"},
					},
					TopologyKey: corev1.LabelTopologyZone,
				},
			},
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})

		ExpectApplied(ctx, env.Client, pod)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
		foundPodCount := 0
		cluster.ForPodsWithAntiAffinity(func(p *corev1.Pod, n *corev1.Node) bool {
			foundPodCount++
			Expect(p.Name).To(Equal(pod.Name))
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 1))

		ExpectDeleted(ctx, env.Client, client.Object(pod))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
		foundPodCount = 0
		cluster.ForPodsWithAntiAffinity(func(p *corev1.Pod, n *corev1.Node) bool {
			foundPodCount++
			Fail("should not be called as the pod was deleted")
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 0))
	})
	It("should handle events out of order", func() {
		pod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU: resource.MustParse("1.5"),
				}},
			PodAntiRequirements: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"foo": "bar"},
					},
					TopologyKey: corev1.LabelTopologyZone,
				},
			},
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU: resource.MustParse("4"),
			},
			ProviderID: test.RandomProviderID(),
		})

		ExpectApplied(ctx, env.Client, pod)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))

		// simulate receiving the node deletion before the pod deletion
		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		foundPodCount := 0
		cluster.ForPodsWithAntiAffinity(func(p *corev1.Pod, n *corev1.Node) bool {
			foundPodCount++
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 0))
	})
})

var _ = Describe("Cluster State Sync", func() {
	It("should consider the cluster state synced when all nodes are tracked", func() {
		// Deploy 1000 nodes and sync them all with the cluster
		var wg sync.WaitGroup
		for range 1000 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				node := test.Node(test.NodeOptions{
					ProviderID: test.RandomProviderID(),
				})
				ExpectApplied(ctx, env.Client, node)
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			}()
		}
		wg.Wait()

		Expect(cluster.Synced(ctx)).To(BeTrue())
		ExpectMetricGaugeValue(state.ClusterStateSynced, 1.0, nil)
		ExpectMetricGaugeValue(state.ClusterStateNodesCount, 1000.0, nil)
		metric, found := FindMetricWithLabelValues("karpenter_cluster_state_unsynced_time_seconds", map[string]string{})
		Expect(found).To(BeTrue())
		Expect(metric.GetGauge().GetValue()).To(BeEquivalentTo(0))
	})
	It("should emit cluster_state_unsynced_time_seconds metric when cluster state is unsynced", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			Status: v1.NodeClaimStatus{
				ProviderID: "",
			},
		})
		nodeClaim.Status.ProviderID = ""
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.Synced(ctx)).To(BeFalse())

		env.Clock.Step(2 * time.Minute)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.Synced(ctx)).To(BeFalse())
		metric, found := FindMetricWithLabelValues("karpenter_cluster_state_unsynced_time_seconds", map[string]string{})
		Expect(found).To(BeTrue())
		Expect(metric.GetGauge().GetValue()).To(BeNumerically(">=", 120))
	})
	It("should consider the cluster state synced when nodes don't have provider id", func() {
		// Deploy 1000 nodes and sync them all with the cluster
		var wg sync.WaitGroup
		for range 1000 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				node := test.Node()
				ExpectApplied(ctx, env.Client, node)
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			}()
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeTrue())
		ExpectMetricGaugeValue(state.ClusterStateSynced, 1.0, nil)
		ExpectMetricGaugeValue(state.ClusterStateNodesCount, 1000.0, nil)

	})
	It("should consider the cluster state synced when nodes register provider id", func() {
		// Deploy 1000 nodes and sync them all with the cluster
		nodes := make([]*corev1.Node, 1000)
		var wg sync.WaitGroup
		for i := range 1000 {
			wg.Add(1)
			go func(index int) {
				defer GinkgoRecover()
				defer wg.Done()
				node := test.Node()
				ExpectApplied(ctx, env.Client, node)
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
				nodes[index] = node
			}(i)
		}
		wg.Wait()
		ExpectMetricGaugeValue(state.ClusterStateNodesCount, 1000.0, nil)
		Expect(cluster.Synced(ctx)).To(BeTrue())
		for i := range 1000 {
			wg.Add(1)
			go func(index int) {
				defer GinkgoRecover()
				defer wg.Done()
				nodes[index].Spec.ProviderID = test.RandomProviderID()
				ExpectApplied(ctx, env.Client, nodes[index])
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(nodes[index]))
			}(i)
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeTrue())
		ExpectMetricGaugeValue(state.ClusterStateSynced, 1.0, nil)
		ExpectMetricGaugeValue(state.ClusterStateNodesCount, 1000.0, nil)
	})
	It("should consider the cluster state synced when all nodeclaims are tracked", func() {
		// Deploy 1000 nodeClaims and sync them all with the cluster
		var wg sync.WaitGroup
		for range 1000 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				nodeClaim := test.NodeClaim(v1.NodeClaim{
					Status: v1.NodeClaimStatus{
						ProviderID: test.RandomProviderID(),
					},
				})
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			}()
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should consider the cluster state synced when a combination of nodeclaims and node are tracked", func() {
		// Deploy 250 nodes to the cluster that also have nodeclaims
		var wg sync.WaitGroup
		for range 250 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				node := test.Node(test.NodeOptions{
					ProviderID: test.RandomProviderID(),
				})
				nodeClaim := test.NodeClaim(v1.NodeClaim{
					Status: v1.NodeClaimStatus{
						ProviderID: node.Spec.ProviderID,
					},
				})
				ExpectApplied(ctx, env.Client, nodeClaim, node)
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			}()
		}
		wg.Wait()
		// Deploy 250 nodes to the cluster
		for range 250 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				node := test.Node(test.NodeOptions{
					ProviderID: test.RandomProviderID(),
				})
				ExpectApplied(ctx, env.Client, node)
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			}()
		}
		wg.Wait()
		// Deploy 500 nodeclaims and sync them all with the cluster
		for range 500 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				nodeClaim := test.NodeClaim(v1.NodeClaim{
					Status: v1.NodeClaimStatus{
						ProviderID: test.RandomProviderID(),
					},
				})
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			}()
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should consider the cluster state synced when the representation of nodes is the same", func() {
		// Deploy 500 nodeClaims to the cluster, apply the linked nodes, but don't sync them
		var wg sync.WaitGroup
		for range 500 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				nodeClaim := test.NodeClaim(v1.NodeClaim{
					Status: v1.NodeClaimStatus{
						ProviderID: test.RandomProviderID(),
					},
				})
				node := test.Node(test.NodeOptions{
					ProviderID: nodeClaim.Status.ProviderID,
				})
				ExpectApplied(ctx, env.Client, nodeClaim, node)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			}()
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("shouldn't consider the cluster state synced if a nodeclaim hasn't resolved its provider id", func() {
		// Deploy 1000 nodeClaims and sync them all with the cluster
		var wg sync.WaitGroup
		for i := range 1000 {
			wg.Add(1)
			go func(index int) {
				defer GinkgoRecover()
				defer wg.Done()
				nodeClaim := test.NodeClaim(v1.NodeClaim{
					Status: v1.NodeClaimStatus{
						ProviderID: test.RandomProviderID(),
					},
				})
				// One of them doesn't have its providerID
				if index == 900 {
					nodeClaim.Status.ProviderID = ""
				}
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			}(i)
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeFalse())
	})
	It("shouldn't consider the cluster state synced if a nodeclaim isn't tracked", func() {
		// Deploy 1000 nodeClaims and sync them all with the cluster
		var wg sync.WaitGroup
		for i := range 1000 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				nodeClaim := test.NodeClaim(v1.NodeClaim{
					Status: v1.NodeClaimStatus{
						ProviderID: test.RandomProviderID(),
					},
				})
				ExpectApplied(ctx, env.Client, nodeClaim)

				// One of them doesn't get synced with the reconciliation
				if i != 900 {
					ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
				}
			}()
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeFalse())
	})
	It("shouldn't consider the cluster state synced if a node isn't tracked", func() {
		// Deploy 1000 nodes and sync them all with the cluster
		var wg sync.WaitGroup
		for i := range 1000 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				node := test.Node(test.NodeOptions{
					ProviderID: test.RandomProviderID(),
				})
				ExpectApplied(ctx, env.Client, node)

				// One of them doesn't get synced with the reconciliation
				if i != 900 {
					ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
				}
			}()
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeFalse())
		ExpectMetricGaugeValue(state.ClusterStateSynced, 0, nil)
	})
	It("shouldn't consider the cluster state synced if a nodeclaim is added manually with UpdateNodeClaim", func() {
		nodeClaim := test.NodeClaim()
		nodeClaim.Status.ProviderID = ""

		cluster.UpdateNodeClaim(nodeClaim)
		Expect(cluster.Synced(ctx)).To(BeFalse())
	})
	It("shouldn't consider the cluster state synced if a nodeclaim without a providerID is deleted", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{})
		nodeClaim.Status.ProviderID = ""

		cluster.UpdateNodeClaim(nodeClaim)
		Expect(cluster.Synced(ctx)).To(BeFalse())
		ExpectMetricGaugeValue(state.ClusterStateSynced, 0, nil)

		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.Synced(ctx)).To(BeFalse())
		ExpectMetricGaugeValue(state.ClusterStateSynced, 0, nil)

		ExpectDeleted(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(cluster.Synced(ctx)).To(BeTrue())
		ExpectMetricGaugeValue(state.ClusterStateSynced, 1, nil)
	})
	// also this test takes a while still
	It("should consider the cluster state synced when a new node is added after the initial sync", func() {
		// Deploy 250 nodes to the cluster that also have nodeclaims
		var wg sync.WaitGroup
		for range 250 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				node := test.Node(test.NodeOptions{
					ProviderID: test.RandomProviderID(),
				})
				nodeClaim := test.NodeClaim(v1.NodeClaim{
					Status: v1.NodeClaimStatus{
						ProviderID: node.Spec.ProviderID,
					},
				})
				ExpectApplied(ctx, env.Client, node, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			}()
		}
		wg.Wait()
		// Deploy 250 nodes to the cluster
		for range 250 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				node := test.Node(test.NodeOptions{
					ProviderID: test.RandomProviderID(),
				})
				ExpectApplied(ctx, env.Client, node)
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			}()
		}
		wg.Wait()
		Expect(cluster.Synced(ctx)).To(BeTrue())

		// Add a new node but don't reconcile it
		node := test.Node(test.NodeOptions{
			ProviderID: test.RandomProviderID(),
		})
		ExpectApplied(ctx, env.Client, node)

		// Cluster state should still be synced because we already synced our changes
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
})

var _ = Describe("DaemonSet Controller", func() {
	It("should not update daemonsetCache when daemonset pod is not present", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))
		daemonsetPod := cluster.GetDaemonSetPod(daemonset)
		Expect(daemonsetPod).To(BeNil())
	})
	It("should update daemonsetCache when daemonset pod is created", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		daemonsetPod := test.UnschedulablePod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "DaemonSet",
							Name:               daemonset.Name,
							UID:                daemonset.UID,
							Controller:         new(true),
							BlockOwnerDeletion: new(true),
						},
					},
				},
			})
		daemonsetPod.Spec = daemonset.Spec.Template.Spec
		ExpectApplied(ctx, env.Client, daemonsetPod)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		Expect(cluster.GetDaemonSetPod(daemonset)).To(Equal(daemonsetPod))
	})
	It("should update daemonsetCache with the newest created pod", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		daemonsetPod1 := test.UnschedulablePod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "DaemonSet",
							Name:               daemonset.Name,
							UID:                daemonset.UID,
							Controller:         new(true),
							BlockOwnerDeletion: new(true),
						},
					},
				},
			})
		daemonsetPod1.Spec = daemonset.Spec.Template.Spec
		ExpectApplied(ctx, env.Client, daemonsetPod1)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		Expect(cluster.GetDaemonSetPod(daemonset)).To(Equal(daemonsetPod1))

		daemonsetPod2 := test.UnschedulablePod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "DaemonSet",
							Name:               daemonset.Name,
							UID:                daemonset.UID,
							Controller:         new(true),
							BlockOwnerDeletion: new(true),
						},
					},
				},
			})
		time.Sleep(time.Second) // Making sure the two pods have different creationTime
		daemonsetPod2.Spec = daemonset.Spec.Template.Spec
		ExpectApplied(ctx, env.Client, daemonsetPod2)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))
		Expect(cluster.GetDaemonSetPod(daemonset)).To(Equal(daemonsetPod2))
	})
	It("should delete daemonset in cache when daemonset is deleted", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		daemonsetPod := test.UnschedulablePod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "DaemonSet",
							Name:               daemonset.Name,
							UID:                daemonset.UID,
							Controller:         new(true),
							BlockOwnerDeletion: new(true),
						},
					},
				},
			})
		daemonsetPod.Spec = daemonset.Spec.Template.Spec
		ExpectApplied(ctx, env.Client, daemonsetPod)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		Expect(cluster.GetDaemonSetPod(daemonset)).To(Equal(daemonsetPod))

		ExpectDeleted(ctx, env.Client, daemonset, daemonsetPod)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		Expect(cluster.GetDaemonSetPod(daemonset)).To(BeNil())
	})
	It("should only return daemonset pods from the daemonset cache", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		otherPods := test.Pods(1000, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: daemonset.Namespace,
			},
		})
		ExpectApplied(ctx, env.Client, lo.Map(otherPods, func(p *corev1.Pod, _ int) client.Object { return p })...)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))
		Expect(cluster.GetDaemonSetPod(daemonset)).To(BeNil())
	})
})

var _ = Describe("Consolidated State", func() {
	It("should update the consolidated value when setting consolidation", func() {
		state := cluster.ConsolidationState()
		Expect(cluster.ConsolidationState()).To(Equal(state))

		// time must pass
		env.Clock.Step(1 * time.Second)

		cluster.MarkUnconsolidated()
		Expect(cluster.ConsolidationState()).ToNot(Equal(state))
	})
	It("should update the consolidated value when state timeout (5m) has passed and state hasn't changed", func() {
		state := cluster.ConsolidationState()

		env.Clock.Step(time.Minute)
		Expect(cluster.ConsolidationState()).To(Equal(state))

		env.Clock.Step(time.Minute * 2)
		Expect(cluster.ConsolidationState()).To(Equal(state))

		env.Clock.Step(time.Minute * 2)
		Expect(cluster.ConsolidationState()).ToNot(Equal(state))
	})
	It("should cause consolidation state to change when a NodePool is updated", func() {
		cluster.MarkUnconsolidated()
		env.Clock.Step(time.Minute)
		ExpectApplied(ctx, env.Client, nodePool)
		state := cluster.ConsolidationState()
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		Expect(cluster.ConsolidationState()).ToNot(Equal(state))
	})
})

var _ = Describe("Data Races", func() {
	var wg sync.WaitGroup
	var cancelCtx context.Context
	var cancel context.CancelFunc
	BeforeEach(func() {
		cancelCtx, cancel = context.WithCancel(ctx)
	})
	AfterEach(func() {
		cancel()
		wg.Wait()
	})
	It("should ensure that calling Synced() is valid while making updates to Nodes", func() {
		// Keep calling Synced for the entirety of this test
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			for {
				_ = cluster.Synced(ctx)
				if cancelCtx.Err() != nil {
					return
				}
			}
		}()

		// Call UpdateNode on 100 nodes (enough to trigger a DATA RACE)
		for range 100 {
			node := test.Node(test.NodeOptions{
				ProviderID: test.RandomProviderID(),
			})
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		}
	})
	It("should ensure that calling Synced() is valid while making updates to NodeClaims", func() {
		// Keep calling Synced for the entirety of this test
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			for {
				_ = cluster.Synced(ctx)
				if cancelCtx.Err() != nil {
					return
				}
			}
		}()

		// Call UpdateNodeClaim on 100 NodeClaims (enough to trigger a DATA RACE)
		for range 100 {
			nodeClaim := test.NodeClaim(v1.NodeClaim{
				Status: v1.NodeClaimStatus{
					ProviderID: test.RandomProviderID(),
				},
			})
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		}
	})
})

var _ = Describe("Taints", func() {
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	BeforeEach(func() {
		instanceType := cloudProvider.InstanceTypes[0]
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				corev1.LabelInstanceTypeStable: instanceType.Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
	})
	Context("Managed", func() {
		It("should not consider ephemeral taints on a managed node that isn't initialized", func() {
			node.Spec.Taints = []corev1.Taint{
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute},
				{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
				{Key: cloudproviderapi.TaintExternalCloudProvider, Effect: corev1.TaintEffectNoSchedule, Value: "true"},
			}
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			stateNode := ExpectStateNodeExists(cluster, node)
			Expect(stateNode.Taints()).To(HaveLen(0))
		})
		It("should not consider NodeReadinessController taints on a managed node that isn't initialized", func() {
			node.Spec.Taints = []corev1.Taint{
				{Key: "readiness.k8s.io/some-rule", Effect: corev1.TaintEffectNoSchedule},
				{Key: "readiness.k8s.io/another-rule", Effect: corev1.TaintEffectNoExecute},
			}
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			stateNode := ExpectStateNodeExists(cluster, node)
			Expect(stateNode.Taints()).To(HaveLen(0))
		})
		It("should consider ephemeral taints on a managed node after the node is initialized", func() {
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectMakeNodesInitialized(ctx, env.Client, env.Clock, node)
			ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)

			node = ExpectExists(ctx, env.Client, node)
			node.Spec.Taints = []corev1.Taint{
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute},
				{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
				{Key: cloudproviderapi.TaintExternalCloudProvider, Effect: corev1.TaintEffectNoSchedule, Value: "true"},
			}
			ExpectApplied(ctx, env.Client, node)

			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			stateNode := ExpectStateNodeExists(cluster, node)
			Expect(stateNode.Taints()).To(HaveLen(4))
			Expect(stateNode.Taints()).To(ContainElements(
				corev1.Taint{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
				corev1.Taint{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute},
				corev1.Taint{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
				corev1.Taint{Key: cloudproviderapi.TaintExternalCloudProvider, Effect: corev1.TaintEffectNoSchedule, Value: "true"},
			))
		})
		It("should consider startup taints on a managed node that isn't initialized", func() {
			nodeClaim.Spec.StartupTaints = []corev1.Taint{
				{Key: "taint-key", Value: "taint-value", Effect: corev1.TaintEffectNoSchedule},
				{Key: "taint-key2", Value: "taint-value2", Effect: corev1.TaintEffectNoExecute},
			}
			node.Spec.Taints = []corev1.Taint{
				{Key: "taint-key", Value: "taint-value", Effect: corev1.TaintEffectNoSchedule},
				{Key: "taint-key2", Value: "taint-value2", Effect: corev1.TaintEffectNoExecute},
			}
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			stateNode := ExpectStateNodeExists(cluster, node)
			Expect(stateNode.Taints()).To(HaveLen(0))
		})
		It("should consider startup taints on a managed node after the node is initialized", func() {
			nodeClaim.Spec.StartupTaints = []corev1.Taint{
				{Key: "taint-key", Value: "taint-value", Effect: corev1.TaintEffectNoSchedule},
				{Key: "taint-key2", Value: "taint-value2", Effect: corev1.TaintEffectNoExecute},
			}
			node.Spec.Taints = []corev1.Taint{
				{Key: "taint-key", Value: "taint-value", Effect: corev1.TaintEffectNoSchedule},
				{Key: "taint-key2", Value: "taint-value2", Effect: corev1.TaintEffectNoExecute},
			}
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectMakeNodesInitialized(ctx, env.Client, env.Clock, node)
			ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)

			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			stateNode := ExpectStateNodeExists(cluster, node)
			Expect(stateNode.Taints()).To(HaveLen(2))
			Expect(stateNode.Taints()).To(ContainElements(
				corev1.Taint{Key: "taint-key", Value: "taint-value", Effect: corev1.TaintEffectNoSchedule},
				corev1.Taint{Key: "taint-key2", Value: "taint-value2", Effect: corev1.TaintEffectNoExecute},
			))
		})
	})
	Context("Unmanaged", func() {
		It("should consider ephemeral taints on an unmanaged node that isn't initialized", func() {
			node.Spec.Taints = []corev1.Taint{
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute},
				{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
				{Key: cloudproviderapi.TaintExternalCloudProvider, Effect: corev1.TaintEffectNoSchedule, Value: "true"},
			}
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			stateNode := ExpectStateNodeExists(cluster, node)
			Expect(stateNode.Taints()).To(HaveLen(4))
			Expect(stateNode.Taints()).To(ContainElements(
				corev1.Taint{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
				corev1.Taint{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute},
				corev1.Taint{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
				corev1.Taint{Key: cloudproviderapi.TaintExternalCloudProvider, Effect: corev1.TaintEffectNoSchedule, Value: "true"},
			))
		})
		It("should consider ephemeral taints on an unmanaged node after the node is initialized", func() {
			ExpectApplied(ctx, env.Client, node)
			ExpectMakeNodesInitialized(ctx, env.Client, env.Clock, node)

			node = ExpectExists(ctx, env.Client, node)
			node.Spec.Taints = []corev1.Taint{
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
				{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute},
				{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
				{Key: cloudproviderapi.TaintExternalCloudProvider, Effect: corev1.TaintEffectNoSchedule, Value: "true"},
			}

			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			stateNode := ExpectStateNodeExists(cluster, node)
			Expect(stateNode.Taints()).To(HaveLen(4))
			Expect(stateNode.Taints()).To(ContainElements(
				corev1.Taint{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoSchedule},
				corev1.Taint{Key: corev1.TaintNodeNotReady, Effect: corev1.TaintEffectNoExecute},
				corev1.Taint{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoSchedule},
				corev1.Taint{Key: cloudproviderapi.TaintExternalCloudProvider, Effect: corev1.TaintEffectNoSchedule, Value: "true"},
			))
		})
	})
})

var _ = Describe("NodePool Resources", func() {
	It("should calculate nodepool resources for multiple nodepools", func() {
		nodePool1NodeClaims, nodePool1Nodes := test.NodeClaimsAndNodes(3, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("2Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("1"),
					corev1.ResourceMemory:           resource.MustParse("1Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
				},
			},
		})
		nodePool2NodeClaims, nodePool2Nodes := test.NodeClaimsAndNodes(3, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("4"),
					corev1.ResourceMemory:           resource.MustParse("4Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("4Gi"),
				},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("3"),
					corev1.ResourceMemory:           resource.MustParse("3Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("3Gi"),
				},
			},
		})
		nodePool3NodeClaims, nodePool3Nodes := test.NodeClaimsAndNodes(3, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("6"),
					corev1.ResourceMemory:           resource.MustParse("6Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("6Gi"),
				},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("5"),
					corev1.ResourceMemory:           resource.MustParse("5Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("5Gi"),
				},
			},
		})
		for _, n := range lo.Flatten([][]*v1.NodeClaim{nodePool1NodeClaims, nodePool2NodeClaims, nodePool3NodeClaims}) {
			cluster.UpdateNodeClaim(n.DeepCopy())
		}
		for _, n := range lo.Flatten([][]*corev1.Node{nodePool1Nodes, nodePool2Nodes, nodePool3Nodes}) {
			Expect(cluster.UpdateNode(ctx, n.DeepCopy())).To(Succeed())
		}
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("6"),
			corev1.ResourceMemory:           resource.MustParse("6Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("6Gi"),
		}, cluster.NodePoolResourcesFor(nodePool1NodeClaims[0].Labels[v1.NodePoolLabelKey]))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("12"),
			corev1.ResourceMemory:           resource.MustParse("12Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("12Gi"),
		}, cluster.NodePoolResourcesFor(nodePool2NodeClaims[0].Labels[v1.NodePoolLabelKey]))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("18"),
			corev1.ResourceMemory:           resource.MustParse("18Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
		}, cluster.NodePoolResourcesFor(nodePool3NodeClaims[0].Labels[v1.NodePoolLabelKey]))

		// Now delete the Nodes and ensure that we keep the resources up-to-date
		cluster.DeleteNode(nodePool1Nodes[len(nodePool1Nodes)-1].Name)
		cluster.DeleteNode(nodePool2Nodes[len(nodePool2Nodes)-1].Name)
		cluster.DeleteNode(nodePool3Nodes[len(nodePool3Nodes)-1].Name)

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("6"),
			corev1.ResourceMemory:           resource.MustParse("6Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("6Gi"),
		}, cluster.NodePoolResourcesFor(nodePool1NodeClaims[0].Labels[v1.NodePoolLabelKey]))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("12"),
			corev1.ResourceMemory:           resource.MustParse("12Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("12Gi"),
		}, cluster.NodePoolResourcesFor(nodePool2NodeClaims[0].Labels[v1.NodePoolLabelKey]))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("18"),
			corev1.ResourceMemory:           resource.MustParse("18Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
		}, cluster.NodePoolResourcesFor(nodePool3NodeClaims[0].Labels[v1.NodePoolLabelKey]))

		// Now delete the NodeClaims to fully delete the state node
		cluster.DeleteNodeClaim(nodePool1NodeClaims[len(nodePool1NodeClaims)-1].Name)
		cluster.DeleteNodeClaim(nodePool2NodeClaims[len(nodePool2NodeClaims)-1].Name)
		cluster.DeleteNodeClaim(nodePool3NodeClaims[len(nodePool3NodeClaims)-1].Name)

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("4"),
			corev1.ResourceMemory:           resource.MustParse("4Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("4Gi"),
		}, cluster.NodePoolResourcesFor(nodePool1NodeClaims[0].Labels[v1.NodePoolLabelKey]))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("8"),
			corev1.ResourceMemory:           resource.MustParse("8Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("8Gi"),
		}, cluster.NodePoolResourcesFor(nodePool2NodeClaims[0].Labels[v1.NodePoolLabelKey]))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("12"),
			corev1.ResourceMemory:           resource.MustParse("12Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("12Gi"),
		}, cluster.NodePoolResourcesFor(nodePool3NodeClaims[0].Labels[v1.NodePoolLabelKey]))

		// Now delete all nodes and expect resources across all NodePools is zero
		for _, n := range lo.Flatten([][]*v1.NodeClaim{nodePool1NodeClaims, nodePool2NodeClaims, nodePool3NodeClaims}) {
			cluster.DeleteNodeClaim(n.Name)
		}
		for _, n := range lo.Flatten([][]*corev1.Node{nodePool1Nodes, nodePool2Nodes, nodePool3Nodes}) {
			cluster.DeleteNode(n.Name)
		}

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodePool1NodeClaims[0].Labels[v1.NodePoolLabelKey]))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodePool2NodeClaims[0].Labels[v1.NodePoolLabelKey]))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodePool3NodeClaims[0].Labels[v1.NodePoolLabelKey]))
	})
	It("should update nodepool resources when a node switches from one nodepool to another", func() {
		oldNodePoolName := test.RandomName()
		nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            oldNodePoolName,
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("2Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("1"),
					corev1.ResourceMemory:           resource.MustParse("1Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
				},
			},
		})
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(oldNodePoolName))

		newNodePoolName := test.RandomName()
		nodeClaim1.Labels[v1.NodePoolLabelKey] = newNodePoolName

		// Update the NodeClaim to change the NodePool that it's assigned to
		// Since the NodeClaim NodePool label
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(oldNodePoolName))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(newNodePoolName))

		node1.Labels[v1.NodePoolLabelKey] = newNodePoolName

		// Update the Node to change the NodePool that it's assigned to
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(oldNodePoolName))
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(newNodePoolName))
	})
	It("should update nodepool resources when the node changes providerID", func() {
		nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("2Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("1"),
					corev1.ResourceMemory:           resource.MustParse("1Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
				},
			},
		})
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		// NodeClaim changes providerID for some reason
		nodeClaim1.Status.ProviderID = test.RandomProviderID()
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("4"),
			corev1.ResourceMemory:           resource.MustParse("4Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("4Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		// Node changes providerID and now matches
		node1.Spec.ProviderID = nodeClaim1.Status.ProviderID
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))
	})
	It("should handle nodepool resources when node inside of the state node is removed", func() {
		nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("2Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				},
			},
		})
		node1.Status.Capacity = corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("1"),
			corev1.ResourceMemory:           resource.MustParse("1Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		}
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("1"),
			corev1.ResourceMemory:           resource.MustParse("1Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		cluster.DeleteNode(node1.Name)

		// Should flip to use the NodeClaim capacity once we delete the Node
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))
	})
	It("should handle nodepool resources when node inside of the state node is removed", func() {
		nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("2Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				},
			},
		})
		node1.Status.Capacity = corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("1"),
			corev1.ResourceMemory:           resource.MustParse("1Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		}
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("1"),
			corev1.ResourceMemory:           resource.MustParse("1Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		cluster.DeleteNodeClaim(nodeClaim1.Name)

		// Should continue to use the Node capacity once we delete the Node
		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("1"),
			corev1.ResourceMemory:           resource.MustParse("1Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))
	})
	It("should update nodepool resources when node is terminating (deletionTimestamp set)", func() {
		nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("2Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				},
			},
		})
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		// Set the deletionTimestamp
		nodeClaim1.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))
	})
	It("should update nodepool resources when node is marked/unmarked for deletion", func() {
		nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("2Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				},
			},
		})
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		// MarkForDeletion and expect the count of resources to reduce
		cluster.MarkForDeletion(nodeClaim1.Status.ProviderID)

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		// UnmarkForDeletion and expect the count of resources to be restored
		cluster.UnmarkForDeletion(nodeClaim1.Status.ProviderID)

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))
	})
	It("should not double subtract resources when marking for deletion and then deleting", func() {
		nodeClaim1, node1 := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            test.RandomName(),
					v1.NodeRegisteredLabelKey:      "true",
					corev1.LabelInstanceTypeStable: "m5.large",
				},
			},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("2"),
					corev1.ResourceMemory:           resource.MustParse("2Gi"),
					corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
				},
			},
		})
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("2"),
			corev1.ResourceMemory:           resource.MustParse("2Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		// MarkForDeletion and expect the count of resources to reduce
		cluster.MarkForDeletion(nodeClaim1.Status.ProviderID)

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		nodeClaim1.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		cluster.UpdateNodeClaim(nodeClaim1.DeepCopy())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		node1.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		Expect(cluster.UpdateNode(ctx, node1.DeepCopy())).To(Succeed())

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		cluster.DeleteNodeClaim(nodeClaim1.Name)

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))

		cluster.DeleteNode(node1.Name)

		ExpectResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("0"),
			corev1.ResourceMemory:           resource.MustParse("0Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("0Gi"),
		}, cluster.NodePoolResourcesFor(nodeClaim1.Labels[v1.NodePoolLabelKey]))
	})
})

var _ = Describe("NodePoolState Tracking", func() {
	var nodeClaim *v1.NodeClaim
	var nodeClaim2 *v1.NodeClaim
	var nodePool2 *v1.NodePool

	BeforeEach(func() {
		// NodePoolState only tracks the NodeClaims of static NodePools. A NodePool can't switch between static and
		// dynamic, so the suite's dynamic NodePool is replaced rather than updated.
		nodePool = test.StaticNodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "static"}, Spec: v1.NodePoolSpec{Replicas: new(int64(1))}})
		nodePool2 = test.StaticNodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "nodepool2"}, Spec: v1.NodePoolSpec{Replicas: new(int64(1))}})
		ExpectApplied(ctx, env.Client, nodePool, nodePool2)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool2))

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
				originalProviderID := nodeClaim.Status.ProviderID
				newProviderID := test.RandomProviderID()
				nodeClaim.Status.ProviderID = newProviderID
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

				// Should still track the NodeClaim correctly
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				// Old ProviderID should not be tracked for deletion
				cluster.MarkForDeletion(originalProviderID)
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1)) // Should not change
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				// New ProviderID should work for deletion
				cluster.MarkForDeletion(newProviderID)
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(0))
				Expect(deleting).To(Equal(1))
				Expect(pendingdisruption).To(Equal(0))
			})

			It("should move NodeClaim to deleting state when Node is marked for deletion", func() {
				// Verify initial running state
				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				// Mark the node for deletion via cluster state
				cluster.MarkForDeletion(nodeClaim.Status.ProviderID)

				// Update NodeClaim - should detect it's marked for deletion
				nodeClaim.Annotations = map[string]string{"updated": "true"}
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

				// Should be in deleting state
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(0))
				Expect(deleting).To(Equal(1))
				Expect(pendingdisruption).To(Equal(0))
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

		Context("Mark NodeClaims pendingdisruption", func() {
			BeforeEach(func() {
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			})

			It("should handle marking multiple NodeClaims", func() {
				nodeClaim2 := test.NodeClaim(v1.NodeClaim{
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

				ExpectApplied(ctx, env.Client, nodeClaim2)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim2))

				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)

				Expect(running).To(Equal(2))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				ExpectDisruptionReasonObserved(ctx, env.Client, nodeClaimController, nodeClaim)
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)

				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(1))

				ExpectDisruptionReasonObserved(ctx, env.Client, nodeClaimController, nodeClaim2)
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)

				Expect(running).To(Equal(0))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(2))
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

		Context("MarkForDeletion", func() {
			BeforeEach(func() {
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			})

			It("should handle marking multiple NodeClaims for deletion", func() {
				nodeClaim2 := test.NodeClaim(v1.NodeClaim{
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

				ExpectApplied(ctx, env.Client, nodeClaim2)
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim2))

				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)

				Expect(running).To(Equal(2))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				cluster.MarkForDeletion(nodeClaim.Status.ProviderID, nodeClaim2.Status.ProviderID)
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)

				Expect(running).To(Equal(0))
				Expect(deleting).To(Equal(2))
				Expect(pendingdisruption).To(Equal(0))

			})

			It("should move NodeClaim from running to deleting state", func() {
				running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
				Expect(running).To(Equal(1))
				Expect(deleting).To(Equal(0))
				Expect(pendingdisruption).To(Equal(0))

				cluster.MarkForDeletion(nodeClaim.Status.ProviderID)
				running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)

				Expect(running).To(Equal(0))
				Expect(deleting).To(Equal(1))
				Expect(pendingdisruption).To(Equal(0))
			})
		})
	})

	Context("UnmarkForDeletion", func() {
		BeforeEach(func() {
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			cluster.MarkForDeletion(nodeClaim.Status.ProviderID)
		})

		It("should move NodeClaim from deleting to running state", func() {
			running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
			Expect(running).To(Equal(0))
			Expect(deleting).To(Equal(1))
			Expect(pendingdisruption).To(Equal(0))

			cluster.UnmarkForDeletion(nodeClaim.Status.ProviderID)
			running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)

			Expect(running).To(Equal(1))
			Expect(deleting).To(Equal(0))
			Expect(pendingdisruption).To(Equal(0))

		})

		It("should handle unmarking multiple NodeClaims", func() {
			nodeClaim2 := test.NodeClaim(v1.NodeClaim{
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

			ExpectApplied(ctx, env.Client, nodeClaim2)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim2))

			cluster.MarkForDeletion(nodeClaim2.Status.ProviderID)
			running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)

			Expect(running).To(Equal(0))
			Expect(deleting).To(Equal(2))
			Expect(pendingdisruption).To(Equal(0))

			cluster.UnmarkForDeletion(nodeClaim.Status.ProviderID, nodeClaim2.Status.ProviderID)
			running, deleting, pendingdisruption = cluster.NodePoolState.GetNodeCount(nodePool.Name)

			Expect(running).To(Equal(2))
			Expect(deleting).To(Equal(0))
			Expect(pendingdisruption).To(Equal(0))
		})
	})

	Context("Transitions", func() {
		It("should handle concurrent NodeClaim updates and state changes", func() {
			nodeClaim := test.NodeClaim(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "race-test-nodeclaim",
					Labels: map[string]string{
						v1.NodePoolLabelKey: nodePool.Name,
					},
				},
				Status: v1.NodeClaimStatus{
					ProviderID: test.RandomProviderID(),
				},
			})

			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			disrupted := nodeClaim.DeepCopy()
			disrupted.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))

			var wg sync.WaitGroup
			numOperations := 50

			// Concurrent mark/unmark operations
			for i := range numOperations {
				wg.Add(1)
				go func(iteration int) {
					defer wg.Done()

					switch iteration % 3 {
					case 0:
						cluster.MarkForDeletion(nodeClaim.Status.ProviderID)
					case 1:
						cluster.UpdateNodeClaim(disrupted)
					case 2:
						cluster.UnmarkForDeletion(nodeClaim.Status.ProviderID)
					}
				}(i)
			}

			wg.Wait()

			// Final state should be consistent
			running, deleting, pendingdisruption := cluster.NodePoolState.GetNodeCount(nodePool.Name)
			Expect(running + deleting + pendingdisruption).To(Equal(1)) // Should have exactly one NodeClaim
		})
	})
})

var _ = Describe("NodePoolState Derived Accounting", func() {
	var unlaunched *v1.NodeClaim

	BeforeEach(func() {
		nodePool = test.StaticNodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "static"}, Spec: v1.NodePoolSpec{Replicas: new(int64(1))}})
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		unlaunched = test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
		unlaunched.Status.ProviderID = ""
		ExpectApplied(ctx, env.Client, unlaunched)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
		ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
	})

	Context("Launch", func() {
		It("should keep the NodePool's reservation when a NodeClaim's providerID resolves", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(BeEquivalentTo(1))
			// Launch the NodeClaim
			unlaunched.Status.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(BeEquivalentTo(0))
		})
	})

	Context("Observed state", func() {
		It("should count an unlaunched NodeClaim with a DeletionTimestamp as deleting", func() {
			ExpectDeletionTimestampSet(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
		})
		It("should not add conditions to the NodeClaim it observes", func() {
			// The informer hands cluster state the cache's own object, which other controllers read concurrently
			observed := ExpectExists(ctx, env.Client, unlaunched)
			observed.Status.Conditions = nil
			cluster.UpdateNodeClaim(observed)
			Expect(observed.Status.Conditions).To(BeEmpty())
		})
		It("should count an unlaunched NodeClaim that is InstanceTerminating as deleting", func() {
			unlaunched.StatusConditions().SetTrue(v1.ConditionTypeInstanceTerminating)
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
		})
		It("should count a launched NodeClaim with a DeletionTimestamp as deleting after UnmarkForDeletion", func() {
			// Launch the NodeClaim
			unlaunched.Status.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			cluster.MarkForDeletion(unlaunched.Status.ProviderID)
			ExpectDeletionTimestampSet(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			cluster.UnmarkForDeletion(unlaunched.Status.ProviderID)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
		})
		It("should count a NodeClaim with the DisruptionReason condition as pending disruption until it is cleared", func() {
			ExpectDisruptionReasonObserved(ctx, env.Client, nodeClaimController, unlaunched)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)

			Expect(unlaunched.StatusConditions().Clear(v1.ConditionTypeDisruptionReason)).To(Succeed())
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
		})
	})

	Context("Observation order", func() {
		var older *v1.NodeClaim

		BeforeEach(func() {
			older = ExpectExists(ctx, env.Client, unlaunched).DeepCopy()
			ExpectDisruptionReasonObserved(ctx, env.Client, nodeClaimController, unlaunched)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)
		})

		It("should ignore a read older than one already observed", func() {
			cluster.UpdateNodeClaim(older)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)
		})
		It("should apply a read of an already observed version without changing the counts", func() {
			current := ExpectExists(ctx, env.Client, unlaunched)
			cluster.UpdateNodeClaim(current)
			cluster.UpdateNodeClaim(current)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)
		})
		It("should apply a read whose resourceVersion can't be compared", func() {
			older.ResourceVersion = "not-a-resource-version"
			cluster.UpdateNodeClaim(older)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
		})
	})

	Context("MarkNodeClaimForDeletion", func() {
		It("should mark a NodeClaim that hasn't launched", func() {
			Expect(cluster.MarkNodeClaimForDeletion(unlaunched.Name)).To(BeTrue())
			Expect(cluster.NodePoolState.MarkedForDeletion(unlaunched.Name)).To(BeTrue())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
		})
		It("should keep the mark when the NodeClaim's providerID resolves", func() {
			Expect(cluster.MarkNodeClaimForDeletion(unlaunched.Name)).To(BeTrue())
			// Launch the NodeClaim
			unlaunched.Status.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, unlaunched).MarkedForDeletion()).To(BeTrue())
		})
		It("should only report setting the mark to the caller that set it", func() {
			Expect(cluster.MarkNodeClaimForDeletion(unlaunched.Name)).To(BeTrue())
			Expect(cluster.MarkNodeClaimForDeletion(unlaunched.Name)).To(BeFalse())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)

			cluster.UnmarkNodeClaimForDeletion(unlaunched.Name)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)
			Expect(cluster.MarkNodeClaimForDeletion(unlaunched.Name)).To(BeTrue())
		})
		It("should return false for a NodeClaim the disruption queue marked by providerID", func() {
			// Launch the NodeClaim
			unlaunched.Status.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			cluster.MarkForDeletion(unlaunched.Status.ProviderID)
			Expect(cluster.MarkNodeClaimForDeletion(unlaunched.Name)).To(BeFalse())
		})
		It("should not mark a NodeClaim that cluster state hasn't observed", func() {
			unobserved := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
			unobserved.Status.ProviderID = ""
			Expect(cluster.MarkNodeClaimForDeletion(unobserved.Name)).To(BeFalse())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

			// Marking it didn't create a record, so it is active once observed
			ExpectApplied(ctx, env.Client, unobserved)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unobserved))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)
		})
		It("should not mark a NodeClaim after it is deleted", func() {
			ExpectDeleted(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			Expect(cluster.MarkNodeClaimForDeletion(unlaunched.Name)).To(BeFalse())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		})
		It("should be what the NodeClaim's StateNode reports", func() {
			launched := test.NodeClaim(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}},
				Status: v1.NodeClaimStatus{
					ProviderID: test.RandomProviderID(),
					Capacity:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
				},
			})
			ExpectApplied(ctx, env.Client, launched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(launched))
			before := ExpectStateNodeExistsForNodeClaim(cluster, launched)
			Expect(lo.ToPtr(cluster.NodePoolResourcesFor(nodePool.Name)[resources.Node]).Value()).To(BeEquivalentTo(1))

			Expect(cluster.MarkNodeClaimForDeletion(launched.Name)).To(BeTrue())
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, launched).MarkedForDeletion()).To(BeTrue())
			// A copy is a snapshot, so a copy taken before the mark doesn't see it
			Expect(before.MarkedForDeletion()).To(BeFalse())
			// A NodeClaim marked for deletion doesn't count toward its NodePool's resources
			Expect(cluster.NodePoolResourcesFor(nodePool.Name)).To(BeEmpty())

			cluster.UnmarkForDeletion(launched.Status.ProviderID)
			Expect(cluster.NodePoolState.MarkedForDeletion(launched.Name)).To(BeFalse())
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, launched).MarkedForDeletion()).To(BeFalse())
			Expect(lo.ToPtr(cluster.NodePoolResourcesFor(nodePool.Name)[resources.Node]).Value()).To(BeEquivalentTo(1))
		})
	})

	Context("Write responses", func() {
		It("should not bring back a NodeClaim whose write response arrives after it is deleted", func() {
			// Launch the NodeClaim
			unlaunched.Status.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			late := ExpectExists(ctx, env.Client, unlaunched)
			late.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
			ExpectDeleted(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)

			Expect(cluster.NodePoolState.ObserveIfTracked(late)).To(BeFalse())
			ExpectNodeClaimNotInClusterState(cluster, unlaunched.Name)
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		})
		It("should apply a write response to NodePoolState only", func() {
			// Launch the NodeClaim
			unlaunched.Status.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			patched := ExpectExists(ctx, env.Client, unlaunched)
			patched.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
			ExpectApplied(ctx, env.Client, patched)
			Expect(cluster.NodePoolState.ObserveIfTracked(patched)).To(BeTrue())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 1)
			// The StateNode only sees what the informer delivers
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, unlaunched).NodeClaim.StatusConditions(status.WithObservedOnly()).Get(v1.ConditionTypeDisruptionReason).IsTrue()).To(BeFalse())
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, unlaunched).NodeClaim.StatusConditions(status.WithObservedOnly()).Get(v1.ConditionTypeDisruptionReason).IsTrue()).To(BeTrue())
		})
		It("should not roll back the StateNode with a write response older than the informer's version", func() {
			// Launch the NodeClaim
			unlaunched.Status.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			late := ExpectExists(ctx, env.Client, unlaunched)
			late.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
			ExpectApplied(ctx, env.Client, late)
			// The informer delivers the deletion before the patch response arrives
			ExpectDeletionTimestampSet(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, unlaunched).Deleted()).To(BeTrue())

			Expect(cluster.NodePoolState.ObserveIfTracked(late)).To(BeTrue())
			Expect(ExpectStateNodeExistsForNodeClaim(cluster, unlaunched).Deleted()).To(BeTrue())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)
		})
	})

	Context("NodePools", func() {
		It("should record a launched NodeClaim seen before its NodePool as soon as the NodePool is seen", func() {
			unseen := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: new(int64(2))}})
			nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: unseen.Name}}})
			marked, markedNode := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: unseen.Name}}})
			ExpectApplied(ctx, env.Client, unseen, nodeClaim, node, marked, markedNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimController, []*corev1.Node{node, markedNode}, []*v1.NodeClaim{nodeClaim, marked})
			Expect(cluster.MarkForDeletion(markedNode.Spec.ProviderID)).To(ConsistOf(markedNode.Spec.ProviderID))
			Expect(cluster.NodePoolState.Tracked(nodeClaim.Name)).To(BeFalse())
			ExpectStateNodePoolCount(cluster, unseen.Name, 0, 0, 0)

			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(unseen))
			ExpectStateNodePoolCount(cluster, unseen.Name, 1, 1, 0)
			Expect(cluster.NodePoolState.MarkedForDeletion(marked.Name)).To(BeTrue())
		})
		It("should record an unlaunched NodeClaim seen before its NodePool when the informer requeues it", func() {
			unseen := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: new(int64(1))}})
			ExpectApplied(ctx, env.Client, unseen)
			nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: unseen.Name}}})
			nodeClaim.Status.ProviderID = ""
			ExpectApplied(ctx, env.Client, nodeClaim)
			result := ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))

			// It has no StateNode, so seeing the NodePool doesn't record it
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(unseen))
			Expect(cluster.NodePoolState.Tracked(nodeClaim.Name)).To(BeFalse())
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectStateNodePoolCount(cluster, unseen.Name, 1, 0, 0)
		})
		It("should keep observing a recorded NodeClaim after its NodePool is deleted", func() {
			ExpectDeleted(ctx, env.Client, nodePool)
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
			ExpectDeletionTimestampSet(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 1, 0)

			// A NodeClaim first seen after its NodePool is deleted isn't recorded
			orphan := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
			orphan.Status.ProviderID = ""
			ExpectApplied(ctx, env.Client, orphan)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(orphan))
			Expect(cluster.NodePoolState.Tracked(orphan.Name)).To(BeFalse())
		})
		It("should forget a deleted NodeClaim after a while", func() {
			ExpectDeleted(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			// A late update for the deleted NodeClaim doesn't add it back
			cluster.UpdateNodeClaim(unlaunched)
			ExpectNodeClaimNotInClusterState(cluster, unlaunched.Name)

			// Deleted NodeClaims are only remembered long enough to drop late updates
			env.Clock.Step(10 * time.Minute)
			cluster.DeleteNodeClaim("another")
			cluster.UpdateNodeClaim(unlaunched)
			Expect(cluster.NodePoolState.Tracked(unlaunched.Name)).To(BeTrue())
		})
		It("should not be synced until it has seen every static NodePool", func() {
			// Launch the NodeClaim, so it doesn't hold back the sync
			unlaunched.Status.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			Expect(cluster.Synced(ctx)).To(BeTrue())

			cluster.SetSynced(false)
			unseen := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: new(int64(1))}})
			ExpectApplied(ctx, env.Client, unseen)
			Expect(cluster.Synced(ctx)).To(BeFalse())
			ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(unseen))
			Expect(cluster.Synced(ctx)).To(BeTrue())
		})
	})

	Context("Mark before the NodeClaim is tracked", func() {
		It("should carry a mark set on the Node's StateNode into the NodeClaim's record", func() {
			nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: "m5.large",
			}}})
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			Expect(cluster.MarkForDeletion(node.Spec.ProviderID)).To(ConsistOf(node.Spec.ProviderID))

			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			Expect(cluster.NodePoolState.MarkedForDeletion(nodeClaim.Name)).To(BeTrue())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 1, 0)

			// The record now holds the mark, so unmarking it isn't undone when the StateNode is rebuilt
			cluster.UnmarkForDeletion(node.Spec.ProviderID)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			Expect(ExpectStateNodeExists(cluster, node).MarkedForDeletion()).To(BeFalse())
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)
		})
	})

	Context("Reservations", func() {
		It("should keep the NodePool's reservation when its last NodeClaim is deleted", func() {
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(BeEquivalentTo(1))
			ExpectDeleted(ctx, env.Client, unlaunched)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(unlaunched))
			ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)

			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 1, 1)).To(BeEquivalentTo(0))
			cluster.NodePoolState.ReleaseNodeCount(nodePool.Name, 1)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 1, 1)).To(BeEquivalentTo(1))
		})
		It("should count pending disruption NodeClaims against the limit", func() {
			ExpectDisruptionReasonObserved(ctx, env.Client, nodeClaimController, unlaunched)
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(BeEquivalentTo(1))
			Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(BeEquivalentTo(0))
		})
	})
})

var _ = Describe("Dynamic NodePool Deletion Marks", func() {
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node

	BeforeEach(func() {
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: "m5.large",
			}},
			Status: v1.NodeClaimStatus{Capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}},
		})
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectReconcileSucceeded(ctx, nodePoolController, client.ObjectKeyFromObject(nodePool))
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
	})

	It("should not record a dynamic NodePool's NodeClaim", func() {
		Expect(cluster.NodePoolState.Tracked(nodeClaim.Name)).To(BeFalse())
		ExpectStateNodePoolCount(cluster, nodePool.Name, 0, 0, 0)
		Expect(cluster.MarkNodeClaimForDeletion(nodeClaim.Name)).To(BeFalse())
	})
	It("should keep the mark on the StateNode, so copies taken before the mark don't see it", func() {
		before := ExpectStateNodeExists(cluster, node)
		Expect(cluster.MarkForDeletion(node.Spec.ProviderID)).To(ConsistOf(node.Spec.ProviderID))
		Expect(cluster.NodePoolState.Tracked(nodeClaim.Name)).To(BeFalse())
		Expect(before.MarkedForDeletion()).To(BeFalse())
		Expect(ExpectStateNodeExists(cluster, node).MarkedForDeletion()).To(BeTrue())
		Expect(cluster.NodePoolResourcesFor(nodePool.Name)).To(BeEmpty())

		// The mark survives the StateNode being rebuilt
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		Expect(ExpectStateNodeExists(cluster, node).MarkedForDeletion()).To(BeTrue())

		cluster.UnmarkForDeletion(node.Spec.ProviderID)
		Expect(ExpectStateNodeExists(cluster, node).MarkedForDeletion()).To(BeFalse())
		Expect(cluster.NodePoolResourcesFor(nodePool.Name)).ToNot(BeEmpty())
	})
	It("should only return the nodes that MarkForDeletion marked", func() {
		Expect(cluster.MarkForDeletion(node.Spec.ProviderID)).To(ConsistOf(node.Spec.ProviderID))
		Expect(cluster.MarkForDeletion(node.Spec.ProviderID, "unknown")).To(BeEmpty())
	})
	It("should ignore a write response for a dynamic NodePool's NodeClaim", func() {
		patched := nodeClaim.DeepCopy()
		patched.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(v1.DisruptionReasonDrifted), string(v1.DisruptionReasonDrifted))
		Expect(cluster.NodePoolState.ObserveIfTracked(patched)).To(BeFalse())
		Expect(cluster.NodePoolState.Tracked(nodeClaim.Name)).To(BeFalse())
	})
})

var _ = Describe("StateNode Capacity", func() {
	It("should include resources.Node in capacity for an initialized node", func() {
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("4"),
				},
			},
		})
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectMakeNodesInitialized(ctx, env.Client, env.Clock, node)
		ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		stateNode := ExpectStateNodeExists(cluster, node)
		capacity := stateNode.Capacity()

		Expect(capacity[resources.Node]).To(Equal(resource.MustParse("1")))
	})
	It("should include resources.Node in capacity for a NodeClaim without a Node", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("4"),
				},
			},
		})
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))

		stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim)
		capacity := stateNode.Capacity()

		Expect(capacity[resources.Node]).To(Equal(resource.MustParse("1")))
	})
	It("should include resources.Node in capacity for uninitialized node with NodeClaim", func() {
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("4"),
				},
			},
		})
		// Don't initialize - leave as uninitialized
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		stateNode := ExpectStateNodeExists(cluster, node)
		capacity := stateNode.Capacity()

		Expect(capacity[resources.Node]).To(Equal(resource.MustParse("1")))
	})
})

func ExpectStateNodeCount(comparator string, count int) int {
	GinkgoHelper()
	c := 0
	for range cluster.Nodes() {
		c++
	}
	Expect(c).To(BeNumerically(comparator, count))
	return c
}

var _ = Describe("Buffer Pod Counts", func() {
	It("should return false for unknown providerIDs", func() {
		Expect(cluster.HasBufferPods("unknown-provider-id")).To(BeFalse())
		Expect(cluster.BufferPodCount("unknown-provider-id")).To(Equal(0))
	})

	It("should track buffer pods after UpdateBufferPodCounts", func() {
		cluster.UpdateBufferPodCounts(map[string]int{
			"provider-a": 3,
			"provider-b": 1,
		})
		Expect(cluster.HasBufferPods("provider-a")).To(BeTrue())
		Expect(cluster.BufferPodCount("provider-a")).To(Equal(3))
		Expect(cluster.HasBufferPods("provider-b")).To(BeTrue())
		Expect(cluster.BufferPodCount("provider-b")).To(Equal(1))
		Expect(cluster.HasBufferPods("provider-c")).To(BeFalse())
	})

	It("should clear old entries when UpdateBufferPodCounts is called with new map", func() {
		cluster.UpdateBufferPodCounts(map[string]int{
			"provider-a": 5,
		})
		Expect(cluster.HasBufferPods("provider-a")).To(BeTrue())

		// Update with a map that doesn't contain provider-a
		cluster.UpdateBufferPodCounts(map[string]int{
			"provider-b": 2,
		})
		Expect(cluster.HasBufferPods("provider-a")).To(BeFalse())
		Expect(cluster.BufferPodCount("provider-a")).To(Equal(0))
		Expect(cluster.HasBufferPods("provider-b")).To(BeTrue())
		Expect(cluster.BufferPodCount("provider-b")).To(Equal(2))
	})

	It("should clear all entries when called with empty map", func() {
		cluster.UpdateBufferPodCounts(map[string]int{
			"provider-a": 3,
			"provider-b": 1,
		})
		cluster.UpdateBufferPodCounts(map[string]int{})
		Expect(cluster.HasBufferPods("provider-a")).To(BeFalse())
		Expect(cluster.HasBufferPods("provider-b")).To(BeFalse())
	})

	It("should not store entries with count zero", func() {
		cluster.UpdateBufferPodCounts(map[string]int{
			"provider-a": 0,
			"provider-b": 3,
		})
		Expect(cluster.HasBufferPods("provider-a")).To(BeFalse())
		Expect(cluster.HasBufferPods("provider-b")).To(BeTrue())
	})
})
