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

package node_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/metrics/node"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *test.Environment
var cluster *state.Cluster
var nodeController reconcile.Reconciler
var nodeClaimController reconcile.Reconciler
var metricsStateController *node.Controller
var cloudProvider *fake.CloudProvider

var cs karpentercluster.State

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "NodeMetrics")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))

	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	cs = karpentercluster.New(ctx, env.Clock, nil, nil, cloudProvider)
	cluster = state.NewCluster(env.Clock, env.Client, cs.Nodes, cs.NodePoolState, cs.CapacityQuotaState, cs.PodScheduling)
	clusterCost := cost.NewClusterCost(ctx, cloudProvider, env.Client)
	nodeController = NodeInformer(env.Client, cluster, nil)
	nodeClaimController = NodeClaimInformer(env.Client, cluster, informer.NewNodeClaimController(env.Client, cloudProvider, clusterCost))
	metricsStateController = node.NewController(cluster.NodeState)
	// Register derives the stores the controller wakes on, as the operator does, on a manager that never starts.
	m, err := controllerruntime.NewManager(env.Config, controllerruntime.Options{
		Scheme:     scheme.Scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: new(true)},
	})
	Expect(err).ToNot(HaveOccurred())
	Expect(metricsStateController.Register(ctx, m)).To(Succeed())
})

var _ = AfterSuite(func() {
	ExpectCleanedUp(ctx, env.Client)
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Node Metrics", func() {
	var node *corev1.Node
	var resources corev1.ResourceList

	BeforeEach(func() {
		resources = corev1.ResourceList{
			corev1.ResourcePods:   resource.MustParse("100"),
			corev1.ResourceCPU:    resource.MustParse("5000"),
			corev1.ResourceMemory: resource.MustParse("32Gi"),
		}
		node = test.Node(test.NodeOptions{Allocatable: resources})
	})
	It("should update the allocatable metric", func() {
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		for k, v := range resources {
			// A plain node with no NodeClaim is not managed by Karpenter.
			metric, found := FindMetricWithLabelValues("karpenter_nodes_allocatable", map[string]string{
				"node_name":               node.GetName(),
				metrics.ResourceTypeLabel: k.String(),
				"managed":                 "false",
			})
			Expect(found).To(BeTrue())
			Expect(metric.GetGauge().GetValue()).To(BeNumerically("~", v.AsApproximateFloat64()))
		}
	})
	It("should set the managed label on per-node metrics for Karpenter-managed nodes", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{
			Status: v1.NodeClaimStatus{
				ProviderID:  test.RandomProviderID(),
				Allocatable: resources,
			},
		})
		managedNode := test.Node(test.NodeOptions{
			ProviderID:  nodeClaim.Status.ProviderID,
			Allocatable: resources,
		})

		ExpectApplied(ctx, env.Client, managedNode, nodeClaim)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeController, nodeClaimController, []*corev1.Node{managedNode}, []*v1.NodeClaim{nodeClaim})

		for k, v := range resources {
			metric, found := FindMetricWithLabelValues("karpenter_nodes_allocatable", map[string]string{
				"node_name":               managedNode.GetName(),
				metrics.ResourceTypeLabel: k.String(),
				"managed":                 "true",
			})
			Expect(found).To(BeTrue())
			Expect(metric.GetGauge().GetValue()).To(BeNumerically("~", v.AsApproximateFloat64()))
		}
	})
	It("should update the node lifetime and cluster utilization metrics", func() {

		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		metric, found := FindMetricWithLabelValues("karpenter_nodes_current_lifetime_seconds", map[string]string{
			"node_name": node.GetName(),
			"managed":   "false",
		})
		Expect(found).To(BeTrue())
		Expect(metric.GetGauge().GetValue()).To(BeNumerically(">=", 0))

		for resourceName := range resources {
			metric, found := FindMetricWithLabelValues("karpenter_cluster_utilization_percent", map[string]string{
				metrics.ResourceTypeLabel: resourceName.String(),
			})
			Expect(found).To(BeTrue())
			Expect(metric.GetGauge().GetValue()).To(BeNumerically("==", 0))
		}
	})
	It("should remove the node metric gauge when the node is deleted", func() {
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		_, found := FindMetricWithLabelValues("karpenter_nodes_allocatable", map[string]string{
			"node_name": node.GetName(),
		})
		Expect(found).To(BeTrue())

		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		_, found = FindMetricWithLabelValues("karpenter_nodes_allocatable", map[string]string{
			"node_name": node.GetName(),
		})
		Expect(found).To(BeFalse())
	})
})

var _ = Describe("Cluster State Node Count", func() {
	BeforeEach(func() {
		ExpectCleanedUp(ctx, env.Client)
		cluster.Reset()
	})
	apply := func(objs ...client.Object) {
		GinkgoHelper()
		ExpectApplied(ctx, env.Client, objs...)
		for _, obj := range objs {
			switch obj.(type) {
			case *v1.NodeClaim:
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(obj))
			case *corev1.Node:
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(obj))
			}
		}
	}
	It("should keep the contribution laws for the node count", func() {
		for _, b := range []node.NodeCounts{{}, {LaunchedNodeClaims: 2, UnmanagedNodes: 1}} {
			statetest.ExpectContribution(GinkgoT(), b, node.LaunchedNodeClaim(1), node.LaunchedNodeClaim(1))
			statetest.ExpectContribution(GinkgoT(), b, node.UnmanagedNode(1), node.UnmanagedNode(1))
		}
	})
	It("should count the launched NodeClaims and the unmanaged Nodes, and a registering Node once its NodePool label says whose it is", func() {
		nodeClaim, registering := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey:            "default",
			corev1.LabelInstanceTypeStable: "small",
		}}})
		unlaunched := test.NodeClaim()
		unlaunched.Status.ProviderID = ""
		unmanaged := test.Node(test.NodeOptions{ProviderID: test.RandomProviderID()})
		apply(nodeClaim, unlaunched, unmanaged)
		expectNodeCount(2)

		// A Node that registers with its NodePool label is its NodeClaim's node before the owner reference lands.
		apply(registering)
		expectNodeCount(2)
		// Without the label, it reads as unmanaged until the owner reference lands.
		registering = ExpectExists(ctx, env.Client, registering)
		delete(registering.Labels, v1.NodePoolLabelKey)
		apply(registering)
		expectNodeCount(3)
		apply(nodeclaimutils.UpdateNodeOwnerReferences(ExpectExists(ctx, env.Client, nodeClaim), ExpectExists(ctx, env.Client, registering)))
		expectNodeCount(2)

		ExpectDeleted(ctx, env.Client, unmanaged)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(unmanaged))
		expectNodeCount(1)
	})
})

// expectNodeCount expects the cluster state node count gauge, as a scrape reads it.
func expectNodeCount(n int) {
	GinkgoHelper()
	metric, found := FindMetricWithLabelValues("karpenter_cluster_state_node_count", map[string]string{})
	Expect(found).To(BeTrue())
	Expect(metric.GetGauge().GetValue()).To(BeNumerically("==", n))
}
