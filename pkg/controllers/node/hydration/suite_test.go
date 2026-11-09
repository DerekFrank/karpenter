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

package hydration_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/node/hydration"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var hydrationController *hydration.Controller
var nodeState *karpentercluster.NodeState
var env *test.Environment
var cloudProvider *fake.CloudProvider

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lifecycle")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...), test.WithFieldIndexers(test.NodeProviderIDFieldIndexer(ctx), test.NodeClaimProviderIDFieldIndexer(ctx)))
	ctx = options.ToContext(ctx, test.Options())

	cloudProvider = fake.NewCloudProvider()
	nodeState = karpentercluster.New(ctx, env.Clock, nil, nil, cloudProvider).Nodes
	hydrationController = hydration.NewController(env.Client, cloudProvider, nodeState)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
	cloudProvider.Reset()
})

var _ = Describe("Hydration", func() {
	DescribeTable(
		"Hydration",
		func(isNodeClaimManaged bool) {
			nodeClassRef := lo.Ternary(isNodeClaimManaged, &v1.NodeClassReference{
				Group: "karpenter.test.sh",
				Kind:  "TestNodeClass",
				Name:  "default",
			}, &v1.NodeClassReference{
				Group: "karpenter.test.sh",
				Kind:  "UnmanagedNodeClass",
				Name:  "default",
			})
			nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
				Spec: v1.NodeClaimSpec{
					NodeClassRef: nodeClassRef,
				},
			})
			delete(node.Labels, v1.NodeClassLabelKey(nodeClassRef.GroupKind()))
			ExpectApplied(ctx, env.Client, nodeClaim)
			node = nodeclaimutils.UpdateNodeOwnerReferences(nodeClaim, node)
			ExpectApplied(ctx, env.Client, node)
			nodeState.NodeClaimStore.Observe(nodeClaim.DeepCopy())
			nodeState.NodeStore.Observe(node.DeepCopy())
			ExpectReconcileSucceeded(ctx, hydrationController, client.ObjectKeyFromObject(nodeClaim))

			// The missing NodeClass label should have been propagated to the Node
			node = ExpectExists(ctx, env.Client, node)
			value, ok := node.Labels[v1.NodeClassLabelKey(nodeClassRef.GroupKind())]
			Expect(ok).To(Equal(isNodeClaimManaged))
			if isNodeClaimManaged {
				Expect(value).To(Equal(nodeClassRef.Name))
			}
		},
		Entry("should hydrate missing metadata onto the Node", true),
		Entry("should ignore Nodes which aren't managed by this Karpenter instance", false),
	)
})

var _ = Describe("Hydration Wakes", func() {
	var nodes *karpentercluster.NodeState
	var queue workqueue.TypedRateLimitingInterface[reconcile.Request]
	BeforeEach(func() {
		nodes = karpentercluster.New(ctx, env.Clock, nil, nil, cloudProvider).Nodes
		c := hydration.NewController(env.Client, cloudProvider, nodes)
		m, err := controllerruntime.NewManager(env.Config, controllerruntime.Options{
			Scheme:     scheme.Scheme,
			Metrics:    metricsserver.Options{BindAddress: "0"},
			Controller: config.Controller{SkipNameValidation: new(true)},
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(c.Register(ctx, m)).To(Succeed())
		queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
		DeferCleanup(queue.ShutDown)
		started, stop := context.WithCancel(ctx)
		DeferCleanup(stop)
		Expect(c.NodeClasses().Start(started, queue)).To(Succeed())
	})
	drain := func() []string {
		var names []string
		for queue.Len() > 0 {
			req, _ := queue.Get()
			names = append(names, req.Name)
			queue.Done(req)
		}
		return names
	}
	It("should wake for a NodeClaim once it's seen with the Node it registered, and not on the NodeClaim's updates", func() {
		nodeClaim, node := test.NodeClaimAndNode()
		ExpectApplied(ctx, env.Client, nodeClaim)
		node = nodeclaimutils.UpdateNodeOwnerReferences(nodeClaim, node)
		ExpectApplied(ctx, env.Client, node)

		nodes.NodeStore.Observe(node.DeepCopy())
		Expect(drain()).To(BeEmpty(), "want nothing before the NodeClaim is seen")
		nodes.NodeClaimStore.Observe(nodeClaim.DeepCopy())
		Expect(drain()).To(Equal([]string{nodeClaim.Name}), "want the NodeClaim once it's seen")

		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		ExpectApplied(ctx, env.Client, nodeClaim)
		nodes.NodeClaimStore.Observe(nodeClaim.DeepCopy())
		Expect(drain()).To(BeEmpty(), "want nothing for the NodeClaim's status")

		nodes.NodeClaimStore.Forget(nodeClaim.Name)
		Expect(drain()).To(Equal([]string{nodeClaim.Name}), "want the NodeClaim once it's gone")
	})
})
