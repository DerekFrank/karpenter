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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	nodeclaimdisruption "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var nodeClaimDisruptionController *nodeclaimdisruption.Controller
var env *test.Environment
var cp *fake.CloudProvider
var it *cloudprovider.InstanceType

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Disruption")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...), test.WithFieldIndexers(test.NodeProviderIDFieldIndexer(ctx)))
	ctx = options.ToContext(ctx, test.Options())
	cp = fake.NewCloudProvider()
	nodeClaimDisruptionController = nodeclaimdisruption.NewController(env.Clock, env.Client, cp)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	resources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("100"),
		corev1.ResourceMemory: resource.MustParse("100Gi"),
	}
	it = fake.NewInstanceType(test.RandomName(),
		fake.WithArchitecture("arch"),
		fake.WithResources(resources),
		fake.WithOperatingSystems(string(corev1.Linux)),
		fake.WithOfferings(
			cloudprovider.Offering{
				Available: true,
				Requirements: scheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:  v1.CapacityTypeSpot,
					corev1.LabelTopologyZone: "test-zone-1a",
				}),
				Price: fake.PriceFromResources(resources),
			},
			cloudprovider.Offering{
				Available: true,
				Requirements: scheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
					corev1.LabelTopologyZone: "test-zone-1a",
				}),
				Price: fake.PriceFromResources(resources),
			},
		),
	)
	cp.InstanceTypes = append(cp.InstanceTypes, it)
	ctx = options.ToContext(ctx, test.Options())
	env.Clock.SetTime(time.Now())
})

var _ = AfterEach(func() {
	cp.Reset()
	nodeClaimDisruptionController.Reset()
	ExpectCleanedUp(ctx, env.Client)
})

var _ = Describe("Disruption", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node

	BeforeEach(func() {
		nodePool = test.NodePool()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: it.Name,
				},
			},
		})
		// set the lastPodEvent to 5 minutes in the past
		nodeClaim.Status.LastPodEventTime.Time = env.Clock.Now().Add(-5 * time.Minute)
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)
	})
	It("should set multiple disruption conditions simultaneously", func() {
		cp.Drifted = "drifted"
		nodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmpty
		nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("30s")
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)

		// step forward to make the node empty
		env.Clock.Step(60 * time.Second)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimDisruptionController, nodeClaim)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).IsTrue()).To(BeTrue())
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable).IsTrue()).To(BeTrue())
	})
	It("should not set consolidatable condition for Static Nodepool", func() {
		nodePool = test.StaticNodePool()
		nodePool.Spec.Replicas = new(int64(1))
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: it.Name,
				},
			},
		})
		// set the lastPodEvent to 5 minutes in the past
		nodeClaim.Status.LastPodEventTime.Time = env.Clock.Now().Add(-5 * time.Minute)
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)

		cp.Drifted = "drifted"
		nodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmpty
		nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("30s")
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)

		// step forward to make the node empty
		env.Clock.Step(60 * time.Second)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimDisruptionController, nodeClaim)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).IsTrue()).To(BeTrue())
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable).IsTrue()).To(BeFalse())
	})
	It("should remove multiple disruption conditions simultaneously", func() {
		nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("Never")

		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)

		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)
		ExpectObjectReconciled(ctx, env.Client, nodeClaimDisruptionController, nodeClaim)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted)).To(BeNil())
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable)).To(BeNil())
	})
})

var _ = Describe("NodePoolChangedPredicate", func() {
	var nodePool, stored *v1.NodePool
	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			v1.NodePoolHashAnnotationKey:        "hash",
			v1.NodePoolHashVersionAnnotationKey: "v1",
		}}})
		ExpectApplied(ctx, env.Client, nodePool)
		stored = nodePool.DeepCopy()
	})
	updated := func() bool {
		return nodeclaimdisruption.NodePoolChangedPredicate().Update(event.UpdateEvent{ObjectOld: stored, ObjectNew: nodePool})
	}
	It("should pass NodePool creates and deletes", func() {
		Expect(nodeclaimdisruption.NodePoolChangedPredicate().Create(event.CreateEvent{Object: nodePool})).To(BeTrue())
		Expect(nodeclaimdisruption.NodePoolChangedPredicate().Delete(event.DeleteEvent{Object: nodePool})).To(BeTrue())
	})
	It("should not pass NodePool status updates", func() {
		nodePool.Status.Nodes = new(int64(3))
		nodePool.Status.Resources = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		ExpectApplied(ctx, env.Client, nodePool)
		Expect(nodePool.Generation).To(Equal(stored.Generation))
		Expect(updated()).To(BeFalse())
	})
	It("should not pass NodePool metadata updates that leave its hash", func() {
		nodePool.Labels = lo.Assign(nodePool.Labels, map[string]string{"test": "label"})
		ExpectApplied(ctx, env.Client, nodePool)
		Expect(updated()).To(BeFalse())
	})
	It("should pass NodePool spec updates", func() {
		nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("5m")
		ExpectApplied(ctx, env.Client, nodePool)
		Expect(nodePool.Generation).ToNot(Equal(stored.Generation))
		Expect(updated()).To(BeTrue())
	})
	DescribeTable("should pass NodePool hash annotation updates",
		func(key string) {
			nodePool.Annotations[key] = "changed"
			ExpectApplied(ctx, env.Client, nodePool)
			Expect(nodePool.Generation).To(Equal(stored.Generation))
			Expect(updated()).To(BeTrue())
		},
		Entry("hash", v1.NodePoolHashAnnotationKey),
		Entry("hash version", v1.NodePoolHashVersionAnnotationKey),
	)
})
