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

package registrationhealth_test

import (
	"context"
	"testing"

	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/nodepool/registrationhealth"

	"github.com/awslabs/operatorpkg/object"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpstate "sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var (
	controller    *registrationhealth.Controller
	ctx           context.Context
	env           *test.Environment
	cloudProvider *fake.CloudProvider
	nodePool      *v1.NodePool
	nodeClass     *v1alpha1.TestNodeClass
	npState       *cluster.NodePoolState
	registrations *cluster.Registrations
	nodePools     *cluster.NodePools
)

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "RegistrationHealth")
}

var _ = BeforeSuite(func() {
	cloudProvider = fake.NewCloudProvider()
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))
	cs := cluster.New(ctx, env.Clock, env.Overlay, cloudProvider)
	npState, registrations = cs.NodePoolState, cs.Registrations
	nodePools = cluster.NewNodePools(env.Overlay, npState)
	controller = registrationhealth.NewController(env.Clock, env.Overlay, cloudProvider, npState)
})
var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

// record records a launch outcome for the NodePool's current spec, as the NodeClaim lifecycle controller does.
func record(registered bool) {
	outcome := cluster.RegistrationOutcome{Registered: registered}
	if !registered {
		outcome.Reason, outcome.Message = "InsufficientCapacity", "no capacity"
	}
	registrations.Record(nodePool, nodeClass.GetGeneration(), outcome)
}

// expectRecordUntouched expects nothing recorded for the NodePool, so the controller wrote no outcome.
func expectRecordUntouched() {
	GinkgoHelper()
	np, err := nodePools.Get(ctx, client.ObjectKeyFromObject(nodePool))
	Expect(err).ToNot(HaveOccurred())
	Expect(np.Intent.Registration.Value).To(BeZero(), "the controller recorded launch outcomes")
}

func expectCondition(want metav1.ConditionStatus) {
	GinkgoHelper()
	nodePool = ExpectExists(ctx, env.Client, nodePool)
	cond := nodePool.StatusConditions().Get(v1.ConditionTypeNodeRegistrationHealthy)
	Expect(cond).ToNot(BeNil(), "NodeRegistrationHealthy is unset")
	Expect(cond.Status).To(Equal(want), "NodeRegistrationHealthy = %+v", cond)
}

var _ = Describe("RegistrationHealth", func() {
	BeforeEach(func() {
		nodePool = test.NodePool()
		nodeClass = test.NodeClass(v1alpha1.TestNodeClass{
			ObjectMeta: metav1.ObjectMeta{Name: nodePool.Spec.Template.Spec.NodeClassRef.Name},
		})
		nodePool.Spec.Template.Spec.NodeClassRef.Group = object.GVK(nodeClass).Group
		nodePool.Spec.Template.Spec.NodeClassRef.Kind = object.GVK(nodeClass).Kind
		_ = nodePool.StatusConditions().Clear(v1.ConditionTypeNodeRegistrationHealthy)
	})
	// apply applies the NodePool and its NodeClass; env.Client waits until the NodePool store has seen the NodePool.
	apply := func() {
		GinkgoHelper()
		ExpectApplied(ctx, env.Client, nodePool, nodeClass)
		nodePool = ExpectExists(ctx, env.Client, nodePool)
	}
	It("should ignore setting NodeRegistrationHealthy status condition on NodePools which aren't managed by this instance of Karpenter", func() {
		nodePool.Spec.Template.Spec.NodeClassRef = &v1.NodeClassReference{
			Group: "karpenter.test.sh",
			Kind:  "UnmanagedNodeClass",
			Name:  "default",
		}
		ExpectApplied(ctx, env.Client, nodePool, nodeClass)
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		nodePool = ExpectExists(ctx, env.Client, nodePool)
		Expect(nodePool.StatusConditions().Get(v1.ConditionTypeNodeRegistrationHealthy)).To(BeNil())
	})
	It("should not set NodeRegistrationHealthy status condition on nodePool when nodeClass does not exist", func() {
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		nodePool = ExpectExists(ctx, env.Client, nodePool)
		Expect(nodePool.StatusConditions().Get(v1.ConditionTypeNodeRegistrationHealthy)).To(BeNil())
	})
	It("should set NodeRegistrationHealthy status condition on a new nodePool as Unknown", func() {
		apply()
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionUnknown)
		Expect(nodePool.Status.NodeClassObservedGeneration).To(Equal(nodeClass.GetGeneration()))
		expectRecordUntouched()
	})
	It("should set NodeRegistrationHealthy status condition to True once a recorded NodeClaim registers", func() {
		apply()
		record(true)
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionTrue)
	})
	It("should set NodeRegistrationHealthy status condition to False with the newest failure's reason once recorded failures reach the threshold", func() {
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		apply()
		record(true)
		record(false)
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionTrue)

		registrations.Record(nodePool, nodeClass.GetGeneration(), cluster.RegistrationOutcome{Reason: "RegistrationFailed", Message: "Failed to register node"})
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionFalse)
		cond := nodePool.StatusConditions().Get(v1.ConditionTypeNodeRegistrationHealthy)
		Expect(cond.Reason).To(Equal("RegistrationFailed"))
		Expect(cond.Message).To(Equal("Failed to register node"))
	})
	It("should keep NodeRegistrationHealthy status condition False until registrations bring the failures under the threshold", func() {
		apply()
		record(false)
		record(false)
		record(true)
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionFalse)

		record(true)
		record(true)
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionTrue)
	})
	It("should set NodeRegistrationHealthy status condition on nodePool as Unknown, ignoring the recorded outcomes, if the nodeClass generation changes", func() {
		nodePool.StatusConditions().SetFalse(v1.ConditionTypeNodeRegistrationHealthy, "unhealthy", "unhealthy")
		nodePool.Status.NodeClassObservedGeneration = int64(1)
		apply()
		record(false)
		record(false)
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionFalse)

		nodeClass.Spec.Tags = map[string]string{"keyTag-1": "valueTag-1"}
		ExpectApplied(ctx, env.Client, nodeClass)
		Expect(nodeClass.GetGeneration()).To(Equal(int64(2)))
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionUnknown)
		Expect(nodePool.Status.NodeClassObservedGeneration).To(Equal(int64(2)))

		// Outcomes recorded for the new NodeClass generation decide the condition.
		record(true)
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionTrue)
	})
	It("should set NodeRegistrationHealthy status condition on nodePool as Unknown, ignoring the recorded outcomes, if the nodePool is updated", func() {
		nodePool.StatusConditions().SetFalse(v1.ConditionTypeNodeRegistrationHealthy, "unhealthy", "unhealthy")
		nodePool.Status.NodeClassObservedGeneration = int64(1)
		apply()
		record(false)
		record(false)

		nodePool.Spec.Limits = map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("14")}
		apply()
		_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
		expectCondition(metav1.ConditionUnknown)
		Expect(npState.RegistrationHealth(nodePool, nodeClass.GetGeneration()).Status).To(Equal(cluster.RegistrationUnknown))
	})
	// After a restart nothing is recorded, and a condition observed for the current spec stands.
	DescribeTable("should keep a persisted NodeRegistrationHealthy status condition when no outcomes are recorded",
		func(set func(*v1.NodePool), want metav1.ConditionStatus) {
			set(nodePool)
			nodePool.Status.NodeClassObservedGeneration = int64(1)
			apply()
			_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			expectCondition(want)
			expectRecordUntouched()

			// A single failure doesn't decide the health, so the persisted condition still stands.
			record(false)
			_ = ExpectObjectReconciled(ctx, env.Client, controller, nodePool)
			expectCondition(want)
		},
		Entry("True", func(np *v1.NodePool) { np.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy) }, metav1.ConditionTrue),
		Entry("False", func(np *v1.NodePool) {
			np.StatusConditions().SetFalse(v1.ConditionTypeNodeRegistrationHealthy, "unhealthy", "unhealthy")
		}, metav1.ConditionFalse),
	)
	It("should drop the NodePool's launch outcomes once the NodePool is deleted", func() {
		apply()
		record(false)
		record(false)
		Expect(npState.RegistrationHealth(nodePool, nodeClass.GetGeneration()).Status).To(Equal(cluster.RegistrationUnhealthy))

		ExpectDeleted(ctx, env.Client, nodePool)
		Expect(npState.RegistrationHealth(nodePool, nodeClass.GetGeneration()).Status).To(Equal(cluster.RegistrationUnknown))
	})
	It("should drop the launch outcomes of a NodePool replaced by another of the same name", func() {
		apply()
		record(false)
		record(false)

		// Delete and re-create the NodePool under the same name; the API server gives it a new UID, so the recorded
		// outcomes, keyed by the original UID, no longer count for either the old or the replacement spec.
		ExpectDeleted(ctx, env.Client, nodePool)
		replacement := test.NodePool(v1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: nodePool.Name},
			Spec:       nodePool.Spec,
		})
		ExpectApplied(ctx, env.Client, replacement)
		replacement = ExpectExists(ctx, env.Client, replacement)
		Expect(npState.RegistrationHealth(replacement, nodeClass.GetGeneration()).Status).To(Equal(cluster.RegistrationUnknown))
		Expect(npState.RegistrationHealth(nodePool, nodeClass.GetGeneration()).Status).To(Equal(cluster.RegistrationUnknown))
	})
})

var _ = Describe("RegistrationHealth Wakes", func() {
	var state *cluster.NodePoolState
	var recorder *cluster.Registrations
	var srcs *statetest.Sources
	var queue workqueue.TypedRateLimitingInterface[reconcile.Request]
	BeforeEach(func() {
		// Build a fresh state per spec over an in-memory source, so this spec drives the source it reads from without
		// the main suite's overlay or cache in the loop.
		srcs = statetest.NewSources()
		cs := cluster.New(ctx, env.Clock, nil, cloudProvider, cluster.WithSources(func(o client.Object) karpstate.Newest { return srcs.For(o) }))
		state, recorder = cs.NodePoolState, cs.Registrations
		c := registrationhealth.NewController(env.Clock, env.Overlay, cloudProvider, state)
		queue = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
		DeferCleanup(queue.ShutDown)
		started, stop := context.WithCancel(ctx)
		DeferCleanup(stop)
		Expect(c.Registrations().Start(started, queue)).To(Succeed())
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
	It("should wake for a NodePool when it appears, its spec or registration health changes, or it goes, and not on its status", func() {
		np := test.NodePool()
		np.UID = "wakes-np"
		np.ResourceVersion = "1"
		np.Generation = 1
		srcs.Put(np.DeepCopy())
		Expect(drain()).To(Equal([]string{np.Name}), "want the NodePool once it's seen")

		recorder.Record(np, 1, cluster.RegistrationOutcome{Registered: true})
		Expect(drain()).To(Equal([]string{np.Name}), "want the NodePool once an outcome makes it healthy")
		recorder.Record(np, 1, cluster.RegistrationOutcome{Registered: true})
		recorder.Record(np, 1, cluster.RegistrationOutcome{Reason: "Failed"})
		Expect(drain()).To(BeEmpty(), "want nothing for outcomes that leave its health")
		recorder.Record(np, 1, cluster.RegistrationOutcome{Reason: "Failed"})
		Expect(drain()).To(Equal([]string{np.Name}), "want the NodePool once an outcome makes it unhealthy")
		recorder.Record(np, 2, cluster.RegistrationOutcome{Reason: "Failed"})
		Expect(drain()).To(Equal([]string{np.Name}), "want the NodePool once outcomes are recorded for a new spec")

		statusOnly := np.DeepCopy()
		statusOnly.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		statusOnly.ResourceVersion = "2"
		srcs.Put(statusOnly)
		Expect(drain()).To(BeEmpty(), "want nothing for the NodePool's status")

		specChange := statusOnly.DeepCopy()
		specChange.Spec.Limits = map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("14")}
		specChange.ResourceVersion = "3"
		specChange.Generation = 2
		srcs.Put(specChange)
		Expect(drain()).To(Equal([]string{np.Name}), "want the NodePool once its spec changes")

		srcs.Remove(np)
		Expect(drain()).To(Equal([]string{np.Name}), "want the NodePool once it's gone")
	})
})
