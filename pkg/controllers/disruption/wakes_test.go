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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Wakes", func() {
	var emptiness *disruption.Emptiness
	var controller *disruption.Controller
	var wakes workqueue.TypedRateLimitingInterface[reconcile.Request]
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	register := func(c *disruption.Controller) {
		GinkgoHelper()
		m, err := controllerruntime.NewManager(env.Config, controllerruntime.Options{
			Scheme:     scheme.Scheme,
			Metrics:    metricsserver.Options{BindAddress: "0"},
			Controller: config.Controller{SkipNameValidation: new(true)},
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(c.Register(ctx, m)).To(Succeed())
	}
	drain := func() bool {
		found := wakes.Len() > 0
		for wakes.Len() > 0 {
			req, _ := wakes.Get()
			wakes.Done(req)
		}
		return found
	}
	// nothingToDo runs emptiness on a cluster with no empty node, which finds nothing to do and marks the cluster
	// consolidated.
	nothingToDo := func() {
		GinkgoHelper()
		Expect(emptiness.IsConsolidated()).To(BeFalse())
		cmds, err := emptiness.ComputeCommands(ctx, map[string]int{})
		Expect(err).ToNot(HaveOccurred())
		Expect(cmds).To(BeEmpty())
	}
	BeforeEach(func() {
		emptiness = disruption.NewEmptiness(disruption.MakeConsolidation(env.Clock, cluster, env.Overlay, prov, cloudProvider, recorder, queue), disruption.WithValidator(NopValidator{}))
		controller = disruption.NewController(ctx, env.Clock, env.Overlay, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(emptiness))
		register(controller)
		wakes = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
		DeferCleanup(wakes.ShutDown)
		started, stop := context.WithCancel(ctx)
		DeferCleanup(stop)
		Expect(controller.Wakes().Start(started, wakes)).To(Succeed())

		nodePool = test.NodePool()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(drain()).To(BeTrue(), "want a wake for the new node")
	})
	It("should wake on the pods and node changes consolidation decides on, and not on pod status or nominations", func() {
		pod := test.Pod(test.PodOptions{NodeName: node.Name})
		ExpectApplied(ctx, env.Client, pod)
		Expect(drain()).To(BeTrue(), "want a wake for the pod binding")

		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(env.Client.Status().Update(ctx, pod)).To(Succeed())
		cs.Nomination.Nominate(ctx, nodeClaim.Name)
		Expect(drain()).To(BeFalse(), "want nothing for the pod's status or the node's nomination")

		pod.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}
		ExpectApplied(ctx, env.Client, pod)
		Expect(drain()).To(BeTrue(), "want a wake for the pod's do-not-disrupt annotation")

		cs.Deletion.Mark(nodeClaim.Name)
		Expect(drain()).To(BeTrue(), "want a wake for the node's deletion mark")
	})
	It("should skip consolidation until a change, and revalidate once the revalidation period passes", func() {
		nothingToDo()
		Expect(emptiness.IsConsolidated()).To(BeTrue(), "want consolidation skipped while nothing changes")

		pod := test.Pod(test.PodOptions{NodeName: node.Name})
		ExpectApplied(ctx, env.Client, pod)
		nothingToDo()
		Expect(emptiness.IsConsolidated()).To(BeTrue())

		env.Clock.Step(disruption.RevalidationPeriod)
		Expect(emptiness.IsConsolidated()).To(BeFalse(), "want consolidation revalidated after the period")
	})
	It("should not skip consolidation on a method the controller didn't register", func() {
		unregistered := disruption.NewEmptiness(disruption.MakeConsolidation(env.Clock, cluster, env.Overlay, prov, cloudProvider, recorder, queue), disruption.WithValidator(NopValidator{}))
		_, err := unregistered.ComputeCommands(ctx, map[string]int{})
		Expect(err).ToNot(HaveOccurred())
		Expect(unregistered.IsConsolidated()).To(BeFalse())
	})
	It("should run at least every revalidation period", func() {
		Expect(ExpectSingletonReconciled(ctx, controller).RequeueAfter).To(Equal(disruption.RevalidationPeriod))
	})
})
