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

package pod_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/metrics/pod"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var podController *pod.Controller
var ctx context.Context
var env *test.Environment
var cluster *state.Cluster
var cloudProvider *fake.CloudProvider

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "PodMetrics")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment()
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	cs := karpentercluster.New(ctx, env.Clock, nil, nil, cloudProvider)
	cluster = state.NewCluster(env.Clock, env.Client, cs.Nodes, cs.NodePoolState, cs.CapacityQuotaState, cs.PodScheduling)
	podController = pod.NewController(cs.Nodes, cs.PodScheduling)
	Expect(podController.Register(ctx, nil)).To(Succeed())
})

var _ = AfterEach(func() {
	cluster.Reset()
	pod.PodStartupDurationSeconds.Reset()
	pod.PodProvisioningStartupDurationSeconds.Reset()
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Pod Metrics", func() {
	It("should update the pod state metrics", func() {
		p := test.Pod()
		ExpectApplied(ctx, env.Client, p)
		sync(p)

		_, found := FindMetricWithLabelValues("karpenter_pods_state", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
	})
	It("should set the managed label based on whether the pod's node is Karpenter-managed", func() {
		// Pod scheduled onto a Karpenter-managed node (identified by the nodepool label).
		managedNode := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey: "default",
		}}})
		managedPod := test.Pod(test.PodOptions{NodeName: managedNode.Name})
		// Pod scheduled onto a node not managed by Karpenter (no nodepool label).
		unmanagedNode := test.Node()
		unmanagedPod := test.Pod(test.PodOptions{NodeName: unmanagedNode.Name})

		ExpectApplied(ctx, env.Client, managedNode, managedPod, unmanagedNode, unmanagedPod)
		sync(managedPod)
		sync(unmanagedPod)

		_, found := FindMetricWithLabelValues("karpenter_pods_state", map[string]string{
			"name":      managedPod.GetName(),
			"namespace": managedPod.GetNamespace(),
			"managed":   "true",
		})
		Expect(found).To(BeTrue())

		_, found = FindMetricWithLabelValues("karpenter_pods_state", map[string]string{
			"name":      unmanagedPod.GetName(),
			"namespace": unmanagedPod.GetNamespace(),
			"managed":   "false",
		})
		Expect(found).To(BeTrue())
	})
	It("should update the pod state metrics with pod phase", func() {
		p := test.Pod()
		ExpectApplied(ctx, env.Client, p)
		sync(p)

		_, found := FindMetricWithLabelValues("karpenter_pods_state", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		p.Status.Phase = corev1.PodRunning
		ExpectApplied(ctx, env.Client, p)
		sync(p)

		_, found = FindMetricWithLabelValues("karpenter_pods_state", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
			"phase":     string(p.Status.Phase),
		})
		Expect(found).To(BeTrue())
	})
	It("should update the pod bound and unbound time metrics", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending

		env.Clock.Step(1 * time.Hour)
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {p}}, map[string][]*corev1.Pod{"nc1": {p}})

		// PodScheduled condition does not exist, emit pods_unbound_time_seconds metric
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will add pod to pending pods and unscheduled pods set
		_, found := FindMetricWithLabelValues("karpenter_pods_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionUnknown, LastTransitionTime: metav1.Now()}}
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will add pod to pending pods and unscheduled pods set
		_, found = FindMetricWithLabelValues("karpenter_pods_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		// Pod is still pending but has bound. At this step pods_unbound_duration holds at the time the pod took to bind.
		p.Status.Phase = corev1.PodPending
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will check if the pod was scheduled or not
		metric, found := FindMetricWithLabelValues("karpenter_pods_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		unboundTime := metric.GetGauge().GetValue()
		time.Sleep(10 * time.Millisecond)
		metric, _ = FindMetricWithLabelValues("karpenter_pods_unbound_time_seconds", map[string]string{"name": p.GetName(), "namespace": p.GetNamespace()})
		Expect(metric.GetGauge().GetValue()).To(Equal(unboundTime))

		metric, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		provisioningUnboundTime := metric.GetGauge().GetValue()
		time.Sleep(10 * time.Millisecond)
		metric, _ = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{"name": p.GetName(), "namespace": p.GetNamespace()})
		Expect(metric.GetGauge().GetValue()).To(Equal(provisioningUnboundTime))

		// Pod is still running and has bound. At this step pods_bound_duration should be fired and pods_unbound_time_seconds should be deleted
		p.Status.Phase = corev1.PodRunning
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will check if the pod was scheduled or not
		_, found = FindMetricWithLabelValues("karpenter_pods_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())

		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())

		_, found = FindMetricWithLabelValues("karpenter_pods_bound_duration_seconds", map[string]string{})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_bound_duration_seconds", map[string]string{})
		Expect(found).To(BeTrue())
	})
	DescribeTable("should label the pod binding metrics with dynamic_resources",
		func(pod *corev1.Pod, expected string) {
			if expected == "true" && env.Version.Minor() < 34 {
				Skip("DRA is only available in K8s versions >= 1.34.x")
			}
			pod.Status.Phase = corev1.PodPending

			env.Clock.Step(1 * time.Hour)
			cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})

			// PodScheduled condition does not exist yet, so the unbound metrics are emitted labeled by dynamic_resources.
			ExpectApplied(ctx, env.Client, pod)
			sync(pod)
			_, found := FindMetricWithLabelValues("karpenter_pods_unbound_time_seconds", map[string]string{
				"name":              pod.GetName(),
				"namespace":         pod.GetNamespace(),
				"dynamic_resources": expected,
			})
			Expect(found).To(BeTrue())
			_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
				"name":              pod.GetName(),
				"namespace":         pod.GetNamespace(),
				"dynamic_resources": expected,
			})
			Expect(found).To(BeTrue())

			// Pod becomes scheduled and running, firing the bound duration metrics labeled by dynamic_resources.
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
			ExpectApplied(ctx, env.Client, pod)
			sync(pod)
			_, found = FindMetricWithLabelValues("karpenter_pods_bound_duration_seconds", map[string]string{
				"dynamic_resources": expected,
			})
			Expect(found).To(BeTrue())
			_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_bound_duration_seconds", map[string]string{
				"dynamic_resources": expected,
			})
			Expect(found).To(BeTrue())
		},
		Entry("when the pod does not request dynamic resources", test.Pod(), "false"),
		Entry("when the pod requests dynamic resources via a pod-level resource claim",
			test.Pod(test.PodOptions{ResourceClaims: []corev1.PodResourceClaim{test.PodResourceClaimReference("gpu", "gpu-claim")}}), "true"),
	)
	It("should update the pod startup and unstarted time metrics", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending

		env.Clock.Step(1 * time.Hour)
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {p}}, map[string][]*corev1.Pod{"nc1": {p}})
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will add pod to pending pods and unscheduled pods set
		_, found := FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		// Pod is now running but readiness condition is not set
		p.Status.Phase = corev1.PodRunning
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will check if the pod was scheduled or not
		_, found = FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		// Pod is now running but readiness is unknown
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionUnknown, LastTransitionTime: metav1.Now()}}
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will check if the pod was scheduled or not
		_, found = FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		// Pod is now running and ready. At this step pods_startup_duration should be fired and pods_unstarted_time should be deleted
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will check if the pod was scheduled or not
		_, found = FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())

		_, found = FindMetricWithLabelValues("karpenter_pods_startup_duration_seconds", nil)
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_startup_duration_seconds", nil)
		Expect(found).To(BeTrue())
	})
	It("should update the pod unstarted time metrics when the pod has succeeded", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending

		env.Clock.Step(1 * time.Hour)
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {p}}, map[string][]*corev1.Pod{"nc1": {p}})
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will add pod to pending pods and unscheduled pods set
		_, found := FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		// Pod has now succeeded but readiness condition is set to false because the pod is now completed
		p.Status.Phase = corev1.PodSucceeded
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.Now()},
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
		}
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will check if the pod was scheduled and completed
		_, found = FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())

		_, found = FindMetricWithLabelValues("karpenter_pods_startup_duration_seconds", nil)
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_startup_duration_seconds", nil)
		Expect(found).To(BeFalse())
	})
	It("should update the pod unstarted time metrics when the pod has failed", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending

		env.Clock.Step(1 * time.Hour)
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {p}}, map[string][]*corev1.Pod{"nc1": {p}})
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will add pod to pending pods and unscheduled pods set
		_, found := FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		// Pod has now failed and readiness condition is set to false
		p.Status.Phase = corev1.PodFailed
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.Now()},
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
		}
		ExpectApplied(ctx, env.Client, p)
		sync(p) //This will check if the pod was scheduled and completed
		_, found = FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())

		_, found = FindMetricWithLabelValues("karpenter_pods_startup_duration_seconds", nil)
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_startup_duration_seconds", nil)
		Expect(found).To(BeFalse())
	})
	It("should create and delete provisioning undecided metrics based on scheduling simulatinos", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending
		ExpectApplied(ctx, env.Client, p)

		sync(p)
		env.Clock.Step(1 * time.Hour)
		_, found := FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())

		// Expect the metric to exist now that we've ack'd the pod
		cluster.AckPods(p)
		sync(p)
		env.Clock.Step(1 * time.Hour)

		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {p}}, map[string][]*corev1.Pod{"nc1": {p}})
		sync(p)

		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
	})
	It("should delete provisioning undecided metrics based on pod deletes", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending
		ExpectApplied(ctx, env.Client, p)

		sync(p)
		env.Clock.Step(1 * time.Hour)
		_, found := FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())

		// Expect the metric to exist now that we've ack'd the pod
		cluster.AckPods(p)
		sync(p)
		env.Clock.Step(1 * time.Hour)

		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		ExpectDeleted(ctx, env.Client, p)
		sync(p)

		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
	})
	It("should delete provisioning undecided metrics when the pod is already bound", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending
		ExpectApplied(ctx, env.Client, p)

		cluster.AckPods(p)
		sync(p)
		env.Clock.Step(1 * time.Hour)

		_, found := FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
		ExpectApplied(ctx, env.Client, p)
		sync(p)

		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
	})
	It("should delete pod unbound and unstarted time metrics on pod delete", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending
		ExpectApplied(ctx, env.Client, p)

		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {p}}, map[string][]*corev1.Pod{"nc1": {p}})
		sync(p)

		_, found := FindMetricWithLabelValues("karpenter_pods_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		ExpectDeleted(ctx, env.Client, p)
		sync(p)
		_, found = FindMetricWithLabelValues("karpenter_pods_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
	})
	It("should delete pod unbound and unstarted time metrics when pod schedulable time is zero", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending
		ExpectApplied(ctx, env.Client, p)

		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{}, map[string][]*corev1.Pod{"n1": {p}}, map[string][]*corev1.Pod{"nc1": {p}})
		sync(p)
		_, found := FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{p: fmt.Errorf("ignoring pod")}, map[string][]*corev1.Pod{}, map[string][]*corev1.Pod{"nc1": {p}})
		sync(p)
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unbound_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
	})
	It("should delete provisioning undecided metrics when the pod has a NodeName but PodScheduled condition is not True", func() {
		node := test.Node()
		p := test.Pod()
		p.Status.Phase = corev1.PodPending
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", LastTransitionTime: metav1.Now()}}
		ExpectApplied(ctx, env.Client, node, p)

		cluster.AckPods(p)
		sync(p)

		_, found := FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		// The pod gets bound, but the PodScheduled condition never transitions to True
		ExpectManualBinding(ctx, env.Client, p, node)
		sync(p)

		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
	})
	It("should delete provisioning undecided metrics when the pod is terminal without PodScheduled condition being True", func() {
		p := test.Pod()
		p.Status.Phase = corev1.PodPending
		ExpectApplied(ctx, env.Client, p)

		cluster.AckPods(p)
		sync(p)

		_, found := FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeTrue())

		// Pod goes directly to Failed phase without PodScheduled condition being True
		p.Status.Phase = corev1.PodFailed
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", LastTransitionTime: metav1.Now()}}
		ExpectApplied(ctx, env.Client, p)
		sync(p)

		_, found = FindMetricWithLabelValues("karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
	})
	It("should delete the pod state metric on pod delete", func() {
		p := test.Pod()
		ExpectApplied(ctx, env.Client, p)
		sync(p)

		ExpectDeleted(ctx, env.Client, p)
		sync(p)

		_, found := FindMetricWithLabelValues("karpenter_pods_state", map[string]string{
			"name":      p.GetName(),
			"namespace": p.GetNamespace(),
		})
		Expect(found).To(BeFalse())
	})
})

// sync feeds the stores the pod as the API server has it now, and its Node, as the informers would.
func sync(p *corev1.Pod) {
	GinkgoHelper()
	latest := &corev1.Pod{}
	if err := env.Client.Get(ctx, client.ObjectKeyFromObject(p), latest); err != nil {
		Expect(client.IgnoreNotFound(err)).To(Succeed())
		cluster.PodStore.Forget(client.ObjectKeyFromObject(p))
		return
	}
	if latest.Spec.NodeName != "" {
		node := &corev1.Node{}
		if err := env.Client.Get(ctx, client.ObjectKey{Name: latest.Spec.NodeName}, node); err == nil {
			cluster.NodeStore.Observe(node)
		}
	}
	cluster.PodStore.Observe(latest)
}
