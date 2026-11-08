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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

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
		cluster.ClearPodSchedulingMappings(nn)
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodSchedulingDecisionTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodNodeClaimMapping(nn)).To(BeEquivalentTo(""))
	})
	It("should not store scheduling times for pods that are already bound to a node", func() {
		pod := test.Pod(test.PodOptions{NodeName: "test-node"})
		ExpectApplied(ctx, env.Client, pod)
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{nodePool.Name: {pod}}, nil)
		nn := client.ObjectKeyFromObject(pod)
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodSchedulingDecisionTime(nn).IsZero()).To(BeTrue())
	})
	It("should keep each scheduling time from the first time it is set", func() {
		pod := test.Pod()
		nn := client.ObjectKeyFromObject(pod)
		cluster.AckPods(pod)
		ackedAt := cluster.PodAckTime(nn)
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"np1": {pod}}, nil)
		decidedAt, schedulableAt := cluster.PodSchedulingDecisionTime(nn), cluster.PodSchedulingSuccessTime(nn)
		Expect(ackedAt.IsZero()).To(BeFalse())
		Expect(decidedAt.IsZero()).To(BeFalse())
		Expect(schedulableAt.IsZero()).To(BeFalse())

		env.Clock.Step(time.Minute)
		cluster.AckPods(pod)
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"np1": {pod}}, nil)
		Expect(cluster.PodAckTime(nn)).To(Equal(ackedAt))
		Expect(cluster.PodSchedulingDecisionTime(nn)).To(Equal(decidedAt))
		Expect(cluster.PodSchedulingSuccessTime(nn)).To(Equal(schedulableAt))

		// A failed decision clears the schedulable time, and the next success sets it anew; the decision time stays.
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{pod: fmt.Errorf("ignoring pod")}, nil, nil)
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeTrue())
		env.Clock.Step(time.Minute)
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"np1": {pod}}, nil)
		Expect(cluster.PodSchedulingSuccessTime(nn)).To(Equal(env.Clock.Now()))
		Expect(cluster.PodSchedulingDecisionTime(nn)).To(Equal(decidedAt))
	})
})

var _ = Describe("Pods Mapped To NodeClaim", func() {
	It("should look up the pods mapped to each NodeClaim, following a pod that moves", func() {
		a, b, c := test.Pod(), test.Pod(), test.Pod()
		cluster.UpdatePodToNodeClaimMapping(map[string][]*corev1.Pod{"nc1": {a, b}, "nc2": {c}})
		Expect(cluster.PodsMappedToNodeClaim("nc1")).To(ConsistOf(client.ObjectKeyFromObject(a), client.ObjectKeyFromObject(b)))
		Expect(cluster.PodsMappedToNodeClaim("nc2")).To(ConsistOf(client.ObjectKeyFromObject(c)))
		Expect(cluster.PodsMappedToNodeClaim("nc3")).To(BeEmpty())

		// The most recent scheduling decision for a pod moves it to its new NodeClaim.
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"np1": {b}}, map[string][]*corev1.Pod{"nc2": {b}})
		Expect(cluster.PodsMappedToNodeClaim("nc1")).To(ConsistOf(client.ObjectKeyFromObject(a)))
		Expect(cluster.PodsMappedToNodeClaim("nc2")).To(ConsistOf(client.ObjectKeyFromObject(b), client.ObjectKeyFromObject(c)))

		// A pod that fails to schedule maps to no NodeClaim.
		cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{a: fmt.Errorf("ignoring pod")}, nil, nil)
		Expect(cluster.PodsMappedToNodeClaim("nc1")).To(BeEmpty())
		Expect(cluster.PodNodeClaimMapping(client.ObjectKeyFromObject(a))).To(BeEmpty())
	})
	It("should drop every scheduling field of a deleted pod, and its NodeClaim mapping", func() {
		pod, other := test.Pod(), test.Pod()
		nn := client.ObjectKeyFromObject(pod)
		cluster.AckPods(pod, other)
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"np1": {pod, other}}, map[string][]*corev1.Pod{"nc1": {pod, other}})
		Expect(cluster.PodsMappedToNodeClaim("nc1")).To(HaveLen(2))

		cluster.ClearPodSchedulingMappings(nn)
		Expect(cluster.PodAckTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodSchedulingDecisionTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodNodeClaimMapping(nn)).To(BeEmpty())
		Expect(cluster.PodsMappedToNodeClaim("nc1")).To(ConsistOf(client.ObjectKeyFromObject(other)))
		Expect(cluster.PodAckTime(client.ObjectKeyFromObject(other)).IsZero()).To(BeFalse())

		cluster.ClearPodSchedulingMappings(client.ObjectKeyFromObject(other))
		Expect(cluster.PodsMappedToNodeClaim("nc1")).To(BeEmpty())
		Expect(cluster.PodAckTime(client.ObjectKeyFromObject(other)).IsZero()).To(BeTrue())
	})
})

var _ = Describe("Reset", func() {
	It("should clear the pod scheduling fields, the NodeClaim mappings and the buffer pod counts", func() {
		pod := test.Pod()
		nn := client.ObjectKeyFromObject(pod)
		cluster.AckPods(pod)
		cluster.MarkPodSchedulingDecisions(ctx, nil, map[string][]*corev1.Pod{"np1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})
		nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "provider-a"}})
		ExpectApplied(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		cluster.UpdateBufferPodCounts(map[string]int{"provider-a": 2})
		Expect(cluster.BufferPodCount("provider-a")).To(Equal(2))

		cluster.Reset()
		Expect(cluster.PodAckTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodSchedulingDecisionTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodSchedulingSuccessTime(nn).IsZero()).To(BeTrue())
		Expect(cluster.PodNodeClaimMapping(nn)).To(BeEmpty())
		Expect(cluster.PodsMappedToNodeClaim("nc1")).To(BeEmpty())
		Expect(cluster.BufferPodCount("provider-a")).To(Equal(0))

		// The derived store still follows its source after a Reset.
		cluster.UpdatePodToNodeClaimMapping(map[string][]*corev1.Pod{"nc2": {pod}})
		Expect(cluster.PodsMappedToNodeClaim("nc2")).To(ConsistOf(nn))
	})
})

var _ = Describe("Buffer Pod Counts", func() {
	BeforeEach(func() {
		// Buffer pod counts are intent kept with each NodeClaim, by name.
		for _, name := range []string{"provider-a", "provider-b", "provider-c"} {
			nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}})
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		}
	})
	It("should return false for unknown NodeClaims", func() {
		Expect(cluster.HasBufferPods("unknown-nodeclaim")).To(BeFalse())
		Expect(cluster.BufferPodCount("unknown-nodeclaim")).To(Equal(0))
	})
	It("should not keep counts for a NodeClaim cluster state doesn't hold", func() {
		cluster.UpdateBufferPodCounts(map[string]int{"unknown-nodeclaim": 2})
		Expect(cluster.BufferPodCount("unknown-nodeclaim")).To(Equal(0))
	})
	It("should drop a NodeClaim's counts once the NodeClaim is gone", func() {
		cluster.UpdateBufferPodCounts(map[string]int{"provider-a": 2})
		cluster.DeleteNodeClaim("provider-a")
		Expect(cluster.BufferPodCount("provider-a")).To(Equal(0))
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
