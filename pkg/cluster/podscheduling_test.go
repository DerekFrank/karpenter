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

package cluster_test

import (
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Pod Ack", func() {
	It("should only mark pods as schedulable once", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		nn := client.ObjectKeyFromObject(pod)

		setTime := PodSchedulingOf(cluster.PodStore, nn).SchedulableAt
		Expect(setTime.IsZero()).To(BeTrue())

		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"n1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})
		setTime = PodSchedulingOf(cluster.PodStore, nn).SchedulableAt
		Expect(setTime.IsZero()).To(BeFalse())

		newTime := PodSchedulingOf(cluster.PodStore, nn).SchedulableAt
		Expect(newTime.Compare(setTime)).To(Equal(0))
		Expect(PodSchedulingOf(cluster.PodStore, nn).NodeClaim).To(BeEquivalentTo("nc1"))
	})
	It("should delete pod schedulable time and pod to nodeClaim mapping if we get error for the pod", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		nn := client.ObjectKeyFromObject(pod)

		setTime := PodSchedulingOf(cluster.PodStore, nn).SchedulableAt
		Expect(setTime.IsZero()).To(BeTrue())
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"n1": {pod}}, nil)
		setTime = PodSchedulingOf(cluster.PodStore, nn).SchedulableAt
		Expect(setTime.IsZero()).To(BeFalse())

		cs.PodScheduling.MarkDecisions(map[*corev1.Pod]error{
			pod: fmt.Errorf("ignoring pod"),
		}, nil, nil)
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).NodeClaim).To(BeEquivalentTo(""))
	})
	It("should drop the pod's scheduling fields with the pod", func() {
		pod := test.Pod()
		nodePool.StatusConditions().SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
		ExpectApplied(ctx, env.Client, pod, nodePool)

		nn := client.ObjectKeyFromObject(pod)
		// This will store the pod mappings
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt.IsZero()).To(BeFalse())
		Expect(PodSchedulingOf(cluster.PodStore, nn).DecidedAt.IsZero()).To(BeFalse())
		Expect(PodSchedulingOf(cluster.PodStore, nn).NodeClaim).To(BeEquivalentTo("nc1"))

		// The fields are intent on the pod's entry, so they drop with it.
		ExpectDeleted(ctx, env.Client, pod)
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).DecidedAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).NodeClaim).To(BeEquivalentTo(""))
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(BeEmpty())
	})
	It("should notify the pod's entry of each write", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		nn := client.ObjectKeyFromObject(pod)
		var mu sync.Mutex
		var seen []karpentercluster.PodSchedulingFields
		DeferCleanup(cluster.PodStore.Subscribe(func(c state.Change[types.NamespacedName, *corev1.Pod, struct{}, karpentercluster.PodIntent]) {
			if c.Key == nn {
				mu.Lock()
				defer mu.Unlock()
				seen = append(seen, c.New.Intent.Scheduling.Value)
			}
		}))

		cs.PodScheduling.Ack(pod)
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})
		mu.Lock()
		defer mu.Unlock()
		Expect(seen).ToNot(BeEmpty())
		Expect(seen[0].AckedAt.IsZero()).To(BeFalse(), "want the ack notified")
		Expect(seen[len(seen)-1]).To(Equal(PodSchedulingOf(cluster.PodStore, nn)), "want the last write notified")
		Expect(seen[len(seen)-1].NodeClaim).To(Equal("nc1"))
	})
	It("should keep nothing for a pod the pod store doesn't hold", func() {
		pod := test.Pod()
		nn := client.ObjectKeyFromObject(pod)
		cs.PodScheduling.Ack(pod)
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})
		Expect(PodSchedulingOf(cluster.PodStore, nn)).To(BeZero())
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(BeEmpty())

		// A pod created afterwards starts with no scheduling fields.
		ExpectApplied(ctx, env.Client, pod)
		Expect(PodSchedulingOf(cluster.PodStore, nn)).To(BeZero())
	})
	It("should not store scheduling times for pods that are already bound to a node", func() {
		pod := test.Pod(test.PodOptions{NodeName: "test-node"})
		ExpectApplied(ctx, env.Client, pod)
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{nodePool.Name: {pod}}, nil)
		nn := client.ObjectKeyFromObject(pod)
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).DecidedAt.IsZero()).To(BeTrue())
	})
	It("should keep each scheduling time from the first time it is set", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		nn := client.ObjectKeyFromObject(pod)
		cs.PodScheduling.Ack(pod)
		ackedAt := PodSchedulingOf(cluster.PodStore, nn).AckedAt
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {pod}}, nil)
		decidedAt, schedulableAt := PodSchedulingOf(cluster.PodStore, nn).DecidedAt, PodSchedulingOf(cluster.PodStore, nn).SchedulableAt
		Expect(ackedAt.IsZero()).To(BeFalse())
		Expect(decidedAt.IsZero()).To(BeFalse())
		Expect(schedulableAt.IsZero()).To(BeFalse())

		env.Clock.Step(time.Minute)
		cs.PodScheduling.Ack(pod)
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {pod}}, nil)
		Expect(PodSchedulingOf(cluster.PodStore, nn).AckedAt).To(Equal(ackedAt))
		Expect(PodSchedulingOf(cluster.PodStore, nn).DecidedAt).To(Equal(decidedAt))
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt).To(Equal(schedulableAt))

		// A failed decision clears the schedulable time, and the next success sets it anew; the decision time stays.
		cs.PodScheduling.MarkDecisions(map[*corev1.Pod]error{pod: fmt.Errorf("ignoring pod")}, nil, nil)
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt.IsZero()).To(BeTrue())
		env.Clock.Step(time.Minute)
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {pod}}, nil)
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt).To(Equal(env.Clock.Now()))
		Expect(PodSchedulingOf(cluster.PodStore, nn).DecidedAt).To(Equal(decidedAt))
	})
})

var _ = Describe("Pods Mapped To NodeClaim", func() {
	It("should look up the pods mapped to each NodeClaim, following a pod that moves", func() {
		a, b, c := test.Pod(), test.Pod(), test.Pod()
		ExpectApplied(ctx, env.Client, a, b, c)
		cs.PodScheduling.MapToNodeClaims(map[string][]*corev1.Pod{"nc1": {a, b}, "nc2": {c}})
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(ConsistOf(client.ObjectKeyFromObject(a), client.ObjectKeyFromObject(b)))
		Expect(cluster.PodStore.SimulatedOn("nc2")).To(ConsistOf(client.ObjectKeyFromObject(c)))
		Expect(cluster.PodStore.SimulatedOn("nc3")).To(BeEmpty())

		// The most recent scheduling decision for a pod moves it to its new NodeClaim.
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {b}}, map[string][]*corev1.Pod{"nc2": {b}})
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(ConsistOf(client.ObjectKeyFromObject(a)))
		Expect(cluster.PodStore.SimulatedOn("nc2")).To(ConsistOf(client.ObjectKeyFromObject(b), client.ObjectKeyFromObject(c)))

		// A pod that fails to schedule maps to no NodeClaim.
		cs.PodScheduling.MarkDecisions(map[*corev1.Pod]error{a: fmt.Errorf("ignoring pod")}, nil, nil)
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(BeEmpty())
		Expect(PodSchedulingOf(cluster.PodStore, client.ObjectKeyFromObject(a)).NodeClaim).To(BeEmpty())
	})
	It("should drop every scheduling field of a deleted pod, and its NodeClaim mapping", func() {
		pod, other := test.Pod(), test.Pod()
		ExpectApplied(ctx, env.Client, pod, other)
		nn := client.ObjectKeyFromObject(pod)
		cs.PodScheduling.Ack(pod, other)
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {pod, other}}, map[string][]*corev1.Pod{"nc1": {pod, other}})
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(HaveLen(2))

		ExpectDeleted(ctx, env.Client, pod)
		Expect(PodSchedulingOf(cluster.PodStore, nn).AckedAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).DecidedAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).NodeClaim).To(BeEmpty())
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(ConsistOf(client.ObjectKeyFromObject(other)))
		Expect(PodSchedulingOf(cluster.PodStore, client.ObjectKeyFromObject(other)).AckedAt.IsZero()).To(BeFalse())

		ExpectDeleted(ctx, env.Client, other)
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(BeEmpty())
		Expect(PodSchedulingOf(cluster.PodStore, client.ObjectKeyFromObject(other)).AckedAt.IsZero()).To(BeTrue())
	})
})

var _ = Describe("Reset", func() {
	It("should clear the pod scheduling fields, the NodeClaim mappings and the buffer pod counts", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		nn := client.ObjectKeyFromObject(pod)
		cs.PodScheduling.Ack(pod)
		cs.PodScheduling.MarkDecisions(nil, map[string][]*corev1.Pod{"np1": {pod}}, map[string][]*corev1.Pod{"nc1": {pod}})
		nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "provider-a"}})
		ExpectApplied(ctx, env.Client, nodeClaim)
		cs.BufferPods.Replace(map[string]int{"provider-a": 2})
		Expect(bufferPodCount("provider-a")).To(Equal(2))

		cs.Reset()
		Expect(PodSchedulingOf(cluster.PodStore, nn).AckedAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).DecidedAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).SchedulableAt.IsZero()).To(BeTrue())
		Expect(PodSchedulingOf(cluster.PodStore, nn).NodeClaim).To(BeEmpty())
		Expect(cluster.PodStore.SimulatedOn("nc1")).To(BeEmpty())
		Expect(bufferPodCount("provider-a")).To(Equal(0))

		// The derived store still follows its source after a Reset.
		cs.PodScheduling.MapToNodeClaims(map[string][]*corev1.Pod{"nc2": {pod}})
		Expect(cluster.PodStore.SimulatedOn("nc2")).To(ConsistOf(nn))
	})
})

var _ = Describe("Buffer Pod Counts", func() {
	BeforeEach(func() {
		// Buffer pod counts are intent kept with each NodeClaim, by name.
		for _, name := range []string{"provider-a", "provider-b", "provider-c"} {
			nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}})
			ExpectApplied(ctx, env.Client, nodeClaim)
		}
	})
	It("should return false for unknown NodeClaims", func() {
		Expect(cluster.HasBufferPods("unknown-nodeclaim")).To(BeFalse())
		Expect(bufferPodCount("unknown-nodeclaim")).To(Equal(0))
	})
	It("should not keep counts for a NodeClaim cluster state doesn't hold", func() {
		cs.BufferPods.Replace(map[string]int{"unknown-nodeclaim": 2})
		Expect(bufferPodCount("unknown-nodeclaim")).To(Equal(0))
	})
	It("should drop a NodeClaim's counts once the NodeClaim is gone", func() {
		cs.BufferPods.Replace(map[string]int{"provider-a": 2})
		nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "provider-a"}}
		ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
		ExpectDeleted(ctx, env.Client, nodeClaim)
		Expect(bufferPodCount("provider-a")).To(Equal(0))
	})

	It("should track buffer pods after UpdateBufferPodCounts", func() {
		cs.BufferPods.Replace(map[string]int{
			"provider-a": 3,
			"provider-b": 1,
		})
		Expect(cluster.HasBufferPods("provider-a")).To(BeTrue())
		Expect(bufferPodCount("provider-a")).To(Equal(3))
		Expect(cluster.HasBufferPods("provider-b")).To(BeTrue())
		Expect(bufferPodCount("provider-b")).To(Equal(1))
		Expect(cluster.HasBufferPods("provider-c")).To(BeFalse())
	})

	It("should clear old entries when UpdateBufferPodCounts is called with new map", func() {
		cs.BufferPods.Replace(map[string]int{
			"provider-a": 5,
		})
		Expect(cluster.HasBufferPods("provider-a")).To(BeTrue())

		// Update with a map that doesn't contain provider-a
		cs.BufferPods.Replace(map[string]int{
			"provider-b": 2,
		})
		Expect(cluster.HasBufferPods("provider-a")).To(BeFalse())
		Expect(bufferPodCount("provider-a")).To(Equal(0))
		Expect(cluster.HasBufferPods("provider-b")).To(BeTrue())
		Expect(bufferPodCount("provider-b")).To(Equal(2))
	})

	It("should clear all entries when called with empty map", func() {
		cs.BufferPods.Replace(map[string]int{
			"provider-a": 3,
			"provider-b": 1,
		})
		cs.BufferPods.Replace(map[string]int{})
		Expect(cluster.HasBufferPods("provider-a")).To(BeFalse())
		Expect(cluster.HasBufferPods("provider-b")).To(BeFalse())
	})

	It("should not store entries with count zero", func() {
		cs.BufferPods.Replace(map[string]int{
			"provider-a": 0,
			"provider-b": 3,
		})
		Expect(cluster.HasBufferPods("provider-a")).To(BeFalse())
		Expect(cluster.HasBufferPods("provider-b")).To(BeTrue())
	})
})

// bufferPodCount returns the buffer pods counted on the NodeClaim's entry.
func bufferPodCount(nodeClaimName string) int {
	e, _ := cluster.NodeClaimStore.Get(nodeClaimName)
	return e.Intent.BufferPods.Value
}
