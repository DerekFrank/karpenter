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

package cluster

import (
	"time"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/state"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// PodEntry is a pod's entry.
type PodEntry = state.Entry[*corev1.Pod, struct{}, PodIntent]

// PodIntent is what Karpenter decided about a pod. It drops with the pod.
type PodIntent struct {
	// Scheduling is what the provisioner decided about the pod as it scheduled it, through the PodScheduling handle.
	Scheduling state.Intended[PodSchedulingFields]
}

// PodSchedulingFields are what Karpenter decides about a pod as it schedules it. Each time is set once, the first time.
type PodSchedulingFields struct {
	// AckedAt is when the provisioner first saw the pod as pending.
	AckedAt time.Time
	// DecidedAt is when the provisioner first decided whether the pod could schedule.
	DecidedAt time.Time
	// SchedulableAt is when a scheduling simulation first fit the pod to a node, until one fails it.
	SchedulableAt time.Time
	// NodeClaim is the NodeClaim the most recent scheduling simulation placed the pod on.
	NodeClaim string
}

// PodIndex is an Index of the pods by values of type V, such as PodsByNode.
type PodIndex[V comparable] = state.Index[types.NamespacedName, *corev1.Pod, struct{}, PodIntent, V]

// Pods are the pods, by namespaced name: an object store, with the intent the provisioner keeps about each.
type Pods struct {
	state.Reader[types.NamespacedName, *corev1.Pod, struct{}, PodIntent]
	store *state.Store[types.NamespacedName, *corev1.Pod, struct{}, PodIntent]
	// byNode, byDaemonSet and bySimulatedNodeClaim are PodsByNode, PodsByDaemonSet and PodsBySimulatedNodeClaim.
	byNode               *PodIndex[string]
	byDaemonSet          *PodIndex[types.NamespacedName]
	bySimulatedNodeClaim *PodIndex[string]
}

func newPods(from state.Newest) *Pods {
	store := state.NewObjectStore[types.NamespacedName, *corev1.Pod, struct{}, PodIntent](from)
	return &Pods{
		Reader: store.Reader,
		store:  store,
		byNode: state.NewIndex(store, func(_ types.NamespacedName, e PodEntry) []string {
			return lo.Ternary(e.Object.Spec.NodeName != "", []string{e.Object.Spec.NodeName}, nil)
		}),
		byDaemonSet: state.NewIndex(store, func(_ types.NamespacedName, e PodEntry) []types.NamespacedName {
			if owner := metav1.GetControllerOf(e.Object); owner != nil && owner.Kind == "DaemonSet" {
				return []types.NamespacedName{{Namespace: e.Object.Namespace, Name: owner.Name}}
			}
			return nil
		}),
		bySimulatedNodeClaim: state.NewIndex(store, func(_ types.NamespacedName, e PodEntry) []string {
			nodeClaim := e.Intent.Scheduling.Value.NodeClaim
			return lo.Ternary(nodeClaim != "", []string{nodeClaim}, nil)
		}),
	}
}

// DaemonSetPod returns a copy of the DaemonSet's newest pod, a sample of its current template, or nil if cluster
// state holds none, read from PodsByDaemonSet. A pod samples its DaemonSet until it's gone, or until the garbage
// collector orphans it from a DaemonSet deleted without its pods.
func DaemonSetPod(podsByDaemonSet *PodIndex[types.NamespacedName], daemonSet *appsv1.DaemonSet) *corev1.Pod {
	var newest *corev1.Pod
	for _, e := range podsByDaemonSet.List(client.ObjectKeyFromObject(daemonSet), client.UnsafeDisableDeepCopy) {
		if newest == nil || e.Object.CreationTimestamp.After(newest.CreationTimestamp.Time) ||
			(e.Object.CreationTimestamp.Equal(&newest.CreationTimestamp) && e.Object.Name > newest.Name) {
			newest = e.Object
		}
	}
	if newest == nil {
		return nil
	}
	return newest.DeepCopy()
}

// PodsOn returns copies of the pods bound to the Node named nodeName, read from PodsByNode, or none for "", a node in
// flight.
func PodsOn(podsByNode *PodIndex[string], nodeName string) []*corev1.Pod {
	if nodeName == "" {
		return nil
	}
	var pods []*corev1.Pod
	for _, e := range podsByNode.List(nodeName) {
		pods = append(pods, e.Object)
	}
	return pods
}

// PodScheduling is the handle of what the provisioner decides about each pod as it schedules it, the Scheduling
// intent on the pod's entry, for the provisioner, its one writer, and the provisioning pod controller, which acks the
// pods the provisioner will schedule. Each write is a read-modify-write of the pod's intent. A pod the store doesn't
// hold has no intent, so a write for it does nothing.
type PodScheduling struct {
	clock  clock.Clock
	intent *state.Intent[types.NamespacedName, *corev1.Pod, struct{}, PodIntent, PodSchedulingFields]
}

func newPodScheduling(clk clock.Clock, pods *Pods) *PodScheduling {
	return &PodScheduling{
		clock:  clk,
		intent: state.NewIntent(pods.store, func(i *PodIntent) *state.Intended[PodSchedulingFields] { return &i.Scheduling }),
	}
}

// Ack records when the provisioner first saw each pod as pending.
func (p *PodScheduling) Ack(pods ...*corev1.Pod) {
	now := p.clock.Now()
	for _, pod := range pods {
		p.intent.Update(client.ObjectKeyFromObject(pod), func(f PodSchedulingFields) PodSchedulingFields {
			f.AckedAt = lo.Ternary(f.AckedAt.IsZero(), now, f.AckedAt)
			return f
		})
	}
}

// MarkDecisions records when the provisioner first tried to schedule each pod, and when a simulation first fit each
// pod for the pod metrics, and maps each pod to the NodeClaim it was simulated against. It observes each pod's decision
// latency once.
func (p *PodScheduling) MarkDecisions(podErrors map[*corev1.Pod]error, npPods map[string][]*corev1.Pod, ncPods map[string][]*corev1.Pod) {
	now := p.clock.Now()
	for pod := range podErrors {
		p.markDecision(client.ObjectKeyFromObject(pod), now, func(f PodSchedulingFields) PodSchedulingFields {
			f.SchedulableAt, f.NodeClaim = time.Time{}, ""
			return f
		})
	}
	for _, pods := range npPods {
		for _, pod := range pods {
			// Skip pods that are already bound to a node (e.g. pods from deleting nodes
			// included in the scheduling simulation for capacity planning). Storing a new
			// timestamp for already-bound pods would cause negative metric values since
			// their PodScheduled LastTransitionTime is in the past.
			if podutils.IsScheduled(pod) {
				continue
			}
			p.markDecision(client.ObjectKeyFromObject(pod), now, func(f PodSchedulingFields) PodSchedulingFields {
				f.SchedulableAt = lo.Ternary(f.SchedulableAt.IsZero(), now, f.SchedulableAt)
				return f
			})
		}
	}
	p.MapToNodeClaims(ncPods)
}

// markDecision applies the decision to the pod's fields, records now as when the pod was first decided on, and
// observes the decision latency from the pod's ack the first time.
func (p *PodScheduling) markDecision(pod types.NamespacedName, now time.Time, decide func(PodSchedulingFields) PodSchedulingFields) {
	var first bool
	var ackedAt time.Time
	p.intent.Update(pod, func(f PodSchedulingFields) PodSchedulingFields {
		f = decide(f)
		if first = f.DecidedAt.IsZero(); first {
			f.DecidedAt = now
		}
		ackedAt = f.AckedAt
		return f
	})
	// We should have ACK'd the pod.
	if first && !ackedAt.IsZero() {
		PodSchedulingDecisionSeconds.Observe(p.clock.Since(ackedAt).Seconds(), nil)
	}
}

// MapToNodeClaims maps each pod to the NodeClaim it was simulated against, by the NodeClaim's name.
func (p *PodScheduling) MapToNodeClaims(ncPods map[string][]*corev1.Pod) {
	for nodeClaim, pods := range ncPods {
		for _, pod := range pods {
			p.intent.Update(client.ObjectKeyFromObject(pod), func(f PodSchedulingFields) PodSchedulingFields {
				f.NodeClaim = nodeClaim
				return f
			})
		}
	}
}
