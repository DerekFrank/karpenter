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

package state

import (
	"context"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/cluster"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// AckPods marks the pod as acknowledged for scheduling from the provisioner. This is only done once per-pod.
func (c *Cluster) AckPods(pods ...*corev1.Pod) {
	now := c.clock.Now()
	for _, pod := range pods {
		c.PodScheduling.Update(client.ObjectKeyFromObject(pod), func(f cluster.PodSchedulingFields) cluster.PodSchedulingFields {
			f.AckedAt = lo.Ternary(f.AckedAt.IsZero(), now, f.AckedAt)
			return f
		})
	}
}

// PodAckTime will return the time the pod was first seen in our cache.
func (c *Cluster) PodAckTime(podKey types.NamespacedName) time.Time {
	f := c.PodScheduling.Get(podKey)
	return f.AckedAt
}

// MarkPodSchedulingDecisions keeps track of when we first tried to schedule a pod to a node, and when the pod is first
// seen as schedulable for pod metrics, and maps each pod to the NodeClaim it was simulated against. We'll only emit a
// metric for a pod if we haven't done it before.
func (c *Cluster) MarkPodSchedulingDecisions(_ context.Context, podErrors map[*corev1.Pod]error, npPods map[string][]*corev1.Pod, ncPods map[string][]*corev1.Pod) {
	now := c.clock.Now()
	for pod := range podErrors {
		c.markPodSchedulingDecision(client.ObjectKeyFromObject(pod), now, func(f cluster.PodSchedulingFields) cluster.PodSchedulingFields {
			f.SchedulableAt, f.NodeClaim = time.Time{}, ""
			return f
		})
	}
	for _, pods := range npPods {
		for _, p := range pods {
			// Skip pods that are already bound to a node (e.g. pods from deleting nodes
			// included in the scheduling simulation for capacity planning). Storing a new
			// timestamp for already-bound pods would cause negative metric values since
			// their PodScheduled LastTransitionTime is in the past.
			if podutils.IsScheduled(p) {
				continue
			}
			c.markPodSchedulingDecision(client.ObjectKeyFromObject(p), now, func(f cluster.PodSchedulingFields) cluster.PodSchedulingFields {
				f.SchedulableAt = lo.Ternary(f.SchedulableAt.IsZero(), now, f.SchedulableAt)
				return f
			})
		}
	}
	c.UpdatePodToNodeClaimMapping(ncPods)
}

// markPodSchedulingDecision applies the decision to the pod's fields, records now as when the pod was first decided
// on, and observes the decision latency from the pod's ack the first time.
func (c *Cluster) markPodSchedulingDecision(podKey types.NamespacedName, now time.Time, decide func(cluster.PodSchedulingFields) cluster.PodSchedulingFields) {
	var first bool
	var ackedAt time.Time
	c.PodScheduling.Update(podKey, func(f cluster.PodSchedulingFields) cluster.PodSchedulingFields {
		f = decide(f)
		if first = f.DecidedAt.IsZero(); first {
			f.DecidedAt = now
		}
		ackedAt = f.AckedAt
		return f
	})
	// We should have ACK'd the pod.
	if first && !ackedAt.IsZero() {
		PodSchedulingDecisionSeconds.Observe(c.clock.Since(ackedAt).Seconds(), nil)
	}
}

func (c *Cluster) UpdatePodToNodeClaimMapping(ncPods map[string][]*corev1.Pod) {
	for ncName, pods := range ncPods {
		for _, p := range pods {
			c.PodScheduling.Update(client.ObjectKeyFromObject(p), func(f cluster.PodSchedulingFields) cluster.PodSchedulingFields {
				f.NodeClaim = ncName
				return f
			})
		}
	}
}

// PodSchedulingDecisionTime returns when Karpenter first decided if a pod could schedule a pod in scheduling simulations.
// This returns 0, false if Karpenter never made a decision on the pod.
func (c *Cluster) PodSchedulingDecisionTime(podKey types.NamespacedName) time.Time {
	f := c.PodScheduling.Get(podKey)
	return f.DecidedAt
}

// PodSchedulingSuccessTime returns when Karpenter first thought it could schedule a pod in its scheduling simulation.
// This returns 0, false if the pod was never considered in scheduling as a pending pod.
func (c *Cluster) PodSchedulingSuccessTime(podKey types.NamespacedName) time.Time {
	f := c.PodScheduling.Get(podKey)
	return f.SchedulableAt
}

// PodNodeClaimMapping returns the nodeClaim against which the pod is simulated to get scheduled
func (c *Cluster) PodNodeClaimMapping(podKey types.NamespacedName) string {
	f := c.PodScheduling.Get(podKey)
	return f.NodeClaim
}

// PodsMappedToNodeClaim returns the keys of the pods that were simulated to schedule against the given nodeClaim
// during the most recent scheduling decision for each pod
func (c *Cluster) PodsMappedToNodeClaim(nodeClaimName string) []types.NamespacedName {
	return c.PodScheduling.PodsOn(nodeClaimName)
}

// ClearPodSchedulingMappings drops what Karpenter decided about the pod as it scheduled it.
func (c *Cluster) ClearPodSchedulingMappings(podKey types.NamespacedName) {
	c.PodScheduling.Update(podKey, func(cluster.PodSchedulingFields) cluster.PodSchedulingFields { return cluster.PodSchedulingFields{} })
}
