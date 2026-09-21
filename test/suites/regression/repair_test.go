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

package integration_test

import (
	"regexp"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	kwokcloudprovider "sigs.k8s.io/karpenter/kwok/cloudprovider"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
)

// These tests exercise node repair (voluntary disruption of unhealthy nodes) and the budgeted-breaker safeguard, using
// the KWOK reference provider. The KWOK provider ships a simulated repair-eligible condition (KWOKUnhealthyCondition)
// that stock KWOK's node lifecycle does not manage, so an injected fault HOLDS while the node stays Ready=True — see
// kwok/cloudprovider/cloudprovider.go and hack/kwok/stages/node-heartbeat-with-lease.yaml. Node repair is behind the
// NodeRepair feature gate (off by default), enabled for this suite in BeforeAll and restored in AfterAll.
var _ = Describe("Repair", Ordered, func() {
	var originalFeatureGates string

	// injectFault stamps the durable, KWOK-unmanaged repair-eligible condition on a node. The node-heartbeat-with-lease
	// stage override re-emits it on every heartbeat, so it persists and the node remains a valid disruption candidate
	// (Ready=True) once the RepairPolicy's TolerationDuration elapses.
	injectFault := func(node *corev1.Node) {
		node = env.ReplaceNodeConditions(node, corev1.NodeCondition{
			Type:               kwokcloudprovider.KWOKUnhealthyCondition,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "E2ETest",
			Message:            "injected repair-eligible fault",
		})
		env.ExpectStatusUpdated(node)
	}

	// clearFault removes the simulated condition; the heartbeat stage stops re-emitting it and the node heals.
	clearFault := func(node *corev1.Node) {
		n := env.GetNode(node.Name)
		n.Status.Conditions = lo.Reject(n.Status.Conditions, func(c corev1.NodeCondition, _ int) bool {
			return c.Type == kwokcloudprovider.KWOKUnhealthyCondition
		})
		env.ExpectStatusUpdated(&n)
	}

	BeforeAll(func() {
		// Node repair is gated by the NodeRepair feature gate (default false). Flip it on without disturbing the other
		// gates the controller was deployed with, and restart karpenter. Restored in AfterAll.
		for _, e := range env.ExpectSettings() {
			if e.Name == "FEATURE_GATES" {
				originalFeatureGates = e.Value
			}
		}
		env.ExpectSettingsOverridden(corev1.EnvVar{Name: "FEATURE_GATES", Value: withNodeRepairGate(originalFeatureGates, true)})
	})
	AfterAll(func() {
		env.ExpectSettingsOverridden(corev1.EnvVar{Name: "FEATURE_GATES", Value: originalFeatureGates})
	})

	It("repairs an isolated unhealthy node (replace-then-terminate)", func() {
		appLabels := map[string]string{"app": "repair-isolated"}
		dep := test.Deployment(test.DeploymentOptions{
			Replicas: 5,
			PodOptions: test.PodOptions{
				ObjectMeta:          metav1.ObjectMeta{Labels: appLabels},
				PodAntiRequirements: hostnameAntiAffinity(appLabels),
			},
		})
		selector := labels.SelectorFromSet(appLabels)
		env.ExpectCreated(nodeClass, nodePool, dep)
		env.EventuallyExpectHealthyPodCount(selector, 5)
		nodes := env.EventuallyExpectNodeCount("==", 5)

		// 1 of 5 unhealthy (20%) is at/under the breaker threshold (trips only when unhealthy > ceil(20%)=1), so repair proceeds.
		injectFault(nodes[0])
		env.EventuallyExpectNotFound(nodes[0]) // original node is replaced
		env.EventuallyExpectNodeCount("==", 5) // replacement brings the pool back
		env.EventuallyExpectHealthyPodCount(selector, 5)
	})

	It("trips the budgeted breaker and freezes repair when >20% of the pool is unhealthy", func() {
		appLabels := map[string]string{"app": "repair-breaker"}
		dep := test.Deployment(test.DeploymentOptions{
			Replicas: 5,
			PodOptions: test.PodOptions{
				ObjectMeta:          metav1.ObjectMeta{Labels: appLabels},
				PodAntiRequirements: hostnameAntiAffinity(appLabels),
			},
		})
		selector := labels.SelectorFromSet(appLabels)
		env.ExpectCreated(nodeClass, nodePool, dep)
		env.EventuallyExpectHealthyPodCount(selector, 5)
		nodes := env.EventuallyExpectNodeCount("==", 5)

		// 2 of 5 unhealthy (40% > 20%) trips the breaker for the pool: repair must NOT disrupt any node. Assert no
		// disruptions for well past the RepairPolicy TolerationDuration (30s) so this proves the breaker, not toleration.
		injectFault(nodes[0])
		injectFault(nodes[1])
		env.ConsistentlyExpectNoDisruptions(5, 2*time.Minute)
		env.ExpectExists(nodes[0])
		env.ExpectExists(nodes[1])
	})

	It("resumes repair once the pool drops back under the breaker threshold", func() {
		appLabels := map[string]string{"app": "repair-reset"}
		dep := test.Deployment(test.DeploymentOptions{
			Replicas: 5,
			PodOptions: test.PodOptions{
				ObjectMeta:          metav1.ObjectMeta{Labels: appLabels},
				PodAntiRequirements: hostnameAntiAffinity(appLabels),
			},
		})
		selector := labels.SelectorFromSet(appLabels)
		env.ExpectCreated(nodeClass, nodePool, dep)
		env.EventuallyExpectHealthyPodCount(selector, 5)
		nodes := env.EventuallyExpectNodeCount("==", 5)

		// Trip the breaker (2/5 unhealthy) -> frozen.
		injectFault(nodes[0])
		injectFault(nodes[1])
		env.ConsistentlyExpectNoDisruptions(5, 30*time.Second)

		// Heal one node -> 1/5 unhealthy is under the threshold -> the breaker resets and the remaining unhealthy node is repaired.
		clearFault(nodes[0])
		env.EventuallyExpectNotFound(nodes[1])
		env.EventuallyExpectNodeCount("==", 5)
		env.EventuallyExpectHealthyPodCount(selector, 5)
	})

	It("force-terminates a drain-blocked unhealthy node past the RepairPolicy termination grace period", func() {
		// Every pod carries do-not-disrupt, so the faulted node's drain is blocked by an un-evictable pod. Node repair is
		// non-discretionary (it ignores do-not-disrupt and PDBs; only the do-not-repair annotation vetoes it), so the node
		// is force-terminated once the RepairPolicy TGP elapses. Use a 5-node pool and fault only one (20%, under the
		// breaker threshold) so repair actually runs — a single-node pool would read 100% unhealthy and trip the breaker.
		appLabels := map[string]string{"app": "repair-drain"}
		dep := test.Deployment(test.DeploymentOptions{
			Replicas: 5,
			PodOptions: test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      appLabels,
					Annotations: map[string]string{v1.DoNotDisruptAnnotationKey: "true"},
				},
				PodAntiRequirements: hostnameAntiAffinity(appLabels),
			},
		})
		selector := labels.SelectorFromSet(appLabels)
		env.ExpectCreated(nodeClass, nodePool, dep)
		env.EventuallyExpectHealthyPodCount(selector, 5)
		nodes := env.EventuallyExpectNodeCount("==", 5)

		injectFault(nodes[0])
		// Repair forces past the blocking do-not-disrupt pod once the policy TGP (45s) after the toleration (30s) elapses.
		env.EventuallyExpectNotFound(nodes[0])
		env.EventuallyExpectNodeCount("==", 5)
		env.EventuallyExpectHealthyPodCount(selector, 5)
	})
})

// hostnameAntiAffinity forces each pod carrying the given labels onto its own node.
func hostnameAntiAffinity(matchLabels map[string]string) []corev1.PodAffinityTerm {
	return []corev1.PodAffinityTerm{{
		TopologyKey:   corev1.LabelHostname,
		LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
	}}
}

// withNodeRepairGate returns the FEATURE_GATES string with NodeRepair set to enabled, preserving all other gates.
func withNodeRepairGate(gates string, enabled bool) string {
	val := "NodeRepair=false"
	if enabled {
		val = "NodeRepair=true"
	}
	re := regexp.MustCompile(`NodeRepair=[a-zA-Z]+`)
	switch {
	case re.MatchString(gates):
		return re.ReplaceAllString(gates, val)
	case gates == "":
		return val
	default:
		return gates + "," + val
	}
}
