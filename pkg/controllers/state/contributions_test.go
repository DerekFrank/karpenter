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
	"maps"
	"reflect"
	"testing"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/equality"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

func TestContributionsKeepTheContributionLaws(t *testing.T) {
	base := NodePoolFields{NodeClaims: NodeClaimCounts{Active: 3, Deleting: 1}, Reserved: 2}
	statetest.ExpectContribution(t, base, NodeClaimCounts{Active: 1}, NodeClaimCounts{Deleting: 1})
	statetest.ExpectContribution(t, base, reservedNodes(1), reservedNodes(2))
}

func TestClusterContributionsKeepTheContributionLaws(t *testing.T) {
	pod := func(name, cpu string, opts ...func(*corev1.Pod)) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: corev1.PodSpec{NodeName: "n", Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			}}}},
		}
		for _, opt := range opts {
			opt(p)
		}
		return p
	}
	withPVC := func(p *corev1.Pod) {
		p.Spec.Volumes = []corev1.Volume{{Name: "v", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "claim"}}}}
	}
	withHostPort := func(p *corev1.Pod) {
		p.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 80, Protocol: corev1.ProtocolTCP}}
	}
	c := NewCluster(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, nil, nil)
	contribution := func(p *corev1.Pod) podContribution { return c.newPodContribution(client.ObjectKeyFromObject(p), p) }
	other := contribution(pod("other", "1", withPVC, withHostPort)).Update(podFields{}, podContribution{})
	for _, base := range []podFields{{}, other} {
		statetest.ExpectContributionWith(t, podFieldsEqual, base, contribution(pod("a", "1")), contribution(pod("a", "2", withPVC)))
		statetest.ExpectContributionWith(t, podFieldsEqual, base, contribution(pod("a", "2", withPVC, withHostPort)), contribution(pod("a", "1")))
	}
	capacity := func(cpu string) nodePoolCapacity {
		return nodePoolCapacity{corev1.ResourceCPU: resource.MustParse(cpu), resources.Node: resource.MustParse("1")}
	}
	statetest.ExpectContribution(t, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}, capacity("2"), capacity("8"))
	statetest.ExpectContributionWith(t, podFieldsEqual, podFields{}, nodeOnNode{&corev1.Node{}}, nodeOnNode{&corev1.Node{}})
	podKey := func(name string) podOnNodeClaim {
		return podOnNodeClaim{types.NamespacedName{Namespace: "default", Name: name}}
	}
	for _, base := range []map[types.NamespacedName]struct{}{nil, {podKey("other").pod: {}}} {
		statetest.ExpectContribution(t, base, podKey("a"), podKey("a"))
		statetest.ExpectContribution(t, base, podKey("a"), podKey("b"))
	}
}

// podFieldsEqual compares pod fields as their readers do: a resource at zero is the resource absent, and host port
// usage compares by what it holds.
func podFieldsEqual(a, b podFields) bool {
	usage := func(l corev1.ResourceList) corev1.ResourceList {
		return lo.OmitBy(l, func(_ corev1.ResourceName, q resource.Quantity) bool { return q.IsZero() })
	}
	same := func(x, y corev1.ResourceList) bool { return equality.Semantic.DeepEqual(usage(x), usage(y)) }
	hostPorts := func(h *scheduling.HostPortUsage) *scheduling.HostPortUsage {
		return lo.Ternary(h == nil, scheduling.NewHostPortUsage(), h)
	}
	return a.Node == b.Node && a.Pods == b.Pods && a.DisruptionCost == b.DisruptionCost &&
		same(a.Requests, b.Requests) && same(a.Limits, b.Limits) && same(a.DaemonSetRequests, b.DaemonSetRequests) &&
		same(a.DaemonSetLimits, b.DaemonSetLimits) && reflect.DeepEqual(hostPorts(a.HostPorts), hostPorts(b.HostPorts)) &&
		maps.Equal(a.PVCs, b.PVCs) && maps.Equal(a.AntiAffinityPods, b.AntiAffinityPods)
}
