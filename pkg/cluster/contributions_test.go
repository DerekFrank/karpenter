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
	"context"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

func TestNodeClaimSideContributionsKeepTheContributionLaws(t *testing.T) {
	statetest.ExpectContribution(t, NodePoolDisruption{Nodes: 4, Disrupting: 1, NotReady: 2}, NodePoolDisruption{Nodes: 1}, NodePoolDisruption{Nodes: 1, Disrupting: 1})
	statetest.ExpectContribution(t, NodePoolDisruption{Nodes: 4, Disrupting: 1, NotReady: 2}, NodePoolDisruption{NotReady: 1}, NodePoolDisruption{NotReady: 1})
	capacity := func(cpu string) nodePoolCapacity {
		return nodePoolCapacity{corev1.ResourceCPU: resource.MustParse(cpu), resources.Node: resource.MustParse("1")}
	}
	statetest.ExpectContribution(t, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}, capacity("2"), capacity("8"))
	for _, base := range []nodePoolMembers{{}, {Launched: 3, Unlaunched: 2}} {
		statetest.ExpectContribution(t, base, launchedNodeClaim(1), launchedNodeClaim(1))
		statetest.ExpectContribution(t, base, unlaunchedNodeClaim(1), unlaunchedNodeClaim(1))
	}
	for _, base := range []ProviderIDFields{{}, {NodeClaims: []string{"b"}, Nodes: []string{"n"}, BareNodes: 1}} {
		statetest.ExpectContribution(t, base, nodeClaimAtProviderID("a"), nodeClaimAtProviderID("c"))
		statetest.ExpectContribution(t, base, nodeClaimAtProviderID("c"), nodeClaimAtProviderID("a"))
	}
	for _, base := range []int{0, 5} {
		statetest.ExpectContribution(t, base, counted(1), counted(3))
	}
}

func TestNodeSideContributionsKeepTheContributionLaws(t *testing.T) {
	for _, base := range []ProviderIDFields{{}, {NodeClaims: []string{"nc"}, Nodes: []string{"m"}, BareNodes: 1}} {
		statetest.ExpectContribution(t, base, nodeAtProviderID{name: "a"}, nodeAtProviderID{name: "z", bare: true})
		statetest.ExpectContribution(t, base, nodeAtProviderID{name: "a", bare: true}, nodeAtProviderID{name: "a"})
	}
	for _, base := range []NodeCounts{{}, {Nodes: 3, Unhealthy: 1}} {
		statetest.ExpectContribution(t, base, newNodeCount(false), newNodeCount(true))
		statetest.ExpectContribution(t, base, newNodeCount(true), newNodeCount(false))
	}
	matcher := testMatcher(t)
	node := func(name, reason string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: "BadNode", Status: corev1.ConditionFalse, Reason: reason, LastTransitionTime: metav1.NewTime(time.Unix(10, 0))},
		}}}
	}
	kernel, other := node("a", "Kernel"), node("a", "Other")
	if !slices.EqualFunc(matcher.Match(kernel), matcher.Match(kernel), repair.RepairPolicyMatch.Equal) ||
		slices.EqualFunc(matcher.Match(kernel), matcher.Match(other), repair.RepairPolicyMatch.Equal) {
		t.Errorf("repair matches compare by the policies they match")
	}
	// Each Node is the only source of its fields by its name, so the law holds from fields no Node set: here, a Node's
	// pod usage.
	usage := podFields{Pods: 1, Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
	for _, base := range []podFields{{}, usage} {
		statetest.ExpectContributionWith(t, podFieldsEqual, base,
			nodeOnNode{node: kernel, matches: matcher.Match(kernel)}, nodeOnNode{node: other, matches: matcher.Match(other)})
	}
}

func TestOwnedNodeKeepsTheContributionLawsAndLeavesTheNodeClaimsIntent(t *testing.T) {
	matcher := testMatcher(t)
	node := func(name, reason string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: "BadNode", Status: corev1.ConditionFalse, Reason: reason, LastTransitionTime: metav1.NewTime(time.Unix(10, 0))},
		}}}
	}
	kernel, other := node("a", "Kernel"), node("a", "Other")
	// A Node's contribution to the NodeClaim its owner reference names sets only the Node and its matches, so Delete
	// leaves the NodeClaim and the intent kept about it.
	nodeClaimFields := NodeClaimFields{
		NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nc"}}, MarkedForDeletion: true,
		NominatedUntil: metav1.NewTime(time.Unix(20, 0)), BufferPods: 2,
	}
	for _, base := range []NodeClaimFields{{}, nodeClaimFields} {
		statetest.ExpectContributionWith(t, nodeClaimFieldsEqual, base,
			ownedNode{node: kernel, matches: matcher.Match(kernel)}, ownedNode{node: other})
	}
	// The intent goes with the NodeClaim, and the Node stays until its own Derive deletes it.
	withNode := ownedNode{node: kernel}.Update(nodeClaimFields, ownedNode{})
	withNode.NodeClaim = nil
	if f := withNode.orNothing(); f.Node != kernel || f.MarkedForDeletion || f.BufferPods != 0 || !f.NominatedUntil.IsZero() {
		t.Errorf("orNothing() = %+v once the NodeClaim is gone, want only the Node", f)
	}
	if f := (ownedNode{node: kernel}).Delete(withNode.orNothing()); !reflect.DeepEqual(f, NodeClaimFields{}) {
		t.Errorf("Delete() = %+v once the NodeClaim is gone, want nothing", f)
	}
}

func TestPodContributionsKeepTheContributionLaws(t *testing.T) {
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
	s := NewNodeState(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, nil)
	contribution := func(p *corev1.Pod) podContribution { return s.newPodContribution(client.ObjectKeyFromObject(p), p) }
	other := contribution(pod("other", "1", withPVC, withHostPort)).Update(podFields{}, podContribution{})
	for _, base := range []podFields{{}, other} {
		statetest.ExpectContributionWith(t, podFieldsEqual, base, contribution(pod("a", "1")), contribution(pod("a", "2", withPVC)))
		statetest.ExpectContributionWith(t, podFieldsEqual, base, contribution(pod("a", "2", withPVC, withHostPort)), contribution(pod("a", "1")))
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
	return a.Pods == b.Pods && a.DisruptionCost == b.DisruptionCost &&
		same(a.Requests, b.Requests) && same(a.Limits, b.Limits) && same(a.DaemonSetRequests, b.DaemonSetRequests) &&
		same(a.DaemonSetLimits, b.DaemonSetLimits) && reflect.DeepEqual(hostPorts(a.HostPorts), hostPorts(b.HostPorts)) &&
		maps.Equal(a.PVCs, b.PVCs) && maps.Equal(a.AntiAffinityPods, b.AntiAffinityPods)
}

// nodeClaimFieldsEqual compares NodeClaim fields by what they hold.
func nodeClaimFieldsEqual(a, b NodeClaimFields) bool {
	return a.NodeClaim == b.NodeClaim && a.Node == b.Node && a.MarkedForDeletion == b.MarkedForDeletion &&
		a.NominatedUntil.Equal(&b.NominatedUntil) && a.BufferPods == b.BufferPods &&
		slices.EqualFunc(a.RepairPolicyMatches, b.RepairPolicyMatches, repair.RepairPolicyMatch.Equal)
}
