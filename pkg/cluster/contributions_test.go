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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Contributions", func() {
	It("should keep the contribution laws for the cluster's counts", func() {
		base := ClusterDerived{LaunchedNodeClaims: 2, UnlaunchedNodeClaims: 1, UnmanagedNodes: 3, Health: NodeCounts{Nodes: 4, Unhealthy: 1}}
		for _, b := range []ClusterDerived{{}, base} {
			statetest.ExpectContribution(GinkgoT(), b, clusterCount[launchedNodeClaims](1), clusterCount[launchedNodeClaims](1))
			statetest.ExpectContribution(GinkgoT(), b, clusterCount[unlaunchedNodeClaims](1), clusterCount[unlaunchedNodeClaims](1))
			statetest.ExpectContribution(GinkgoT(), b, clusterCount[unmanagedNodes](1), clusterCount[unmanagedNodes](1))
			statetest.ExpectContribution(GinkgoT(), b, clusterHealth(newNodeCount(true)), clusterHealth(newNodeCount(false)))
		}
		for _, b := range []NodeCounts{{}, {Nodes: 3, Unhealthy: 1}} {
			statetest.ExpectContribution(GinkgoT(), b, nodeCount{nodes: 1}, nodeCount{nodes: 1, unhealthy: 1})
		}
		for _, b := range []DaemonSetDerived{{}, {Pods: 3}} {
			statetest.ExpectContribution(GinkgoT(), b, daemonSetPod(1), daemonSetPod(1))
		}
		for _, b := range []int{0, 5} {
			statetest.ExpectContribution(GinkgoT(), b, mountsPVC(1), mountsPVC(1))
			statetest.ExpectContribution(GinkgoT(), b, podOnNodeClaim(1), podOnNodeClaim(1))
		}
	})
	It("should keep the contribution laws for Node side contributions", func() {
		matcher := testMatcher()
		kernel, other := badNode("a", "Kernel"), badNode("a", "Other")
		// Repair matches compare by the policies they match.
		Expect(slices.EqualFunc(matcher.Match(kernel), matcher.Match(kernel), repair.RepairPolicyMatch.Equal)).To(BeTrue())
		Expect(slices.EqualFunc(matcher.Match(kernel), matcher.Match(other), repair.RepairPolicyMatch.Equal)).To(BeFalse())
		// Each contribution to a Node's entry sets only its own fields, so the laws hold from fields the others set.
		usage := NodeDerived{Usage: NodeUsage{Pods: 1, Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}}
		for _, base := range []NodeDerived{{}, usage} {
			statetest.ExpectContributionWith(GinkgoT(), nodeDerivedEqual, base, repairMatches(matcher.Match(kernel)), repairMatches(matcher.Match(other)))
			statetest.ExpectContributionWith(GinkgoT(), nodeDerivedEqual, base,
				csiDrivers([]storagev1.CSINodeDriver{{Name: "a"}}), csiDrivers([]storagev1.CSINodeDriver{{Name: "a"}, {Name: "b"}}))
		}
		// Each NodeClaim has one Node, so the law holds from its fields with no Node.
		statetest.ExpectContribution(GinkgoT(), NodeClaimDerived{}, linkedNode("a"), linkedNode("b"))
	})
	It("should keep the contribution laws for pod contributions", func() {
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
		terminating := func(p *corev1.Pod) { p.DeletionTimestamp = &metav1.Time{Time: time.Unix(10, 0)} }
		s := NewNodeState(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, nil, nil, fake.NewCloudProvider(), nil)
		of := podContributionOf(options.ToContext(context.Background(), test.Options()), s.PVCStore)
		contribution := func(p *corev1.Pod) podContribution { return of(client.ObjectKeyFromObject(p), PodEntry{Object: p}) }
		other := contribution(pod("other", "1", withPVC, withHostPort)).Update(NodeDerived{CSIDrivers: []storagev1.CSINodeDriver{{Name: "a"}}}, podContribution{})
		for _, base := range []NodeDerived{{}, other} {
			statetest.ExpectContributionWith(GinkgoT(), nodeDerivedEqual, base, contribution(pod("a", "1")), contribution(pod("a", "2", withPVC)))
			statetest.ExpectContributionWith(GinkgoT(), nodeDerivedEqual, base, contribution(pod("a", "2", withPVC, withHostPort)), contribution(pod("a", "1", terminating)))
		}
	})
})

// badNode is a Node whose BadNode condition is False for reason.
func badNode(name, reason string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
		{Type: "BadNode", Status: corev1.ConditionFalse, Reason: reason, LastTransitionTime: metav1.NewTime(time.Unix(10, 0))},
	}}}
}

// nodeDerivedEqual compares a Node's derived fields as their readers do: a resource at zero is the resource absent,
// and host port usage compares by what it holds.
func nodeDerivedEqual(a, b NodeDerived) bool {
	usage := func(l corev1.ResourceList) corev1.ResourceList {
		return lo.OmitBy(l, func(_ corev1.ResourceName, q resource.Quantity) bool { return q.IsZero() })
	}
	same := func(x, y corev1.ResourceList) bool { return equality.Semantic.DeepEqual(usage(x), usage(y)) }
	hostPorts := func(h *scheduling.HostPortUsage) *scheduling.HostPortUsage {
		return lo.Ternary(h == nil, scheduling.NewHostPortUsage(), h)
	}
	x, y := a.Usage, b.Usage
	return x.Pods == y.Pods && x.Terminating == y.Terminating && x.DoNotDisrupt == y.DoNotDisrupt && x.DisruptionCost == y.DisruptionCost &&
		same(x.Requests, y.Requests) && same(x.Limits, y.Limits) && same(x.DaemonSetRequests, y.DaemonSetRequests) &&
		same(x.DaemonSetLimits, y.DaemonSetLimits) && reflect.DeepEqual(hostPorts(x.HostPorts), hostPorts(y.HostPorts)) &&
		maps.Equal(x.Volumes, y.Volumes) && maps.Equal(x.AntiAffinityPods, y.AntiAffinityPods) &&
		slices.EqualFunc(a.RepairPolicyMatches, b.RepairPolicyMatches, repair.RepairPolicyMatch.Equal) &&
		equality.Semantic.DeepEqual(a.CSIDrivers, b.CSIDrivers)
}

// testMatcher returns a matcher of one policy: a Node whose BadNode condition is False needs replacing.
func testMatcher() *repair.RepairPolicyMatcher {
	GinkgoHelper()
	cloudProvider := fake.NewCloudProvider()
	cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
		{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, Action: cloudprovider.ReplaceNode},
		{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, ReasonRegex: "^Kernel$", Action: cloudprovider.ReplaceNode},
	}
	ctx := options.ToContext(context.Background(), test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: new(true)}}))
	matcher, err := repair.NewRepairPolicyMatcher(ctx, cloudProvider)
	Expect(err).ToNot(HaveOccurred())
	Expect(matcher).ToNot(BeNil())
	return matcher
}
