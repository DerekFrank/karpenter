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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

// A Node registers, with kubelet, before the registration controller sets its NodeClaim owner reference. The provider
// contract is that it carries its karpenter.sh/nodepool label from kubelet registration: with the label, it's
// registering, and its NodeClaim's entry represents it; without, it's unmanaged until the owner reference lands. Either
// way the counts and the scheduler's node list converge once it does.
var _ = Describe("Registering window", func() {
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	BeforeEach(func() {
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
			}},
			Status: v1.NodeClaimStatus{
				Capacity:    corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("110")},
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("110")},
			},
		})
		Expect(node.OwnerReferences).To(BeEmpty(), "the Node registers before its owner reference is set")
	})
	apply := func(objs ...client.Object) {
		GinkgoHelper()
		ExpectApplied(ctx, env.Client, objs...)
		for _, obj := range objs {
			switch obj.(type) {
			case *v1.NodeClaim:
				ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(obj))
			case *corev1.Node:
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(obj))
			case *corev1.Pod:
				ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(obj))
			}
		}
	}
	// linkOwner sets the owner reference registration sets, as the registration controller does.
	linkOwner := func() {
		GinkgoHelper()
		node = nodeclaimutils.UpdateNodeOwnerReferences(ExpectExists(ctx, env.Client, nodeClaim), ExpectExists(ctx, env.Client, node))
		apply(node)
	}
	// counts are the counts a decision reads: the scheduler's node list and its NodeClaims, the cluster's counts, and
	// the NodePool's.
	type counts struct {
		Scheduler, SchedulerNodeClaims, Gauge, Launched, Unmanaged, Active, Launches int
	}
	countsNow := func() counts {
		GinkgoHelper()
		nodes := cluster.DeepCopyNodes()
		c := counts{Scheduler: len(nodes), Launched: cluster.Cluster.Counts().LaunchedNodeClaims, Unmanaged: cluster.Cluster.Counts().UnmanagedNodes}
		for _, n := range nodes {
			if n.NodeClaim != nil {
				c.SchedulerNodeClaims++
			}
		}
		c.Gauge = cluster.Cluster.NodeCount()
		ExpectMetricGaugeValue(karpentercluster.ClusterStateNodesCount, float64(c.Gauge), nil)
		c.Active, _, _ = cluster.NodePoolState.GetNodeCount(nodePool.Name)
		c.Launches = len(cluster.NodePoolState.LaunchedNodes(nodePool.Name))
		return c
	}

	It("should count a Node that registers with its NodePool label once, as its NodeClaim's node, before and after the owner reference lands", func() {
		apply(nodeClaim, node)
		window := countsNow()
		Expect(window).To(Equal(counts{Scheduler: 1, SchedulerNodeClaims: 1, Gauge: 1, Launched: 1, Active: 1, Launches: 1}), "counts in the window")
		n := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim)
		Expect(n.Node).To(BeNil(), "the NodeClaim's node links its Node before the owner reference lands")
		Expect(n.Managed()).To(BeTrue())

		linkOwner()
		Expect(countsNow()).To(Equal(window), "counts once the owner reference landed")
		n = ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim)
		Expect(n.Node).ToNot(BeNil())
		Expect(n.Node.Name).To(Equal(node.Name))
	})
	It("should read a Node that registers without its NodePool label as unmanaged until the owner reference lands, then converge", func() {
		delete(node.Labels, v1.NodePoolLabelKey)
		apply(nodeClaim, node)
		window := countsNow()
		// The scheduler can count the machine twice for a loop, and the unmanaged-node gauge blips.
		Expect(window).To(Equal(counts{Scheduler: 2, SchedulerNodeClaims: 1, Gauge: 2, Launched: 1, Unmanaged: 1, Active: 1, Launches: 1}), "counts in the window")
		Expect(ExpectStateNodeExists(cluster, node).Managed()).To(BeFalse(), "the Node reads as unmanaged in the window")
		AddReportEntry("registering window without the NodePool label", fmt.Sprintf("%+v", window))

		linkOwner()
		Expect(countsNow()).To(Equal(counts{Scheduler: 1, SchedulerNodeClaims: 1, Gauge: 1, Launched: 1, Active: 1, Launches: 1}), "counts once the owner reference landed")
		n := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim)
		Expect(n.Node).ToNot(BeNil())
		Expect(n.Node.Name).To(Equal(node.Name))
	})
	It("should count DaemonSet pods bound in the window on the Node, and on the NodeClaim's node once the owner reference lands", func() {
		daemonSet := test.DaemonSet()
		ExpectApplied(ctx, env.Client, daemonSet)
		apply(nodeClaim, node)
		var pods []*corev1.Pod
		for range 3 {
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
					APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "DaemonSet", Name: daemonSet.Name, UID: daemonSet.UID, Controller: new(true),
				}}},
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}},
			})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
			pods = append(pods, pod)
			apply(ExpectExists(ctx, env.Client, pod))
		}
		// seen is what the scheduler sees of the NodeClaim's node.
		type seen struct {
			Pods          int64
			PodCPU        string
			DaemonSetCPU  string
			AvailableCPU  string
			NodeEntryPods int
			NodeEntryCPU  string
			LinksNode     bool
		}
		see := func() seen {
			GinkgoHelper()
			n := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim)
			e, _ := cluster.NodeStore.Get(node.Name)
			requests, daemonSets, available := n.PodRequests(), n.DaemonSetRequests(), n.Available()
			return seen{
				Pods:          requests.Pods().Value(),
				PodCPU:        requests.Cpu().String(),
				DaemonSetCPU:  daemonSets.Cpu().String(),
				AvailableCPU:  available.Cpu().String(),
				NodeEntryPods: e.Derived.Usage.Pods,
				NodeEntryCPU:  e.Derived.Usage.Requests.Cpu().String(),
				LinksNode:     n.Node != nil,
			}
		}
		window := see()
		// The pods count on the Node's entry in the window, and the NodeClaim's node reads none of them.
		Expect(window).To(Equal(seen{Pods: 0, PodCPU: "0", DaemonSetCPU: "0", AvailableCPU: "4", NodeEntryPods: 3, NodeEntryCPU: "300m"}), "what the scheduler sees in the window")
		AddReportEntry("daemonset pods in the registering window, before the owner reference", fmt.Sprintf("%+v", window))

		linkOwner()
		after := see()
		Expect(after).To(Equal(seen{Pods: 3, PodCPU: "300m", DaemonSetCPU: "300m", AvailableCPU: "3700m", NodeEntryPods: 3, NodeEntryCPU: "300m", LinksNode: true}),
			"what the scheduler sees once the owner reference landed")
		AddReportEntry("daemonset pods in the registering window, after the owner reference", fmt.Sprintf("%+v", after))
		ExpectDeleted(ctx, env.Client, daemonSet)
		for _, pod := range pods {
			ExpectDeleted(ctx, env.Client, pod)
		}
	})
})
