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

package provisioning_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	autoscalingv1beta1 "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("CapacityQuota", func() {
	var quotas []*autoscalingv1beta1.CapacityQuota
	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{CapacityQuota: new(true)}}))
		quotas = nil
	})
	AfterEach(func() {
		for _, q := range quotas {
			ExpectDeleted(ctx, env.Client, q)
		}
	})
	// applyQuota creates a CapacityQuota limiting the CPU of the nodes selector selects, and shows it to cluster state as
	// the CapacityQuota informer would.
	applyQuota := func(name string, selector *metav1.LabelSelector, cpu string) {
		q := &autoscalingv1beta1.CapacityQuota{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: autoscalingv1beta1.CapacityQuotaSpec{Selector: selector, Limits: autoscalingv1beta1.CapacityQuotaLimits{
				Resources: autoscalingv1beta1.ResourceList{autoscalingv1beta1.ResourceCPU: resource.MustParse(cpu)},
			}},
		}
		ExpectApplied(ctx, env.Client, q)
		quotas = append(quotas, q)
		cluster.CapacityQuotaState.Observe(ExpectExists(ctx, env.Client, q))
	}
	nodePool := func(name string) {
		ExpectApplied(ctx, env.Client, test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}}))
	}
	// pod requires a 2 CPU node, in the NodePool named np
	pod := func(np string) *corev1.Pod {
		return test.UnschedulablePod(test.PodOptions{
			NodeSelector:         map[string]string{v1.NodePoolLabelKey: np},
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1.75")}},
		})
	}
	inNodePools := func(names ...string) *metav1.LabelSelector {
		return &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: v1.NodePoolLabelKey, Operator: metav1.LabelSelectorOpIn, Values: names}}}
	}

	It("should only apply a quota to the NodePools it selects", func() {
		nodePool("a")
		nodePool("b")
		applyQuota("team-a", inNodePools("a"), "1")
		blocked, allowed := pod("a"), pod("b")
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, blocked, allowed)
		ExpectNotScheduled(ctx, env.Client, blocked)
		ExpectScheduled(ctx, env.Client, allowed)
	})
	It("should share a quota between the NodePools it selects", func() {
		nodePool("a")
		nodePool("b")
		applyQuota("teams", inNodePools("a", "b"), "3")
		results := ExpectProvisionedResults(ctx, env.Client, cluster, cloudProvider, prov, pod("a"))
		Expect(results.NewNodeClaims).To(HaveLen(1))
		_, err := prov.CreateNodeClaims(ctx, results.NewNodeClaims)
		Expect(err).ToNot(HaveOccurred())

		// The first NodeClaim hasn't launched; what it holds leaves too little for a second in the other NodePool.
		results = ExpectProvisionedResults(ctx, env.Client, cluster, cloudProvider, prov, pod("b"))
		Expect(results.NewNodeClaims).To(BeEmpty())
	})
	It("should refuse a NodeClaim two solves both planned within a quota, and release its NodePool reservation", func() {
		np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "a"}, Spec: v1.NodePoolSpec{
			Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("100")},
		}})
		ExpectApplied(ctx, env.Client, np)
		applyQuota("team-a", inNodePools("a"), "3")
		first := ExpectProvisionedResults(ctx, env.Client, cluster, cloudProvider, prov, pod("a"))
		second := ExpectProvisionedResults(ctx, env.Client, cluster, cloudProvider, prov, pod("a"))
		Expect(first.NewNodeClaims).To(HaveLen(1))
		Expect(second.NewNodeClaims).To(HaveLen(1))

		_, err := prov.CreateNodeClaims(ctx, first.NewNodeClaims)
		Expect(err).ToNot(HaveOccurred())
		heldByFirst := cluster.NodePoolState.Get("a").ReservedResources.DeepCopy()

		// The second fits the NodePool's limits but not the quota, so its NodePool reservation is released.
		_, err = prov.CreateNodeClaims(ctx, second.NewNodeClaims)
		Expect(err).To(MatchError(ContainSubstring("capacityquota")))
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(1))
		Expect(cluster.NodePoolState.Get("a").ReservedResources[corev1.ResourceCPU]).To(BeComparableTo(heldByFirst[corev1.ResourceCPU]))
	})
	It("should hold a quota on a zone against a NodeClaim that may launch in it", func() {
		nodePool("a")
		applyQuota("zone-1", &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelTopologyZone: "test-zone-1"}}, "1")
		anyZone := pod("a")
		otherZone := pod("a")
		otherZone.Spec.NodeSelector[corev1.LabelTopologyZone] = "test-zone-2"
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, anyZone)
		ExpectNotScheduled(ctx, env.Client, anyZone)
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, otherZone)
		ExpectScheduled(ctx, env.Client, otherZone)
	})
	It("should count a launched NodeClaim in place of its reservation", func() {
		nodePool("a")
		applyQuota("team-a", inNodePools("a"), "3")
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, pod("a"))
		nodeClaims := ExpectNodeClaims(ctx, env.Client)
		Expect(nodeClaims).To(HaveLen(1))
		fields := cluster.CapacityQuotaState.Get("team-a")
		Expect(fields.Reserved).To(BeEmpty())
		Expect(fields.Used[corev1.ResourceCPU]).To(BeComparableTo(nodeClaims[0].Status.Capacity[corev1.ResourceCPU]))
	})
	It("should stop enforcing a quota once it's deleted", func() {
		nodePool("a")
		applyQuota("team-a", inNodePools("a"), "1")
		blocked := pod("a")
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, blocked)
		ExpectNotScheduled(ctx, env.Client, blocked)

		ExpectDeleted(ctx, env.Client, quotas[0])
		cluster.CapacityQuotaState.Forget(quotas[0].Name)
		quotas = nil
		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, blocked)
		ExpectScheduled(ctx, env.Client, blocked)
		Expect(cluster.CapacityQuotaState.Get("team-a")).To(BeZero())
	})
})
