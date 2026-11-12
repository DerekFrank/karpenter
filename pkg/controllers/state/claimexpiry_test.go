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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Claim expiry", func() {
	limits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10")}
	held := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}
	BeforeEach(func() {
		karpentercluster.ClaimsExpiredTotal.Reset()
	})

	reservedNodes := func() int {
		_, _, reserved := cluster.NodePoolState.GetNodeCount(nodePool.Name)
		return reserved
	}
	It("should expire a node reservation that's never bound at its TTL, and count it", func() {
		Expect(cluster.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(HaveLen(1))
		Expect(reservedNodes()).To(Equal(1))

		env.Clock.Step(karpentercluster.ReservationTTL)
		Eventually(reservedNodes).Should(Equal(0), "an expired reservation is held without another Reserve")
		ExpectMetricCounterValue(karpentercluster.ClaimsExpiredTotal, 1, map[string]string{metrics.ClaimStoreLabel: metrics.NodePoolClaimStore.Name})
	})
	reservedResources := func() corev1.ResourceList { return cluster.NodePoolState.ReservedResources(nodePool.Name) }
	It("should expire a resource reservation that's never bound at its TTL", func() {
		Expect(cluster.NodePoolState.ReserveResources(nodePool.Name, limits, held, nil)).ToNot(BeNil())
		env.Clock.Step(karpentercluster.ReservationTTL)
		Eventually(reservedResources).Should(BeEmpty())
		ExpectMetricCounterValue(karpentercluster.ClaimsExpiredTotal, 1, map[string]string{metrics.ClaimStoreLabel: metrics.NodePoolClaimStore.Name})
	})
	It("should hold a reservation bound to a NodeClaim cluster state sees past its TTL, until the NodeClaim goes", func() {
		nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
		nodeClaim.Status = v1.NodeClaimStatus{}
		ExpectApplied(ctx, env.Client, nodeClaim)
		cluster.NodeClaimStore.Observe(nodeClaim)
		reservation := cluster.NodePoolState.ReserveResources(nodePool.Name, limits, held, nil)
		Expect(reservation).ToNot(BeNil())
		reservation.Bind(nodeClaim.Name)

		// The NodeClaim hasn't launched, however long it takes; its reservation holds until it does or goes.
		env.Clock.Step(10 * karpentercluster.ReservationTTL)
		Consistently(func() *resource.Quantity { return new(reservedResources()[corev1.ResourceCPU]) }, 100*time.Millisecond).
			Should(BeComparableTo(new(resource.MustParse("8"))))
		_, expired := FindMetricWithLabelValues("karpenter_cluster_state_claims_expired_total", map[string]string{metrics.ClaimStoreLabel: metrics.NodePoolClaimStore.Name})
		Expect(expired).To(BeFalse(), "counted an expiry")

		cluster.NodeClaimStore.Forget(nodeClaim.Name)
		Expect(cluster.NodePoolState.ReservedResources(nodePool.Name)).To(BeEmpty())
	})
	It("should expire a reservation bound to a NodeClaim that went before cluster state saw it", func() {
		reservation := cluster.NodePoolState.ReserveResources(nodePool.Name, limits, held, nil)
		Expect(reservation).ToNot(BeNil())
		reservation.Bind("never-seen")
		env.Clock.Step(karpentercluster.ReservationTTL)
		Eventually(reservedResources).Should(BeEmpty())
		ExpectMetricCounterValue(karpentercluster.ClaimsExpiredTotal, 1, map[string]string{metrics.ClaimStoreLabel: metrics.NodePoolClaimStore.Name})
	})
})
