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

package scheduling_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	pscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
)

// These specs pin down offeringsToReserve's handling of a reserved offering whose ReservationCapacity is 0 once
// reservation capacity and offering availability are decoupled at the provider (a full-but-healthy reservation is
// Available=true, ReservationCapacity=0). The invariant under test: a full reservation is treated as non-reservable
// (skipped), exactly as an unavailable offering would be — which (a) preserves strict/provisioning fall-back behavior,
// (b) keeps normal reserved scheduling working when capacity remains, (c) still respects Available=false regardless of
// capacity, and (d) in fallback mode leaves the reserved-only NodeClaim intact (which is what disruption terminate-first
// keys off of).
var _ = Describe("Reserved Instance Types/Full-but-available (capacity-availability decoupled)", func() {
	var nodePool *v1.NodePool
	// reservedOffering builds a reserved offering in test-zone-1 with the given availability and remaining capacity.
	reservedOffering := func(id string, available bool, capacity int) *cloudprovider.Offering {
		return &cloudprovider.Offering{
			Available:           available,
			ReservationCapacity: capacity,
			Price:               0.001,
			Requirements: pscheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:     v1.CapacityTypeReserved,
				corev1.LabelTopologyZone:    "test-zone-1",
				v1alpha1.LabelReservationID: id,
			}),
		}
	}
	// newReservedType builds a fake instance type (keeps its default on-demand/spot offerings) that also advertises the
	// given reserved offering.
	newReservedType := func(name string, reserved *cloudprovider.Offering) *cloudprovider.InstanceType {
		it := fake.NewInstanceType("reserved-type", fake.WithResources(map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		}))
		it.Name = name
		it.Requirements.Get(v1.CapacityTypeLabelKey).Insert(v1.CapacityTypeReserved)
		it.Offerings = append(it.Offerings, reserved)
		return it
	}
	reservedOnlyNodePool := func(instanceTypeName string) *v1.NodePool {
		return test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
			Requirements: []v1.NodeSelectorRequirementWithMinValues{
				{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{instanceTypeName}},
				{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeReserved}},
			},
		}}}})
	}
	smallPod := func() *corev1.Pod {
		return test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		}})
	}
	// solveFallback runs the scheduler in the default (fallback) ReservedOfferingMode — the mode the disruption
	// simulation uses — and returns the results.
	solveFallback := func(pods ...*corev1.Pod) scheduling.Results {
		GinkgoHelper()
		s, err := prov.NewScheduler(ctx, pods, nil, nil)
		Expect(err).ToNot(HaveOccurred())
		results, err := s.Solve(injection.WithControllerName(ctx, "provisioner"), pods)
		Expect(err).ToNot(HaveOccurred())
		return results
	}

	BeforeEach(func() {
		cloudProvider.Reset()
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{ReservedCapacity: lo.ToPtr(true)}}))
	})

	It("fallback: a full-but-available reservation still yields a reserved NodeClaim (disruption terminate-first keys off this)", func() {
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{newReservedType("reserved-type", reservedOffering("r-full", true, 0))}
		nodePool = reservedOnlyNodePool("reserved-type")
		ExpectApplied(ctx, env.Client, nodePool)

		results := solveFallback(smallPod())

		Expect(results.NewNodeClaims).To(HaveLen(1))
		nc := results.NewNodeClaims[0]
		// The pod scheduled onto a reserved-only NodeClaim...
		Expect(nc.Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeReserved)).To(BeTrue())
		Expect(nc.Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeOnDemand)).To(BeFalse())
		// ...but no reservation was held (it was full), so no reservation-id was pinned onto the NodeClaim.
		Expect(nc.Requirements.Get(v1alpha1.LabelReservationID).Values()).To(BeEmpty())
	})

	It("fallback: a reservation that still has capacity is reserved normally (regression guard)", func() {
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{newReservedType("reserved-type", reservedOffering("r-has-cap", true, 3))}
		nodePool = reservedOnlyNodePool("reserved-type")
		ExpectApplied(ctx, env.Client, nodePool)

		results := solveFallback(smallPod())

		Expect(results.NewNodeClaims).To(HaveLen(1))
		nc := results.NewNodeClaims[0]
		Expect(nc.Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeReserved)).To(BeTrue())
		// A reservable offering IS held, so the reservation id is pinned.
		Expect(nc.Requirements.Get(v1alpha1.LabelReservationID).Values()).To(ConsistOf("r-has-cap"))
	})

	It("a reservation marked unavailable is skipped even when it reports capacity (Available=false respected)", func() {
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{newReservedType("reserved-type", reservedOffering("r-unavail", false, 5))}
		nodePool = reservedOnlyNodePool("reserved-type")
		ExpectApplied(ctx, env.Client, nodePool)

		pod := smallPod()
		results := solveFallback(pod)

		// The only offering for a reserved-only pool is unavailable, so the pod cannot be placed on a new NodeClaim.
		Expect(results.NewNodeClaims).To(HaveLen(0))
		Expect(results.PodErrors).To(HaveKey(pod))
	})

	It("strict (provisioning): a full-but-available reservation does not block scheduling and holds no reservation", func() {
		// A mixed NodePool (allows reserved + on-demand/spot). The instance type's reservation is full but Available.
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{newReservedType("reserved-type", reservedOffering("r-full", true, 0))}
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
			Requirements: []v1.NodeSelectorRequirementWithMinValues{
				{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"reserved-type"}},
			},
		}}}})
		ExpectApplied(ctx, env.Client, nodePool)

		// Strict mode (the provisioning path). A full reservation must NOT raise a blocking ReservedOfferingError:
		// the pod schedules, and the NodeClaim holds no reservation (it can launch on-demand/spot). We assert on the
		// scheduling decision rather than the launched node's capacity-type, because the fake cloudprovider's launch
		// still picks the cheapest Available offering (the cap=0 reserved one) — the real provider's launch filter
		// skips full reservations, but that is a provider-side concern, not the scheduler's.
		pod := smallPod()
		s, err := prov.NewScheduler(ctx, []*corev1.Pod{pod}, nil, nil, scheduling.DisableReservedCapacityFallback)
		Expect(err).ToNot(HaveOccurred())
		results, err := s.Solve(injection.WithControllerName(ctx, "provisioner"), []*corev1.Pod{pod})
		Expect(err).ToNot(HaveOccurred())
		Expect(results.PodErrors).ToNot(HaveKey(pod)) // not blocked by a ReservedOfferingError
		Expect(results.NewNodeClaims).To(HaveLen(1))
		// No reservation was held (it was full), so the NodeClaim wasn't pinned to reserved.
		Expect(results.NewNodeClaims[0].Requirements.Get(v1alpha1.LabelReservationID).Values()).To(BeEmpty())
		Expect(results.NewNodeClaims[0].Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeOnDemand)).To(BeTrue())
	})

	It("strict (provisioning): a reserved-only pool with a full reservation defers instead of launching on-demand", func() {
		// A reserved-ONLY NodePool (capacity-type In [reserved], no on-demand/spot fallback) whose reservation is full
		// but Available. There is no non-reserved fallback, so strict must defer (ReservedOfferingError) rather than
		// create a NodeClaim that would be launched on-demand — this is the reserved-only mislaunch guard.
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{newReservedType("reserved-type", reservedOffering("r-full", true, 0))}
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
			Requirements: []v1.NodeSelectorRequirementWithMinValues{
				{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"reserved-type"}},
				{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeReserved}},
			},
		}}}})
		ExpectApplied(ctx, env.Client, nodePool)

		pod := smallPod()
		s, err := prov.NewScheduler(ctx, []*corev1.Pod{pod}, nil, nil, scheduling.DisableReservedCapacityFallback)
		Expect(err).ToNot(HaveOccurred())
		results, err := s.Solve(injection.WithControllerName(ctx, "provisioner"), []*corev1.Pod{pod})
		Expect(err).ToNot(HaveOccurred())
		// No on-demand mislaunch: no NodeClaim is created and the pod defers via a (retryable) ReservedOfferingError.
		Expect(results.NewNodeClaims).To(HaveLen(0))
		Expect(results.ReservedOfferingErrors()).To(HaveKey(pod))
	})

	It("strict (provisioning): a reservation with capacity is preferred over on-demand (regression guard)", func() {
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{newReservedType("reserved-type", reservedOffering("r-has-cap", true, 1))}
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
			Requirements: []v1.NodeSelectorRequirementWithMinValues{
				{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"reserved-type"}},
			},
		}}}})
		ExpectApplied(ctx, env.Client, nodePool)

		bindings := ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, smallPod())
		Expect(bindings.Bindings).To(HaveLen(1))
		node := lo.Values(bindings.Bindings)[0].Node
		Expect(node.Labels).To(HaveKeyWithValue(v1.CapacityTypeLabelKey, v1.CapacityTypeReserved))
		Expect(node.Labels).To(HaveKeyWithValue(cloudprovider.ReservationIDLabel, "r-has-cap"))
	})
})
