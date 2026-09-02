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

package disruption_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// Terminate-First Disruption (RFC kubernetes-sigs/karpenter#3203) end-to-end. A voluntary disruption of a
// capacity-constrained pool (a static NodePool at its replica count, or a reserved candidate whose reservation is
// full) issues a delete-only command and lets reactive provisioning refill the freed slot, instead of the
// replace-first it cannot stage.

var _ = Describe("TerminateFirst", func() {
	// bindReschedulablePod places a ReplicaSet-owned (reschedulable) pod on the node so the scheduling simulation has a
	// workload to protect — the replacement it produces is what terminate-first suppresses.
	bindReschedulablePod := func(node *corev1.Node) {
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
		pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
		}}}})
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
	}

	Context("StaticDrift", func() {
		var nodePool *v1.NodePool
		var nodeClaim *v1.NodeClaim
		var node *corev1.Node
		var staticDriftController *disruption.Controller

		BeforeEach(func() {
			nodePool = test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{
				Replicas:   lo.ToPtr(int64(1)),
				Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: "100%"}}},
			}})
			nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				}},
				Status: v1.NodeClaimStatus{
					ProviderID:  test.RandomProviderID(),
					Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")},
				},
			})
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			staticDriftController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
				disruption.WithMethods(disruption.NewStaticDrift(cluster, prov, cloudProvider)))
		})

		It("issues a delete-only command for a static NodePool when TerminateFirst is enabled", func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirst: lo.ToPtr(true)}}))
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

			ExpectSingletonReconciled(ctx, staticDriftController)

			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
			Expect(cmds[0].Replacements).To(HaveLen(0))
		})

		It("replaces-first for a static NodePool when TerminateFirst is disabled", func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirst: lo.ToPtr(false)}}))
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

			ExpectSingletonReconciled(ctx, staticDriftController)

			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
			Expect(cmds[0].Replacements).To(HaveLen(1))
		})
	})

	Context("Drift/Reserved", func() {
		var nodePool *v1.NodePool
		var nodeClaim *v1.NodeClaim
		var node *corev1.Node
		var driftController *disruption.Controller
		var reservationID string

		// setupReservedOffering appends a reserved offering to mostExpensiveInstance with the given remaining capacity
		// and constrains the NodePool to that instance type. capacityTypes are the capacity types the NodePool permits.
		setupReservedOffering := func(reservationCapacity int, capacityTypes ...string) {
			reservationID = "r-" + mostExpensiveInstance.Name
			mostExpensiveInstance.Requirements.Add(scheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpIn, reservationID))
			mostExpensiveInstance.Requirements.Get(v1.CapacityTypeLabelKey).Insert(v1.CapacityTypeReserved)
			mostExpensiveInstance.Offerings = append(mostExpensiveInstance.Offerings, &cloudprovider.Offering{
				Price:               mostExpensiveOffering.Price / 1_000_000.0,
				Available:           true,
				ReservationCapacity: reservationCapacity,
				Requirements: scheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:          v1.CapacityTypeReserved,
					corev1.LabelTopologyZone:         mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					cloudprovider.ReservationIDLabel: reservationID,
				}),
			})
			ExpectSingletonReconciled(ctx, pricingController)

			nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{ConsolidateAfter: v1.MustParseNillableDuration("1h"), Budgets: []v1.Budget{{Nodes: "100%"}}},
				Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{Requirements: []v1.NodeSelectorRequirementWithMinValues{
					{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{mostExpensiveInstance.Name}},
					{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: capacityTypes},
				}}},
			}})
			nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					v1.NodePoolLabelKey:              nodePool.Name,
					corev1.LabelInstanceTypeStable:   mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:          v1.CapacityTypeReserved,
					corev1.LabelTopologyZone:         mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					cloudprovider.ReservationIDLabel: reservationID,
				}},
				Status: v1.NodeClaimStatus{
					ProviderID:  test.RandomProviderID(),
					Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourcePods: resource.MustParse("100")},
				},
			})
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			driftController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
				disruption.WithMethods(disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock)))
		}

		It("issues a delete-only command when the reservation is full and there is no fallback", func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirst: lo.ToPtr(true), ReservedCapacity: lo.ToPtr(true)}}))
			setupReservedOffering(0, v1.CapacityTypeReserved) // full reservation, reserved-only pool
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			bindReschedulablePod(node)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

			ExpectSingletonReconciled(ctx, driftController)

			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
			Expect(cmds[0].Replacements).To(HaveLen(0))
		})

		It("replaces-first (into the full reservation) when TerminateFirst is disabled — the sim already yields the reserved-only replacement, no crediting needed", func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirst: lo.ToPtr(false), ReservedCapacity: lo.ToPtr(true)}}))
			setupReservedOffering(0, v1.CapacityTypeReserved) // full reservation, reserved-only pool
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			bindReschedulablePod(node)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

			ExpectSingletonReconciled(ctx, driftController)

			// With the gate off, drift takes its normal replace-first path — and the plain (uncredited) fallback-mode
			// simulation still returns a reserved-only replacement for the full reservation, so a replacement is staged
			// (it would ICE at launch until the slot frees; terminate-first is exactly what avoids that).
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
			Expect(cmds[0].Replacements).To(HaveLen(1))
			Expect(cmds[0].Replacements[0].Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeReserved)).To(BeTrue())
		})

		It("replaces-first when the reservation is full but an on-demand fallback exists", func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirst: lo.ToPtr(true), ReservedCapacity: lo.ToPtr(true)}}))
			setupReservedOffering(0, v1.CapacityTypeReserved, v1.CapacityTypeOnDemand) // full reservation, but on-demand fallback allowed
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			bindReschedulablePod(node)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

			ExpectSingletonReconciled(ctx, driftController)

			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
			Expect(cmds[0].Replacements).To(HaveLen(1))
		})

		It("replaces-first when the reservation still has a spare slot", func() {
			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{TerminateFirst: lo.ToPtr(true), ReservedCapacity: lo.ToPtr(true)}}))
			setupReservedOffering(5, v1.CapacityTypeReserved) // spare reservation capacity -> can grow in place
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			bindReschedulablePod(node)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

			ExpectSingletonReconciled(ctx, driftController)

			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		})
	})
})
