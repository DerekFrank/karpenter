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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// These tests exercise correlated-failure restraint (F3 / "Node Repair Under Correlated Failure"). Restraint layers
// beneath the disruption budget: even when the budget would allow many, restraint starts a correlated burst at one
// probe (width 1) and widens only as probes prove out. To isolate restraint from the budget, these tests set a wide
// budget (100%), so any limiting below the eligible count is restraint, not the budget.

var _ = Describe("Repair/Restraint", func() {
	var nodePool *v1.NodePool
	var repairController *disruption.Controller

	markUnhealthy := func(n *corev1.Node) {
		n = ExpectExists(ctx, env.Client, n)
		n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
			Type:               "BadNode",
			Status:             corev1.ConditionFalse,
			LastTransitionTime: metav1.Time{Time: env.Clock.Now()},
		})
		ExpectApplied(ctx, env.Client, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	}

	// applyBurst creates n unhealthy nodes in the same NodePool + zone (one shared failure domain), all bound with a
	// reschedulable pod so pre-spin produces a replacement, and steps past toleration.
	applyBurst := func(n int) ([]*v1.NodeClaim, []*corev1.Node) {
		nodeClaims, nodes := test.NodeClaimsAndNodes(n, v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"},
		}})
		for i := range nodes {
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
		for i := range nodes {
			pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
			}}}})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, nodes[i])
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(nodes[i]))
			markUnhealthy(nodes[i])
		}
		env.Clock.Step(31 * time.Minute)
		return nodeClaims, nodes
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute},
		}
		// Wide budget so restraint, not the budget, is what limits concurrency.
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: "100%"}}}}})
		ExpectApplied(ctx, env.Client, nodePool)
		repairController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(disruption.NewRepair(env.Client, cluster, prov, cloudProvider, recorder, env.Clock, queue)))
	})

	// INV-F3-1 / INV-F3-6: a correlated burst starts at width 1 — one probe first, even though the budget allows all.
	// A fresh (stateless) controller cold-starts every domain at the floor, so this is also the no-memory-safe case.
	It("should start a correlated burst at a single probe", func() {
		applyBurst(10)

		// The disruption loop issues at most one command per pass anyway, but restraint is what holds the fanout at 1
		// across passes until a probe proves out. Assert the queue holds exactly one in-flight probe.
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	// INV-F3-3 / INV-F3-4: width doubles only after a probe is PROVEN — its replacement holds healthy for the dwell.
	// Before the dwell elapses, restraint has not widened, so a second concurrent probe is not admitted.
	It("should not widen until a probe is proven healthy for the dwell", func() {
		applyBurst(10)

		// First probe.
		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))

		// Make the replacement healthy and terminate the original, but do NOT advance past the dwell yet.
		ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmds[0])
		ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
		ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmds[0].Candidates[0].NodeClaim)

		// Reconcile again immediately: the probe is terminated but not yet proven (dwell not elapsed), so width stays 1
		// and no new probe is admitted.
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0))
	})

	// INV-F3-2: repair never fully stops. A failed probe pins width at the floor (1) and arms a cooldown, but the floor
	// is 1 (never 0), so after the cooldown a probe is admitted again — repair keeps trying, just slower.
	It("should pause but never halt after a failed probe (cooldown, then floor)", func() {
		// A single unhealthy node in the pool+zone+policy domain.
		nc, n := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"},
		}})
		ExpectApplied(ctx, env.Client, nc, n)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{n}, []*v1.NodeClaim{nc})
		markUnhealthy(n)
		env.Clock.Step(31 * time.Minute)

		// First probe.
		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))

		// Fail the probe: never make the replacement ready, step past the queue retry timeout so the command times out.
		// On timeout the queue clears the command WITHOUT terminating the original — the "replacement never came up
		// healthy" outcome restraint reads as a failure.
		env.Clock.Step(2 * time.Hour)
		ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
		Expect(queue.IsEmpty()).To(BeTrue())

		// The next reconcile observes the failed probe: width resets to the floor and the pool+zone+policy domains are
		// put in cooldown. A FRESH unhealthy node in those same domains is now blocked — repair has paused.
		freshNC, freshNode := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"},
		}})
		ExpectApplied(ctx, env.Client, freshNC, freshNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{freshNode}, []*v1.NodeClaim{freshNC})
		markUnhealthy(freshNode)
		env.Clock.Step(31 * time.Minute)
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0)) // paused: domain in cooldown

		// After the cooldown ceiling elapses, the fresh node is admitted — repair paused, it did not halt (floor ≥ 1).
		env.Clock.Step(11 * time.Minute)
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})
})
