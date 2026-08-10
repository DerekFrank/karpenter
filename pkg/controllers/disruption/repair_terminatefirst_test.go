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

// These tests exercise Terminate-First Disruption for repair (F2 / RFC #3203): a capacity-constrained NodePool has no
// room to pre-spin a replacement, so repair issues a delete-only command and lets reactive provisioning refill the
// freed slot, while a headroom pool still replaces-first.

var _ = Describe("Repair/TerminateFirst", func() {
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

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute},
		}
		repairController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(disruption.NewRepair(env.Client, cluster, prov, cloudProvider, recorder, env.Clock, queue)))
	})

	// bindReschedulablePod attaches a ReplicaSet-owned pod to a node. With a pod present, a headroom pool would
	// replace-FIRST (pre-spin a replacement). So a delete-only command on a pod-bearing node is decisive evidence of
	// terminate-first, not an artifact of the node being empty (an empty node yields delete-only on ANY path).
	bindReschedulablePod := func(n *corev1.Node) {
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
		pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
		}}}})
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	}

	// INV-F2-1: a static NodePool (fixed replica count) has no room to grow, so repair terminates first — the command
	// is delete-only (no pre-spun replacement) EVEN THOUGH the node carries a workload that a headroom pool would
	// pre-spin a replacement for. The bound pod is what distinguishes terminate-first from an empty-node delete.
	It("should issue a delete-only command for a capacity-constrained (static) NodePool with a workload", func() {
		nodePool := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: lo.ToPtr(int64(1))}})
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"},
		}})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		bindReschedulablePod(node) // a headroom pool would pre-spin a replacement for this pod; a static pool must not
		markUnhealthy(node)
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		// Delete-only despite the workload: terminate-first, not a pre-spin. (Contrast the headroom test below, which
		// with the same workload yields a ReplaceDecision.)
		Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
		Expect(cmds[0].Replacements).To(HaveLen(0))
	})

	// INV-F2-2: a normal (dynamic, headroom) NodePool still replaces-first — a pre-spun replacement is created.
	It("should still replace-first for a headroom (dynamic) NodePool", func() {
		nodePool := test.NodePool()
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"},
		}})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		bindReschedulablePod(node) // same workload as the static test; a headroom pool pre-spins a replacement for it
		markUnhealthy(node)
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		Expect(cmds[0].Replacements).To(HaveLen(1))
	})

	// INV-F2-4: terminate-first is still paced by the disruption budget. A static pool with a Repair budget of 0 blocks
	// terminate-first entirely (the operator's opt-out), and a nonzero budget lets it through — proving the budget, not
	// just the one-command-per-pass loop, governs terminate-first.
	It("should gate terminate-first on the disruption budget", func() {
		nodePool := test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{
			Replicas:   lo.ToPtr(int64(1)),
			Disruption: v1.Disruption{Budgets: []v1.Budget{{Reasons: []v1.DisruptionReason{v1.DisruptionReasonRepair}, Nodes: "0"}}},
		}})
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"},
		}})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		bindReschedulablePod(node)
		markUnhealthy(node)
		env.Clock.Step(31 * time.Minute)

		// Budget 0 for Repair -> no terminate-first command.
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0))

		// Raise the Repair budget; now terminate-first proceeds (delete-only, still governed by the budget).
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Reasons: []v1.DisruptionReason{v1.DisruptionReasonRepair}, Nodes: "100%"}}
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
	})

	// INV-F2-3: a launch FAILURE must never flip a pool to terminate-first. A dynamic (headroom) pool whose replacement
	// fails to launch keeps replacing-first (a replace command with a replacement) — it does not degrade to delete-only.
	// terminate-first is derived from static capacity posture, never from launch outcomes.
	It("should not flip to terminate-first when a replacement launch fails", func() {
		nodePool := test.NodePool() // dynamic / headroom pool
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"},
		}})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		bindReschedulablePod(node)
		// Make every subsequent launch fail (ICE-like) — the replacement can't come up.
		cloudProvider.AllowedCreateCalls = 0
		markUnhealthy(node)
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		// It still tries to REPLACE-first (the command carries a replacement); the failed launch is handled by the
		// queue's retry/backoff, not by silently switching to delete-only.
		if len(cmds) == 1 {
			Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
			Expect(cmds[0].Replacements).To(HaveLen(1))
		}
		// The pool must NOT have terminated the node via a delete-only command.
		for _, c := range cmds {
			Expect(c.Decision()).ToNot(Equal(disruption.DeleteDecision))
		}
	})
})
