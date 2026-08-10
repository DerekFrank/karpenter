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
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// These tests exercise end-user behavior of voluntary node repair (Node Repair Resiliency design). Each It maps to an
// invariant (INV-S*) from the design. The disruption controller is built with only the Repair method so the behavior
// under test is isolated from consolidation/drift.

var _ = Describe("Repair", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	var repairController *disruption.Controller

	labels := func() map[string]string {
		return map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"}
	}

	// initNode applies + initializes a node/nodeclaim and syncs cluster state. Initialization overwrites
	// Status.Conditions with Ready=True, so unhealthy conditions must be stamped AFTER this via markUnhealthy.
	initNode := func(nc *v1.NodeClaim, n *corev1.Node) {
		ExpectApplied(ctx, env.Client, nc, n)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{n}, []*v1.NodeClaim{nc})
	}

	// bindReschedulablePod places a ReplicaSet-owned (reschedulable) pod on the node so pre-spin has a workload to
	// protect — the scheduling simulation then produces a replacement NodeClaim (a replace-then-terminate command).
	// An empty node correctly yields a delete-only command instead, so pre-spin tests must have a pod.
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

	// markUnhealthy appends a condition matching a RepairPolicy at the current fake-clock time, re-applies the node,
	// and re-syncs cluster state. Must be called AFTER initNode.
	markUnhealthy := func(n *corev1.Node, condType corev1.NodeConditionType) {
		n = ExpectExists(ctx, env.Client, n)
		n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
			Type:               condType,
			Status:             corev1.ConditionFalse,
			LastTransitionTime: metav1.Time{Time: env.Clock.Now()},
		})
		ExpectApplied(ctx, env.Client, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	}

	BeforeEach(func() {
		// Enable the NodeRepair feature gate for the repair suite.
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		// Single default policy: BadNode/False, 30m toleration (the fake cloud provider default).
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute},
		}
		nodePool = test.NodePool()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels()}})
		ExpectApplied(ctx, env.Client, nodePool)
		// Repair runs as an isolated method so tests assert repair behavior only.
		repairController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(disruption.NewRepair(env.Client, cluster, prov, cloudProvider, recorder, env.Clock, queue)))
	})

	// INV-S6 / INV-S9: repair pre-spins a replacement (replace-then-terminate) and only after the toleration elapses.
	It("should pre-spin a replacement and terminate the unhealthy node only after the replacement is healthy", func() {
		initNode(nodeClaim, node)
		bindReschedulablePod(node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(31 * time.Minute) // past toleration

		ExpectSingletonReconciled(ctx, repairController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		// Pre-spin: the command carries a replacement (replace-then-terminate), it is NOT a bare delete.
		Expect(cmds[0].Decision()).To(Equal(disruption.ReplaceDecision))
		Expect(cmds[0].Replacements).To(HaveLen(1))

		// Before the replacement is healthy, the original is NOT terminated.
		Expect(ExpectExists(ctx, env.Client, nodeClaim).DeletionTimestamp.IsZero()).To(BeTrue())

		// Once the replacement comes up healthy, the original is terminated.
		ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmds[0])
		ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
		ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim)
		ExpectNotFound(ctx, env.Client, nodeClaim)
	})

	// INV-S9: repair never fires before the policy toleration elapses.
	It("should not repair before the toleration duration elapses", func() {
		initNode(nodeClaim, node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(10 * time.Minute) // still within the 30m toleration

		ExpectSingletonReconciled(ctx, repairController)

		Expect(queue.GetCommands()).To(HaveLen(0))
	})

	// INV-S7: a replacement that boots unhealthy (bad AMI / partitioned zone) never leads to terminating the original —
	// pre-spin is the circuit breaker for the bad-component loop.
	It("should never terminate the original when the replacement fails to come up healthy", func() {
		initNode(nodeClaim, node)
		bindReschedulablePod(node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))

		// Do NOT make the replacement ready — simulate a replacement that never becomes healthy. Reconciling the queue
		// leaves the replacement uninitialized, so the candidate is never deleted.
		ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
		Expect(ExpectExists(ctx, env.Client, nodeClaim).DeletionTimestamp.IsZero()).To(BeTrue())
	})

	// INV-S8: do-not-repair blocks repair; do-not-disrupt does NOT (no behavior change for that annotation).
	It("should not repair a node carrying the do-not-repair annotation", func() {
		node.Annotations = lo.Assign(node.Annotations, map[string]string{v1.DoNotRepairAnnotationKey: "true"})
		initNode(nodeClaim, node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0))
	})

	It("should still repair a node carrying only the do-not-disrupt annotation", func() {
		node.Annotations = lo.Assign(node.Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
		initNode(nodeClaim, node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		// do-not-disrupt does not block repair: a command is still produced.
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	// INV-S1 / INV-S5: repair is paced by the disruption budget — a hard, self-clearing cap, not a fixed breaker. The
	// default budget is 10%, exposed as the reason-labeled allowed-disruptions metric; and a Repair budget of 0 blocks
	// repair entirely (the operator opt-out the old breaker lacked). Both distinguish the budget from the
	// one-command-per-pass loop.
	It("should pace repair by the disruption budget and expose the 10% default", func() {
		const count = 10
		nodeClaims, nodes := test.NodeClaimsAndNodes(count, v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels()}})
		for i := range nodes {
			initNode(nodeClaims[i], nodes[i])
			markUnhealthy(nodes[i], "BadNode")
		}
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		// Default 10% budget over 10 unhealthy nodes rounds up to 1 concurrent repair.
		Expect(queue.GetCommands()).To(HaveLen(1))
		// The budget is legible: the reason-labeled allowed-disruptions gauge reads 1 (= 10% of 10) for Repair.
		ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 1, map[string]string{
			metrics.NodePoolLabel: nodePool.Name,
			metrics.ReasonLabel:   string(v1.DisruptionReasonRepair),
		})
	})

	It("should block repair entirely when the Repair budget is zero", func() {
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Reasons: []v1.DisruptionReason{v1.DisruptionReasonRepair}, Nodes: "0"}}
		ExpectApplied(ctx, env.Client, nodePool)
		initNode(nodeClaim, node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0)) // budget 0 -> no repair (self-clearing opt-out, not a latched breaker)
	})

	// INV-S4: ordering is prioritizable — a higher-priority fault repairs before a lower-priority one in the same pass.
	It("should repair the higher-priority condition first", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "LowPriority", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Priority: 10},
			{ConditionType: "HighPriority", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Priority: 90},
		}
		lowClaim, lowNode := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels()}})
		highClaim, highNode := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels()}})
		initNode(lowClaim, lowNode)
		initNode(highClaim, highNode)
		markUnhealthy(lowNode, "LowPriority")
		markUnhealthy(highNode, "HighPriority")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		// The budget allows one; ordering must pick the high-priority node.
		Expect(cmds[0].Candidates[0].Node.Name).To(Equal(highNode.Name))
	})

	// INV-S10: repair supplies a bounded drain even when the NodePool set no TGP — a forceful (0) policy stamps a
	// termination deadline of NOW, so termination skips the drain window entirely.
	It("should stamp a now (forceful) termination deadline for a forceful policy", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, TerminationGracePeriod: lo.ToPtr(time.Duration(0))},
		}
		initNode(nodeClaim, node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1))
		// Assert the stamped VALUE, not just presence: a forceful (0) policy sets the deadline to now, so the drain is
		// skipped. (A graceful policy would stamp now+TGP; presence alone would not distinguish the two.)
		deadline, err := time.Parse(time.RFC3339, ExpectExists(ctx, env.Client, nodeClaim).Annotations[v1.NodeClaimTerminationTimestampAnnotationKey])
		Expect(err).ToNot(HaveOccurred())
		Expect(deadline).To(BeTemporally("~", env.Clock.Now(), time.Minute))
	})

	// INV-S10 (inherit): a policy with no TerminationGracePeriod leaves the NodeClaim's own TGP untouched — repair does
	// not stamp a termination deadline, so the drain follows the NodePool/NodeClaim TGP as before (no behavior change).
	It("should not stamp a termination deadline when the policy inherits the TGP", func() {
		// Default policy (BadNode) has a nil TerminationGracePeriod.
		initNode(nodeClaim, node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1))
		Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).ToNot(HaveKey(v1.NodeClaimTerminationTimestampAnnotationKey))
	})

	// INV-S4 (starvation-free / aging): a LOW-priority node that has waited long enough overtakes a freshly-eligible
	// HIGH-priority node, because age past toleration adds to the score (E = rank + age/τ). Aging guarantees even a
	// low-priority real fault is eventually served rather than starved behind higher-priority churn.
	It("should let a long-waiting low-priority node overtake a fresh high-priority node", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "LowPriority", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Priority: 10},
			{ConditionType: "HighPriority", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Priority: 90},
		}
		lowClaim, lowNode := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels()}})
		highClaim, highNode := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels()}})
		initNode(lowClaim, lowNode)
		initNode(highClaim, highNode)
		// The low-priority node has been unhealthy for a long time (accruing age past its toleration)...
		markUnhealthy(lowNode, "LowPriority")
		env.Clock.Step(10 * time.Hour)
		// ...while the high-priority node only just became eligible (near-zero age past toleration).
		markUnhealthy(highNode, "HighPriority")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		// With a rank gap of one tier (τ=30m) and 10h+ of accrued age, the low-priority node's age term dwarfs the
		// one-tier rank deficit, so it is served first — no starvation.
		Expect(cmds[0].Candidates[0].Node.Name).To(Equal(lowNode.Name))
	})

	// A node whose unhealthy condition does not match any RepairPolicy is left alone.
	It("should not repair a node whose condition does not match any policy", func() {
		initNode(nodeClaim, node)
		markUnhealthy(node, "SomeOtherCondition")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0))
	})

	// Feature-gate off: repair does nothing even for an unhealthy node past toleration.
	It("should not repair when the NodeRepair feature gate is disabled", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(false)}}))
		initNode(nodeClaim, node)
		markUnhealthy(node, "BadNode")
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0))
	})
})
