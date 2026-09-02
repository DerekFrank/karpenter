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
	"fmt"
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

// These tests exercise reason-level policy & matching (F1). A node condition carries an onset-bearing reason list
// ("token@epoch;token@epoch"); policies key on (condition, ReasonMatcher) with per-reason toleration measured from
// each reason's own onset, and multiple eligible reasons merge by an idempotent lattice join.

var _ = Describe("Repair/ReasonPolicy", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	var repairController *disruption.Controller

	// markUnhealthyReason stamps the condition with an onset-bearing reason string at the current fake-clock time.
	markUnhealthyReason := func(n *corev1.Node, reason string) {
		n = ExpectExists(ctx, env.Client, n)
		n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
			Type:               "BadNode",
			Status:             corev1.ConditionFalse,
			Reason:             reason,
			LastTransitionTime: metav1.Time{Time: env.Clock.Now()},
		})
		ExpectApplied(ctx, env.Client, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	}

	initNode := func() {
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		nodePool = test.NodePool()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"},
		}})
		ExpectApplied(ctx, env.Client, nodePool)
		repairController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(disruption.NewRepair(env.Client, cluster, prov, cloudProvider, recorder, env.Clock, queue)))
	})

	// INV-F1-1: a ReasonMatcher scopes a policy to a reason. A node whose reason doesn't match is not repaired; one
	// whose reason matches is.
	It("should only repair reasons that match the policy's ReasonMatcher", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, ReasonMatcher: "NvidiaDoubleBitError"},
		}
		initNode()
		markUnhealthyReason(node, "SomeUnrelatedReason")
		env.Clock.Step(31 * time.Minute)
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0)) // reason doesn't match → not repaired
	})

	It("should repair a reason that matches the policy's ReasonMatcher", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, ReasonMatcher: "NvidiaDoubleBitError"},
		}
		initNode()
		markUnhealthyReason(node, "NvidiaDoubleBitError")
		env.Clock.Step(31 * time.Minute)
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1)) // reason matches → repaired
	})

	// INV-F1-1 (superset): an empty ReasonMatcher (or the pre-F1 default) matches any reason — condition-only behavior.
	It("should match any reason when the policy has no ReasonMatcher", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute},
		}
		initNode()
		markUnhealthyReason(node, "AnyReasonAtAll")
		env.Clock.Step(31 * time.Minute)
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	// INV-F1-2: eligibility is measured from each matched reason's OWN onset carried in the reason field (token@epoch),
	// NOT the condition's single transition time. This test is discriminating BY CONSTRUCTION: the apiserver forces the
	// condition's LastTransitionTime to server-now on the status write, but we set the matched reason's onset to 31m in
	// the PAST and take NO clock step. So:
	//   - a correct reason-onset implementation reads onset = now-31m → past the 30m toleration → repairs immediately.
	//   - an implementation that (wrongly) measured toleration from cond.LastTransitionTime (= now) would NOT be
	//     eligible and would produce zero commands.
	// The assertion below only holds for the reason-onset implementation, so it fails if the onset parsing is removed.
	It("should measure toleration from the reason's own onset, not the condition transition time", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, ReasonMatcher: "NvidiaFabricError"},
		}
		initNode()
		pastOnset := env.Clock.Now().Add(-31 * time.Minute).Unix() // matched reason has been firing for 31m already
		markUnhealthyReason(node, fmt.Sprintf("NvidiaFabricError@%d", pastOnset))
		// No clock step: the condition's transition time is now, but the reason's onset is 31m ago.
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1)) // eligible via the reason's own (past) onset, despite cond time = now
	})

	// INV-F1-2 (complement): a matched reason whose OWN onset is recent is NOT yet eligible, even long after the
	// condition first transitioned — the reason's onset, not the condition's, gates eligibility.
	It("should wait when the matched reason's own onset is recent", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, ReasonMatcher: "NvidiaFabricError"},
		}
		initNode()
		// The condition has been firing for an unrelated reason for 2h; NvidiaFabricError only appeared 5m ago.
		oldOnset := env.Clock.Now().Add(-2 * time.Hour).Unix()
		recentOnset := env.Clock.Now().Add(-5 * time.Minute).Unix()
		markUnhealthyReason(node, fmt.Sprintf("SomethingOld@%d;NvidiaFabricError@%d", oldOnset, recentOnset))
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0)) // waits — matched reason's own 5m age < 30m toleration
	})

	// INV-F1-3: the merge is eligible-only + idempotent lattice join. TerminationGracePeriod merges by MIN across
	// eligible reasons (drain-ability is a conjunction), so a forceful (0) reason forces a forceful drain even when it
	// co-occurs with a graceful reason.
	It("should merge co-occurring eligible reasons by min TGP (forceful wins)", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, ReasonMatcher: "GracefulReason", TerminationGracePeriod: lo.ToPtr(time.Hour)},
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, ReasonMatcher: "WedgedKernel", TerminationGracePeriod: lo.ToPtr(time.Duration(0))},
		}
		initNode()
		onset := env.Clock.Now().Unix()
		markUnhealthyReason(node, fmt.Sprintf("GracefulReason@%d;WedgedKernel@%d", onset, onset))
		env.Clock.Step(31 * time.Minute)
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1))
		// The stamped deadline is now (forceful), because min(1h, 0) = 0.
		updated := ExpectExists(ctx, env.Client, nodeClaim)
		deadline, err := time.Parse(time.RFC3339, updated.Annotations[v1.NodeClaimTerminationTimestampAnnotationKey])
		Expect(err).ToNot(HaveOccurred())
		Expect(deadline).To(BeTemporally("~", env.Clock.Now(), time.Minute))
	})

	// INV-F1-3 (eligible-only): a co-occurring reason that has NOT cleared its own toleration does not contribute to
	// the merge. A not-yet-eligible forceful reason must not force a forceful drain early.
	It("should exclude a not-yet-eligible reason from the merge", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, ReasonMatcher: "GracefulReason", TerminationGracePeriod: lo.ToPtr(time.Hour)},
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 90 * time.Minute, ReasonMatcher: "SlowForcefulReason", TerminationGracePeriod: lo.ToPtr(time.Duration(0))},
		}
		initNode()
		onset := env.Clock.Now().Unix()
		markUnhealthyReason(node, fmt.Sprintf("GracefulReason@%d;SlowForcefulReason@%d", onset, onset))
		env.Clock.Step(31 * time.Minute) // GracefulReason eligible (>30m); SlowForcefulReason not (needs 90m)
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1))
		// Only GracefulReason is eligible, so the merged TGP is 1h (graceful) — NOT forceful.
		updated := ExpectExists(ctx, env.Client, nodeClaim)
		deadline, err := time.Parse(time.RFC3339, updated.Annotations[v1.NodeClaimTerminationTimestampAnnotationKey])
		Expect(err).ToNot(HaveOccurred())
		Expect(deadline).To(BeTemporally("~", env.Clock.Now().Add(time.Hour), 2*time.Minute))
	})

	// Cross-condition merge: a node unhealthy on TWO different conditions must be considered for BOTH, so a
	// false-positive flood on one condition can't mask a genuine fault on another. Here a graceful flood condition
	// (AcceleratedHardwareReady, 1h drain) co-occurs with a forceful real-fault condition (KernelReady, 0 = forceful);
	// the merged drain bound is the MIN across conditions → forceful. If repair only considered the nearest-deadline
	// condition, the real fault's forceful bound could be dropped.
	It("should merge across multiple unhealthy conditions, not just one", func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "AcceleratedHardwareReady", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, TerminationGracePeriod: lo.ToPtr(time.Hour)},
			{ConditionType: "KernelReady", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, TerminationGracePeriod: lo.ToPtr(time.Duration(0))},
		}
		initNode()
		n := ExpectExists(ctx, env.Client, node)
		n.Status.Conditions = append(n.Status.Conditions,
			corev1.NodeCondition{Type: "AcceleratedHardwareReady", Status: corev1.ConditionFalse, LastTransitionTime: metav1.Time{Time: env.Clock.Now()}},
			corev1.NodeCondition{Type: "KernelReady", Status: corev1.ConditionFalse, LastTransitionTime: metav1.Time{Time: env.Clock.Now()}},
		)
		ExpectApplied(ctx, env.Client, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(1))
		// Merged min TGP across both conditions is 0 → forceful (deadline ≈ now). The graceful condition alone would be 1h.
		updated := ExpectExists(ctx, env.Client, nodeClaim)
		deadline, err := time.Parse(time.RFC3339, updated.Annotations[v1.NodeClaimTerminationTimestampAnnotationKey])
		Expect(err).ToNot(HaveOccurred())
		Expect(deadline).To(BeTemporally("~", env.Clock.Now(), time.Minute))
	})
})
