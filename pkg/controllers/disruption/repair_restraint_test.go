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
// probe (width 1) and widens only as probes prove out. The budget is 100% throughout, so ANY limiting below the
// eligible count is restraint, not the budget. Because the disruption loop issues one command per reconcile, these
// tests reconcile ACROSS MULTIPLE PASSES and let probes accumulate — the number of *concurrent* in-flight probes is
// the observable that distinguishes restraint from the one-command-per-pass structure.

var _ = Describe("Repair/Restraint", func() {
	var nodePool *v1.NodePool
	var repairController *disruption.Controller

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

	// bindPod attaches a reschedulable (ReplicaSet-owned) pod so the node's repair command replaces-then-terminates —
	// giving a probe a distinct success (make the replacement ready) vs. failure (never ready → queue times out, the
	// original survives) outcome. An empty node's delete-only command instead terminates the original (a success).
	bindPod := func(n *corev1.Node) {
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

	// newUnhealthyNode creates + initializes an unhealthy (empty) node in the given zone with a matching condition.
	newUnhealthyNode := func(zone string, condType corev1.NodeConditionType) (*v1.NodeClaim, *corev1.Node) {
		nc, n := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: zone},
		}})
		ExpectApplied(ctx, env.Client, nc, n)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{n}, []*v1.NodeClaim{nc})
		markUnhealthy(n, condType)
		return nc, n
	}

	// burst creates n unhealthy nodes sharing NodePool + zone + policy (one collinear failure domain), stepping past
	// toleration once at the end.
	burst := func(n int, zone string, condType corev1.NodeConditionType) {
		for i := 0; i < n; i++ {
			newUnhealthyNode(zone, condType)
		}
		env.Clock.Step(31 * time.Minute)
	}

	// reconcileN runs the repair loop n times (each issues at most one command) so probes can accumulate in the queue.
	reconcileN := func(n int) {
		for i := 0; i < n; i++ {
			ExpectSingletonReconciled(ctx, repairController)
		}
	}

	// failInFlightProbe drives the single in-flight probe to a genuine FAILURE: never make its replacement ready, step
	// past the queue's retry timeout, and reconcile the queue so the command times out and clears WITHOUT terminating
	// the original. The timed-out command leaves an un-launched replacement NodeClaim (empty providerID); in production
	// the NodeClaim GC / #3190 heal reaps it, but this focused suite doesn't run that controller, so we reap it here
	// (delete it + drop it from cluster state) — otherwise cluster.Synced() stays false and the loop early-returns.
	failInFlightProbe := func() {
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		env.Clock.Step(2 * time.Hour)
		ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
		Expect(queue.IsEmpty()).To(BeTrue())
		// Reap the stranded, un-launched replacement so cluster state re-syncs (simulating the NodeClaim GC / #3190).
		for _, nc := range ExpectNodeClaims(ctx, env.Client) {
			if nc.Status.ProviderID == "" {
				ExpectDeleted(ctx, env.Client, nc)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
			}
		}
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

	// INV-F3-1 / INV-F3-6: a correlated burst holds at ONE concurrent probe across passes even though the budget allows
	// all 10. Reconciling 5 times would let 5 concurrent probes through if restraint were absent (budget 100%);
	// restraint's width-1 floor keeps it at one. Cold-start at the floor is also the stateless/no-memory-safe case.
	It("should hold a correlated burst at a single concurrent probe across passes", func() {
		burst(10, "test-zone-1a", "BadNode")
		reconcileN(5)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	// INV-F3-3 / INV-F3-4: width doubles ONLY after a probe is PROVEN (its replacement holds healthy for the dwell).
	// Before proving: one concurrent probe. After one proves out: TWO concurrent probes (width 1 -> 2). The widen does
	// not happen until the dwell elapses (delayed success, F3-4).
	It("should widen to two concurrent probes only after a probe proves healthy for the dwell", func() {
		burst(10, "test-zone-1a", "BadNode")

		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))

		// Prove it: replacement healthy, original terminated and gone.
		ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmds[0])
		ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
		ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmds[0].Candidates[0].NodeClaim)

		// Observe once: terminated but the dwell has NOT elapsed → not yet proven → width stays 1, no new probe.
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0))

		// Advance past the dwell and observe: proven → width doubles → two concurrent probes admitted.
		env.Clock.Step(6 * time.Minute)
		reconcileN(3)
		Expect(queue.GetCommands()).To(HaveLen(2))
	})

	// INV-F3-2: repair pauses on failure but never halts. A failed probe pins width at the floor and arms a cooldown, so
	// a still-unhealthy node is not re-probed immediately; once the capped cooldown elapses it is admitted again — the
	// floor is 1 (never 0), so repair always eventually retries.
	It("should pause after a failed probe, then re-admit once the cooldown elapses", func() {
		_, n := newUnhealthyNode("test-zone-1a", "BadNode")
		bindPod(n)
		env.Clock.Step(31 * time.Minute)

		ExpectSingletonReconciled(ctx, repairController)
		failInFlightProbe()

		// Observe the failure: the domain is in cooldown, so the still-unhealthy node is not re-probed — paused.
		ExpectSingletonReconciled(ctx, repairController)
		Expect(queue.GetCommands()).To(HaveLen(0))

		// After the capped cooldown elapses, the node is admitted again — paused, not halted.
		env.Clock.Step(11 * time.Minute)
		reconcileN(2)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	// INV-F3-5 (past success doesn't mask a new correlated failure — the idle-domain width reset) and the multi-domain
	// combine rules (min-width across domains; eligible if ANY domain is out of cooldown) are verified deterministically
	// in the white-box Ginkgo specs in repair_restraint_whitebox_test.go. Those dial mechanics are pure functions, and
	// exercising them end-to-end here would require driving a full fault episode to completion and back — which in this
	// focused harness collides with the fact that the per-condition toleration (30m) exceeds the cooldown cap (10m), so
	// any clock step taken to make a fresh fault eligible also expires the cooldown under test.
})
