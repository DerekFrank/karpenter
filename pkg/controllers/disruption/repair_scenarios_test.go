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

// Adversarial scenario suite for correlated-failure restraint. These are integration tests through the REAL disruption
// controller (envtest + a fake cloud provider), designed to null-test the design against weird, correlated situations —
// especially the network-partition case (a whole zone goes unhealthy while its workloads are actually fine). Assertions
// inspect the real objects (the DisruptedNoScheduleTaint / deletion marking via ExpectTaintedNodeCount), never the
// internal queue. Where the current design behaves surprisingly, the spec asserts the ACTUAL behavior and is labelled
// FINDING: — the implementation is kept faithful to the design; these tests surface its edges rather than paper over them.

var _ = Describe("Repair/Scenarios", func() {
	var nodePool *v1.NodePool
	var repairController *disruption.Controller

	// markUnhealthy stamps a repair-policy condition while keeping the node otherwise-Ready, then syncs cluster state.
	markUnhealthy := func(n *corev1.Node, condType corev1.NodeConditionType) {
		ExpectMakeNodesStatusChanged(ctx, env.Client, env.Clock, []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady"},
			{Type: condType, Status: corev1.ConditionFalse},
		}, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	}

	// initNode creates + initializes a Ready node in the given zone.
	initNode := func(zone string) (*v1.NodeClaim, *corev1.Node) {
		nc, n := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: zone},
		}})
		ExpectApplied(ctx, env.Client, nc, n)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{n}, []*v1.NodeClaim{nc})
		return nc, n
	}

	// addReschedulablePod binds a ReplicaSet-owned pod so repair pre-spins a replacement (replace-then-terminate). If
	// pinnedZone is non-empty the pod is hard-pinned to that zone — modelling a workload (e.g. zonal storage) that cannot
	// be evicted/rescheduled out of the impaired zone.
	addReschedulablePod := func(n *corev1.Node, pinnedZone string) {
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
		opts := test.PodOptions{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
		}}}}
		if pinnedZone != "" {
			opts.NodeRequirements = []corev1.NodeSelectorRequirement{{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{pinnedZone}}}
		}
		pod := test.Pod(opts)
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, n)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
	}

	// markOfferingsUnavailable ICEs every offering in a zone (as ARC / Zonal Shift would): pre-spin can no longer launch
	// a replacement there, so repair must defer rather than add load to the impaired zone.
	markOfferingsUnavailable := func(zone string) {
		for _, it := range cloudProvider.InstanceTypes {
			for i := range it.Offerings {
				if it.Offerings[i].Requirements.Get(corev1.LabelTopologyZone).Has(zone) {
					it.Offerings[i].Available = false
				}
			}
		}
	}

	reconcileN := func(n int) {
		for i := 0; i < n; i++ {
			ExpectSingletonReconciled(ctx, repairController)
		}
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "KubeletNotReady", ConditionStatus: corev1.ConditionFalse, TolerationDuration: time.Minute},
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: time.Minute},
		}
		// Budget 100% so restraint (not the budget) is what paces; every taint is a repair the design chose to make.
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: "100%"}}}}})
		ExpectApplied(ctx, env.Client, nodePool)
		repairController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(disruption.NewRepair(env.Client, cluster, prov, cloudProvider, recorder, env.Clock, queue)))
	})

	// NETWORK PARTITION — the motivating adversarial case: a whole zone's kubelets stop heart-beating (flagged unhealthy)
	// but the pods are actually fine and cannot be evicted/rescheduled out of the zone. With the zone's capacity gone
	// (ARC / Zonal Shift), pre-spin cannot stage a replacement in-zone and the pinned pods cannot move, so repair is
	// blocked and defers — it does NOT non-gracefully disrupt the healthy-but-unreachable workloads. This is the
	// pre-spin-as-circuit-breaker property doing the right thing (invariant 8).
	It("network partition: defers when the impaired zone's capacity is gone and workloads are zone-bound", func() {
		_, n := initNode("test-zone-1")
		addReschedulablePod(n, "test-zone-1") // workload pinned to the impaired zone — cannot be moved out
		markUnhealthy(n, "KubeletNotReady")
		markOfferingsUnavailable("test-zone-1") // the zone is gone: no replacement can launch here
		env.Clock.Step(2 * time.Minute)         // past toleration

		reconcileN(5)

		// Repair cannot pre-spin a replacement (pod is zone-bound, zone has no capacity) so it defers: the healthy-but-
		// unreachable node is never cordoned or marked for deletion, and no command was ever queued.
		ExpectTaintedNodeCount(ctx, env.Client, 0)
		Expect(ExpectExists(ctx, env.Client, n).DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(queue.GetCommands()).To(BeEmpty())
	})

	// Positive control for the deferral above: without the zone-bound pod + offering unavailability, repair DOES act on
	// the same underlying condition. This proves the deferral test's PASS is due to pre-spin blocking, not silent skip.
	It("network partition: [positive control] repair acts on the same condition when scheduling is unblocked", func() {
		_, n := initNode("test-zone-1")
		addReschedulablePod(n, "") // zone-flexible workload; offerings all available
		markUnhealthy(n, "KubeletNotReady")
		env.Clock.Step(2 * time.Minute)

		reconcileN(2)

		ExpectTaintedNodeCount(ctx, env.Client, 1)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	// Network partition with zone-FLEXIBLE workloads: repair CAN pre-spin (the pod relocates to a healthy zone), so it
	// evacuates the unreachable zone. The key property (learn-from-replacement, #7): a replacement that comes up in a
	// HEALTHY zone does not credit the candidate's IMPAIRED zone, so the impaired zone's width never widens — repair drains
	// the unreachable zone ONE node at a time, the intended paced behavior for moving workloads off unreachable nodes,
	// rather than the runaway acceleration the pre-#7 design would have had. This asserts that "one at a time" holds.
	It("network partition: evacuates zone-flexible workloads one at a time (the impaired zone never widens)", func() {
		nodes := []*corev1.Node{}
		for i := 0; i < 3; i++ {
			_, n := initNode("test-zone-1")
			addReschedulablePod(n, "") // zone-flexible: can be rescheduled to a healthy zone
			markUnhealthy(n, "KubeletNotReady")
			nodes = append(nodes, n)
		}
		markOfferingsUnavailable("test-zone-1") // impaired zone gone, but 1b/1c are healthy
		env.Clock.Step(2 * time.Minute)

		reconcileN(3)

		// Adversarial result: repair acts on the partitioned-but-healthy nodes (relocating their workloads to a healthy
		// zone), so at least one is disrupted — and, because of learn-from-replacement, no more than one concurrently
		// (the healthy-zone success never widens the impaired zone).
		tainted := ExpectTaintedNodeCount(ctx, env.Client, 1)
		Expect(tainted).To(HaveLen(1))

		// Verify a replacement is actually in flight (this test isn't passing because repair silently gave up).
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	// SMALL POOL — a lone genuine fault must be repaired promptly regardless of pool size (the width floor is 1 node, not
	// a percentage that rounds to zero). One bad node among healthy ones is cordoned for repair.
	It("small pool: repairs a lone genuine fault promptly (width floor is one node, not a percentage)", func() {
		_, bad := initNode("test-zone-1")
		addReschedulablePod(bad, "")
		for i := 0; i < 3; i++ {
			initNode("test-zone-1") // healthy siblings
		}
		markUnhealthy(bad, "BadNode")
		env.Clock.Step(2 * time.Minute)

		reconcileN(2)

		tainted := ExpectTaintedNodeCount(ctx, env.Client, 1)
		Expect(tainted[0].Name).To(Equal(bad.Name))
	})

	// NETWORK PARTITION (registrationTTL): EC2 gives capacity, but a replacement launched into the partitioned zone never
	// joins the cluster (its kubelet can't reach the API server), so registration times out. Because repair pre-spins and
	// only terminates the original once the replacement initializes, the healthy-but-unreachable original is NEVER
	// terminated — the replace-then-terminate circuit breaker holds even when the replacement launches successfully but
	// can't register.
	It("network partition: preserves the original when the replacement launches but never registers (registrationTTL)", func() {
		_, n := initNode("test-zone-1")
		addReschedulablePod(n, "test-zone-1") // pinned to the impaired zone; EC2 has capacity, but a new node can't join
		markUnhealthy(n, "KubeletNotReady")
		env.Clock.Step(2 * time.Minute) // past toleration (offerings stay AVAILABLE — the failure is registration, not ICE)

		// Repair cordons the original and stages a replacement into the impaired zone, then waits for it to initialize.
		ExpectSingletonReconciled(ctx, repairController)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1)) // a replacement was launched into the impaired zone

		// The replacement never registers → registrationTTL / command timeout, WITHOUT terminating the original.
		env.Clock.Step(2 * time.Hour)
		ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
		for _, nc := range ExpectNodeClaims(ctx, env.Client) { // reap the stranded, never-registered replacement (NodeClaim GC in prod)
			if nc.Status.ProviderID == "" {
				ExpectDeleted(ctx, env.Client, nc)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
			}
		}

		// The original healthy-but-unreachable node was never terminated (pre-spin circuit breaker) and is un-cordoned.
		Expect(ExpectExists(ctx, env.Client, n).DeletionTimestamp.IsZero()).To(BeTrue())
		ExpectTaintedNodeCount(ctx, env.Client, 0)
	})
})
