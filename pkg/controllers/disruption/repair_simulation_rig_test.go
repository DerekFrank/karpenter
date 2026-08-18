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

// The simulation runner + fault models + implementation options.
//
// Each cell runs an ITERATED loop (decision → advance fault model on any fresh replacements → drain+terminate marked
// nodes → step clock) to steady state, on a single stepped clock — so the differentiating behavior (multi-pass
// pre-spin/dwell/terminate, re-flag loops, burst pacing) and decision latency are actually exercised, not just the
// shared teardown. The teardown drives the REAL termination state machine so PDB/TGP/forceful drain execute and are
// measurable.
//
// At this base (main), the only runnable option is the 20% breaker (node.health exists here; NewRepair does not). The
// budget+restraint options (pinned/unpinned) plug into `simImpl` from the voluntary-repair POC layer stacked above.

import (
	"context"
	"time"

	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	nodehealth "sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
)

const (
	simTick       = time.Minute      // clock advance per decision round
	simMaxRounds  = 40               // bound on decision rounds (episode never converging shows as wedged/unmitigated)
	simMaxDrain   = 40               // bound on the per-node drain loop
	simBurst      = 5                // nodes in a correlated burst
	simFiller     = 10               // healthy filler nodes per pool (so the unhealthy fraction is realistic)
	simToleration = 30 * time.Minute // RepairPolicy toleration used across the matrix
	repairCond    = corev1.NodeConditionType("BadNode")
)

// simImpl is one swappable implementation OPTION. reconcileDecision runs ONE decision pass; the runner iterates it.
type simImpl struct {
	name              string
	reconcileDecision func()
}

// fleet is the objects a cell's scenario created, retained so the runner can drive + observe them.
type fleet struct {
	nodePools []*v1.NodePool
	nodes     []*corev1.Node    // the originally-faulted nodes
	claims    []*v1.NodeClaim
	podLabels map[string]string // workload selector (for PDB matching), or nil
}

// --- drain measurement -------------------------------------------------------------------------------------------

// drainOutcome is what a single node teardown produced.
type drainOutcome struct {
	terminated bool
	evictions  int
}

// drainAndTerminate drives a node marked for deletion through the REAL termination state machine — cordon, drain
// (evicting pods through the eviction API, which honors PodDisruptionBudgets), forceful deletion once the node's
// termination-grace-period deadline passes, then finalize — reconciling the termination controller + eviction queue
// and stepping the clock until the node is gone or simMaxDrain is exhausted. It is a MEASUREMENT instrument (returns
// data), deliberately not named Expect*/Eventually* and not an assert-or-fail helper: a node that never terminates
// (e.g. a fully-blocking PDB) is a result we want to record (terminated=false), not a spec failure.
func drainAndTerminate(ctx context.Context, tc *termination.Controller, eq *terminator.Queue, node *corev1.Node) drainOutcome {
	out := drainOutcome{}
	// The node-termination controller only drains a node that has the termination finalizer; without it a Delete would
	// vanish the node instantly (no drain). Ensure it before deleting so the drain path actually runs.
	if !lo.Contains(node.Finalizers, v1.TerminationFinalizer) {
		stored := node.DeepCopy()
		node.Finalizers = append(node.Finalizers, v1.TerminationFinalizer)
		_ = env.Client.Patch(ctx, node, client.MergeFrom(stored))
	}
	// Count the workload pods on the node at teardown start — the observable disruption, whether they end up gracefully
	// evicted or force-deleted past the TGP (the eviction-queue counter alone misses forceful deletions).
	if pods, err := nodeutils.GetPods(ctx, env.Client, node.Name); err == nil {
		out.evictions += len(pods)
	}
	if node.DeletionTimestamp.IsZero() {
		if err := client.IgnoreNotFound(env.Client.Delete(ctx, node)); err != nil {
			return out
		}
	}
	for i := 0; i < simMaxDrain; i++ {
		fresh := &corev1.Node{}
		if err := env.Client.Get(ctx, client.ObjectKeyFromObject(node), fresh); err != nil {
			out.terminated = true
			return out
		}
		// Direct Reconcile (not ExpectObjectReconciled): this is a measurement instrument, so a transient reconcile
		// error must not fail the spec — a node that can't finish terminating is a RESULT (recorded as not-terminated).
		_, _ = tc.Reconcile(ctx, fresh)
		if pods, err := nodeutils.GetPods(ctx, env.Client, fresh.Name); err == nil {
			for _, pod := range pods {
				if eq.Has(pod) {
					_, _ = eq.Reconcile(ctx, pod)
				}
			}
		}
		env.Clock.Step(2 * termination.MinDrainTime)
	}
	if err := env.Client.Get(ctx, client.ObjectKeyFromObject(node), &corev1.Node{}); err != nil {
		out.terminated = true
	}
	return out
}

// buildTerminationRig constructs the shared drain/termination machinery the runner uses to tear down every candidate.
func buildTerminationRig() (*termination.Controller, *terminator.Queue) {
	eq := terminator.NewQueue(env.Client, recorder)
	tc := termination.NewController(env.Clock, env.Client, cloudProvider, terminator.NewTerminator(env.Clock, env.Client, eq, recorder), recorder)
	return tc, eq
}

// --- breaker option (runnable at this base; node.health exists on main) ------------------------------------------

func buildBreakerImpl(hc *nodehealth.Controller) simImpl {
	return simImpl{
		name: "breaker-20pct",
		reconcileDecision: func() {
			// The health controller is Node-keyed; feed it every node still carrying the repair condition.
			for _, n := range ExpectNodes(ctx, env.Client) {
				if cond := nodeutils.GetCondition(n, repairCond); cond.Status == corev1.ConditionFalse || cond.Status == corev1.ConditionUnknown {
	_, _ = hc.Reconcile(ctx, n)
				}
			}
		},
	}
}

// --- fleet setup -------------------------------------------------------------------------------------------------

// zonesFor / amisFor spread a burst across the domains a correlation shares (or not).
func poolCountFor(c simCell) int {
	switch c.topo {
	case topoPerAZ, topoPerAMI:
		return 3
	default:
		return 1
	}
}

// setupFleet realizes a cell as cluster objects and returns them. It marks the fault (past toleration) so the decision
// loop can act immediately.
func setupFleet(c simCell) fleet {
	f := fleet{}
	nPools := poolCountFor(c)
	for i := 0; i < nPools; i++ {
		var np *v1.NodePool
		if c.strat == stratTerminateFirst {
			np = test.StaticNodePool(v1.NodePool{Spec: v1.NodePoolSpec{Replicas: lo.ToPtr(int64(simBurst))}})
		} else {
			np = test.NodePool()
		}
		ExpectApplied(ctx, env.Client, np)
		f.nodePools = append(f.nodePools, np)
	}

	// Healthy filler so the unhealthy FRACTION is realistic — otherwise every pool is 100% unhealthy and the 20% breaker
	// always freezes, collapsing the whole matrix. With filler, an isolated fault is a small fraction (breaker acts) and
	// a correlated burst pushes the pool over the threshold (breaker freezes) — the behavior we want to measure.
	for _, pool := range f.nodePools {
		for i := 0; i < simFiller; i++ {
			nc, n := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey: pool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a",
			}}})
			ExpectApplied(ctx, env.Client, nc, n)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{n}, []*v1.NodeClaim{nc})
		}
	}

	count := 1
	if !c.corr.isolated() {
		count = simBurst
	}
	// A workload label so PDB cases can select the pods.
	if c.drain.hasPDB() {
		f.podLabels = map[string]string{"sim-workload": "app"}
	}
	for i := 0; i < count; i++ {
		pool := f.nodePools[i%len(f.nodePools)]
		zone := "test-zone-1a"
		if c.corr == corrZone || c.topo == topoPerAZ {
			zone = []string{"test-zone-1a", "test-zone-1b", "test-zone-1c"}[i%3]
			if c.topo == topoPerAZ {
				zone = []string{"test-zone-1a", "test-zone-1b", "test-zone-1c"}[(i % len(f.nodePools))]
			}
		}
		nc, n := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
			Finalizers: []string{v1.TerminationFinalizer}, // so a decision's Delete lingers (deletion timestamp) and flows through drain, as in prod
			Labels: map[string]string{
				v1.NodePoolLabelKey: pool.Name, v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: zone,
			}}})
		ExpectApplied(ctx, env.Client, nc, n)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{n}, []*v1.NodeClaim{nc})
		bindWorkload(n, f.podLabels)
		f.nodes = append(f.nodes, n)
		f.claims = append(f.claims, nc)
	}
	if c.drain.hasPDB() {
		mu := intstr.FromString("100%")
		if c.drain == drainNoPDBBlock {
			mu = intstr.FromInt(0)
		}
		ExpectApplied(ctx, env.Client, test.PodDisruptionBudget(test.PDBOptions{Labels: f.podLabels, MaxUnavailable: &mu}))
	}
	// Inject the fault, then advance past toleration so it is immediately actionable.
	for _, n := range f.nodes {
		markFault(c, n)
	}
	env.Clock.Step(simToleration + time.Minute)
	return f
}

// bindWorkload attaches a ReplicaSet-owned pod (so termination has something to drain and reactive provisioning has a
// pod to replace). labels, if set, are stamped for PDB matching.
func bindWorkload(n *corev1.Node, labels map[string]string) {
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, rs)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
	pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{
		Labels: labels,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true)}},
	}})
	ExpectApplied(ctx, env.Client, pod)
	ExpectManualBinding(ctx, env.Client, pod, n)
	ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
}

// markFault stamps the repair-matching condition. kubelet-dead is represented as Ready=Unknown (drain can't proceed
// gracefully); otherwise the node stays Ready with the BadNode condition False.
func markFault(c simCell, n *corev1.Node) {
	n = ExpectExists(ctx, env.Client, n)
	ready := corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady",
		LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())}
	if c.drain == drainNoKubeletDead {
		// kubelet-dead: Ready=Unknown (node controller lost the heartbeat) → graceful drain can't proceed.
		ready = corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionUnknown, Reason: "NodeStatusUnknown",
			LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())}
	}
	n.Status.Phase = corev1.NodeRunning
	n.Status.Conditions = []corev1.NodeCondition{
		ready,
		{Type: repairCond, Status: corev1.ConditionFalse, Reason: "SimFault",
			LastHeartbeatTime: metav1.NewTime(env.Clock.Now()), LastTransitionTime: metav1.NewTime(env.Clock.Now())},
	}
	ExpectApplied(ctx, env.Client, n)
	ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
}
