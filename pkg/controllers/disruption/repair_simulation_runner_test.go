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
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	nodehealth "sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
)

// runCell runs one cell under one option to steady state on a single stepped clock, returning observable metrics.
// The loop: decision pass → reactively provision replacements for pending pods → apply the cell's fault model to those
// fresh replacements (healthy / re-flag / launch-fail / workload-broken) → drain+terminate marked nodes → step clock.
func runCell(impl simImpl, c simCell) simMetrics {
	tc, eq := buildTerminationRig()
	cloudProvider.Reset()
	onset := env.Clock.Now()
	f := setupFleet(c)
	origClaims := claimNameSet(f.claims)

	// launch-fail: every replacement Create fails (ICE/capacity), for the whole run.
	if c.help == helpNoLaunchFail {
		cloudProvider.AllowedCreateCalls = 0
	}

	m := simMetrics{outcome: outcomeDeclined}
	acted := false
	var mitigatedAt *time.Time

	for round := 0; round < simMaxRounds; round++ {
		impl.reconcileDecision()

		// A decision removes a node by marking its NODECLAIM for deletion (that's what both the breaker and the repair
		// method do). In production the nodeclaim lifecycle controller then deletes the Node; the rig bridges that here
		// and drives the REAL node-termination drain (PDB/TGP honored) rather than a cascade shortcut.
		for _, nc := range ExpectNodeClaims(ctx, env.Client) {
			if !nc.DeletionTimestamp.IsZero() {
				acted = true
				if node := nodeForClaim(nc); node != nil {
					out := drainAndTerminate(ctx, tc, eq, node)
					m.disruptedPods += out.evictions
					if !out.terminated {
						m.outcome = outcomeWedged
					}
				}
				// Finalize the (already deleting) nodeclaim so it doesn't linger as a phantom in-flight repair.
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nc)
			}
		}

		// Reactive provisioning: any pending pods get a replacement (unless launch is failing).
		if pending := pendingPods(); len(pending) > 0 {
			ExpectProvisionedNoBinding(ctx, env.Client, cluster, cloudProvider, prov, pending...)
			applyFaultModel(c, origClaims)
		}

		// True mitigation = the originally-faulted nodes are gone and no repair-eligible unhealthy node remains
		// (a healthy replacement is carrying the workload). Only meaningful when repair can actually help.
		if mitigatedAt == nil && c.help == helpYes && faultResolved(f) {
			t := env.Clock.Now()
			mitigatedAt = &t
		}
		if episodeSettled(c, f) {
			break
		}
		env.Clock.Step(simTick)
	}

	m.cpWrites = len(cloudProvider.CreateCalls) + len(cloudProvider.DeleteCalls)
	m.outcome, m.timeToMitigation = classify(c, f, acted, mitigatedAt, onset, m.outcome)
	return m
}

// classify turns the loop's observations into an outcome + TTM, keeping wedged (from the drain loop) sticky.
func classify(c simCell, f fleet, acted bool, mitigatedAt *time.Time, onset time.Time, cur simOutcome) (simOutcome, *time.Duration) {
	if cur == outcomeWedged {
		return outcomeWedged, nil
	}
	if c.help == helpYes {
		if mitigatedAt != nil {
			d := mitigatedAt.Sub(onset)
			return outcomeMitigated, &d
		}
		return outcomeUnmitigated, nil // genuine fault never resolved (frozen breaker, or launch-fail)
	}
	// Repair could not truly help. Acting at all is waste; declining is correct.
	if acted {
		return outcomeChurned, nil
	}
	return outcomeDeclined, nil
}

// applyFaultModel realizes the ground-truth behavior on freshly-provisioned replacement NodeClaims (those not in the
// original set). It is what makes a cell a false-positive vs a true fault, in observable terms.
func applyFaultModel(c simCell, orig map[string]bool) {
	fresh := lo.Filter(ExpectNodeClaims(ctx, env.Client), func(nc *v1.NodeClaim, _ int) bool {
		return !orig[nc.Name] && nc.StatusConditions().Get(v1.ConditionTypeInitialized).IsFalse()
	})
	if len(fresh) == 0 {
		return
	}
	nodes := make([]*corev1.Node, 0, len(fresh))
	for _, nc := range fresh {
		nodes = append(nodes, test.NodeClaimLinkedNode(nc))
	}
	// Bring the replacements up as healthy nodes first (the common prefix of every non-launch-fail model).
	ExpectApplied(ctx, env.Client, lo.Map(nodes, func(n *corev1.Node, _ int) client.Object { return n })...)
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, fresh)

	switch c.help {
	case helpNoUnhealthyInDwell, helpNoUnhealthyAfterDwell:
		// The systematic false positive / bad-component re-fires on the fresh replacement.
		for _, n := range nodes {
			markFault(c, n)
		}
	case helpYes, helpNoWorkloadBroken:
		// Replacement is Ready+healthy to Karpenter. (workload-broken is the blind spot: identical observable, but the
		// oracle in classify() never counts it as resolved.)
	}
}

// --- observation helpers -----------------------------------------------------------------------------------------

func nodeForClaim(nc *v1.NodeClaim) *corev1.Node {
	for _, n := range ExpectNodes(ctx, env.Client) {
		if nc.Status.ProviderID != "" && n.Spec.ProviderID == nc.Status.ProviderID {
			return n
		}
		if nc.Status.NodeName != "" && n.Name == nc.Status.NodeName {
			return n
		}
	}
	return nil
}

func claimNameSet(claims []*v1.NodeClaim) map[string]bool {
	m := map[string]bool{}
	for _, nc := range claims {
		m[nc.Name] = true
	}
	return m
}

func pendingPods() []*corev1.Pod {
	list := &corev1.PodList{}
	_ = env.Client.List(ctx, list)
	var out []*corev1.Pod
	for i := range list.Items {
		p := &list.Items[i]
		if p.Spec.NodeName == "" && p.DeletionTimestamp.IsZero() {
			out = append(out, p)
		}
	}
	return out
}

// faultResolved: none of the originally-faulted nodes remain AND nothing currently carries the repair condition.
func faultResolved(f fleet) bool {
	for _, n := range f.nodes {
		if err := env.Client.Get(ctx, client.ObjectKeyFromObject(n), &corev1.Node{}); err == nil {
			return false // an original fault node still exists
		}
	}
	for _, n := range ExpectNodes(ctx, env.Client) {
		if cond := nodeutils.GetCondition(n, repairCond); cond.Status == corev1.ConditionFalse || cond.Status == corev1.ConditionUnknown {
			return false // something still unhealthy
		}
	}
	return true
}

// episodeSettled: nothing left to do — no unhealthy repair-eligible node and no node mid-termination.
func episodeSettled(c simCell, f fleet) bool {
	for _, n := range ExpectNodes(ctx, env.Client) {
		if !n.DeletionTimestamp.IsZero() {
			return false
		}
		if cond := nodeutils.GetCondition(n, repairCond); cond.Status == corev1.ConditionFalse || cond.Status == corev1.ConditionUnknown {
			// still unhealthy: settled only if repair can't/won't act further this run — let the round cap catch it.
			return false
		}
	}
	return true
}

var _ = Describe("Repair/Simulation", func() {
	var healthController *nodehealth.Controller
	BeforeEach(func() {
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: repairCond, ConditionStatus: corev1.ConditionFalse, TolerationDuration: simToleration},
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionUnknown, TolerationDuration: simToleration},
		}
		healthController = nodehealth.NewController(env.Client, cloudProvider, env.Clock, recorder)
	})

	// The runnable options at THIS base. Restraint (pinned/unpinned) is appended from the POC layer stacked above.
	options := func() []simImpl { return []simImpl{buildBreakerImpl(healthController)} }

	for _, cell := range enumerateCells() {
		cell := cell
		It("cell "+cell.id(), func() {
			for _, impl := range options() {
				recordResult(cell, impl.name, runCell(impl, cell))
			}
		})
	}
})
