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

// Rig for the node-repair simulation matrix: the end-to-end machinery each cell runs through, and the swappable
// implementation OPTIONS. The rig deliberately steps a candidate all the way through drain + termination (not the
// ExpectNodeClaimsCascadeDeletion shortcut the focused unit suites use) so PodDisruptionBudgets, the termination grace
// period, and forceful-vs-graceful drain actually execute and are measurable — which is what makes Axis 3 real.

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodehealth "sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	. "sigs.k8s.io/karpenter/pkg/test/expectations" //nolint:staticcheck
)

// maxDrainSteps bounds the drain/termination loop so a wedged (PDB-blocked, never-forceful) node can't spin forever;
// hitting it is itself observable (the node never terminated).
const maxDrainSteps = 40

// EventuallyExpectNodeDrainedAndTerminated drives a node marked for deletion all the way through the real termination
// state machine — cordon, drain (evicting pods through the eviction API, which honors PodDisruptionBudgets), forceful
// deletion once the node's termination-grace-period deadline passes, then finalize — until the node object is gone or
// maxDrainSteps is exhausted. It reconciles the termination controller and the eviction queue together, stepping the
// clock each round, exactly as a running cluster would. Returns whether the node actually terminated and how many
// pod evictions were issued (the PDB-paced drain cost).
func EventuallyExpectNodeDrainedAndTerminated(
	ctx context.Context,
	c client.Client,
	terminationController *termination.Controller,
	evictionQueue *terminator.Queue,
	stepClock func(time.Duration),
	node *corev1.Node,
) (terminated bool, evictions int) {
	GinkgoHelper()
	// Marking the node for deletion is what the disruption queue (or the breaker) does when it decides to remove the
	// candidate; the termination controller only acts on a node with a deletion timestamp + the termination finalizer.
	if node.DeletionTimestamp.IsZero() {
		if err := client.IgnoreNotFound(c.Delete(ctx, node)); err != nil {
			return false, evictions
		}
	}
	for i := 0; i < maxDrainSteps; i++ {
		fresh := &corev1.Node{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(node), fresh); err != nil {
			return true, evictions // node is gone → terminated
		}
		// One termination pass: taints + starts/continues the drain (enqueues evictable pods, or force-deletes past TGP).
		ExpectObjectReconciled(ctx, c, terminationController, fresh)
		// Process any pods the drain enqueued for eviction — this is where the PDB is honored (a blocking PDB fails the
		// eviction and the pod stays, stalling the drain until the TGP deadline forces it).
		if pods, err := nodeutils.GetPods(ctx, c, fresh.Name); err == nil {
			for _, pod := range pods {
				if evictionQueue.Has(pod) {
					ExpectObjectReconciled(ctx, c, evictionQueue, pod)
					evictions++
				}
			}
		}
		stepClock(2 * termination.MinDrainTime)
	}
	// Exhausted the budget without the node disappearing → not terminated (e.g. a fully-blocking PDB with no TGP).
	if err := c.Get(ctx, client.ObjectKeyFromObject(node), &corev1.Node{}); err != nil {
		return true, evictions
	}
	return false, evictions
}

// simImpl is one swappable implementation OPTION under test. The cases are identical across options; only impl varies.
//   - name: report column.
//   - reconcileDecision: run ONE "should I repair, and how" pass (a disruption-controller reconcile for the repair
//     method options; a health-controller reconcile over the unhealthy nodes for the breaker).
// Teardown (drain + terminate) is shared across options via EventuallyExpectNodeDrainedAndTerminated.
type simImpl struct {
	name              string
	reconcileDecision func()
}

// buildBreakerImpl restores the pre-repair behavior: the node.health controller with its hardcoded 20% breaker. It is
// a Node-keyed reconciler that force-deletes unhealthy NodeClaims directly (no pre-spin, no budget), gated only by the
// breaker. Comparison-only — not registered in production.
func buildBreakerImpl(healthController *nodehealth.Controller, unhealthyNodes func() []*corev1.Node) simImpl {
	return simImpl{
		name: "breaker-20pct",
		reconcileDecision: func() {
			for _, n := range unhealthyNodes() {
				ExpectObjectReconciled(ctx, env.Client, healthController, n)
			}
		},
	}
}

// NOTE: the restraint-based options (budget+restraint, pinned/unpinned) plug into this seam from the voluntary-repair
// POC layer that stacks ON TOP of this rig — they reference disruption.NewRepair, which does not exist at this base.
// The base rig deliberately carries only the implementation-agnostic cases + the breaker baseline (node.health exists
// here). See buildBreakerImpl above; each implementation branch adds its own simImpl builder.

// buildTerminationRig constructs the shared drain/termination machinery (eviction queue + termination controller) the
// runner uses to tear down every candidate, for every option.
func buildTerminationRig() (*termination.Controller, *terminator.Queue) {
	evictionQueue := terminator.NewQueue(env.Client, recorder)
	tc := termination.NewController(env.Clock, env.Client, cloudProvider, terminator.NewTerminator(env.Clock, env.Client, evictionQueue, recorder), recorder)
	return tc, evictionQueue
}

// unhealthyNodesForBreaker lists nodes carrying a RepairPolicy-matching unhealthy condition (the breaker's input set).
func unhealthyNodesForBreaker(policies func() []corev1.NodeConditionType) func() []*corev1.Node {
	return func() []*corev1.Node {
		var out []*corev1.Node
		for _, n := range ExpectNodes(ctx, env.Client) {
			for _, ct := range policies() {
				if cond := nodeutils.GetCondition(n, ct); cond.Status == corev1.ConditionFalse || cond.Status == corev1.ConditionUnknown {
					out = append(out, n)
					break
				}
			}
		}
		return out
	}
}
