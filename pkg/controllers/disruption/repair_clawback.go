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

package disruption

import (
	"context"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/utils/launchcontext"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

// ClawbackController is the writer side of correlated-failure restraint, in its optimistic + clawback mode. Repair
// credits a probe optimistically — the shared Window widens a domain the instant a repair-launched replacement holds
// Ready — and this controller watches that replacement for a re-break: if it goes unhealthy within the clawback window,
// the credit was wrong, so the domain is slammed shut and backed off.
//
// The whole thing is stateless and event-driven, which is what the durable launch-context annotation buys: a repair
// replacement carries "I was launched for repair" (LaunchContext.Cause=Unhealthy), *when* (CreationTimestamp), and
// *where* (its own NodePool/zone labels). So the controller needs no in-memory probe ledger — a restart just keeps
// reconciling the same annotated NodeClaims — and it records its verdict as durable, idempotent NodeClaim conditions
// (RepairCredited / RepairClawedBack) so it never credits or claws back the same replacement twice.
type ClawbackController struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	clock         clock.Clock
	window        *Window
}

func NewClawbackController(clk clock.Clock, kubeClient client.Client, cp cloudprovider.CloudProvider, w *Window) *ClawbackController {
	return &ClawbackController{kubeClient: kubeClient, cloudProvider: cp, clock: clk, window: w}
}

func (c *ClawbackController) Name() string { return "node.repair.clawback" }

//nolint:gocyclo // straight-line reconcile: annotation guard + condition idempotency + one credit/clawback decision.
func (c *ClawbackController) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	if !nodeclaimutils.IsManaged(nodeClaim, c.cloudProvider) || !nodeClaim.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	// Only repair-launched replacements carry launch provenance we act on.
	lc, ok := launchcontext.Get(nodeClaim)
	if !ok || lc.Cause != launchcontext.CauseUnhealthy {
		return reconcile.Result{}, nil
	}
	// Terminal: once clawed back, the verdict is final — nothing more to do (idempotent).
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeRepairClawedBack).IsTrue() {
		return reconcile.Result{}, nil
	}
	if nodeClaim.Status.NodeName != "" {
		ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("Node", klog.KRef("", nodeClaim.Status.NodeName)))
	}

	// Resolve the replacement's Node — its conditions carry both the health signal and the policy failure domain.
	var node *corev1.Node
	if nodeClaim.Status.NodeName != "" {
		node = &corev1.Node{}
		if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Status.NodeName}, node); err != nil {
			// A reaped replacement Node is handled as its own repair candidate elsewhere; nothing to credit/claw here.
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}

	age := c.clock.Now().Sub(nodeClaim.CreationTimestamp.Time)
	// Failure domains from the replacement's OWN labels (nodepool, zone) plus its current unhealthy conditions (policy).
	// Attributing to where the replacement actually landed is the learn-from-replacement rule: a bad zone cools its own
	// zone, and a healthy replacement (no False conditions) widens only nodepool+zone.
	domains := domainsForNode(nodeClaim.Labels[v1.NodePoolLabelKey], nodeClaim.Labels[corev1.LabelTopologyZone], node)
	credited := nodeClaim.StatusConditions().Get(v1.ConditionTypeRepairCredited).IsTrue()

	stored := nodeClaim.DeepCopy()
	switch {
	case node != nil && c.unhealthy(node) && age <= restraintClawbackWindow:
		// Re-break inside the clawback window: the optimistic credit was wrong. Slam every domain shut and back off, and
		// record the terminal verdict so we never claw back this replacement twice.
		c.window.SlamShut(domains...)
		nodeClaim.StatusConditions(status.WithClock(c.clock)).SetTrueWithReason(v1.ConditionTypeRepairClawedBack, "RebrokeInClawbackWindow", "replacement went unhealthy within the clawback window")
	case !credited && node != nil && c.ready(node) && !c.unhealthy(node):
		// Optimistic credit on first healthy Ready: widen the domains, once.
		c.window.Widen(domains...)
		nodeClaim.StatusConditions(status.WithClock(c.clock)).SetTrueWithReason(v1.ConditionTypeRepairCredited, "ReplacementReady", "replacement came up Ready and healthy")
	}
	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		if err := c.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			if apierrors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	// Keep watching until the clawback window closes so a later re-break is still caught even if no Node event fires.
	// After the window, a credited replacement's verdict is final; a re-break is then just a fresh repair candidate.
	if rem := restraintClawbackWindow - age; rem > 0 && !nodeClaim.StatusConditions().Get(v1.ConditionTypeRepairClawedBack).IsTrue() {
		return reconcile.Result{RequeueAfter: rem}, nil
	}
	return reconcile.Result{}, nil
}

// ready reports whether the replacement's Node is Ready=True.
func (c *ClawbackController) ready(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// unhealthy reports whether the replacement's Node currently matches a RepairPolicy — i.e. it re-broke.
func (c *ClawbackController) unhealthy(node *corev1.Node) bool {
	policy, _ := matchRepairPolicy(node, c.cloudProvider.RepairPolicies())
	return policy != nil
}

func (c *ClawbackController) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1.NodeClaim{}, builder.WithPredicates(nodeclaimutils.IsManagedPredicateFuncs(c.cloudProvider))).
		// Wake on the replacement Node's health transitions so a re-break inside the window is caught promptly.
		Watches(&corev1.Node{}, nodeclaimutils.NodeEventHandler(c.kubeClient, c.cloudProvider)).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
