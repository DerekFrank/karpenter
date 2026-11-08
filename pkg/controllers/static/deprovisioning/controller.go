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

package static

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"

	"sigs.k8s.io/karpenter/pkg/utils/pod"
)

const (
	TerminationReason = "overprovisioned"
)

type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	cluster       *state.Cluster
	clock         clock.Clock
	recorder      events.Recorder
}

func NewController(kubeClient client.Client, cluster *state.Cluster, cloudProvider cloudprovider.CloudProvider, clock clock.Clock, recorder events.Recorder) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		cluster:       cluster,
		clock:         clock,
		recorder:      recorder,
	}
}

// Reconcile the resource
// Requeue after computing Static NodePool to ensure we don't miss any events
func (c *Controller) Name() string {
	return "static.deprovisioning"
}

func (c *Controller) Reconcile(ctx context.Context, np *v1.NodePool) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	if !nodepoolutils.IsManaged(np, c.cloudProvider) || np.Spec.Replicas == nil || !np.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}

	// We dont have to wait for cluster sync as we cannot really have internal state representing more NodeClaims than actual
	// During controller crashes we gradually populate our cluster/NodePoolState, as and when we populate we delete NC if we are over-provisioned
	runningNodeClaims, _, _ := c.cluster.NodePoolState.GetNodeCount(np.Name)
	desiredReplicas := lo.FromPtr(np.Spec.Replicas)
	// To avoid race conditions between deprovisioning and the disruption controller,
	// we only include running NodeClaims when counting for deprovisioning purposes.
	// Including both active NodeClaims and those pending disruption could cause us
	// to temporarily exceed the desired replica count while replacements are being created.
	nodeClaimsToDeprovision := int64(runningNodeClaims) - desiredReplicas

	// Only handle scale down - scale up is handled by provisioning controller
	if nodeClaimsToDeprovision <= 0 {
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}

	log.FromContext(ctx).WithValues("current", runningNodeClaims, "desired", desiredReplicas, "deprovision-count", nodeClaimsToDeprovision).
		Info("deprovisioning nodeclaims to satisfy replica count")

	// Get deprovisioning candidates
	candidates := c.getDeprovisioningCandidates(ctx, np, int(nodeClaimsToDeprovision))

	scaleDownErrs := make([]error, len(candidates))
	// Terminate selected NodeClaims
	workqueue.ParallelizeUntil(ctx, len(candidates), len(candidates), func(i int) {
		candidate := candidates[i]

		if err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
			return c.kubeClient.Delete(ctx, candidate)
		}); err != nil && client.IgnoreNotFound(err) != nil {
			scaleDownErrs[i] = err
			return
		}

		log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(candidate)).V(1).Info("deleting nodeclaim")

		c.cluster.UpdateNodeClaim(candidate)
	})

	if scaleDownErr := multierr.Combine(scaleDownErrs...); scaleDownErr != nil {
		return reconcile.Result{}, fmt.Errorf("failed to deprovision nodeclaims, %w", scaleDownErr)
	}

	return reconcile.Result{RequeueAfter: time.Minute}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		// Reoncile on NodePool Create and Update (when replicas change)
		For(&v1.NodePool{}, builder.WithPredicates(nodepoolutils.IsManagedPredicateFuncs(c.cloudProvider), nodepoolutils.IsStaticPredicateFuncs(),
			predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool {
					return true
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldNP := e.ObjectOld.(*v1.NodePool)
					newNP := e.ObjectNew.(*v1.NodePool)
					return HasNodePoolReplicaCountChanged(oldNP, newNP)
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					return false
				},
				GenericFunc: func(e event.GenericEvent) bool {
					return false
				},
			})).
		// We care about Static NodeClaims creating as we might have over provisioned and need to deprovision
		Watches(&v1.NodeClaim{}, nodepoolutils.NodeClaimEventHandler(nodepoolutils.WithClient(c.kubeClient), nodepoolutils.WithStaticOnly), builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				return true
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				return false
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				return false
			},
			GenericFunc: func(e event.GenericEvent) bool {
				return false
			},
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

func HasNodePoolReplicaCountChanged(oldNP, newNP *v1.NodePool) bool {
	return lo.FromPtr(oldNP.Spec.Replicas) != lo.FromPtr(newNP.Spec.Replicas)
}

// Returns NodeClaims suitable for deprovisioning, prioritizing:
// 1. Unresolved NodeClaims (no ProviderID yet - haven't launched)
// 2. Empty nodes (nodes with no pods or only DaemonSet pods without do-not-disrupt annotation)
// 3. If more nodes needed, nodes with lowest disruption cost (nodes with pods that have do-not-disrupt will have highest cost)
func (c *Controller) getDeprovisioningCandidates(ctx context.Context, np *v1.NodePool, count int) []*v1.NodeClaim {
	candidates := make([]*v1.NodeClaim, 0, count)

	// Unresolved NodeClaims (haven't launched yet or that failed Create call), from the NodePool's index in cluster state
	for _, nc := range lo.Slice(c.cluster.UnlaunchedNodeClaims(np.Name), 0, count) {
		candidates = append(candidates, nc.DeepCopy())
	}
	remaining := count - len(candidates)

	if remaining == 0 {
		return candidates
	}

	// The NodePool's StateNodes, from its index in cluster state
	nodes := lo.Filter(c.cluster.NodePoolNodes(np.Name), func(n *karpentercluster.StateNode, _ int) bool { return !n.MarkedForDeletion() })

	// Resolved nodes (empty first, then by disruption cost)
	for _, nc := range c.resolvedDeprovisioningCandidates(ctx, nodes, np, remaining) {
		candidates = append(candidates, nc.DeepCopy())
	}

	return candidates
}

// isEmpty returns whether the node has no pods, or only DaemonSet pods without an active do-not-disrupt annotation. The
// counts decide a node without pods, or with a pod no DaemonSet owns. A node with only DaemonSet pods needs the pods
// themselves, to check their do-not-disrupt annotations against the clock.
func (c *Controller) isEmpty(ctx context.Context, node *karpentercluster.StateNode) bool {
	switch bound, daemonSet := podCounts(node); {
	case bound == 0:
		return true
	case daemonSet < bound:
		return false
	}
	pods, err := node.Pods(ctx, c.kubeClient)
	if err != nil {
		log.FromContext(ctx).WithValues("node", node.Name()).Error(err, "unable to list pods, treating as non-empty")
		return false
	}
	return len(pods) == 0 || lo.EveryBy(pods, pod.IsOwnedByDaemonSet) && lo.NoneBy(pods, func(p *corev1.Pod) bool { return pod.IsDoNotDisruptActive(p, c.clock, c.recorder) })
}

// podCounts returns the number of pods bound to the node and how many of them DaemonSets own, from the fields cluster
// state derives about the node, where each pod requests one of the pods resource.
func podCounts(node *karpentercluster.StateNode) (pods, daemonSetPods int64) {
	return node.Derived.PodRequests.Pods().Value(), node.Derived.DaemonSetRequests.Pods().Value()
}

// resolvedDeprovisioningCandidates returns resolved NodeClaims (those with ProviderID) up to the specified count,
// prioritizing empty nodes first, then nodes with lowest disruption cost
func (c *Controller) resolvedDeprovisioningCandidates(ctx context.Context, nodes []*karpentercluster.StateNode, np *v1.NodePool, count int) []*v1.NodeClaim {
	if len(nodes) == 0 {
		return nil
	}

	candidates := make([]*v1.NodeClaim, 0, count)

	// Priority 1: Empty nodes
	emptyNodes := lo.Filter(nodes, func(node *karpentercluster.StateNode, _ int) bool { return c.isEmpty(ctx, node) })

	for _, node := range lo.Slice(emptyNodes, 0, count) {
		candidates = append(candidates, node.NodeClaim)
	}

	remaining := count - len(candidates)

	if remaining == 0 {
		return candidates
	}

	// Get non-empty nodes with their costs
	type NonEmptyNode struct {
		node            *karpentercluster.StateNode
		pods            []*corev1.Pod
		hasDoNotDisrupt bool
	}

	emptyNodesSet := sets.New(emptyNodes...)
	nonEmptyNodes := lo.FilterMap(nodes, func(node *karpentercluster.StateNode, _ int) (NonEmptyNode, bool) {
		if emptyNodesSet.Has(node) {
			return NonEmptyNode{}, false
		}

		// Ranking needs the pods themselves: their do-not-disrupt annotations against the clock, and their eviction costs,
		// which DaemonSet pods count toward here, unlike the node's derived DisruptionCost.
		pods, err := node.Pods(ctx, c.kubeClient)
		if err != nil {
			log.FromContext(ctx).WithValues("node", node.Name()).Error(err, "unable to list pods, skipping node")
			return NonEmptyNode{}, false
		}

		return NonEmptyNode{
			node:            node,
			pods:            pods,
			hasDoNotDisrupt: lo.SomeBy(pods, func(p *corev1.Pod) bool { return pod.IsDoNotDisruptActive(p, c.clock, c.recorder) }),
		}, true
	})

	slices.SortFunc(nonEmptyNodes, func(i, j NonEmptyNode) int {
		// If one node has do-not-disrupt pods and the other doesn't, the one without should come first
		if i.hasDoNotDisrupt != j.hasDoNotDisrupt {
			return lo.Ternary(i.hasDoNotDisrupt, 1, -1)
		}
		// If neither has do-not-disrupt pods, compare their costs
		return cmp.Compare(
			disruptionutils.ReschedulingCost(ctx, i.pods)*disruptionutils.LifetimeRemaining(c.clock, np, i.node.NodeClaim),
			disruptionutils.ReschedulingCost(ctx, j.pods)*disruptionutils.LifetimeRemaining(c.clock, np, j.node.NodeClaim),
		)
	})

	// Take the remaining needed nodes with lowest cost
	for _, nwc := range lo.Slice(nonEmptyNodes, 0, remaining) {
		candidates = append(candidates, nwc.node.NodeClaim)
	}

	return candidates
}
