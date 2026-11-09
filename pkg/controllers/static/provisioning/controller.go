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
	"context"
	"fmt"
	"math"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
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
	"sigs.k8s.io/controller-runtime/pkg/source"

	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	provisioner   *provisioning.Provisioner
	cluster       *state.Cluster
	// counts wakes the controller on each NodePool's nodeCounts, built in Register.
	counts source.SyncingSource
}

// nodeCounts are the counts that decide how many NodeClaims a static NodePool is short of: its active and deleting
// NodeClaims, both of which count against its node limit, and the nodes reserved for NodeClaims being created. They
// come from NodePoolState, which counts a NodeClaim until it's gone.
type nodeCounts struct {
	NodeClaims karpentercluster.NodeClaimCounts
	Reserved   int64
}

func nodeCountsOf(_ string, e karpentercluster.NodePoolEntry) any {
	return nodeCounts{NodeClaims: e.Derived.NodeClaims, Reserved: e.Derived.Reserved}
}

func NewController(kubeClient client.Client, cluster *state.Cluster, recorder events.Recorder, cloudProvider cloudprovider.CloudProvider, provisioner *provisioning.Provisioner, clock clock.Clock, deviceAllocationController *deviceallocation.Controller, virtualPodCache *virtualpods.Cache) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		cluster:       cluster,
		// The controller creates NodeClaims and schedules no pods, so its provisioner writes neither nominations nor buffer
		// pod counts.
		provisioner: provisioning.NewProvisioner(kubeClient, recorder, cloudProvider, cluster, clock, deviceAllocationController, virtualPodCache, nil, nil, nil),
	}
}

func (c *Controller) Name() string {
	return "static.provisioning"
}

func (c *Controller) Reconcile(ctx context.Context, np *v1.NodePool) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	if !nodepoolutils.IsManaged(np, c.cloudProvider) || !np.StatusConditions().Root().IsTrue() || np.Spec.Replicas == nil || !np.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}

	runningNodeClaims, _, reservedNodes := c.cluster.NodePoolState.GetNodeCount(np.Name)
	desiredReplicas := lo.FromPtr(np.Spec.Replicas)
	// Size down of replicas will be handled in deprovisioning controller to drain nodes and delete NodeClaims.
	// Reserved nodes are NodeClaims being created that the counts don't include yet: this controller's own from an
	// earlier reconcile, held until their NodeClaims count (or, for a create that failed ambiguously, until the
	// reservation expires), and replacements the disruption controller is creating. The counts, including a reservation
	// expiring, and the node limit wake the controller when they change.
	if int64(runningNodeClaims)+int64(reservedNodes) >= desiredReplicas {
		return reconcile.Result{}, nil
	}

	limit, ok := np.Spec.Limits[resources.Node]
	nodeLimit := lo.Ternary(ok, limit.Value(), int64(math.MaxInt64))
	// The shortfall read above can be stale by the time it reserves, such as once the disruption controller reserves a
	// replacement, so the reservation admits against replicas as well as the node limit.
	reservations := c.cluster.NodePoolState.ReserveReplicas(np.Name, desiredReplicas, nodeLimit)
	countNodeClaimsToProvision := len(reservations)

	if countNodeClaimsToProvision <= 0 {
		if running, _, reserved := c.cluster.NodePoolState.GetNodeCount(np.Name); int64(running+reserved) < desiredReplicas {
			log.FromContext(ctx).Info("nodepool node limit reached")
		}
		return reconcile.Result{}, nil
	}

	log.FromContext(ctx).WithValues("current", runningNodeClaims, "desired", desiredReplicas, "provision-count", countNodeClaimsToProvision).
		Info("provisioning nodeclaims to satisfy replica count")

	nodeClaims := make([]*scheduling.NodeClaim, 0, countNodeClaimsToProvision)
	for _, reservation := range reservations {
		nct := scheduling.NewNodeClaimTemplate(np)
		nodeClaims = append(nodeClaims, &scheduling.NodeClaim{
			NodeClaimTemplate: *nct,
			Reservation:       reservation,
		})
	}

	// CreateNodeClaims binds each created NodeClaim's reservation, and releases the reservation of every NodeClaim it
	// didn't create, unless its create may have succeeded anyway, which leaves the reservation to expire.
	_, err := c.provisioner.CreateNodeClaims(ctx, nodeClaims, provisioning.WithReason(metrics.ProvisionedReason))
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("creating nodeclaims, %w", err)
	}
	return reconcile.Result{}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	// counts narrows the NodePool fields to the ones this controller decides on, so it reconciles a NodePool that may be
	// short of its replicas, or have room under its node limit again, and not one whose usage, capacity or health changed.
	c.counts = c.cluster.NodePoolState.SourceOn(nodeCountsOf)
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		// Reconcile on NodePool Create and Update (when replicas or the node limit change, or when NodePool status moves
		// from NotReady to Ready)
		For(&v1.NodePool{}, builder.WithPredicates(nodepoolutils.IsManagedPredicateFuncs(c.cloudProvider), nodepoolutils.IsStaticPredicateFuncs(),
			predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool {
					return true
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldNP := e.ObjectOld.(*v1.NodePool)
					newNP := e.ObjectNew.(*v1.NodePool)
					return HasNodePoolReplicaOrStatusChanged(oldNP, newNP)
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					return false
				},
				GenericFunc: func(e event.GenericEvent) bool {
					return false
				},
			})).
		// A NodeClaim starting to delete, going away or being created, and a reservation ending, change the counts.
		WatchesRawSource(c.counts).
		// Cluster state's NodePoolState counts the NodePool's NodeClaims only once it has synced; before, it would count
		// fewer than exist and provision too many.
		WatchesRawSource(karpenterstate.Requires(c.cluster)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

func HasNodePoolReplicaOrStatusChanged(oldNP, newNP *v1.NodePool) bool {
	if lo.FromPtr(oldNP.Spec.Replicas) != lo.FromPtr(newNP.Spec.Replicas) {
		return true
	}
	oldLimit, oldOK := oldNP.Spec.Limits[resources.Node]
	newLimit, newOK := newNP.Spec.Limits[resources.Node]
	if oldOK != newOK || oldLimit.Cmp(newLimit) != 0 {
		return true
	}
	// np.StatusConditions.Root() may mutate the np which is unsafe when operating directly against the informer cache
	isReady := func(np *v1.NodePool) bool {
		for i := range np.Status.Conditions {
			if np.Status.Conditions[i].Type != status.ConditionReady {
				continue
			}
			return np.Status.Conditions[i].IsTrue()
		}
		return false
	}
	return !isReady(oldNP) && isReady(newNP)
}
