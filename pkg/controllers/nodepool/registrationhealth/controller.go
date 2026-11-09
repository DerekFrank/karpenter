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

package registrationhealth

import (
	"context"

	"github.com/awslabs/operatorpkg/status"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/operator/injection"

	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cluster"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
)

// Controller derives each NodePool's NodeRegistrationHealthy condition from the launch outcomes the NodeClaim
// lifecycle controller records for the NodePool's current spec. It's the condition's one writer, and writes nothing
// to the recorded outcomes.
type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	// npState holds each NodePool's recorded launch outcomes, which go once the NodePool does.
	npState *cluster.NodePoolState
	clock   clock.Clock
}

// NewController returns the controller that sets each NodePool's NodeRegistrationHealthy condition.
func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, npState *cluster.NodePoolState) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		npState:       npState,
		clock:         clk,
	}
}

func (c *Controller) Name() string {
	return "nodepool.registrationhealth"
}

func (c *Controller) Reconcile(ctx context.Context, nodePool *v1.NodePool) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	nodeClass, err := nodepoolutils.GetNodeClass(ctx, c.kubeClient, nodePool, c.cloudProvider)
	if err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	// Ignore NodePools which aren't using a supported NodeClass
	if nodeClass == nil {
		return reconcile.Result{}, nil
	}
	stored := nodePool.DeepCopy()

	// The outcomes count only for the spec they were recorded against, so a new NodePool or NodeClass spec has none.
	health := c.npState.RegistrationHealth(nodePool, nodeClass.GetGeneration())
	switch health.Status {
	case cluster.RegistrationHealthy:
		nodePool.StatusConditions(status.WithClock(c.clock)).SetTrue(v1.ConditionTypeNodeRegistrationHealthy)
	case cluster.RegistrationUnhealthy:
		nodePool.StatusConditions(status.WithClock(c.clock)).SetFalse(v1.ConditionTypeNodeRegistrationHealthy, health.Reason, health.Message)
	default:
		// Without outcomes for the current spec, such as after a restart, a condition observed for the current spec
		// stands. A condition observed for an earlier spec, or none, is Unknown.
		if cond := nodePool.StatusConditions().Get(v1.ConditionTypeNodeRegistrationHealthy); cond == nil ||
			nodePool.Status.NodeClassObservedGeneration != nodeClass.GetGeneration() ||
			nodePool.Generation != cond.ObservedGeneration {
			nodePool.StatusConditions(status.WithClock(c.clock)).SetUnknown(v1.ConditionTypeNodeRegistrationHealthy)
		}
	}
	nodePool.Status.NodeClassObservedGeneration = nodeClass.GetGeneration()

	if !equality.Semantic.DeepEqual(stored, nodePool) {
		// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch
		// can cause races due to the fact that it fully replaces the list on a change
		// Here, we are updating the status condition list
		if err := c.kubeClient.Status().Patch(ctx, nodePool, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); client.IgnoreNotFound(err) != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

// registration is what this controller reads of a NodePool's entry: the NodePool's generation, and the spec and health
// of its recorded launch outcomes.
type registration struct {
	Generation int64
	Key        cluster.RegistrationKey
	Health     cluster.RegistrationHealth
}

// registrations is the NodePool store narrowed to registration: it reconciles each NodePool at startup, on a NodePool
// spec change, and when a recorded outcome changes its health, and not on the NodePool's every status update. A
// NodePool that's gone takes its outcomes with it, and needs no reconcile.
func (c *Controller) registrations() source.SyncingSource {
	return c.npState.SourceOn(func(_ string, e cluster.NodePoolEntry) any {
		r := e.Intent.Registration.Value
		return registration{Generation: e.Object.Generation, Key: r.RegistrationKey, Health: r.Health()}
	})
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	b := controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(c.registrations()).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), 10, 1000)})
	for _, nodeClass := range c.cloudProvider.GetSupportedNodeClasses() {
		// A NodeClass spec change starts its NodePools' health over.
		b.Watches(nodeClass, nodepoolutils.NodeClassEventHandler(c.kubeClient), builder.WithPredicates(predicate.GenerationChangedPredicate{}))
	}
	return b.Complete(reconcile.AsReconciler(m.GetClient(), c))
}
