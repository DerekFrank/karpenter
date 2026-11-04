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

package counter

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// Controller for the resource
type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
}

var BaseResources = corev1.ResourceList{
	corev1.ResourceCPU:              resource.MustParse("0"),
	corev1.ResourceMemory:           resource.MustParse("0"),
	corev1.ResourcePods:             resource.MustParse("0"),
	corev1.ResourceEphemeralStorage: resource.MustParse("0"),
	resources.Node:                  resource.MustParse("0"),
}

// NewController is a constructor
func NewController(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
	}
}

// Reconcile a control loop for the resource
func (c *Controller) Name() string {
	return "nodepool.counter"
}

func (c *Controller) Reconcile(ctx context.Context, nodePool *v1.NodePool) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	if !nodepoolutils.IsManaged(nodePool, c.cloudProvider) {
		return reconcile.Result{}, nil
	}
	usage, err := c.usage(ctx, nodePool.Name)
	if err != nil {
		return reconcile.Result{}, err
	}
	stored := nodePool.DeepCopy()
	nodePool.Status.Resources = lo.Assign(BaseResources, usage)
	nodeQuantity := nodePool.Status.Resources[resources.Node]
	nodePool.Status.Nodes = new(nodeQuantity.Value())
	if !equality.Semantic.DeepEqual(stored, nodePool) {
		if err := c.kubeClient.Status().Patch(ctx, nodePool, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	return reconcile.Result{}, nil
}

// usage sums the status.capacity of the NodePool's NodeClaims, and counts each as one resources.Node. A NodeClaim
// stops counting once it's being deleted or disrupted.
func (c *Controller) usage(ctx context.Context, nodePool string) (corev1.ResourceList, error) {
	nodeClaims := &v1.NodeClaimList{}
	if err := c.kubeClient.List(ctx, nodeClaims, client.MatchingLabels{v1.NodePoolLabelKey: nodePool}, client.UnsafeDisableDeepCopy); err != nil {
		return nil, fmt.Errorf("listing nodeclaims, %w", err)
	}
	usage := corev1.ResourceList{}
	var nodes int64
	for i := range nodeClaims.Items {
		if leaving(&nodeClaims.Items[i]) {
			continue
		}
		usage = resources.MergeInto(usage, nodeClaims.Items[i].Status.Capacity)
		nodes++
	}
	usage[resources.Node] = *resource.NewQuantity(nodes, resource.DecimalSI)
	return usage, nil
}

func leaving(nodeClaim *v1.NodeClaim) bool {
	if !nodeClaim.DeletionTimestamp.IsZero() {
		return true
	}
	// Read the condition directly, since StatusConditions() builds a condition set on every call.
	for i := range nodeClaim.Status.Conditions {
		if nodeClaim.Status.Conditions[i].Type == v1.ConditionTypeDisruptionReason && nodeClaim.Status.Conditions[i].Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1.NodePool{}, builder.WithPredicates(nodepoolutils.IsManagedPredicateFuncs(c.cloudProvider), predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool { return true },
			UpdateFunc: func(e event.UpdateEvent) bool { return false },
			DeleteFunc: func(e event.DeleteEvent) bool { return false },
		})).
		// Any change to a NodeClaim can change its NodePool's usage.
		Watches(&v1.NodeClaim{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			if name := o.GetLabels()[v1.NodePoolLabelKey]; name != "" {
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
			}
			return nil
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), 10, 1000)}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
