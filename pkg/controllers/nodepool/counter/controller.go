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
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// Controller for the resource
type Controller struct {
	// kubeClient only writes: NodePools are read through nodePools.
	kubeClient    writer
	cloudProvider cloudprovider.CloudProvider
	nodePools     *cluster.NodePools
	usage         *cluster.NodePoolUsage
	source        chan event.TypedGenericEvent[*v1.NodePool]
}

var BaseResources = corev1.ResourceList{
	corev1.ResourceCPU:              resource.MustParse("0"),
	corev1.ResourceMemory:           resource.MustParse("0"),
	corev1.ResourcePods:             resource.MustParse("0"),
	corev1.ResourceEphemeralStorage: resource.MustParse("0"),
	resources.Node:                  resource.MustParse("0"),
}

type writer interface {
	client.Writer
	client.StatusClient
}

// NewController is a constructor. The controller reconciles a NodePool whenever its usage changes.
func NewController(kubeClient writer, cloudProvider cloudprovider.CloudProvider, nodePools *cluster.NodePools, usage *cluster.NodePoolUsage) *Controller {
	c := &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		nodePools:     nodePools,
		usage:         usage,
		source:        make(chan event.TypedGenericEvent[*v1.NodePool], 10000),
	}
	usage.Subscribe(func(name string) {
		c.source <- event.TypedGenericEvent[*v1.NodePool]{Object: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}}}
	})
	return c
}

// Reconcile a control loop for the resource
func (c *Controller) Name() string {
	return "nodepool.counter"
}

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	nodePool, err := c.nodePools.Get(ctx, req.NamespacedName)
	if err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !nodepoolutils.IsManaged(nodePool.Object, c.cloudProvider) {
		return reconcile.Result{}, nil
	}

	// We need to ensure that the usage aggregate has summed the NodeClaims before we proceed
	// Otherwise, we have the potential to patch over the status with a lower value for the nodepool resource
	// counts on startup
	if !c.usage.Synced() {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	stored := nodePool.Object.DeepCopy()
	nodePool.Object.Status.Resources = lo.Assign(BaseResources, nodePool.Derived.Usage)
	nodes := nodePool.Object.Status.Resources[resources.Node]
	nodePool.Object.Status.Nodes = new(nodes.Value())
	if !equality.Semantic.DeepEqual(stored, nodePool.Object) {
		if err := c.kubeClient.Status().Patch(ctx, nodePool.Object, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	return reconcile.Result{}, nil
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	if err := m.Add(c.usage.NodeClaims); err != nil {
		return fmt.Errorf("adding nodepool usage aggregate, %w", err)
	}
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1.NodePool{}, builder.WithPredicates(nodepoolutils.IsManagedPredicateFuncs(c.cloudProvider), predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool { return true },
			UpdateFunc: func(e event.UpdateEvent) bool { return false },
			DeleteFunc: func(e event.DeleteEvent) bool { return false },
		})).
		WatchesRawSource(source.Channel(c.source, &handler.TypedEnqueueRequestForObject[*v1.NodePool]{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), 10, 1000)}).
		Complete(c)
}
