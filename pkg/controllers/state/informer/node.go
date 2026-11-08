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

package informer

import (
	"context"
	"fmt"

	"github.com/awslabs/operatorpkg/serrors"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
)

// NodeController keeps the CSI drivers of each tracked Node's CSINode in cluster state, which reads them to count the
// volumes a node can attach. Cluster state's Node store follows the Nodes itself.
type NodeController struct {
	kubeClient client.Client
	cluster    *state.Cluster
}

// NewNodeController constructs a controller instance
func NewNodeController(kubeClient client.Client, cluster *state.Cluster) *NodeController {
	return &NodeController{
		kubeClient: kubeClient,
		cluster:    cluster,
	}
}

func (c *NodeController) Name() string {
	return "state.node"
}

func (c *NodeController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	node := &corev1.Node{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, node, client.UnsafeDisableDeepCopy); err != nil {
		if errors.IsNotFound(err) {
			c.cluster.SetCSIDrivers(req.Name, nil)
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !cluster.Tracked(node) {
		return reconcile.Result{}, nil
	}
	var csiNode storagev1.CSINode
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: node.Name}, &csiNode); client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, serrors.Wrap(fmt.Errorf("getting CSINode to determine volume limit, %w", err), "CSINode", klog.KRef("", node.Name))
	}
	c.cluster.SetCSIDrivers(node.Name, csiNode.Spec.Drivers)
	// A CSINode's events don't reach this controller, so read it again in a while.
	return reconcile.Result{RequeueAfter: stateRetryPeriod}, nil
}

func (c *NodeController) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Node{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)}).
		Complete(c)
}
