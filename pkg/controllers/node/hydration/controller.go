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

package hydration

import (
	"context"

	"github.com/awslabs/operatorpkg/reasonable"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
)

// Controller hydrates information to the Node which is expected in newer versions of Karpenter, but would not exist on
// pre-existing nodes. It reconciles NodeClaims, the key of the store it wakes on, and labels each one's Node.
type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	nodes         *karpentercluster.NodeState
}

// NewController returns the node hydration controller, which reconciles a NodeClaim when the Node it registered links
// to it, or its NodeClass changes, as the NodeClaim store records them in the node state.
func NewController(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, nodes *karpentercluster.NodeState) *Controller {
	return &Controller{
		nodes:         nodes,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
	}
}

// Reconcile labels the Node of the NodeClaim the request names with the NodeClaim's NodeClass. The NodeClaim store
// holds only the cloud provider's NodeClaims.
func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	e, ok := c.nodes.NodeClaimStore.Get(req.Name)
	if !ok || e.Derived.Node == "" || e.Object.Spec.NodeClassRef == nil {
		return reconcile.Result{}, nil
	}
	nc := e.Object
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(nc)))
	n := &corev1.Node{}
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: e.Derived.Node}, n); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	stored := n.DeepCopy()
	n.Labels = lo.Assign(n.Labels, map[string]string{
		v1.NodeClassLabelKey(nc.Spec.NodeClassRef.GroupKind()): nc.Spec.NodeClassRef.Name,
	})
	if !equality.Semantic.DeepEqual(stored, n) {
		if err := c.kubeClient.Patch(ctx, n, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	return reconcile.Result{}, nil
}

func (c *Controller) Name() string {
	return "node.hydration"
}

// nodeClasses is the NodeClaim store narrowed to what this controller labels a Node with: the Node the NodeClaim's
// entry names and its NodeClass. It reconciles a NodeClaim once its Node links to it, such as after both at startup,
// and not on the NodeClaim's every status update.
func (c *Controller) nodeClasses() source.SyncingSource {
	return c.nodes.NodeClaimStore.SourceOn(func(_ string, e karpentercluster.NodeClaimEntry) any {
		return nodeClassOf{Node: e.Derived.Node, NodeClass: e.Object.Spec.NodeClassRef}
	})
}

// nodeClassOf is what this controller reads of a NodeClaim's entry.
type nodeClassOf struct {
	Node      string
	NodeClass *v1.NodeClassReference
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(c.nodeClasses()).
		WithOptions(controller.Options{
			RateLimiter:             reasonable.RateLimiter(),
			MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), 1000, 5000),
		}).
		Complete(c)
}
