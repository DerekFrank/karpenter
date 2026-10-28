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

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
)

// ReplicaSetController tracks the ReplicaSets that scheduling reads: the Deployment controlling each, which VPA
// predictions resolve pods through, and each selector, which default topology spread folds in.
type ReplicaSetController struct {
	kubeClient client.Client
	cluster    *state.Cluster
}

func NewReplicaSetController(kubeClient client.Client, cluster *state.Cluster) *ReplicaSetController {
	return &ReplicaSetController{
		kubeClient: kubeClient,
		cluster:    cluster,
	}
}

func (c *ReplicaSetController) Name() string {
	return "state.replicaset"
}

func (c *ReplicaSetController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	rs := &appsv1.ReplicaSet{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, rs); err != nil {
		if errors.IsNotFound(err) {
			c.cluster.DeleteReplicaSet(req.NamespacedName)
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	c.cluster.UpdateReplicaSet(rs)
	return reconcile.Result{}, nil
}

func (c *ReplicaSetController) Register(ctx context.Context, m manager.Manager) error {
	c.cluster.TrackReplicaSets()
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&appsv1.ReplicaSet{}).
		WithEventFilter(controllerChangedPredicate()).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)}).
		Complete(c)
}

// controllerChangedPredicate filters updates down to those that change an object's controller, since adoption and
// orphaning change it without recreating the object. The other tracked fields, e.g. a ReplicaSet's selector, are
// immutable.
func controllerChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			return !equality.Semantic.DeepEqual(metav1.GetControllerOfNoCopy(e.ObjectOld), metav1.GetControllerOfNoCopy(e.ObjectNew))
		},
	}
}
