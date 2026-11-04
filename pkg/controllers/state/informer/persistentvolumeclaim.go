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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
)

// PersistentVolumeClaimController keeps the CSI driver of each PersistentVolumeClaim in cluster state, which counts
// the volumes of the pods that mount it against the driver's limit on their node.
type PersistentVolumeClaimController struct {
	kubeClient client.Client
	cluster    *state.Cluster
}

func NewPersistentVolumeClaimController(kubeClient client.Client, cluster *state.Cluster) *PersistentVolumeClaimController {
	return &PersistentVolumeClaimController{kubeClient: kubeClient, cluster: cluster}
}

func (c *PersistentVolumeClaimController) Name() string {
	return "state.persistentvolumeclaim"
}

func (c *PersistentVolumeClaimController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	pvc := &v1.PersistentVolumeClaim{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, pvc); err != nil {
		if errors.IsNotFound(err) {
			c.cluster.DeletePersistentVolumeClaim(req.NamespacedName)
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	return reconcile.Result{}, c.cluster.UpdatePersistentVolumeClaim(ctx, pvc)
}

func (c *PersistentVolumeClaimController) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1.PersistentVolumeClaim{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)}).
		Complete(c)
}
