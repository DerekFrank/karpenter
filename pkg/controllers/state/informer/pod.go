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
	"time"

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

const (
	minReconciles = 10
	maxReconciles = 3000
)

var stateRetryPeriod = 1 * time.Minute

// PodController resolves the CSI drivers of the PVCs each pod mounts, and drops what the provisioner decided about a pod
// once it's gone. Cluster state's pod store follows the pods itself.
type PodController struct {
	kubeClient client.Client
	cluster    *state.Cluster
}

func NewPodController(kubeClient client.Client, cluster *state.Cluster) *PodController {
	return &PodController{
		kubeClient: kubeClient,
		cluster:    cluster,
	}
}

func (c *PodController) Name() string {
	return "state.pod"
}

func (c *PodController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	pod := &v1.Pod{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, pod, client.UnsafeDisableDeepCopy); err != nil {
		if errors.IsNotFound(err) {
			c.cluster.ClearPodSchedulingMappings(req.NamespacedName)
			c.cluster.MarkUnconsolidated()
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if err := c.cluster.ResolvePVCDrivers(ctx, pod); err != nil {
		// A PVC's volume or StorageClass may not exist yet.
		if errors.IsNotFound(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (c *PodController) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1.Pod{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)}).
		Complete(c)
}
