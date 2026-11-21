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

// Package driver keeps the CSI driver of each PersistentVolumeClaim in cluster state, which counts the volumes of the
// pods that mount it against the driver's limit on their node.
package driver

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

const (
	minReconciles = 10
	maxReconciles = 3000
)

// Controller resolves each PVC's CSI driver from its PersistentVolume or StorageClass and feeds it to cluster state. It
// reconciles a PVC on its own events, and on the events of a pod that mounts a PVC whose driver cluster state doesn't
// hold yet, for a PVC cluster state sees through a pod before its own event.
type Controller struct {
	kubeClient client.Client
	pvcs       *cluster.PVCs
}

func NewController(kubeClient client.Client, pvcs *cluster.PVCs) *Controller {
	return &Controller{kubeClient: kubeClient, pvcs: pvcs}
}

func (c *Controller) Name() string {
	return "persistentvolumeclaim.driver"
}

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.kubeClient.Get(ctx, req.NamespacedName, pvc); err != nil {
		// A PVC that's gone was deleted and its finalizer removed, so its volume no longer counts.
		if errors.IsNotFound(err) {
			c.pvcs.Forget(req.NamespacedName)
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if err := c.pvcs.Resolve(ctx, c.kubeClient, pvc); err != nil {
		// A PVC's volume or StorageClass may not exist yet.
		if errors.IsNotFound(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.PersistentVolumeClaim{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			return c.unresolved(o.(*corev1.Pod))
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)}).
		Complete(c)
}

// unresolved returns the PVCs the pod mounts whose drivers cluster state doesn't hold.
func (c *Controller) unresolved(pod *corev1.Pod) []reconcile.Request {
	var requests []reconcile.Request
	for _, volume := range pod.Spec.Volumes {
		name := volumeutil.PersistentVolumeClaimName(pod, volume)
		if key := (types.NamespacedName{Namespace: pod.Namespace, Name: name}); name != "" && !c.pvcs.Resolved(key) {
			requests = append(requests, reconcile.Request{NamespacedName: key})
		}
	}
	return requests
}
