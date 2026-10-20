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

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
)

// JobController tracks the CronJob controlling each Job, which VPA predictions resolve pods through. Only metadata is
// watched, since nothing else is read.
type JobController struct {
	kubeClient client.Client
	cluster    *state.Cluster
}

func NewJobController(kubeClient client.Client, cluster *state.Cluster) *JobController {
	return &JobController{
		kubeClient: kubeClient,
		cluster:    cluster,
	}
}

func (c *JobController) Name() string {
	return "state.job"
}

func (c *JobController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	job := &metav1.PartialObjectMetadata{}
	job.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("Job"))
	if err := c.kubeClient.Get(ctx, req.NamespacedName, job); err != nil {
		if errors.IsNotFound(err) {
			c.cluster.DeleteJob(req.NamespacedName)
		}
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	c.cluster.UpdateJob(job)
	return reconcile.Result{}, nil
}

func (c *JobController) Register(ctx context.Context, m manager.Manager) error {
	c.cluster.TrackJobs()
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&batchv1.Job{}, builder.OnlyMetadata).
		WithEventFilter(controllerChangedPredicate()).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)}).
		Complete(c)
}
