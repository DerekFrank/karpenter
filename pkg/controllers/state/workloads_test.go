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

package state_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

func controllerRef(apiVersion, kind, name string, uid string) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: name, UID: k8stypes.UID(uid), Controller: lo.ToPtr(true)}
}

var _ = Describe("ReplicaSet Controller", func() {
	var replicaSetController *informer.ReplicaSetController
	BeforeEach(func() {
		replicaSetController = informer.NewReplicaSetController(env.Client, cluster)
	})

	It("should track a ReplicaSet's controller and selector", func() {
		rs := test.ReplicaSet(test.ReplicaSetOptions{Selector: map[string]string{"app": "web"}})
		rs.OwnerReferences = []metav1.OwnerReference{controllerRef("apps/v1", "Deployment", "web", "deployment-uid")}
		ExpectApplied(ctx, env.Client, rs)
		ExpectReconcileSucceeded(ctx, replicaSetController, client.ObjectKeyFromObject(rs))

		ref := controllerRef("apps/v1", "ReplicaSet", rs.Name, string(rs.UID))
		owner, ok := cluster.ControllerOf(rs.Namespace, &ref)
		Expect(ok).To(BeTrue())
		Expect(owner.Kind).To(Equal("Deployment"))
		Expect(owner.Name).To(Equal("web"))
		selector, ok := cluster.ReplicaSetSelector(rs.Namespace, &ref)
		Expect(ok).To(BeTrue())
		Expect(selector.MatchLabels).To(Equal(map[string]string{"app": "web"}))
	})
	It("should not resolve a reference to an earlier ReplicaSet with the same name", func() {
		rs := test.ReplicaSet()
		rs.OwnerReferences = []metav1.OwnerReference{controllerRef("apps/v1", "Deployment", "web", "deployment-uid")}
		ExpectApplied(ctx, env.Client, rs)
		ExpectReconcileSucceeded(ctx, replicaSetController, client.ObjectKeyFromObject(rs))

		ref := controllerRef("apps/v1", "ReplicaSet", rs.Name, "earlier-uid")
		_, ok := cluster.ControllerOf(rs.Namespace, &ref)
		Expect(ok).To(BeFalse())
		_, ok = cluster.ReplicaSetSelector(rs.Namespace, &ref)
		Expect(ok).To(BeFalse())
	})
	It("should stop resolving a ReplicaSet to its controller once it's orphaned", func() {
		rs := test.ReplicaSet()
		rs.OwnerReferences = []metav1.OwnerReference{controllerRef("apps/v1", "Deployment", "web", "deployment-uid")}
		ExpectApplied(ctx, env.Client, rs)
		ExpectReconcileSucceeded(ctx, replicaSetController, client.ObjectKeyFromObject(rs))

		rs.OwnerReferences = nil
		ExpectApplied(ctx, env.Client, rs)
		ExpectReconcileSucceeded(ctx, replicaSetController, client.ObjectKeyFromObject(rs))
		ref := controllerRef("apps/v1", "ReplicaSet", rs.Name, string(rs.UID))
		_, ok := cluster.ControllerOf(rs.Namespace, &ref)
		Expect(ok).To(BeFalse())
	})
	It("should stop tracking a deleted ReplicaSet", func() {
		rs := test.ReplicaSet()
		rs.OwnerReferences = []metav1.OwnerReference{controllerRef("apps/v1", "Deployment", "web", "deployment-uid")}
		ExpectApplied(ctx, env.Client, rs)
		ExpectReconcileSucceeded(ctx, replicaSetController, client.ObjectKeyFromObject(rs))

		ExpectDeleted(ctx, env.Client, rs)
		ExpectReconcileSucceeded(ctx, replicaSetController, client.ObjectKeyFromObject(rs))
		ref := controllerRef("apps/v1", "ReplicaSet", rs.Name, string(rs.UID))
		_, ok := cluster.ReplicaSetSelector(rs.Namespace, &ref)
		Expect(ok).To(BeFalse())
	})
})

var _ = Describe("Job Controller", func() {
	It("should track a Job's controller", func() {
		jobController := informer.NewJobController(env.Client, cluster)
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: test.RandomName(), Namespace: "default",
				OwnerReferences: []metav1.OwnerReference{controllerRef("batch/v1", "CronJob", "etl", "cronjob-uid")},
			},
			Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: "etl", Image: "etl"}},
			}}},
		}
		ExpectApplied(ctx, env.Client, job)
		ExpectReconcileSucceeded(ctx, jobController, client.ObjectKeyFromObject(job))

		ref := controllerRef("batch/v1", "Job", job.Name, string(job.UID))
		owner, ok := cluster.ControllerOf(job.Namespace, &ref)
		Expect(ok).To(BeTrue())
		Expect(owner.Kind).To(Equal("CronJob"))
		Expect(owner.Name).To(Equal("etl"))
	})
})

var _ = Describe("Workload State Sync", func() {
	It("should not be synced until every tracked ReplicaSet is in state", func() {
		c := state.NewCluster(env.Clock, env.Client, cloudProvider)
		c.TrackReplicaSets()
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		Expect(c.Synced(ctx)).To(BeFalse())

		c.UpdateReplicaSet(rs)
		Expect(c.Synced(ctx)).To(BeTrue())
	})
	It("should ignore ReplicaSets when they aren't tracked", func() {
		c := state.NewCluster(env.Clock, env.Client, cloudProvider)
		ExpectApplied(ctx, env.Client, test.ReplicaSet())
		Expect(c.Synced(ctx)).To(BeTrue())
	})
})
