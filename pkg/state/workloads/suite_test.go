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

package workloads_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	"sigs.k8s.io/karpenter/pkg/state/workloads"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *test.Environment
var reader *workloads.Reader

func TestWorkloads(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Workloads")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...))
	reader = workloads.NewReader(env.Client)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed())
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

func controllerRef(apiVersion, kind, name string, uid types.UID) *metav1.OwnerReference {
	return &metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: name, UID: uid, Controller: lo.ToPtr(true)}
}

var _ = Describe("Reader", func() {
	Context("ControllerOf", func() {
		It("should resolve a ReplicaSet to its Deployment", func() {
			rs := test.ReplicaSet()
			rs.OwnerReferences = []metav1.OwnerReference{*controllerRef("apps/v1", "Deployment", "web", "deployment-uid")}
			ExpectApplied(ctx, env.Client, rs)

			owner, ok, err := reader.ControllerOf(ctx, rs.Namespace, controllerRef("apps/v1", "ReplicaSet", rs.Name, rs.UID))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(owner.Kind).To(Equal("Deployment"))
			Expect(owner.Name).To(Equal("web"))
		})
		It("should resolve a Job to its CronJob", func() {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: test.RandomName(), Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{*controllerRef("batch/v1", "CronJob", "etl", "cronjob-uid")},
				},
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "etl", Image: "etl"}},
				}}},
			}
			ExpectApplied(ctx, env.Client, job)
			// Deleting a Job orphans its pods by default, which needs the garbage collector envtest doesn't run
			DeferCleanup(func() {
				Expect(env.Client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
			})

			owner, ok, err := reader.ControllerOf(ctx, job.Namespace, controllerRef("batch/v1", "Job", job.Name, job.UID))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(owner.Kind).To(Equal("CronJob"))
		})
		It("should not resolve a reference to an earlier ReplicaSet with the same name", func() {
			rs := test.ReplicaSet()
			rs.OwnerReferences = []metav1.OwnerReference{*controllerRef("apps/v1", "Deployment", "web", "deployment-uid")}
			ExpectApplied(ctx, env.Client, rs)

			_, ok, err := reader.ControllerOf(ctx, rs.Namespace, controllerRef("apps/v1", "ReplicaSet", rs.Name, "earlier-uid"))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
		It("should not resolve a ReplicaSet that doesn't exist", func() {
			_, ok, err := reader.ControllerOf(ctx, "default", controllerRef("apps/v1", "ReplicaSet", "missing", "missing-uid"))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
		It("should not resolve a ReplicaSet without a controller", func() {
			rs := test.ReplicaSet()
			ExpectApplied(ctx, env.Client, rs)

			_, ok, err := reader.ControllerOf(ctx, rs.Namespace, controllerRef("apps/v1", "ReplicaSet", rs.Name, rs.UID))
			Expect(err).ToNot(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
		It("should error for a kind it doesn't resolve", func() {
			_, _, err := reader.ControllerOf(ctx, "default", controllerRef("apps/v1", "StatefulSet", "db", "uid"))
			Expect(err).To(HaveOccurred())
		})
	})
	Context("ScalableTemplate", func() {
		It("should return a copy of a ReplicaSet's template and its replicas", func() {
			rs := test.ReplicaSet()
			rs.Spec.Replicas = lo.ToPtr(int32(4))
			ExpectApplied(ctx, env.Client, rs)

			template, replicas, err := reader.ScalableTemplate(ctx, workloads.ReplicaSetKind, rs.Namespace, rs.Name)
			Expect(err).ToNot(HaveOccurred())
			Expect(replicas).To(Equal(int32(4)))
			template.Spec.Containers[0].Image = "mutated"

			template, _, err = reader.ScalableTemplate(ctx, workloads.ReplicaSetKind, rs.Namespace, rs.Name)
			Expect(err).ToNot(HaveOccurred())
			Expect(template.Spec.Containers[0].Image).ToNot(Equal("mutated"))
		})
		It("should error for a kind that isn't scalable", func() {
			_, _, err := reader.ScalableTemplate(ctx, workloads.JobKind, "default", "etl")
			Expect(err).To(HaveOccurred())
		})
	})
	Context("GroupKind", func() {
		It("should parse grouped and core API versions", func() {
			Expect(workloads.GroupKind(controllerRef("apps/v1", "ReplicaSet", "", ""))).To(Equal(workloads.ReplicaSetKind))
			Expect(workloads.GroupKind(controllerRef("v1", "ReplicationController", "", ""))).To(Equal(workloads.ReplicationControllerKind))
		})
	})
})
