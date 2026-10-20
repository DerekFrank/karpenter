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

// Package workloads is the one way Karpenter reads the objects that control pods and carry their templates:
// ReplicaSets, Jobs, StatefulSets, ReplicationControllers, Deployments, DaemonSets and PodTemplates.
//
// Reads go straight to the manager's informer cache, by namespace and name, without deep copying. The cache already
// indexes objects by namespace and name, so a lookup is a map read, and nothing duplicates it. A lookup through an
// owner reference also checks the reference's UID, so a reference to an earlier object with the same name never
// resolves.
//
// Every object returned aliases the informer cache. DO NOT MUTATE it: copy anything that's modified or kept beyond
// the call.
package workloads

import (
	"context"
	"fmt"
	"strings"

	"github.com/awslabs/operatorpkg/serrors"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	ReplicaSetKind            = schema.GroupKind{Group: appsv1.GroupName, Kind: "ReplicaSet"}
	StatefulSetKind           = schema.GroupKind{Group: appsv1.GroupName, Kind: "StatefulSet"}
	DeploymentKind            = schema.GroupKind{Group: appsv1.GroupName, Kind: "Deployment"}
	DaemonSetKind             = schema.GroupKind{Group: appsv1.GroupName, Kind: "DaemonSet"}
	JobKind                   = schema.GroupKind{Group: batchv1.GroupName, Kind: "Job"}
	ReplicationControllerKind = schema.GroupKind{Group: corev1.GroupName, Kind: "ReplicationController"}
)

type Reader struct {
	kubeClient client.Reader
}

// NewReader reads through kubeClient, which must be backed by the informer cache for the kinds read; see Register.
func NewReader(kubeClient client.Reader) *Reader {
	return &Reader{kubeClient: kubeClient}
}

// GroupKind returns the reference's group and kind without allocating, since it runs on the scheduling hot path.
func GroupKind(ref *metav1.OwnerReference) schema.GroupKind {
	group, _, found := strings.Cut(ref.APIVersion, "/")
	if !found {
		// A core API version, e.g. "v1", has no group
		group = ""
	}
	return schema.GroupKind{Group: group, Kind: ref.Kind}
}

// get reads the object a reference in namespace points to, reporting false if it doesn't exist or is a different object
// with the same name.
func get[T any, PT interface {
	*T
	client.Object
}](ctx context.Context, c client.Reader, namespace string, ref *metav1.OwnerReference) (PT, bool, error) {
	obj := PT(new(T))
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, obj, client.UnsafeDisableDeepCopy); err != nil {
		if errors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if obj.GetUID() != ref.UID {
		return nil, false, nil
	}
	return obj, true, nil
}

func (r *Reader) ReplicaSet(ctx context.Context, namespace string, ref *metav1.OwnerReference) (*appsv1.ReplicaSet, bool, error) {
	return get[appsv1.ReplicaSet](ctx, r.kubeClient, namespace, ref)
}

func (r *Reader) StatefulSet(ctx context.Context, namespace string, ref *metav1.OwnerReference) (*appsv1.StatefulSet, bool, error) {
	return get[appsv1.StatefulSet](ctx, r.kubeClient, namespace, ref)
}

func (r *Reader) ReplicationController(ctx context.Context, namespace string, ref *metav1.OwnerReference) (*corev1.ReplicationController, bool, error) {
	return get[corev1.ReplicationController](ctx, r.kubeClient, namespace, ref)
}

func (r *Reader) Job(ctx context.Context, namespace string, ref *metav1.OwnerReference) (*batchv1.Job, bool, error) {
	return get[batchv1.Job](ctx, r.kubeClient, namespace, ref)
}

// ControllerOf returns the controller of the ReplicaSet or Job that ref, in namespace, points to.
func (r *Reader) ControllerOf(ctx context.Context, namespace string, ref *metav1.OwnerReference) (*metav1.OwnerReference, bool, error) {
	var obj metav1.Object
	var ok bool
	var err error
	switch GroupKind(ref) {
	case ReplicaSetKind:
		obj, ok, err = r.ReplicaSet(ctx, namespace, ref)
	case JobKind:
		obj, ok, err = r.Job(ctx, namespace, ref)
	default:
		return nil, false, serrors.Wrap(fmt.Errorf("resolving controller, unsupported kind"), "kind", ref.Kind)
	}
	if err != nil || !ok {
		return nil, false, err
	}
	controller := metav1.GetControllerOfNoCopy(obj)
	return controller, controller != nil, nil
}

// ScalableTemplate returns a copy of the pod template and the replica count of the Deployment, StatefulSet or
// ReplicaSet named name in namespace. Replicas defaults to 1.
func (r *Reader) ScalableTemplate(ctx context.Context, kind schema.GroupKind, namespace, name string) (*corev1.PodTemplateSpec, int32, error) {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	switch kind {
	case DeploymentKind:
		obj := &appsv1.Deployment{}
		if err := r.kubeClient.Get(ctx, key, obj, client.UnsafeDisableDeepCopy); err != nil {
			return nil, 0, err
		}
		return obj.Spec.Template.DeepCopy(), replicas(obj.Spec.Replicas), nil
	case StatefulSetKind:
		obj := &appsv1.StatefulSet{}
		if err := r.kubeClient.Get(ctx, key, obj, client.UnsafeDisableDeepCopy); err != nil {
			return nil, 0, err
		}
		return obj.Spec.Template.DeepCopy(), replicas(obj.Spec.Replicas), nil
	case ReplicaSetKind:
		obj := &appsv1.ReplicaSet{}
		if err := r.kubeClient.Get(ctx, key, obj, client.UnsafeDisableDeepCopy); err != nil {
			return nil, 0, err
		}
		return obj.Spec.Template.DeepCopy(), replicas(obj.Spec.Replicas), nil
	default:
		return nil, 0, serrors.Wrap(fmt.Errorf("unsupported scalable kind"), "kind", kind.Kind)
	}
}

// PodTemplate returns a copy of the template and the generation of the PodTemplate named name in namespace.
func (r *Reader) PodTemplate(ctx context.Context, namespace, name string) (*corev1.PodTemplateSpec, int64, error) {
	pt := &corev1.PodTemplate{}
	if err := r.kubeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, pt, client.UnsafeDisableDeepCopy); err != nil {
		return nil, 0, err
	}
	return pt.Template.DeepCopy(), pt.Generation, nil
}

// DaemonSets returns every DaemonSet. They alias the informer cache.
func (r *Reader) DaemonSets(ctx context.Context) ([]appsv1.DaemonSet, error) {
	list := &appsv1.DaemonSetList{}
	if err := r.kubeClient.List(ctx, list, client.UnsafeDisableDeepCopy); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func replicas(r *int32) int32 {
	if r == nil {
		return 1
	}
	return *r
}
