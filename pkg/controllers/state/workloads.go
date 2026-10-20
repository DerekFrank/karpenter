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

package state

import (
	"context"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	replicaSetGroupKind = schema.GroupKind{Group: appsv1.GroupName, Kind: "ReplicaSet"}
	jobGroupKind        = schema.GroupKind{Group: batchv1.GroupName, Kind: "Job"}
)

// workload is the state tracked for a ReplicaSet or Job: what's needed to resolve its pods to the workload controlling
// it and, for a ReplicaSet, the selector default topology spread folds in.
type workload struct {
	uid        types.UID
	controller *metav1.OwnerReference
	selector   *metav1.LabelSelector
}

// TrackReplicaSets makes Synced wait until every ReplicaSet in the api-server is tracked. Call it before the manager
// starts when registering the ReplicaSet informer.
func (c *Cluster) TrackReplicaSets() {
	c.trackReplicaSets.Store(true)
}

// TrackJobs makes Synced wait until every Job in the api-server is tracked. Call it before the manager starts when
// registering the Job informer.
func (c *Cluster) TrackJobs() {
	c.trackJobs.Store(true)
}

func (c *Cluster) UpdateReplicaSet(rs *appsv1.ReplicaSet) {
	c.replicaSets.Store(client.ObjectKeyFromObject(rs), newWorkload(rs, rs.Spec.Selector))
}

func (c *Cluster) DeleteReplicaSet(key types.NamespacedName) {
	c.replicaSets.Delete(key)
}

func (c *Cluster) UpdateJob(job client.Object) {
	c.jobs.Store(client.ObjectKeyFromObject(job), newWorkload(job, nil))
}

func (c *Cluster) DeleteJob(key types.NamespacedName) {
	c.jobs.Delete(key)
}

func newWorkload(o metav1.Object, selector *metav1.LabelSelector) *workload {
	return &workload{uid: o.GetUID(), controller: metav1.GetControllerOf(o), selector: selector.DeepCopy()}
}

// ControllerOf returns the controller of the ReplicaSet or Job that ref points to, if it's tracked.
// DO NOT MUTATE the returned reference.
func (c *Cluster) ControllerOf(namespace string, ref *metav1.OwnerReference) (*metav1.OwnerReference, bool) {
	w, ok := c.workload(namespace, ref)
	if !ok || w.controller == nil {
		return nil, false
	}
	return w.controller, true
}

// ReplicaSetSelector returns a copy of the selector of the ReplicaSet that ref points to, if it's tracked.
func (c *Cluster) ReplicaSetSelector(namespace string, ref *metav1.OwnerReference) (*metav1.LabelSelector, bool) {
	w, ok := c.workload(namespace, ref)
	if !ok {
		return nil, false
	}
	return w.selector.DeepCopy(), true
}

func (c *Cluster) workload(namespace string, ref *metav1.OwnerReference) (*workload, bool) {
	var workloads *sync.Map
	switch schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).GroupKind() {
	case replicaSetGroupKind:
		workloads = &c.replicaSets
	case jobGroupKind:
		workloads = &c.jobs
	default:
		return nil, false
	}
	v, ok := workloads.Load(types.NamespacedName{Namespace: namespace, Name: ref.Name})
	if !ok {
		return nil, false
	}
	w := v.(*workload)
	// The reference can point to an earlier object with the same name
	if w.uid != ref.UID {
		return nil, false
	}
	return w, true
}

// workloadsSynced reports whether every tracked kind's objects in the api-server are tracked in state. It lists through
// the same informers that feed state, typed for ReplicaSets and metadata-only for Jobs, so it doesn't start new ones.
func (c *Cluster) workloadsSynced(ctx context.Context) (bool, error) {
	if c.trackReplicaSets.Load() {
		// DO NOT MUTATE ReplicaSets in this function as this will affect the underlying cached ReplicaSet
		list := &appsv1.ReplicaSetList{}
		if err := c.kubeClient.List(ctx, list, client.UnsafeDisableDeepCopy); err != nil {
			return false, err
		}
		keys := sets.New[types.NamespacedName]()
		for i := range list.Items {
			keys.Insert(client.ObjectKeyFromObject(&list.Items[i]))
		}
		if !tracksAll(&c.replicaSets, keys) {
			return false, nil
		}
	}
	if c.trackJobs.Load() {
		list := &metav1.PartialObjectMetadataList{}
		list.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("JobList"))
		if err := c.kubeClient.List(ctx, list, client.UnsafeDisableDeepCopy); err != nil {
			return false, err
		}
		keys := sets.New[types.NamespacedName]()
		for i := range list.Items {
			keys.Insert(client.ObjectKeyFromObject(&list.Items[i]))
		}
		if !tracksAll(&c.jobs, keys) {
			return false, nil
		}
	}
	return true, nil
}

func tracksAll(workloads *sync.Map, keys sets.Set[types.NamespacedName]) bool {
	for key := range keys {
		if _, ok := workloads.Load(key); !ok {
			return false
		}
	}
	return true
}
