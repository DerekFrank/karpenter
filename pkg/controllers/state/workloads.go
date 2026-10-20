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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// workload is the state tracked for a ReplicaSet or Job: what's needed to resolve its pods to the workload controlling
// it and, for a ReplicaSet, the selector default topology spread folds in.
type workload struct {
	controller *metav1.OwnerReference
	selector   *metav1.LabelSelector
}

// workloadIndex tracks ReplicaSets and Jobs by UID, which pods' controller references carry, so a lookup is a single
// map read and a reference to an earlier object with the same name never resolves. Names map to UIDs for deletes,
// which only know the name.
type workloadIndex struct {
	mu             sync.RWMutex
	byUID          map[types.UID]*workload
	replicaSetUIDs map[types.NamespacedName]types.UID
	jobUIDs        map[types.NamespacedName]types.UID
}

func newWorkloadIndex() *workloadIndex {
	return &workloadIndex{
		byUID:          map[types.UID]*workload{},
		replicaSetUIDs: map[types.NamespacedName]types.UID{},
		jobUIDs:        map[types.NamespacedName]types.UID{},
	}
}

func (w *workloadIndex) update(uids map[types.NamespacedName]types.UID, o client.Object, selector *metav1.LabelSelector) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := client.ObjectKeyFromObject(o)
	// A recreated object with the same name replaces the earlier one
	if uid, ok := uids[key]; ok && uid != o.GetUID() {
		delete(w.byUID, uid)
	}
	uids[key] = o.GetUID()
	w.byUID[o.GetUID()] = &workload{controller: metav1.GetControllerOf(o), selector: selector.DeepCopy()}
}

func (w *workloadIndex) delete(uids map[types.NamespacedName]types.UID, key types.NamespacedName) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if uid, ok := uids[key]; ok {
		delete(w.byUID, uid)
		delete(uids, key)
	}
}

func (w *workloadIndex) get(uid types.UID) (*workload, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	workload, ok := w.byUID[uid]
	return workload, ok
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
	c.workloads.update(c.workloads.replicaSetUIDs, rs, rs.Spec.Selector)
}

func (c *Cluster) DeleteReplicaSet(key types.NamespacedName) {
	c.workloads.delete(c.workloads.replicaSetUIDs, key)
}

func (c *Cluster) UpdateJob(job client.Object) {
	c.workloads.update(c.workloads.jobUIDs, job, nil)
}

func (c *Cluster) DeleteJob(key types.NamespacedName) {
	c.workloads.delete(c.workloads.jobUIDs, key)
}

// ControllerOf returns the controller of the tracked ReplicaSet or Job that ref points to.
// DO NOT MUTATE the returned reference.
func (c *Cluster) ControllerOf(ref *metav1.OwnerReference) (*metav1.OwnerReference, bool) {
	w, ok := c.workloads.get(ref.UID)
	if !ok || w.controller == nil {
		return nil, false
	}
	return w.controller, true
}

// ReplicaSetSelector returns a copy of the selector of the tracked ReplicaSet that ref points to.
func (c *Cluster) ReplicaSetSelector(ref *metav1.OwnerReference) (*metav1.LabelSelector, bool) {
	w, ok := c.workloads.get(ref.UID)
	if !ok || w.selector == nil {
		return nil, false
	}
	return w.selector.DeepCopy(), true
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
		if !c.workloads.tracksAll(c.workloads.replicaSetUIDs, uidsOf(list.Items)) {
			return false, nil
		}
	}
	if c.trackJobs.Load() {
		list := &metav1.PartialObjectMetadataList{}
		list.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("JobList"))
		if err := c.kubeClient.List(ctx, list, client.UnsafeDisableDeepCopy); err != nil {
			return false, err
		}
		if !c.workloads.tracksAll(c.workloads.jobUIDs, uidsOf(list.Items)) {
			return false, nil
		}
	}
	return true, nil
}

// tracksAll reports whether every object in want is tracked under the same UID.
func (w *workloadIndex) tracksAll(uids map[types.NamespacedName]types.UID, want map[types.NamespacedName]types.UID) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for key, uid := range want {
		if uids[key] != uid {
			return false
		}
	}
	return true
}

func uidsOf[T any, PT interface {
	*T
	client.Object
}](items []T) map[types.NamespacedName]types.UID {
	uids := make(map[types.NamespacedName]types.UID, len(items))
	for i := range items {
		o := PT(&items[i])
		uids[client.ObjectKeyFromObject(o)] = o.GetUID()
	}
	return uids
}
