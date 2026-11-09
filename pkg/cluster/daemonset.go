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

package cluster

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/state"
)

// DaemonSet is a DaemonSet as its pods describe it, by namespaced name: a synthetic object, built from the key while a
// pod names it as its controller. Cluster state reads no DaemonSet object; a DaemonSet's pods sample its template.
type DaemonSet struct{ types.NamespacedName }

// DaemonSetDerived are the fields derived about a DaemonSet from its pods.
type DaemonSetDerived struct {
	// Pods counts the DaemonSet's pods. A pod counts until it's gone, or until the garbage collector orphans it from a
	// DaemonSet deleted without its pods, so a DaemonSet's entry goes with its last pod.
	Pods int
}

// DaemonSets are the DaemonSets cluster state sees through their pods: a synthetic store. Each DaemonSet's newest pod,
// a sample of its current template, is chosen at read time from the Derivation's Sources.
type DaemonSets struct {
	store *state.Store[types.NamespacedName, DaemonSet, DaemonSetDerived, struct{}]
	pods  *Pods
	// members is the Derive from the pod store, which knows each DaemonSet's pods.
	members *state.Derivation[types.NamespacedName, types.NamespacedName, DaemonSetDerived, daemonSetPod]
}

func newDaemonSets(pods *Pods) *DaemonSets {
	store := state.NewSyntheticStore[types.NamespacedName, DaemonSet, DaemonSetDerived, struct{}](func(k types.NamespacedName) DaemonSet {
		return DaemonSet{NamespacedName: k}
	})
	return &DaemonSets{
		store:   store,
		pods:    pods,
		members: state.DeriveObject(pods.Reader, store, daemonSetKey, func(types.NamespacedName, *corev1.Pod) daemonSetPod { return 1 }),
	}
}

// daemonSetPod is a pod's contribution to its DaemonSet: one pod.
type daemonSetPod int

func (p daemonSetPod) Update(f DaemonSetDerived, prev daemonSetPod) DaemonSetDerived {
	f.Pods += int(p - prev)
	return f
}

func (p daemonSetPod) Delete(f DaemonSetDerived) DaemonSetDerived {
	f.Pods -= int(p)
	return f
}

func daemonSetKey(_ types.NamespacedName, pod *corev1.Pod) types.NamespacedName {
	if owner := metav1.GetControllerOf(pod); owner != nil && owner.Kind == "DaemonSet" {
		return types.NamespacedName{Namespace: pod.Namespace, Name: owner.Name}
	}
	return types.NamespacedName{}
}

// GetDaemonSetPod returns a copy of the DaemonSet's newest pod, or nil if cluster state holds none.
func (d *DaemonSets) GetDaemonSetPod(daemonset *appsv1.DaemonSet) *corev1.Pod {
	var newest *corev1.Pod
	for _, name := range d.members.Sources(client.ObjectKeyFromObject(daemonset)) {
		e, ok := d.pods.Get(name)
		if !ok {
			continue
		}
		if newest == nil || e.Object.CreationTimestamp.After(newest.CreationTimestamp.Time) ||
			(e.Object.CreationTimestamp.Equal(&newest.CreationTimestamp) && e.Object.Name > newest.Name) {
			newest = e.Object
		}
	}
	if newest == nil {
		return nil
	}
	return newest.DeepCopy()
}
