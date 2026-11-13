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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/state"
)

// PodEntry is a pod's entry.
type PodEntry = state.Entry[*corev1.Pod, struct{}, struct{}]

// Pods are the pods, by namespaced name: an object store.
type Pods struct {
	state.Reader[types.NamespacedName, *corev1.Pod, struct{}, struct{}]
	store *state.Store[types.NamespacedName, *corev1.Pod, struct{}, struct{}]
}

func newPods(from state.Newest) *Pods {
	store := state.NewObjectStore[types.NamespacedName, *corev1.Pod, struct{}, struct{}](from)
	return &Pods{Reader: store.Reader, store: store}
}
