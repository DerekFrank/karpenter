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
)

// daemonSetFields are the fields derived about a DaemonSet from its pods.
type daemonSetFields struct {
	// Pod is the DaemonSet's newest pod.
	Pod *corev1.Pod
}

// newestPod is a pod's contribution to its DaemonSet: the DaemonSet's newest pod, as a sample of its current template.
// A pod going away leaves the sample in place until a newer one arrives or the DaemonSet goes, so Delete doesn't undo
// Update, unlike a Contribution that counts.
type newestPod struct{ pod *corev1.Pod }

func (n newestPod) Update(f daemonSetFields, _ newestPod) daemonSetFields {
	if f.Pod == nil || n.pod.UID == f.Pod.UID || n.pod.CreationTimestamp.After(f.Pod.CreationTimestamp.Time) {
		f.Pod = n.pod
	}
	return f
}

func (n newestPod) Delete(f daemonSetFields) daemonSetFields { return f }
func (n newestPod) Equal(o newestPod) bool                   { return n.pod == o.pod }

func daemonSetKey(_ types.NamespacedName, pod *corev1.Pod) types.NamespacedName {
	if owner := metav1.GetControllerOf(pod); owner != nil && owner.Kind == "DaemonSet" {
		return types.NamespacedName{Namespace: pod.Namespace, Name: owner.Name}
	}
	return types.NamespacedName{}
}

// GetDaemonSetPod returns a copy of the DaemonSet's newest pod, or nil if cluster state hasn't seen one.
func (s *NodeState) GetDaemonSetPod(daemonset *appsv1.DaemonSet) *corev1.Pod {
	f, _ := s.daemonSetPods.Get(client.ObjectKeyFromObject(daemonset))
	if f.Pod == nil {
		return nil
	}
	return f.Pod.DeepCopy()
}

// DeleteDaemonSet drops the DaemonSet's pod.
func (s *NodeState) DeleteDaemonSet(key types.NamespacedName) {
	s.daemonSetPods.Update(key, func(daemonSetFields) daemonSetFields { return daemonSetFields{} })
}
