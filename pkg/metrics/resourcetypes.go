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

package metrics

import (
	corev1 "k8s.io/api/core/v1"
)

// ResourceTypeValues enumerates the well-known values of the `resource_type`
// dimension, referencing the corev1 resource-name consts as the source of truth.
// The set is not exhaustive — a metric may report any resource (extended
// resources, hugepages, etc.) — so this documents the common ones.
var ResourceTypeValues = []Value{
	{Name: string(corev1.ResourceCPU), Help: "CPU, in cores."},
	{Name: string(corev1.ResourceMemory), Help: "Memory, in bytes."},
	{Name: string(corev1.ResourcePods), Help: "The number of pods."},
	{Name: string(corev1.ResourceEphemeralStorage), Help: "Ephemeral storage, in bytes."},
}
