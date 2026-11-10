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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// newStatefulSetFixture is newFixture with each pod mounting pvcsPerPod PVCs of its own, as a StatefulSet's pods
// mount their volumeClaimTemplates, every PVC's driver resolved.
func newStatefulSetFixture(numNodes, podsPerNode, pvcsPerPod int) *fixture {
	f := newFixture(numNodes, podsPerNode)
	for _, pods := range f.pods {
		for _, pod := range pods {
			for k := range pvcsPerPod {
				claim := fmt.Sprintf("data-%d-%s", k, pod.Name)
				pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: fmt.Sprintf("data-%d", k), VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
				}})
				f.cluster.SetPVCDriver(types.NamespacedName{Namespace: pod.Namespace, Name: claim}, "ebs.csi.aws.com")
			}
			f.cache.put(pod)
		}
	}
	return f
}

// BenchmarkClusterIngestStatefulSet is BenchmarkClusterIngest with each pod mounting 3 PVCs.
func BenchmarkClusterIngestStatefulSet(b *testing.B) {
	const n = 1000
	for range b.N {
		b.StopTimer()
		f := newStatefulSetFixture(n, 30, 3)
		b.StartTimer()
		f.ingest()
		_ = f.cluster.DeepCopyNodes()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n*31)/1e3, "µs/event")
}

// BenchmarkClusterPodChurnStatefulSet is BenchmarkClusterPodChurn with each pod mounting 3 PVCs.
func BenchmarkClusterPodChurnStatefulSet(b *testing.B) {
	f := newStatefulSetFixture(1000, 30, 3)
	f.ingest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		pod := f.pods[i%len(f.pods)][i%30]
		f.cache.remove(pod)
		f.cluster.PodStore.Forget(client.ObjectKeyFromObject(pod))
		next := pod.DeepCopy()
		f.version(next)
		f.cache.put(next)
		f.cluster.PodStore.Observe(next)
	}
}

// BenchmarkClusterPVCDriverChange changes the driver of one PVC in a namespace of 5000 pods that each mount one: the
// cost of Touching the pods that mount it.
func BenchmarkClusterPVCDriverChange(b *testing.B) {
	f := newStatefulSetFixture(200, 25, 1)
	f.ingest()
	b.ReportAllocs()
	b.ResetTimer()
	pods := len(f.pods) * 25
	for i := range b.N {
		// Each PVC in turn, its driver alternating each round so that every change is one.
		pod := f.pods[(i%pods)/25][i%25]
		f.cluster.SetPVCDriver(types.NamespacedName{Namespace: pod.Namespace, Name: "data-0-" + pod.Name}, fmt.Sprintf("driver-%d", (i/pods)%2))
	}
}
