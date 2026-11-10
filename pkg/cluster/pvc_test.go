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
	"context"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("PVCs", func() {
	var s *NodeState
	var rv int
	// mounting returns a pod in namespace bound to Node n that mounts each of claims.
	mounting := func(namespace, name string, claims ...string) *corev1.Pod {
		rv++
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(namespace + "/" + name), ResourceVersion: strconv.Itoa(rv)},
			Spec:       corev1.PodSpec{NodeName: "n", Containers: []corev1.Container{{Name: "c"}}},
		}
		for _, claim := range claims {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: claim, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
			}})
		}
		return pod
	}
	pvc := func(name string) types.NamespacedName { return types.NamespacedName{Namespace: "default", Name: name} }
	volumes := func() map[volume]int {
		e, _ := s.NodeStore.Get("n")
		return e.Derived.Usage.Volumes
	}
	BeforeEach(func() {
		ctx := options.ToContext(context.Background(), test.Options())
		s = NewNodeState(ctx, clock.RealClock{}, nil, nil, fake.NewCloudProvider(), nil)
	})
	It("should count a pod that mounts several PVCs on each of them", func() {
		s.SetPVCDriver(pvc("p1"), "x")
		s.SetPVCDriver(pvc("p2"), "y")
		s.PodStore.Observe(mounting("default", "a", "p1", "p2"))
		for _, name := range []string{"p1", "p2"} {
			e, _ := s.PVCStore.Get(pvc(name))
			Expect(e.Derived).To(Equal(1), "pods that mount %s, want a counted once", name)
			Expect(s.PVCStore.mounts.Sources(pvc(name))).To(ConsistOf(types.NamespacedName{Namespace: "default", Name: "a"}), "sources of %s", name)
		}
		Expect(volumes()).To(Equal(map[volume]int{{PVC: pvc("p1"), Driver: "x"}: 1, {PVC: pvc("p2"), Driver: "y"}: 1}), "the Node's volumes")
		s.SetPVCDriver(pvc("p2"), "z")
		Expect(volumes()).To(Equal(map[volume]int{{PVC: pvc("p1"), Driver: "x"}: 1, {PVC: pvc("p2"), Driver: "z"}: 1}), "the Node's volumes once p2's driver changed")
		s.PodStore.Observe(mounting("default", "a", "p1"))
		Expect(s.PVCStore.mounts.Sources(pvc("p2"))).To(BeEmpty(), "sources of p2 once a stopped mounting it")
		Expect(volumes()).To(Equal(map[volume]int{{PVC: pvc("p1"), Driver: "x"}: 1}), "the Node's volumes once a stopped mounting p2")
	})
	It("should Touch exactly the pods that mount a PVC whose driver changes", func() {
		touched := []types.NamespacedName{}
		s.PodStore.Subscribe(func(c state.Change[types.NamespacedName, *corev1.Pod, struct{}, struct{}]) {
			if c.Existed && c.Exists && c.Old == c.New {
				touched = append(touched, c.Key)
			}
		})
		s.PodStore.Observe(mounting("default", "a", "p1", "p2"))
		s.PodStore.Observe(mounting("default", "b", "p2"))
		s.PodStore.Observe(mounting("default", "c", "p3"))
		s.PodStore.Observe(mounting("default", "d"))
		s.PodStore.Observe(mounting("other", "e", "p1"))
		Expect(touched).To(BeEmpty(), "touched by pods starting to mount PVCs, want none: no pod reads the counts")
		s.SetPVCDriver(pvc("p1"), "x")
		Expect(touched).To(ConsistOf(types.NamespacedName{Namespace: "default", Name: "a"}), "touched once p1 resolved")
		touched = touched[:0]
		s.SetPVCDriver(pvc("p2"), "x")
		Expect(touched).To(ConsistOf(types.NamespacedName{Namespace: "default", Name: "a"}, types.NamespacedName{Namespace: "default", Name: "b"}), "touched once p2 resolved")
		touched = touched[:0]
		s.SetPVCDriver(pvc("p2"), "x")
		Expect(touched).To(BeEmpty(), "touched once p2 resolved to the driver it had")
		s.ForgetPVC(pvc("p2"))
		Expect(touched).To(ConsistOf(types.NamespacedName{Namespace: "default", Name: "a"}, types.NamespacedName{Namespace: "default", Name: "b"}), "touched once p2 was forgotten")
	})
})
