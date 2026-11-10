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
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

// PVCDriver is a PersistentVolumeClaim's CSI driver, which Karpenter resolves with I/O from the PVC's
// PersistentVolume or StorageClass: a synthetic object, fed by the PVC informer controller. Driver is empty for a PVC
// whose volume no driver limits.
type PVCDriver struct{ Driver string }

// PVCs are the resolved CSI drivers of the PersistentVolumeClaims, by namespaced name: a fed store. A pod's
// contribution to its Node's usage reads its PVCs' drivers, so each PVC's entry counts the pods that mount it, and a
// change of its driver Touches them.
type PVCs struct {
	state.Reader[types.NamespacedName, PVCDriver, int, struct{}]
	store *state.Store[types.NamespacedName, PVCDriver, int, struct{}]
	// mounts counts each pod on every PVC it mounts: its Sources are the dependents of a PVC's driver.
	mounts *state.EachDerivation[types.NamespacedName, types.NamespacedName, int, mountsPVC]
}

func newPVCs() *PVCs {
	store := state.NewFedStore[types.NamespacedName, PVCDriver, int, struct{}]()
	return &PVCs{Reader: store.Reader, store: store}
}

// Resolve sets the PVC's driver, which the caller resolved; "" is a PVC whose volume no driver limits.
func (p *PVCs) Resolve(pvc types.NamespacedName, driver string) {
	p.store.Feed(pvc, func(PVCDriver, bool) (PVCDriver, bool) { return PVCDriver{Driver: driver}, true })
}

// Forget drops the PVC's driver.
func (p *PVCs) Forget(pvc types.NamespacedName) {
	p.store.Feed(pvc, func(d PVCDriver, _ bool) (PVCDriver, bool) { return d, false })
}

// Resolved returns whether cluster state holds the PVC's driver.
func (p *PVCs) Resolved(pvc types.NamespacedName) bool {
	_, ok := p.Get(pvc)
	return ok
}

func (p *PVCs) driver(pvc types.NamespacedName) string {
	e, _ := p.Get(pvc)
	return e.Object.Driver
}

// mountsPVC is a pod's contribution to the count of the pods that mount a PVC, on the PVC's entry. The pods
// themselves are the index's sources.
type mountsPVC int

func (m mountsPVC) Update(f int, prev mountsPVC) int { return f + int(m-prev) }
func (m mountsPVC) Delete(f int) int                 { return f - int(m) }

// index keeps the pods that mount a PVC current with its driver: a DeriveEach onto each PVC a pod mounts, whose
// Sources are the pods, and a subscriber that Touches them when the PVC's driver changes.
func (p *PVCs) index(pods *Pods) {
	p.mounts = state.DeriveEachObject(pods.Reader, p.store,
		func(_ types.NamespacedName, pod *corev1.Pod) []types.NamespacedName {
			var pvcs []types.NamespacedName
			for _, v := range pod.Spec.Volumes {
				if name := volumeutil.PersistentVolumeClaimName(pod, v); name != "" {
					pvcs = append(pvcs, types.NamespacedName{Namespace: pod.Namespace, Name: name})
				}
			}
			return pvcs
		},
		func(types.NamespacedName, *corev1.Pod, types.NamespacedName) mountsPVC { return 1 })
	p.Subscribe(func(c state.Change[types.NamespacedName, PVCDriver, int, struct{}]) {
		// A change of the count alone is a pod starting or stopping to mount the PVC, which no pod's contribution reads.
		if c.Existed == c.Exists && c.Old.Object == c.New.Object {
			return
		}
		for _, name := range p.mounts.Sources(c.Key) {
			pods.store.Touch(name)
		}
	})
}
