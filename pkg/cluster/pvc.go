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
type PVCDriver struct {
	Driver string
	// Resolved is true for every driver the store holds, so that a PVC whose volume no driver limits, "", isn't the
	// zero driver, which Feed drops.
	Resolved bool
}

// PVCs are the resolved CSI drivers of the PersistentVolumeClaims, by namespaced name: a fed store. A pod's
// contribution to its Node's usage reads its PVCs' drivers, so the pods are indexed by the namespace their PVCs are
// in, and a PVC's change Touches the pods of its namespace that mount it.
type PVCs struct {
	state.Reader[types.NamespacedName, PVCDriver, struct{}, struct{}]
	store *state.Store[types.NamespacedName, PVCDriver, struct{}, struct{}]
	// mounts indexes the pods that mount a PVC by namespace: its Sources are the dependents of a namespace's PVCs.
	mounts *state.Derivation[types.NamespacedName, string, int, mountsPVCs]
}

func newPVCs() *PVCs {
	store := state.NewFedStore[types.NamespacedName, PVCDriver, struct{}, struct{}]()
	return &PVCs{Reader: store.Reader, store: store}
}

// ResolvedPVCDriver is the driver the caller resolved, which the feed reports; "" is a PVC whose volume no driver
// limits, and is held, because Resolved makes it other than the zero object.
func ResolvedPVCDriver(driver string) PVCDriver {
	return PVCDriver{Driver: driver, Resolved: true}
}

// Feed reports the PVC's driver. The zero PVCDriver, which a feed reports for a PVC that's gone, drops the entry.
func (p *PVCs) Feed(pvc types.NamespacedName, driver PVCDriver) {
	p.store.Feed(pvc, driver)
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

// mountsPVCs is a pod that mounts a PVC's contribution to its namespace's count in the index. The pods themselves are
// the index Derive's sources.
type mountsPVCs int

func (m mountsPVCs) Update(f int, prev mountsPVCs) int { return f + int(m-prev) }
func (m mountsPVCs) Delete(f int) int                  { return f - int(m) }

// index keeps the pods that mount a PVC current with the PVCs' drivers: a Derive keyed by the namespace whose
// Sources are the pods, and a subscriber that Touches the pods that mount a PVC when its driver changes.
func (p *PVCs) index(pods *Pods) {
	namespaces := state.NewSyntheticStore[string, string, int, struct{}](func(ns string) string { return ns })
	p.mounts = state.DeriveObject(pods.Reader, namespaces,
		func(_ types.NamespacedName, pod *corev1.Pod) string {
			for _, v := range pod.Spec.Volumes {
				if volumeutil.PersistentVolumeClaimName(pod, v) != "" {
					return pod.Namespace
				}
			}
			return ""
		},
		func(types.NamespacedName, *corev1.Pod) mountsPVCs { return 1 })
	state.TouchOn(p.Reader, pods.store, func(c state.Change[types.NamespacedName, PVCDriver, struct{}, struct{}]) []types.NamespacedName {
		var mounting []types.NamespacedName
		for _, name := range p.mounts.Sources(c.Key.Namespace) {
			if e, ok := pods.Get(name); ok && mounts(e.Object, c.Key) {
				mounting = append(mounting, name)
			}
		}
		return mounting
	})
}

// mounts returns whether the pod mounts the PVC.
func mounts(pod *corev1.Pod, pvc types.NamespacedName) bool {
	if pod.Namespace != pvc.Namespace {
		return false
	}
	for _, v := range pod.Spec.Volumes {
		if volumeutil.PersistentVolumeClaimName(pod, v) == pvc.Name {
			return true
		}
	}
	return false
}
