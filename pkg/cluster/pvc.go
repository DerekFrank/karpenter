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
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state"
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

// PVCDriver is a PersistentVolumeClaim's CSI driver, which Karpenter resolves with I/O from the PVC's
// PersistentVolume or StorageClass: a synthetic object, fed by the PVC driver controller. Driver is empty for a PVC
// whose volume no driver limits.
type PVCDriver struct {
	Driver string
	// Resolved is true for every driver the store holds, so that a PVC whose volume no driver limits, "", isn't the
	// zero driver, which Feed drops.
	Resolved bool
}

// PVCs are the resolved CSI drivers of the PersistentVolumeClaims, by namespaced name: a fed store. A pod's
// contribution to its Node's usage reads its PVCs' drivers, so the pods depend on the PVCs they mount
// (state.DependsOn): a PVC's change recomputes the pods that mount it.
type PVCs struct {
	state.Reader[types.NamespacedName, PVCDriver, struct{}, struct{}]
	store *state.Store[types.NamespacedName, PVCDriver, struct{}, struct{}]
}

func newPVCs() *PVCs {
	store := state.NewFedStore[types.NamespacedName, PVCDriver, struct{}, struct{}]()
	return &PVCs{Reader: store.Reader, store: store}
}

// Resolve resolves the PVC's CSI driver from its PersistentVolume or StorageClass, read through kubeClient, and feeds
// it. "" is a PVC whose volume no driver limits, and is held, because Resolved makes it other than the zero object.
func (p *PVCs) Resolve(ctx context.Context, kubeClient client.Client, pvc *corev1.PersistentVolumeClaim) error {
	driver, err := scheduling.ResolveDriver(ctx, kubeClient, nil, "", pvc, lo.FromPtr(pvc.Spec.StorageClassName))
	if err != nil {
		return fmt.Errorf("tracking volume usage, %w", err)
	}
	p.store.Feed(client.ObjectKeyFromObject(pvc), PVCDriver{Driver: driver, Resolved: true})
	return nil
}

// Forget drops the driver of a PVC that's gone: the feed of the zero PVCDriver.
func (p *PVCs) Forget(pvc types.NamespacedName) {
	p.store.Feed(pvc, PVCDriver{})
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

// dependOn keeps the pods' contributions current with the drivers of the PVCs they mount: an Index of the pods by
// each PVC they mount, and DependsOn, which recomputes the pods that mount a PVC when its driver changes.
func (p *PVCs) dependOn(pods *Pods) {
	mountsPVC := state.NewIndex(pods.store, func(_ types.NamespacedName, e PodEntry) []types.NamespacedName {
		var pvcs []types.NamespacedName
		for _, v := range e.Object.Spec.Volumes {
			if name := volumeutil.PersistentVolumeClaimName(e.Object, v); name != "" {
				pvcs = append(pvcs, types.NamespacedName{Namespace: e.Object.Namespace, Name: name})
			}
		}
		return pvcs
	})
	state.DependsOn(pods.store, p.Reader, mountsPVC)
}
