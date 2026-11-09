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
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/state"
)

// PodSchedulingFields are what Karpenter decides about a pod as it schedules it, keyed by its namespaced name. Each
// time is set once, the first time; they go with the pod when it's deleted or has started.
type PodSchedulingFields struct {
	// AckedAt is when the provisioner first saw the pod as pending.
	AckedAt time.Time
	// DecidedAt is when the provisioner first decided whether the pod could schedule.
	DecidedAt time.Time
	// SchedulableAt is when a scheduling simulation first fit the pod to a node, until one fails it.
	SchedulableAt time.Time
	// NodeClaim is the NodeClaim the most recent scheduling simulation placed the pod on.
	NodeClaim string
}

// PodScheduling keeps what the provisioner decides about each pod as it schedules it, for the pod metrics: a fed store
// of PodSchedulingFields, a synthetic object, by pod, fed by the provisioner and the pod metrics. It's not node state:
// it's keyed by pod, and by the NodeClaim a pod was last simulated against, and no node read needs it.
type PodScheduling struct {
	pods *state.Store[types.NamespacedName, PodSchedulingFields, struct{}, struct{}]
	// nodeClaims is the Derive keyed by the NodeClaim each pod was last simulated against, whose Sources are the pods.
	nodeClaims *state.Derivation[types.NamespacedName, string, int, podOnNodeClaim]
}

// NewPodScheduling returns empty pod scheduling state.
func NewPodScheduling() *PodScheduling {
	pods := state.NewFedStore[types.NamespacedName, PodSchedulingFields, struct{}, struct{}]()
	nodeClaims := state.NewSyntheticStore[string, string, int, struct{}](func(name string) string { return name })
	return &PodScheduling{
		pods: pods,
		nodeClaims: state.DeriveObject(pods.Reader, nodeClaims,
			func(_ types.NamespacedName, f PodSchedulingFields) string { return f.NodeClaim },
			func(types.NamespacedName, PodSchedulingFields) podOnNodeClaim { return 1 }),
	}
}

// Get returns what Karpenter decided about the pod.
func (p *PodScheduling) Get(pod types.NamespacedName) PodSchedulingFields {
	e, _ := p.pods.Get(pod)
	return e.Object
}

// Update replaces what Karpenter decided about the pod with what update returns of it. The zero fields drop the pod.
func (p *PodScheduling) Update(pod types.NamespacedName, update func(PodSchedulingFields) PodSchedulingFields) {
	p.pods.Feed(pod, func(f PodSchedulingFields, _ bool) (PodSchedulingFields, bool) {
		f = update(f)
		return f, f != PodSchedulingFields{}
	})
}

// PodsOn returns the pods last simulated to schedule against the NodeClaim.
func (p *PodScheduling) PodsOn(nodeClaimName string) []types.NamespacedName {
	return p.nodeClaims.Sources(nodeClaimName)
}

// Clear empties the store in place, so that its subscribers stay subscribed.
func (p *PodScheduling) Clear() { p.pods.Clear() }

// podOnNodeClaim is a pod's contribution to the NodeClaim it was simulated to schedule against: one pod. The pods
// themselves are the Derive's sources.
type podOnNodeClaim int

func (p podOnNodeClaim) Update(f int, prev podOnNodeClaim) int { return f + int(p-prev) }
func (p podOnNodeClaim) Delete(f int) int                      { return f - int(p) }
