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
	"maps"
	"time"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/state"
)

// PodSchedulingFields are the fields Karpenter decides about a pod as it schedules it, keyed by its namespaced name.
// Each time is set once, the first time; they go with the pod when it's deleted or has started.
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

// newPodSchedulingStores returns the store of what the provisioner decides about each pod, and the store of the pods
// derived from it about each NodeClaim, by NodeClaim name: the pods last simulated to schedule against it.
func newPodSchedulingStores() (*state.Store[types.NamespacedName, PodSchedulingFields], *state.Store[string, map[types.NamespacedName]struct{}]) {
	pods := state.NewStore[types.NamespacedName, PodSchedulingFields]()
	nodeClaimPods := state.NewStore[string, map[types.NamespacedName]struct{}]()
	state.Derive(pods, nodeClaimPods,
		func(_ types.NamespacedName, f PodSchedulingFields) string { return f.NodeClaim },
		func(pod types.NamespacedName, _ PodSchedulingFields) podOnNodeClaim { return podOnNodeClaim{pod} })
	return pods, nodeClaimPods
}

// podOnNodeClaim is a pod's contribution to the NodeClaim it was simulated to schedule against: the pod's key.
type podOnNodeClaim struct{ pod types.NamespacedName }

func (p podOnNodeClaim) Update(pods map[types.NamespacedName]struct{}, prev podOnNodeClaim) map[types.NamespacedName]struct{} {
	pods = maps.Clone(pods)
	if pods == nil {
		pods = map[types.NamespacedName]struct{}{}
	}
	if prev != (podOnNodeClaim{}) {
		delete(pods, prev.pod)
	}
	pods[p.pod] = struct{}{}
	return pods
}

func (p podOnNodeClaim) Delete(pods map[types.NamespacedName]struct{}) map[types.NamespacedName]struct{} {
	pods = maps.Clone(pods)
	delete(pods, p.pod)
	return lo.Ternary(len(pods) == 0, nil, pods)
}
