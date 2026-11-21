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
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// DisruptionSources are the NodeClaim and Node stores narrowed to what disruption decides on, for the controllers that
// decide on it: disruption, and the pod deletion cost, which ranks the nodes as disruption would disrupt them. Each
// enqueues every key it holds, then each key whose inputs change.
func (s *NodeState) DisruptionSources() []source.SyncingSource {
	return []source.SyncingSource{
		s.NodeClaimStore.SourceOn(nodeClaimInputsOf),
		s.NodeStore.SourceOn(nodeInputsOf),
	}
}

// nodeClaimInputs are what disruption decides on about each NodeClaim's entry: whether it's a node yet and linked to
// its Node, and the conditions and intent that make it a candidate or keep it from being one. A NodeClaim's other
// changes, such as its nomination for a pending pod each scheduling pass, change none of them.
type nodeClaimInputs struct {
	Launched          bool
	Linked            bool
	Deleting          bool
	MarkedForDeletion bool
	Drifted           bool
	Consolidatable    bool
	DoNotDisrupt      bool
	BufferPods        bool
}

func nodeClaimInputsOf(_ string, e NodeClaimEntry) any {
	nc := e.Object
	return nodeClaimInputs{
		Launched:          nc.Status.ProviderID != "",
		Linked:            e.Derived.Node != "",
		Deleting:          !nc.DeletionTimestamp.IsZero() || conditionTrue(nc, v1.ConditionTypeInstanceTerminating),
		MarkedForDeletion: e.Intent.Deletion.Value,
		Drifted:           conditionTrue(nc, v1.ConditionTypeDrifted),
		Consolidatable:    conditionTrue(nc, v1.ConditionTypeConsolidatable),
		DoNotDisrupt:      nc.Annotations[v1.DoNotDisruptAnnotationKey] == "true",
		BufferPods:        e.Intent.BufferPods.Value > 0,
	}
}

// nodeInputs are what disruption decides on about each Node's entry: whether it has initialized, its do-not-disrupt
// annotation, how many repair policies its conditions match, and the pods bound to it: how many there are, which
// decides whether the Node is empty and what its consolidation reschedules; how many are terminating, which no longer
// reschedule; and how many carry the do-not-disrupt annotation, which blocks the Node. A pod counts on its Node until
// it's terminal or gone, so a pod's status, its other labels and annotations, and pods waiting to schedule change
// nothing; nor does the Node's status heartbeat.
type nodeInputs struct {
	Initialized      bool
	DoNotDisrupt     bool
	RepairMatches    int
	Pods             int
	Terminating      int
	DoNotDisruptPods int
}

func nodeInputsOf(_ string, e NodeEntry) any {
	u := e.Derived.Usage
	return nodeInputs{
		Initialized:      e.Object.Labels[v1.NodeInitializedLabelKey] == "true",
		DoNotDisrupt:     e.Object.Annotations[v1.DoNotDisruptAnnotationKey] == "true",
		RepairMatches:    len(e.Derived.RepairPolicyMatches),
		Pods:             u.Pods,
		Terminating:      u.Terminating,
		DoNotDisruptPods: u.DoNotDisrupt,
	}
}
