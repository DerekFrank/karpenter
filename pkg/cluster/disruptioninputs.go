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

// DisruptionSource is the NodeClaim store narrowed to what disruption decides on, for the controllers that decide on
// it: disruption, and the pod deletion cost, which ranks the nodes as disruption would disrupt them. It enqueues every
// NodeClaim it holds, then each one whose inputs change. A managed node's Node and the pods bound to it are on its
// NodeClaim's entry, so the one store covers them.
func (n *NodeClaims) DisruptionSource() source.SyncingSource {
	return n.SourceOn(disruptionInputsOf)
}

// disruptionInputs are what disruption decides on about each NodeClaim's entry: whether it's a node yet and linked to
// its Node, and the conditions and intent that make it a candidate or keep it from being one; and of its Node, whether
// it has initialized, its do-not-disrupt annotation, how many repair policies its conditions match, and the pods bound
// to it: how many there are, which decides whether the Node is empty and what its consolidation reschedules; how many
// are terminating, which no longer reschedule; and how many carry the do-not-disrupt annotation, which blocks the
// Node. A NodeClaim's other changes, such as its nomination for a pending pod each scheduling pass, change none of
// them. A pod counts on its Node until it's terminal or gone, so a pod's status, its other labels and annotations, and
// pods waiting to schedule change nothing; nor does the Node's status heartbeat.
type disruptionInputs struct {
	Launched          bool
	Linked            bool
	Deleting          bool
	MarkedForDeletion bool
	Drifted           bool
	Consolidatable    bool
	DoNotDisrupt      bool
	BufferPods        bool

	Initialized      bool
	NodeDoNotDisrupt bool
	RepairMatches    int
	Pods             int
	Terminating      int
	DoNotDisruptPods int
}

func disruptionInputsOf(_ string, e NodeClaimEntry) any {
	nc := e.Object
	in := disruptionInputs{
		Launched:          nc.Status.ProviderID != "",
		Linked:            e.Derived.Node != nil,
		Deleting:          !nc.DeletionTimestamp.IsZero() || conditionTrue(nc, v1.ConditionTypeInstanceTerminating),
		MarkedForDeletion: e.Intent.Deletion.Value,
		Drifted:           conditionTrue(nc, v1.ConditionTypeDrifted),
		Consolidatable:    conditionTrue(nc, v1.ConditionTypeConsolidatable),
		DoNotDisrupt:      nc.Annotations[v1.DoNotDisruptAnnotationKey] == "true",
		BufferPods:        e.Intent.BufferPods.Value > 0,
	}
	if node := e.Derived.Node; node != nil {
		u := e.Derived.Usage
		in.Initialized = node.Labels[v1.NodeInitializedLabelKey] == "true"
		in.NodeDoNotDisrupt = node.Annotations[v1.DoNotDisruptAnnotationKey] == "true"
		in.RepairMatches = len(e.Derived.RepairPolicyMatches)
		in.Pods, in.Terminating, in.DoNotDisruptPods = u.Pods, u.Terminating, u.DoNotDisrupt
	}
	return in
}
