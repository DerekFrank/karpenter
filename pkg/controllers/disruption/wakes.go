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

package disruption

import (
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
)

// revalidationPeriod is the longest the controller goes without running, and the longest a consolidation method skips
// its work while nothing it watches changes. Some inputs show only with time, or aren't in a store the controller
// watches: disruption budget schedules, repair policy tolerations, nominations lapsing, do-not-disrupt durations,
// PodDisruptionBudgets, instance type availability and prices, NodeOverlays and VPA predictions.
const revalidationPeriod = 5 * time.Minute

// changes counts the changes to the stores the disruption controller wakes on, so that a consolidation method that
// found nothing to do skips its work until one comes: it's the "woken since the method last ran" bit, kept per method
// as the count it last saw.
type changes struct{ n atomic.Uint64 }

func (c *changes) add()          { c.n.Add(1) }
func (c *changes) count() uint64 { return c.n.Load() }

// changeTracker is a method that skips its work while nothing changes, as consolidation does.
type changeTracker interface{ trackChanges(*changes) }

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

func nodeClaimInputsOf(_ string, e karpentercluster.NodeClaimEntry) any {
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

func nodeInputsOf(_ string, e karpentercluster.NodeEntry) any {
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

// wakes are the sources the controller wakes on. Each change of the NodeClaim and Node stores' inputs counts, as the
// sources enqueue it, so the run it wakes sees it; a NodeClaim launching, which can end the wait on NodeClaimsLaunched,
// doesn't count, since a launch also changes the NodeClaim's inputs.
func (c *Controller) wakes() source.SyncingSource {
	return karpenterstate.Wake(
		c.cluster.NodeClaimStore.UnlaunchedSource(),
		karpenterstate.WakeCounting(c.changes.add,
			c.cluster.NodeClaimStore.SourceOn(nodeClaimInputsOf),
			c.cluster.NodeStore.SourceOn(nodeInputsOf),
		),
	)
}

// conditionTrue returns whether the NodeClaim's condition is true, without StatusConditions, which would set the
// missing conditions on the NodeClaim the store shares.
func conditionTrue(nodeClaim *v1.NodeClaim, conditionType string) bool {
	for i := range nodeClaim.Status.Conditions {
		if nodeClaim.Status.Conditions[i].Type == conditionType {
			return nodeClaim.Status.Conditions[i].Status == metav1.ConditionTrue
		}
	}
	return false
}
