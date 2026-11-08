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
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/events"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

//go:generate go tool -modfile=../../go.tools.mod controller-gen object:headerFile="../../hack/boilerplate.go.txt" paths="."

// StateNodes is a typed version of a list of *Node
// nolint: revive
type StateNodes []*StateNode

// Active filters StateNodes that are not in a MarkedForDeletion state
func (n StateNodes) Active() StateNodes {
	return lo.Filter(n, func(node *StateNode, _ int) bool {
		return !node.MarkedForDeletion()
	})
}

// Deleting filters StateNodes that are in a MarkedForDeletion state
func (n StateNodes) Deleting() StateNodes {
	return lo.Filter(n, func(node *StateNode, _ int) bool {
		return node.MarkedForDeletion()
	})
}

// Pods gets the pods assigned to all StateNodes based on the kubernetes api-server bindings
func (n StateNodes) Pods(ctx context.Context, kubeClient client.Client) ([]*corev1.Pod, error) {
	var pods []*corev1.Pod
	for _, node := range n {
		p, err := node.Pods(ctx, kubeClient)
		if err != nil {
			return nil, err
		}
		pods = append(pods, p...)
	}
	return pods, nil
}

func (n StateNodes) CurrentlyReschedulablePods(ctx context.Context, kubeClient client.Client, clk clock.Clock, recorder events.Recorder) ([]*corev1.Pod, error) {
	var pods []*corev1.Pod
	for _, node := range n {
		p, err := node.CurrentlyReschedulablePods(ctx, kubeClient, clk, recorder)
		if err != nil {
			return nil, err
		}
		pods = append(pods, p...)
	}
	return pods, nil
}

// StateNode is a view of one node Karpenter schedules onto or disrupts, built at read time from cluster state's
// stores: a NodeClaim with the intent Karpenter keeps about it and its linked Node, if any, or an unmanaged Node that
// has no NodeClaim. The linked Node is the one whose owner reference names the NodeClaim, or else, before registration
// adds the owner reference, the Node with the NodeClaim's providerID.
//
// The NodeClaim's name keys the intent Karpenter keeps about a node (MarkForDeletion, NominateNodeForPod, buffer pod
// counts, NodePool membership): pass NodeClaim.Name to those. Name is for display and logs: the Node's name once the
// node is registered, and the NodeClaim's before.
// +k8s:deepcopy-gen=true
// nolint: revive
type StateNode struct {
	Node      *corev1.Node
	NodeClaim *v1.NodeClaim

	// Derived are the fields Karpenter derives about the node from the pods bound to its Node. They come from their own
	// store, so they can lag Node and NodeClaim, which come from theirs.
	Derived NodeDerived

	// markedForDeletion and nominatedUntil are the intent kept with the NodeClaim; an unmanaged Node has none.
	markedForDeletion bool
	nominatedUntil    metav1.Time

	// repairPolicyMatches are the repair policies matching the Node's conditions, recomputed when they change.
	repairPolicyMatches []repair.RepairPolicyMatch
}

// NodeDerived are the fields Karpenter derives about a node from the pods bound to it.
// +k8s:deepcopy-gen=true
type NodeDerived struct {
	PodRequests corev1.ResourceList
	PodLimits   corev1.ResourceList
	// DaemonSetRequests and DaemonSetLimits are the part of PodRequests and PodLimits from DaemonSet pods, which lets
	// users of the node identify the resources future DaemonSets are expected to consume.
	DaemonSetRequests corev1.ResourceList
	DaemonSetLimits   corev1.ResourceList
	// DisruptionCost sums the positive eviction costs of the node's non-DaemonSet pods.
	DisruptionCost float64
	HostPorts      *scheduling.HostPortUsage
	Volumes        *scheduling.VolumeUsage
}

func NewNode() *StateNode {
	return &StateNode{Derived: NodeDerived{HostPorts: scheduling.NewHostPortUsage(), Volumes: scheduling.NewVolumeUsage()}}
}

func (in *StateNode) ShallowCopy() *StateNode {
	return &StateNode{
		Node:                in.Node,
		NodeClaim:           in.NodeClaim,
		Derived:             in.Derived,
		markedForDeletion:   in.markedForDeletion,
		nominatedUntil:      in.nominatedUntil,
		repairPolicyMatches: in.repairPolicyMatches,
	}
}

// GetRepairResult returns the repair decision for the Node at now, from the policy matches the Node informer keeps
// current. An empty Action means no policy applies or none has waited out its toleration yet. Tolerations are measured
// from no earlier than the end of the last reboot, since a node rejoins uninitialized after a successful reboot and its
// conditions may not have caught up yet.
func (in *StateNode) GetRepairResult(now time.Time) repair.RepairResult {
	if in.Node == nil {
		return repair.RepairResult{}
	}
	var rebootFinishedAt time.Time
	if in.NodeClaim != nil {
		for i := range in.NodeClaim.Status.Conditions {
			if c := in.NodeClaim.Status.Conditions[i]; c.Type == v1.ConditionTypeRebooting && c.Status == metav1.ConditionFalse {
				rebootFinishedAt = c.LastTransitionTime.Time
			}
		}
	}
	return repair.Resolve(in.repairPolicyMatches, now, rebootFinishedAt)
}

// Name is the Node's name once the node is registered, and the NodeClaim's before, so it stays the same name for a
// registered node across reads. Key intent by NodeClaim.Name, not Name.
func (in *StateNode) Name() string {
	if in.Node == nil {
		return in.NodeClaim.Name
	}
	if in.NodeClaim == nil {
		return in.Node.Name
	}
	if !in.Registered() {
		return in.NodeClaim.Name
	}
	return in.Node.Name
}

// ProviderID is the node's providerID: the Node's, or the NodeClaim's without a Node. An unmanaged Node without a
// providerID reads as its name.
func (in *StateNode) ProviderID() string {
	if in.Node == nil {
		return in.NodeClaim.Status.ProviderID
	}
	if in.Node.Spec.ProviderID == "" {
		return in.Node.Name
	}
	return in.Node.Spec.ProviderID
}

// Pods gets the pods assigned to the Node based on the kubernetes api-server bindings
func (in *StateNode) Pods(ctx context.Context, kubeClient client.Client) ([]*corev1.Pod, error) {
	if in.Node == nil {
		return nil, nil
	}
	return nodeutils.GetPods(ctx, kubeClient, in.Node.Name)
}

// CurrentlyReschedulablePods gets the pods assigned to the Node that are currently reschedulable based on the kubernetes api-server bindings
func (in *StateNode) CurrentlyReschedulablePods(ctx context.Context, kubeClient client.Client, clk clock.Clock, recorder events.Recorder) ([]*corev1.Pod, error) {
	if in.Node == nil {
		return nil, nil
	}
	return nodeutils.GetCurrentlyReschedulablePods(ctx, kubeClient, clk, recorder, in.Node)
}

func (in *StateNode) HostName() string {
	if in.Labels()[corev1.LabelHostname] == "" {
		return in.Name()
	}
	return in.Labels()[corev1.LabelHostname]
}

func (in *StateNode) Annotations() map[string]string {
	// If the nodeclaim exists and the state node isn't initialized
	// use the nodeclaim representation of the annotations
	if in.Node == nil {
		return in.NodeClaim.Annotations
	}
	if in.NodeClaim == nil {
		return in.Node.Annotations
	}
	if !in.Registered() {
		return in.NodeClaim.Annotations
	}
	return in.Node.Annotations
}

func (in *StateNode) Labels() map[string]string {
	// If the nodeclaim exists and the state node isn't registered
	// use the nodeclaim representation of the labels
	if in.Node == nil {
		return in.NodeClaim.Labels
	}
	if in.NodeClaim == nil {
		return in.Node.Labels
	}
	if !in.Registered() {
		return in.NodeClaim.Labels
	}
	return in.Node.Labels
}

// RebootInProgress returns true if the node's NodeClaim carries an active (True) Rebooting condition,
// i.e. a reboot has been committed and has not yet reached a terminal outcome. It is used to advertise
// the rebooting node's capacity as returning (rather than gone) and to exclude it from other disruption.
func (in *StateNode) RebootInProgress() bool {
	if in.NodeClaim == nil {
		return false
	}
	return rebooting(in.NodeClaim)
}

func (in *StateNode) Taints() []corev1.Taint {
	// If we have a managed node that isn't registered, we should use its NodeClaim
	// representation of taints. Likewise, if we don't have a Node representation for this
	// providerID in our state, we should also just use the NodeClaim since this is all that we have
	var taints []corev1.Taint
	if (!in.Registered() && in.Managed()) || in.Node == nil {
		taints = in.NodeClaim.Spec.Taints
	} else {
		taints = in.Node.Spec.Taints
	}
	if (!in.Initialized() || in.RebootInProgress()) && in.Managed() {
		// We reject well-known ephemeral taints, startup taints, and the reboot-owned scheduling fence
		// while the node is uninitialized or rebooting. Without this the scheduling simulation would
		// treat these transient taints as permanent and incorrectly assume pods can't schedule onto the
		// node in the future. Stripping the reboot taint here lets the rebooting node's capacity be
		// modeled as returning (so we don't over-provision) even though the real scheduler is fenced.
		return lo.Reject(taints, func(taint corev1.Taint, _ int) bool {
			if scheduling.IsKnownEphemeralTaint(&taint) {
				return true
			}
			if taint.MatchTaint(&v1.RebootingNoScheduleTaint) {
				return true
			}
			if _, found := lo.Find(in.NodeClaim.Spec.StartupTaints, func(t corev1.Taint) bool {
				return t.MatchTaint(&taint)
			}); found {
				return true
			}
			return false
		})
	}
	return taints
}

func (in *StateNode) Registered() bool {
	// Node is managed by Karpenter, so we can check for the Registered label
	if in.Managed() {
		return in.Node != nil && in.Node.Labels[v1.NodeRegisteredLabelKey] == "true"
	}
	// Nodes not managed by Karpenter are always considered Registered
	return true
}

func (in *StateNode) Initialized() bool {
	// Node is managed by Karpenter, so we can check for the Initialized label
	if in.Managed() {
		return in.Node != nil && in.Node.Labels[v1.NodeInitializedLabelKey] == "true"
	}
	// Nodes not managed by Karpenter are always considered Initialized
	return true
}

func (in *StateNode) Capacity() corev1.ResourceList {
	if !in.Initialized() && in.NodeClaim != nil {
		// Override any zero quantity values in the node status
		if in.Node != nil {
			ret := lo.Assign(in.Node.Status.Capacity)
			for resourceName, quantity := range in.NodeClaim.Status.Capacity {
				if resources.IsZero(ret[resourceName]) {
					ret[resourceName] = quantity
				}
			}
			// A StateNode will always have a capacity of 1 node.
			return lo.Assign(ret, corev1.ResourceList{resources.Node: resource.MustParse("1")})
		}
		return lo.Assign(in.NodeClaim.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")})
	}
	return lo.Assign(in.Node.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")})
}

func (in *StateNode) Allocatable() corev1.ResourceList {
	if !in.Initialized() && in.NodeClaim != nil {
		// Override any zero quantity values in the node status
		if in.Node != nil {
			ret := lo.Assign(in.Node.Status.Allocatable)
			for resourceName, quantity := range in.NodeClaim.Status.Allocatable {
				if resources.IsZero(ret[resourceName]) {
					ret[resourceName] = quantity
				}
			}
			return ret
		}
		return in.NodeClaim.Status.Allocatable
	}
	return in.Node.Status.Allocatable
}

// Available is allocatable minus anything allocated to pods.
func (in *StateNode) Available() corev1.ResourceList {
	return resources.Subtract(in.Allocatable(), in.PodRequests())
}

func (in *StateNode) DaemonSetRequests() corev1.ResourceList {
	return resources.Merge(in.Derived.DaemonSetRequests)
}

func (in *StateNode) DaemonSetLimits() corev1.ResourceList {
	return resources.Merge(in.Derived.DaemonSetLimits)
}

func (in *StateNode) HostPortUsage() *scheduling.HostPortUsage {
	return in.Derived.HostPorts
}

func (in *StateNode) VolumeUsage() *scheduling.VolumeUsage {
	return in.Derived.Volumes
}

func (in *StateNode) PodRequests() corev1.ResourceList {
	if len(in.Derived.PodRequests) == 0 {
		return nil
	}
	return resources.Merge(in.Derived.PodRequests)
}

func (in *StateNode) PodLimits() corev1.ResourceList {
	return resources.Merge(in.Derived.PodLimits)
}

// DisruptionCost returns the exact disruption cost for this node:
// This is maintained incrementally as pods are added/removed.
func (in *StateNode) DisruptionCost() float64 {
	return disruptionutils.PerNodeBaseDisruptionCost + in.Derived.DisruptionCost
}

func (in *StateNode) MarkedForDeletion() bool {
	// The Node is marked for deletion if:
	//  1. The Node has MarkedForDeletion set
	//  2. The Node has a NodeClaim counterpart and is actively deleting (or the nodeclaim is marked as terminating)
	//  3. The Node has no NodeClaim counterpart and is actively deleting
	return in.markedForDeletion || in.Deleted()
}

func (in *StateNode) Deleted() bool {
	return (in.NodeClaim != nil && deleted(in.NodeClaim)) ||
		(in.Node != nil && in.NodeClaim == nil && !in.Node.DeletionTimestamp.IsZero())
}

func (in *StateNode) Nominated(clk clock.Clock) bool {
	return in.nominatedUntil.After(clk.Now())
}

func (in *StateNode) Managed() bool {
	return in.NodeClaim != nil
}

func nominationWindow(ctx context.Context) time.Duration {
	nominationPeriod := max(2*options.FromContext(ctx).BatchMaxDuration, 10*time.Second)
	return nominationPeriod
}
