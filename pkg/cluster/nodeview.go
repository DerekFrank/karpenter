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
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// NodeKey is a node's identity. Exactly one field is set: a managed node keys by its NodeClaim's name from the moment
// the NodeClaim is created, before it has a providerID or a Node, until its entry drops, so a Node arriving or leaving
// never changes the key; an unmanaged node keys by its Node's name. The providerID is never a key: it's missing before
// launch, and an index (NodeClaimsByProviderID).
type NodeKey struct {
	// NodeClaim is a managed node's NodeClaim's name.
	NodeClaim string
	// Node is an unmanaged node's Node's name, empty when NodeClaim is set.
	Node string
}

// String is the name the key holds.
func (k NodeKey) String() string { return lo.Ternary(k.NodeClaim != "", k.NodeClaim, k.Node) }

// NodeView is how a reader sees one node's facts: a managed node's NodeClaim and the Node its owner reference links, or
// an unmanaged Node, with the usage of the pods bound to the Node. Make one with ViewOfNodeClaim or ViewOfNode. It's a
// value of three pointers to what the stores hold, so it's cheap to pass, and it shares them: its NodeClaim, Node and
// usage are read-only. HostPortUsage and VolumeUsage return the view's own copies, which a scheduler adds simulated
// pods to.
type NodeView struct {
	nodeClaim *v1.NodeClaim
	node      *corev1.Node
	usage     *NodeUsage
}

// ViewOfNodeClaim returns the view of a managed node: its NodeClaim, and the Node and usage its entry carries, nil and
// empty while it's in flight or registering. (A method on NodeClaimEntry would be the natural spelling, but Go defines
// no methods on an alias of another package's instantiated generic type.)
func ViewOfNodeClaim(e NodeClaimEntry) NodeView {
	return NodeView{nodeClaim: e.Object, node: e.Derived.Node, usage: &e.Derived.Usage}
}

// ViewOfNode returns the view of an unmanaged Node, with the usage of its pods.
func ViewOfNode(e NodeEntry) NodeView {
	return NodeView{node: e.Object, usage: &e.Derived.Usage}
}

// Key is the node's identity: its NodeClaim's name for a managed node, its Node's name for an unmanaged one.
func (v NodeView) Key() NodeKey {
	if v.nodeClaim != nil {
		return NodeKey{NodeClaim: v.nodeClaim.Name}
	}
	return NodeKey{Node: v.node.Name}
}

// Managed returns whether Karpenter manages the node: it has a NodeClaim.
func (v NodeView) Managed() bool { return v.nodeClaim != nil }

// NodeName is the Node's name, or "" while the node is in flight. It's what pods' spec.nodeName, DRA slices and
// events compare against: a fact, not the node's identity, which is Key.
func (v NodeView) NodeName() string {
	if v.node == nil {
		return ""
	}
	return v.node.Name
}

// Name is the node's name for logs and messages: the Node's once the node is registered, and the NodeClaim's before.
// Address a node by Key, and match pods by NodeName.
func (v NodeView) Name() string {
	if v.node == nil || (v.nodeClaim != nil && !v.registered()) {
		return v.nodeClaim.Name
	}
	return v.node.Name
}

// NodeClaim is the managed node's NodeClaim, nil for an unmanaged node. It's the store's object: read it, and copy it
// before changing or writing it.
func (v NodeView) NodeClaim() *v1.NodeClaim { return v.nodeClaim }

// Node is the node's Node, nil while it's in flight. It's the store's object: read it, and copy it before changing or
// writing it.
func (v NodeView) Node() *corev1.Node { return v.node }

// ProviderID is the node's providerID: the Node's, or the NodeClaim's without a Node. An unmanaged Node without a
// providerID reads as its name.
func (v NodeView) ProviderID() string {
	if v.node == nil {
		return v.nodeClaim.Status.ProviderID
	}
	if v.node.Spec.ProviderID == "" {
		return v.node.Name
	}
	return v.node.Spec.ProviderID
}

// HostName is the node's hostname label, or its Name without one.
func (v NodeView) HostName() string {
	if h := v.Labels()[corev1.LabelHostname]; h != "" {
		return h
	}
	return v.Name()
}

// registered returns whether a managed node's Node carries the registered label; an unmanaged node is registered.
func (v NodeView) registered() bool { return registered(v.nodeClaim, v.node) }

// registered returns whether a managed node's Node carries the registered label; an unmanaged node, without a
// NodeClaim, is registered.
func registered(nodeClaim *v1.NodeClaim, node *corev1.Node) bool {
	if nodeClaim == nil {
		return true
	}
	return node != nil && node.Labels[v1.NodeRegisteredLabelKey] == "true"
}

// Initialized returns whether a managed node's Node carries the initialized label; an unmanaged node is initialized.
func (v NodeView) Initialized() bool {
	if v.nodeClaim == nil {
		return true
	}
	return v.node != nil && v.node.Labels[v1.NodeInitializedLabelKey] == "true"
}

// Labels are the NodeClaim's until the Node registers, then the Node's.
func (v NodeView) Labels() map[string]string { return nodeLabels(v.nodeClaim, v.node) }

// nodeLabels are the labels of the node a NodeClaim and its Node describe, either of which may be nil but not both:
// the NodeClaim's until the Node registers, then the Node's. NodeView and capacity quotas read labels through it, so
// they can't disagree.
func nodeLabels(nodeClaim *v1.NodeClaim, node *corev1.Node) map[string]string {
	if node == nil {
		return nodeClaim.Labels
	}
	if nodeClaim == nil || registered(nodeClaim, node) {
		return node.Labels
	}
	return nodeClaim.Labels
}

// Annotations are the NodeClaim's until the Node registers, then the Node's.
func (v NodeView) Annotations() map[string]string {
	if v.node == nil {
		return v.nodeClaim.Annotations
	}
	if v.nodeClaim == nil || v.registered() {
		return v.node.Annotations
	}
	return v.nodeClaim.Annotations
}

// RebootInProgress returns whether the node's NodeClaim carries an active (True) Rebooting condition: a reboot has been
// committed and hasn't reached a terminal outcome. It advertises the rebooting node's capacity as returning rather
// than gone, and excludes it from other disruption.
func (v NodeView) RebootInProgress() bool {
	return v.nodeClaim != nil && rebooting(v.nodeClaim)
}

// Taints are the NodeClaim's until a managed node's Node registers, then the Node's. While a managed node is
// uninitialized or rebooting, they leave out the well-known ephemeral taints, its startup taints and the reboot's
// scheduling fence: a scheduling simulation would otherwise treat those transient taints as permanent and assume pods
// can't schedule to the node, and the rebooting node's capacity is modeled as returning, so it isn't over-provisioned,
// though the real scheduler is fenced.
func (v NodeView) Taints() []corev1.Taint {
	var taints []corev1.Taint
	if (!v.registered() && v.Managed()) || v.node == nil {
		taints = v.nodeClaim.Spec.Taints
	} else {
		taints = v.node.Spec.Taints
	}
	if (!v.Initialized() || v.RebootInProgress()) && v.Managed() {
		return lo.Reject(taints, func(taint corev1.Taint, _ int) bool {
			if scheduling.IsKnownEphemeralTaint(&taint) || taint.MatchTaint(&v1.RebootingNoScheduleTaint) {
				return true
			}
			_, found := lo.Find(v.nodeClaim.Spec.StartupTaints, func(t corev1.Taint) bool { return t.MatchTaint(&taint) })
			return found
		})
	}
	return taints
}

// Capacity is the Node's capacity once a managed node initializes; before, the NodeClaim's in place of each zero or
// missing quantity of the Node's, or the NodeClaim's alone without a Node. It always counts one resources.Node.
func (v NodeView) Capacity() corev1.ResourceList {
	one := corev1.ResourceList{resources.Node: resource.MustParse("1")}
	if !v.Initialized() && v.nodeClaim != nil {
		if v.node != nil {
			return lo.Assign(overrideZero(v.node.Status.Capacity, v.nodeClaim.Status.Capacity), one)
		}
		return lo.Assign(v.nodeClaim.Status.Capacity, one)
	}
	return lo.Assign(v.node.Status.Capacity, one)
}

// Allocatable is the Node's allocatable once a managed node initializes; before, the NodeClaim's in place of each zero
// or missing quantity of the Node's, or the NodeClaim's alone without a Node.
func (v NodeView) Allocatable() corev1.ResourceList {
	if !v.Initialized() && v.nodeClaim != nil {
		if v.node != nil {
			return overrideZero(v.node.Status.Allocatable, v.nodeClaim.Status.Allocatable)
		}
		return v.nodeClaim.Status.Allocatable
	}
	return v.node.Status.Allocatable
}

// overrideZero returns a copy of base with each of overrides' quantities in place of a zero or missing one.
func overrideZero(base, overrides corev1.ResourceList) corev1.ResourceList {
	ret := lo.Assign(base)
	for name, quantity := range overrides {
		if resources.IsZero(ret[name]) {
			ret[name] = quantity
		}
	}
	return ret
}

// Available is Allocatable minus the requests of the pods bound to the Node.
func (v NodeView) Available() corev1.ResourceList {
	return resources.Subtract(v.Allocatable(), v.PodRequests())
}

// PodRequests are the requests of the pods bound to the Node, in a new list, or nil for none.
func (v NodeView) PodRequests() corev1.ResourceList {
	if len(v.usage.Requests) == 0 {
		return nil
	}
	return resources.Merge(v.usage.Requests)
}

// PodLimits are the limits of the pods bound to the Node, in a new list.
func (v NodeView) PodLimits() corev1.ResourceList { return resources.Merge(v.usage.Limits) }

// DaemonSetRequests are the part of PodRequests from DaemonSet pods, which identifies the resources future DaemonSets
// are expected to consume, in a new list.
func (v NodeView) DaemonSetRequests() corev1.ResourceList {
	return resources.Merge(v.usage.DaemonSetRequests)
}

// DaemonSetLimits are the part of PodLimits from DaemonSet pods, in a new list.
func (v NodeView) DaemonSetLimits() corev1.ResourceList {
	return resources.Merge(v.usage.DaemonSetLimits)
}

// DisruptionCost is the node's base disruption cost plus the positive eviction costs of its non-DaemonSet pods.
func (v NodeView) DisruptionCost() float64 {
	return disruptionutils.PerNodeBaseDisruptionCost + v.usage.DisruptionCost
}

// Usage is the usage of the pods bound to the Node, shared with the store: read it, never change it.
func (v NodeView) Usage() *NodeUsage { return v.usage }

// HostPortUsage returns a copy of the host ports the pods bound to the Node use, which the caller may add to.
func (v NodeView) HostPortUsage() *scheduling.HostPortUsage {
	if v.usage.HostPorts == nil {
		return scheduling.NewHostPortUsage()
	}
	return v.usage.HostPorts.DeepCopy()
}

// VolumeUsage returns the volumes the pods bound to the Node mount, by driver, against its CSINode's limits, built for
// the caller, who may add to it.
func (v NodeView) VolumeUsage() *scheduling.VolumeUsage { return v.usage.volumeUsage() }
