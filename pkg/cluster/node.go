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
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/state"
)

// NodeDerived are the fields derived about a Node from other stores, and from its own conditions.
type NodeDerived struct {
	// Usage is what the pods bound to the Node use of it, contributed by the pod store keyed by spec.nodeName. Pods
	// bound before cluster state sees the Node wait on its key, so they count once it does.
	Usage NodeUsage
	// RepairPolicyMatches are the repair policies the Node's conditions match, regardless of toleration, while cluster
	// state tracks it, contributed by the Node store itself when node repair is enabled.
	RepairPolicyMatches []repair.RepairPolicyMatch // bounded: repair policies
	// CSIDrivers are the drivers of the Node's CSINode, contributed by the CSINode store.
	CSIDrivers []storagev1.CSINodeDriver // bounded: CSI drivers per node
}

// NodeEntry is a Node's entry: the Node, and what its pods, its CSINode and its conditions contribute.
type NodeEntry = state.Entry[*corev1.Node, NodeDerived, struct{}]

type nodeStore = state.Store[string, *corev1.Node, NodeDerived, struct{}]

// Nodes are the Nodes, by name: an object store. A managed Node is read through its NodeClaim's entry, which names
// it; an unmanaged Node has no owning NodeClaim and no karpenter.sh/nodepool label, and is read from this store.
type Nodes struct {
	state.Reader[string, *corev1.Node, NodeDerived, struct{}]
	store *nodeStore
}

// newNodes returns the Node store. A Node is cluster-scoped, so its name is its key.
func newNodes(from state.Newest) *Nodes {
	store := state.NewObjectStore[string, *corev1.Node, NodeDerived, struct{}](from, state.WithKey(state.ByName))
	return &Nodes{Reader: store.Reader, store: store}
}

// Tracked returns whether cluster state reads a Node: an unmanaged Node always, and a managed one (with the NodePool
// label) once it has a providerID and either its instance type label or has initialized, so that a Node that's
// registering isn't read before its labels propagate. An untracked Node links to no NodeClaim.
func Tracked(node *corev1.Node) bool {
	if node.Labels[v1.NodePoolLabelKey] == "" {
		return true
	}
	return node.Spec.ProviderID != "" && (node.Labels[corev1.LabelInstanceTypeStable] != "" || node.Labels[v1.NodeInitializedLabelKey] != "")
}

// ownerNodeClaim returns the name of the NodeClaim in the Node's owner references, which registration sets
// (nodeclaimutils.UpdateNodeOwnerReferences), or "" for none.
func ownerNodeClaim(node *corev1.Node) string {
	for _, o := range node.OwnerReferences {
		if o.Kind == "NodeClaim" && strings.HasPrefix(o.APIVersion, apis.Group+"/") {
			return o.Name
		}
	}
	return ""
}

// Unmanaged returns whether a Node is unmanaged: it has neither a NodeClaim owner reference nor the NodePool label. A
// Node with the label and no owner reference is registering: its NodeClaim's entry already represents it.
func Unmanaged(node *corev1.Node) bool {
	return ownerNodeClaim(node) == "" && node.Labels[v1.NodePoolLabelKey] == ""
}

// repairMatches are a tracked Node's contribution to its own entry: the repair policies its conditions match.
type repairMatches []repair.RepairPolicyMatch

func (m repairMatches) Update(f NodeDerived, _ repairMatches) NodeDerived {
	f.RepairPolicyMatches = m
	return f
}

func (m repairMatches) Delete(f NodeDerived) NodeDerived {
	f.RepairPolicyMatches = nil
	return f
}

func (m repairMatches) Equal(o repairMatches) bool {
	return slices.EqualFunc(m, o, repair.RepairPolicyMatch.Equal)
}

// deriveRepairMatches matches each tracked Node against the repair policies as it changes. Matching only runs for a
// Node with a condition a policy covers.
func (n *Nodes) deriveRepairMatches(matcher *repair.RepairPolicyMatcher) {
	state.DeriveObject(n.Reader, n.store,
		func(name string, node *corev1.Node) string {
			if !Tracked(node) || !matcher.MatchesAny(node) {
				return ""
			}
			return name
		},
		func(_ string, node *corev1.Node) repairMatches { return matcher.Match(node) })
}

// csiDrivers are a CSINode's contribution to its Node's entry: its drivers.
type csiDrivers []storagev1.CSINodeDriver

func (d csiDrivers) Update(f NodeDerived, _ csiDrivers) NodeDerived {
	f.CSIDrivers = d
	return f
}

func (d csiDrivers) Delete(f NodeDerived) NodeDerived {
	f.CSIDrivers = nil
	return f
}

// CSINodes are the CSINodes, by name, which is their Node's: an object store. Each one's drivers count the volumes its
// Node can attach, on its Node's entry.
type CSINodes struct {
	state.Reader[string, *storagev1.CSINode, struct{}, struct{}]
	store *state.Store[string, *storagev1.CSINode, struct{}, struct{}]
}

func newCSINodes(from state.Newest) *CSINodes {
	store := state.NewObjectStore[string, *storagev1.CSINode, struct{}, struct{}](from, state.WithKey(state.ByName))
	return &CSINodes{Reader: store.Reader, store: store}
}

// deriveDrivers carries each CSINode's drivers to its Node's entry.
func (c *CSINodes) deriveDrivers(nodes *Nodes) {
	state.DeriveObject(c.Reader, nodes.store,
		func(name string, _ *storagev1.CSINode) string { return name },
		func(_ string, csiNode *storagev1.CSINode) csiDrivers { return csiDrivers(csiNode.Spec.Drivers) })
}
