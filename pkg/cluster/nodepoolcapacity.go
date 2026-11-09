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

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Capacity returns the capacity counted against the NodePool's limits (NodePoolFields.Capacity).
func (n *NodePoolState) Capacity(nodePoolName string) corev1.ResourceList {
	return maps.Clone(n.Get(nodePoolName).Capacity)
}

// nodePoolCapacity is a launched NodeClaim's contribution to its NodePool's Capacity: its node's capacity, as
// StateNode.Capacity reads it from the NodeClaim and the Node on its entry, and one resources.Node.
type nodePoolCapacity corev1.ResourceList

func (n nodePoolCapacity) Update(f NodePoolFields, prev nodePoolCapacity) NodePoolFields {
	f.Capacity = replaceResources(f.Capacity, corev1.ResourceList(n), corev1.ResourceList(prev))
	return f
}

func (n nodePoolCapacity) Delete(f NodePoolFields) NodePoolFields {
	f.Capacity = replaceResources(f.Capacity, nil, corev1.ResourceList(n))
	return f
}

func (n nodePoolCapacity) Equal(o nodePoolCapacity) bool {
	return resourcesEqual(corev1.ResourceList(n), corev1.ResourceList(o))
}

// nodePoolOfCapacity is the NodePool a NodeClaim's capacity counts against: its node's NodePool label (the Node's once
// it registers) while it's launched, until it's marked for deletion, deleting or its instance is terminating.
func nodePoolOfCapacity(nodes *Nodes) func(string, NodeClaimEntry) string {
	return func(_ string, e NodeClaimEntry) string {
		if !launched(e) || e.Intent.Deletion.Value || deleted(e.Object) {
			return ""
		}
		return (&StateNode{Node: nodes.linked(e), NodeClaim: e.Object}).Labels()[v1.NodePoolLabelKey]
	}
}

func capacityOf(nodes *Nodes) func(string, NodeClaimEntry) nodePoolCapacity {
	return func(_ string, e NodeClaimEntry) nodePoolCapacity {
		return nodePoolCapacity((&StateNode{Node: nodes.linked(e), NodeClaim: e.Object}).Capacity())
	}
}
