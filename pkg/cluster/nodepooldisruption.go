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
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
)

// NodePoolDisruption counts a NodePool's nodes for its disruption budgets, from the NodeClaim store: each NodeClaim's
// entry holds the Node its owner reference links, so one source decides all three counts.
type NodePoolDisruption struct {
	// Nodes counts the NodePool's launched NodeClaims that are initialized (their Initialized condition is true) or
	// rebooting, until the instance is terminating. A NodeClaim still initializing has no pods scheduled to it yet and
	// counts toward no budget, so that a replacement doesn't round a percentage budget up.
	Nodes int
	// Disrupting counts the NodeClaims among Nodes that are marked for deletion, deleting or rebooting, which every
	// disruption budget subtracts.
	Disrupting int
	// NotReady counts the NodeClaims among Nodes that aren't Disrupting and whose linked Node isn't Ready or is missing,
	// which every disruption budget but repair's subtracts.
	NotReady int
}

func (d NodePoolDisruption) Update(f NodePoolDisruption, prev NodePoolDisruption) NodePoolDisruption {
	f.Nodes += d.Nodes - prev.Nodes
	f.Disrupting += d.Disrupting - prev.Disrupting
	f.NotReady += d.NotReady - prev.NotReady
	return f
}

func (d NodePoolDisruption) Delete(f NodePoolDisruption) NodePoolDisruption {
	f.Nodes -= d.Nodes
	f.Disrupting -= d.Disrupting
	f.NotReady -= d.NotReady
	return f
}

func (d NodePoolDisruption) Equal(o NodePoolDisruption) bool { return d == o }

// nodePoolDisruptionOfNodeClaim returns the NodePool a NodeClaim counts toward for disruption budgets, or "" if it
// counts toward none. A rebooting NodeClaim counts until its reboot completes, though it's uninitialized meanwhile. A
// NodeClaim's instance is terminating only once the node is drained and the cloud provider deleted it.
func nodePoolDisruptionOfNodeClaim(_ string, f NodeClaimFields) string {
	if !f.launched() || conditionTrue(f.NodeClaim, v1.ConditionTypeInstanceTerminating) {
		return ""
	}
	if !conditionTrue(f.NodeClaim, v1.ConditionTypeInitialized) && !rebooting(f.NodeClaim) {
		return ""
	}
	return f.NodeClaim.Labels[v1.NodePoolLabelKey]
}

// nodeClaimDisruption returns a counted NodeClaim's contribution to its NodePool's disruption counts. A NodeClaim
// without a linked Node reads as not Ready: an initialized NodeClaim's Node has registered, so it has the owner
// reference that links it, and a missing one is gone.
func nodeClaimDisruption(_ string, f NodeClaimFields) NodePoolDisruption {
	if f.MarkedForDeletion || deleted(f.NodeClaim) || rebooting(f.NodeClaim) {
		return NodePoolDisruption{Nodes: 1, Disrupting: 1}
	}
	if f.Node == nil || nodeutils.GetCondition(f.Node, corev1.NodeReady).Status != corev1.ConditionTrue {
		return NodePoolDisruption{Nodes: 1, NotReady: 1}
	}
	return NodePoolDisruption{Nodes: 1}
}

// NodePoolDisruption returns the NodePool's nodes as its disruption budgets count them.
func (s *NodeState) NodePoolDisruption(nodePoolName string) NodePoolDisruption {
	d, _ := s.nodePoolDisruption.Get(nodePoolName)
	return d
}
