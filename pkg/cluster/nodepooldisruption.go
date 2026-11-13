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

// NodePoolDisruption counts a NodePool's nodes for its disruption budgets. Each NodeClaim's entry holds the Node its
// owner reference links, so one source decides all three counts.
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

// Disruption returns the NodePool's nodes as its disruption budgets count them.
func (n *NodePoolState) Disruption(nodePoolName string) NodePoolDisruption {
	return n.Get(nodePoolName).Disruption
}

// countsForDisruption returns whether a NodeClaim counts toward its NodePool's disruption budgets. A rebooting
// NodeClaim counts until its reboot completes, though it's uninitialized meanwhile. A NodeClaim's instance is
// terminating only once the node is drained and the cloud provider deleted it.
func countsForDisruption(e NodeClaimEntry) bool {
	if !Launched(e) || conditionTrue(e.Object, v1.ConditionTypeInstanceTerminating) {
		return false
	}
	return conditionTrue(e.Object, v1.ConditionTypeInitialized) || rebooting(e.Object)
}

// nodeClaimDisruption returns a counted NodeClaim's contribution to its NodePool's disruption counts. A NodeClaim
// without a linked Node reads as not Ready: an initialized NodeClaim's Node has registered, so it has the owner
// reference that links it, and a missing one is gone.
func nodeClaimDisruption(e NodeClaimEntry, node *corev1.Node) NodePoolDisruption {
	if e.Intent.Deletion.Value || deleted(e.Object) || rebooting(e.Object) {
		return NodePoolDisruption{Nodes: 1, Disrupting: 1}
	}
	if node == nil || nodeutils.GetCondition(node, corev1.NodeReady).Status != corev1.ConditionTrue {
		return NodePoolDisruption{Nodes: 1, NotReady: 1}
	}
	return NodePoolDisruption{Nodes: 1}
}
