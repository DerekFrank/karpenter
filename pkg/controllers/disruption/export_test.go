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
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
)

// NewTestCandidate returns a Candidate of the NodeClaim entry, with only its node and its NodePool, for a spec that
// builds a command by hand.
func NewTestCandidate(entry karpentercluster.NodeClaimEntry, nodePool *v1.NodePool) *Candidate {
	c := newCandidateNode(entry)
	c.NodePool = nodePool
	return c
}

// testCandidate returns a Candidate with only its node, built from objects: a managed node of nodeClaim and its Node,
// or, with no NodeClaim, a Node alone.
func testCandidate(nodeClaim *v1.NodeClaim, node *corev1.Node) *Candidate {
	if nodeClaim == nil {
		return &Candidate{NodeView: karpentercluster.ViewOfNode(karpentercluster.NodeEntry{Object: node}), Node: node}
	}
	return newCandidateNode(karpentercluster.NodeClaimEntry{Object: nodeClaim, Derived: karpentercluster.NodeClaimDerived{Node: node}})
}

// Wakes is the sources Register wakes the controller on, without the NodePool watch, for tests to start on a queue.
func (c *Controller) Wakes() source.SyncingSource { return c.wakes() }

// RevalidationPeriod is revalidationPeriod, for tests.
const RevalidationPeriod = revalidationPeriod
