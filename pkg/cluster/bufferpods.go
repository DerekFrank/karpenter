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

// UpdateBufferPodCounts replaces the buffer pod counts with counts, by NodeClaim name. The provisioner calls it after
// each scheduling pass with the virtual buffer pods it placed on each node, counted from Results.ExistingNodes; a node
// missing from counts no longer hosts any. When real pods consume buffer capacity, the virtual pods move to other
// nodes or new NodeClaims, and the next pass's counts follow them.
func (s *NodeState) UpdateBufferPodCounts(counts map[string]int) {
	for name, f := range s.NodeClaimStore.List() {
		if _, ok := counts[name]; !ok && f.BufferPods != 0 {
			s.updateIntent(name, func(f NodeClaimFields) NodeClaimFields { f.BufferPods = 0; return f })
		}
	}
	for name, count := range counts {
		s.updateIntent(name, func(f NodeClaimFields) NodeClaimFields { f.BufferPods = count; return f })
	}
}

// HasBufferPods returns true if the NodeClaim's node has at least one virtual buffer pod placed on it during the last
// provisioning pass.
func (s *NodeState) HasBufferPods(nodeClaimName string) bool {
	return s.BufferPodCount(nodeClaimName) > 0
}

// BufferPodCount returns the number of virtual buffer pods on the NodeClaim's node.
func (s *NodeState) BufferPodCount(nodeClaimName string) int {
	f, _ := s.NodeClaimStore.Get(nodeClaimName)
	return f.BufferPods
}
