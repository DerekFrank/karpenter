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

// Package cluster stands in for pkg/cluster: it builds the shared stores, and only State.Register adds them.
package cluster

import (
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/karpenter/pkg/state"
)

type NodePoolUsage struct {
	NodeClaims *state.Store[string, int]
}

type State struct {
	NodePoolUsage *NodePoolUsage
}

func New() State {
	nodeClaims := state.NewStore[string, int]()
	state.Derive(nodeClaims, state.NewStore[string, bool]())
	return State{NodePoolUsage: &NodePoolUsage{NodeClaims: nodeClaims}}
}

func (s State) Register(m manager.Manager) error {
	return m.Add(s.NodePoolUsage.NodeClaims)
}

func (s State) Start(m manager.Manager) error {
	return m.Add(s.NodePoolUsage.NodeClaims) // want `s.NodePoolUsage.NodeClaims is a shared store: only cluster.State.Register adds it to the manager`
}
