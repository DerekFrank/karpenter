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

// Package cluster is a stand-in for pkg/cluster: it builds the shared stores, and only its State.Register adds them to
// the manager.
package cluster

import (
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/karpenter/pkg/state"
)

// Fields are a store's Derived fields: each collection says what bounds it.
type Fields struct {
	Names map[string]int // bounded: names per pool
	Pods  []string       // want `Fields.Pods is a collection in a store's fields`
	Usage Usage
}

// Usage is nested in Fields, so its collections are a store's fields too.
type Usage struct {
	// bounded: resources
	Resources map[string]int
	Ports     []int // want `Usage.Ports is a collection in a store's fields`
}

// Unrelated is no store's fields.
type Unrelated struct{ Pods []string }

type NodePoolUsage struct {
	NodeClaims state.Reader[string, string, Fields, struct{}]
	store      *state.Store[string, string, Fields, struct{}]
	Raw        *state.Store[string, string, int, struct{}] // want `NodePoolUsage.Raw exports a raw store`
}

// Store returns the raw store.
func (u *NodePoolUsage) Store() *state.Store[string, string, Fields, struct{}] { return u.store } // want `Store returns a raw store`

type State struct {
	NodePoolUsage *NodePoolUsage
}

func New() State {
	nodeClaims := state.NewSyntheticStore[string, string, Fields, struct{}](func(k string) string { return k })
	d := state.Derive(nodeClaims.Reader, state.NewSyntheticStore[string, string, bool, struct{}](func(k string) string { return k }))
	_ = d.Sources("")
	nodeClaims.Subscribe(func() {})
	return State{NodePoolUsage: &NodePoolUsage{NodeClaims: nodeClaims.Reader, store: nodeClaims}}
}

func (s State) Register(m manager.Manager) error {
	return m.Add(s.NodePoolUsage.store)
}

func (s State) Start(m manager.Manager) error {
	return m.Add(s.NodePoolUsage.store) // want `s.NodePoolUsage.store is a shared store: only cluster.State.Register adds it to the manager`
}

type NodeClaim struct{ Conditions []string }

func (n *NodeClaim) DeepCopy() *NodeClaim {
	return &NodeClaim{Conditions: append([]string(nil), n.Conditions...)}
}

func (n *NodeClaim) StatusConditions(observedOnly ...bool) []string { return n.Conditions }

type StateNode struct{ NodeClaim *NodeClaim }
