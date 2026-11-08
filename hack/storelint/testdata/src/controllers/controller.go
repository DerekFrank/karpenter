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

// Package controllers holds a controller that builds a local store, and the places it may not.
package controllers

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/state"
)

type Controller struct {
	usage *cluster.NodePoolUsage
	local *state.Store[string, int]
}

func NewController(usage *cluster.NodePoolUsage) *Controller {
	return &Controller{usage: usage, local: state.NewStore[string, int]()} // want `state.NewStore builds a store`
}

// Register builds the controller's local store and adds it, but not the shared store it derives from.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	c.local = state.NewStore[string, int]()
	state.Derive(c.usage.NodeClaims, c.local)
	func() { state.Derive(c.usage.NodeClaims, state.NewStore[string, bool]()) }()
	if err := m.Add(c.local); err != nil {
		return err
	}
	return m.Add(c.usage.NodeClaims) // want `c.usage.NodeClaims is a shared store`
}

type Other struct{}

// Register without a context isn't a controller's Register.
func (Other) Register(manager.Manager) error {
	_ = state.NewObjectStore[int]() // want `state.NewObjectStore builds a store`
	return nil
}

func helper() {
	_ = state.NewJoinStore( // want `state.NewJoinStore builds a store`
		state.Join(func(int) string { return "" }, func(f bool, _ int) bool { return f })) // want `state.Join builds a store`
}

var _ = state.NewStore[string, string]() // want `state.NewStore builds a store`
