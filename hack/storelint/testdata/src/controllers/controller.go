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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/state"
)

type Controller struct {
	usage *cluster.NodePoolUsage
	local *state.Store[string, string, int, struct{}]
	// wakes is a Derive target read only as a source.
	wakes *state.Store[string, string, int, struct{}]
	// read is a Derive target the controller reads.
	read *state.Store[string, string, int, struct{}]
}

func newLocal() *state.Store[string, string, int, struct{}] {
	return state.NewSyntheticStore[string, string, int, struct{}](func(k string) string { return k }) // want `state.NewSyntheticStore builds a store`
}

func NewController(usage *cluster.NodePoolUsage) *Controller {
	usage.NodeClaims.Subscribe(func() {}) // want `Subscribe in a constructor`
	return &Controller{
		usage: usage,
		local: state.NewSyntheticStore[string, string, int, struct{}](func(k string) string { return k }), // want `state.NewSyntheticStore in a constructor`
	}
}

// NewMoving builds a store it's being moved off, on a line that says so.
func NewMoving() *state.Store[string, string, int, struct{}] {
	//storelint:allow moving to pkg/cluster
	return state.NewSyntheticStore[string, string, int, struct{}](func(k string) string { return k })
}

// Register builds the controller's local stores and adds one, but not the shared store it derives from.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	c.local = state.NewSyntheticStore[string, string, int, struct{}](func(k string) string { return k })
	c.wakes = state.NewSyntheticStore[string, string, int, struct{}](func(k string) string { return k })
	c.read = state.NewSyntheticStore[string, string, int, struct{}](func(k string) string { return k })
	state.Derive(c.usage.NodeClaims, c.wakes) // want `wakes is read only as a source`
	state.Derive(c.usage.NodeClaims, c.read)
	_ = c.wakes.Source()
	c.usage.NodeClaims.Subscribe(func() {})
	if err := m.Add(c.local); err != nil {
		return err
	}
	return m.Add(c.usage.Store()) // want `c.usage.Store\(\) is a shared store`
}

// Reconcile reads what Register derived.
func (c *Controller) Reconcile() Result {
	_, _ = c.read.Get("a")
	return Result{RequeueAfter: time.Minute} // want `RequeueAfter in a controller that reads stores`
}

// Expire requeues at a deadline, on a line that says so.
func (c *Controller) Expire() Result {
	return Result{RequeueAfter: time.Minute} //storelint:deadline a reservation's TTL
}

func (c *Controller) Requeue(r *Result) {
	r.RequeueAfter = time.Second // want `RequeueAfter in a controller that reads stores`
}

// Result stands in for reconcile.Result.
type Result struct{ RequeueAfter time.Duration }

type Other struct{}

// Register without a context isn't a controller's Register.
func (Other) Register(manager.Manager) error {
	_ = state.NewObjectStore[string, int, struct{}, struct{}]() // want `state.NewObjectStore builds a store`
	return nil
}

func helper() {
	_ = state.NewFedStore[string, int, struct{}, struct{}]() // want `state.NewFedStore builds a store`
}

var _ = state.NewObjectStore[string, string, struct{}, struct{}]() // want `state.NewObjectStore builds a store`

func wipe(s *state.Store[string, int, struct{}, struct{}]) {
	s.Clear() // want `Clear wipes cluster state: only tests clear stores`
}

// tested is a Contribution with a law test.
type tested int

func (t tested) Update(f int, prev tested) int { return f + int(t-prev) }
func (t tested) Delete(f int) int              { return f - int(t) }

// untested is a Contribution without one.
type untested int // want `untested is a Contribution without a statetest.ExpectContribution test`

func (u untested) Update(f int, prev untested) int { return f + int(u-prev) }
func (u untested) Delete(f int) int                { return f - int(u) }

// disrupt writes a copy of the NodeClaim a store holds, never the NodeClaim itself.
func disrupt(ctx context.Context, kubeClient client.Writer, node *cluster.StateNode) {
	_ = kubeClient.Delete(ctx, node.NodeClaim)       // want `node.NodeClaim is the informer cache's object, which a client write overwrites`
	_ = kubeClient.Patch(ctx, (node.NodeClaim), nil) // want `node.NodeClaim is the informer cache's object, which a client write overwrites`
	_ = kubeClient.Delete(ctx, node.NodeClaim.DeepCopy())
	_ = node.NodeClaim.StatusConditions() // want `node.NodeClaim is the informer cache's object, which StatusConditions\(\) writes`
	_ = node.NodeClaim.StatusConditions(true)
	_ = node.NodeClaim.DeepCopy().StatusConditions()
}
