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

// Poll requeues to poll what no store holds, on a line that says so.
func (c *Controller) Poll() Result {
	return Result{RequeueAfter: time.Minute} //storelint:poll an API that may not be installed
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

func reset(s cluster.State) {
	s.Reset() // want `Reset wipes cluster state: only tests clear stores`
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

// writeEntries writes the objects of entries read without a copy, which are the store's, and of entries read with one,
// which are the caller's.
func writeEntries(ctx context.Context, kubeClient client.Writer, nodeClaims *cluster.NodeClaims, idx *state.Index[string, *cluster.NodeClaim, cluster.NodeClaimDerived, struct{}, string]) {
	copied, _ := nodeClaims.Get("a")
	_ = kubeClient.Patch(ctx, copied.Object, nil)
	shared, _ := nodeClaims.Get("a", client.UnsafeDisableDeepCopy)
	_ = kubeClient.Patch(ctx, shared.Object, nil)   // want `shared.Object is the informer cache's object, which a client write overwrites`
	_ = kubeClient.Delete(ctx, shared.Derived.Node) // want `shared.Derived.Node is the informer cache's object, which a client write overwrites`
	_ = kubeClient.Delete(ctx, copied.Derived.Node) // want `copied.Derived.Node is the informer cache's object, which a client write overwrites`
	nc := shared.Object
	_ = nc.StatusConditions() // want `nc is the informer cache's object, which StatusConditions\(\) writes`
	_ = nc.DeepCopy().StatusConditions()
	for _, e := range nodeClaims.List(client.UnsafeDisableDeepCopy) {
		_ = kubeClient.Patch(ctx, e.Object, nil)                               // want `e.Object is the informer cache's object, which a client write overwrites`
		_ = kubeClient.Patch(ctx, cluster.ViewOfNodeClaim(e).NodeClaim(), nil) // want `cluster.ViewOfNodeClaim\(e\).NodeClaim\(\) is the informer cache's object, which a client write overwrites`
	}
	for _, e := range nodeClaims.List() {
		_ = kubeClient.Patch(ctx, e.Object, nil)
	}
	for _, e := range idx.List("x", client.UnsafeDisableDeepCopy) {
		_ = e.Object.StatusConditions() // want `e.Object is the informer cache's object, which StatusConditions\(\) writes`
	}
	for _, e := range idx.List("x") {
		_ = e.Object.StatusConditions()
	}
	for _, e := range cluster.LaunchedNodes() {
		_ = kubeClient.Delete(ctx, e.Object) // want `e.Object is the informer cache's object, which a client write overwrites`
	}
	for _, nodeClaim := range cluster.Unlaunched() {
		_ = kubeClient.Delete(ctx, nodeClaim) // want `nodeClaim is the informer cache's object, which a client write overwrites`
	}
}

// fromParameter writes the object of an entry it's passed, which may be a store's own.
func fromParameter(ctx context.Context, kubeClient client.Writer, e cluster.NodeClaimEntry) {
	_ = kubeClient.Delete(ctx, e.Object) // want `e.Object is the informer cache's object, which a client write overwrites`
	_ = kubeClient.Delete(ctx, e.Object.DeepCopy())
}

// listByField matches a field with a store's List, which panics on it.
func listByField(nodeClaims *cluster.NodeClaims, idx *state.Index[string, *cluster.NodeClaim, cluster.NodeClaimDerived, struct{}, string]) {
	for range nodeClaims.List(client.MatchingFields{"status.providerID": "a"}) { // want `client.MatchingFields in a store's List, which panics on it`
	}
	for range idx.List("a", client.UnsafeDisableDeepCopy, client.MatchingFields{"x": "y"}) { // want `client.MatchingFields in a store's List`
	}
	for range nodeClaims.List(client.MatchingLabels{"a": "b"}, client.UnsafeDisableDeepCopy) {
	}
}

// readFacts reads node facts from a node entry, which readers read through the NodeView, and the NodeClaim's own spec
// and status, which they read from the entry.
func readFacts(e cluster.NodeClaimEntry) {
	_ = e.Object.Labels             // want `e.Object.Labels reads a node fact from a node entry's object`
	_ = e.Object.Spec.Taints        // want `e.Object.Spec.Taints reads a node fact`
	_ = e.Object.Status.Capacity    // want `e.Object.Status.Capacity reads a node fact`
	_ = e.Object.Status.Allocatable // want `e.Object.Status.Allocatable reads a node fact`
	_ = e.Derived.Node.Name         // want `e.Derived.Node.Name reads the Node a NodeClaim entry carries`
	_ = e.Object.Conditions
	_ = e.Object.Spec.NodeClassRef
	_ = e.Object.Status.ProviderID
	_ = e.Object.DeepCopy()
	_ = cluster.ViewOfNodeClaim(e).NodeClaim()
}
