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

package state

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// listGate holds a client's List until it's opened, as a slow first List of a large cluster would.
type listGate struct {
	client.Client
	open chan struct{}
}

func (g listGate) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	<-g.open
	return g.Client.List(ctx, list, opts...)
}

// TestAControllerRequiringStoresReconcilesOnlyOnceTheyAreSynced starts a controller and the store it requires
// together, as the manager does after its caches sync, and checks that the controller holds its workers until the
// store's first List is applied, though its other source has a request queued from the start.
func TestAControllerRequiringStoresReconcilesOnlyOnceTheyAreSynced(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	listed := nodeClaim("a", "x", 0, "1")
	listed.ResourceVersion = ""
	gate := listGate{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(listed).Build(), open: make(chan struct{})}
	informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
	_, err := informers.FakeInformerFor(ctx, &v1.NodeClaim{})
	must(t, err)
	nodeClaims := NewObjectStore[*v1.NodeClaim](gate, informers)
	pools := NewStore[string, poolFields]()
	Derive(nodeClaims, pools, poolOf, usageOf)

	reconciled := make(chan poolFields, 1)
	c, err := controller.NewTypedUnmanaged("requires", controller.TypedOptions[reconcile.Request]{
		SkipNameValidation: new(true),
		Reconciler: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
			reconciled <- fields(pools, "x")
			return reconcile.Result{}, nil
		}),
	})
	must(t, err)
	must(t, c.Watch(source.Func(func(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		q.Add(reconcile.Request{})
		return nil
	})))
	must(t, c.Watch(Requires(pools)))
	go func() { _ = nodeClaims.Start(ctx) }()
	go func() { _ = c.Start(ctx) }()

	select {
	case <-reconciled:
		t.Fatal("the controller reconciled before the store it requires synced")
	case <-time.After(200 * time.Millisecond):
	}
	close(gate.open)
	select {
	case f := <-reconciled:
		if f.Count != 1 {
			t.Fatalf("the controller reconciled with x = %+v, want the listed NodeClaim counted", f)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the controller didn't reconcile once the store it requires synced")
	}
}

type syncFlag struct{ synced atomic.Bool }

func (f *syncFlag) Synced() bool { return f.synced.Load() }

func TestRequiresHoldsUntilEveryStoreIsSyncedAndEnqueuesNothing(t *testing.T) {
	a, b := &syncFlag{}, &syncFlag{}
	a.synced.Store(true)
	src := Requires(a, b)
	if err := src.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := src.WaitForSync(short); err == nil {
		t.Fatal("WaitForSync returned before every store synced")
	}
	b.synced.Store(true)
	if err := src.WaitForSync(ctx); err != nil {
		t.Fatalf("WaitForSync = %v once every store synced", err)
	}
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	s := NewStore[string, poolFields]()
	Derive(nodeClaims, s, poolOf, usageOf)
	if err := Requires(nodeClaims, s).WaitForSync(ctx); err != nil {
		t.Fatalf("WaitForSync = %v for stores fed by a caller, which are synced from the start", err)
	}
}
