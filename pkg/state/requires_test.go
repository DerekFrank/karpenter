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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

type syncFlag struct{ synced atomic.Bool }

func (f *syncFlag) Synced() bool { return f.synced.Load() }

var _ = ginkgo.Describe("Requires", func() {
	// A controller and the store it requires start together, as the manager starts them after its caches sync, and
	// the controller holds its workers until the store's first List is applied, though its other source has a request
	// queued from the start.
	ginkgo.It("should hold a controller's reconciles until the stores it requires are synced", func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		listed := nodeClaim("a", "x", 0, "1")
		listed.ResourceVersion = ""
		gate := listGate{Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(listed).Build(), open: make(chan struct{})}
		informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
		_, err := informers.FakeInformerFor(ctx, &v1.NodeClaim{})
		Expect(err).ToNot(HaveOccurred())
		nodeClaims := NewObjectStore[*v1.NodeClaim, struct{}, nodeClaimIntent](gate, informers)
		pools := newPools()
		Derive(nodeClaims.Reader, pools, poolOf, usageOf)

		reconciled := make(chan poolFields, 1)
		c, err := controller.NewTypedUnmanaged("requires", controller.TypedOptions[reconcile.Request]{
			SkipNameValidation: new(true),
			Reconciler: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
				reconciled <- fields(pools, "x")
				return reconcile.Result{}, nil
			}),
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(c.Watch(source.Func(func(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
			q.Add(reconcile.Request{})
			return nil
		}))).To(Succeed())
		Expect(c.Watch(Requires(pools))).To(Succeed())
		go func() { _ = nodeClaims.Start(ctx) }()
		go func() { _ = c.Start(ctx) }()

		Consistently(reconciled).WithTimeout(200*time.Millisecond).ShouldNot(Receive(), "the controller reconciled before the store it requires synced")
		close(gate.open)
		var f poolFields
		Eventually(reconciled).WithTimeout(10*time.Second).Should(Receive(&f), "the controller didn't reconcile once the store it requires synced")
		Expect(f.Count).To(Equal(1), "the controller reconciled with x = %+v, want the listed NodeClaim counted", f)
	})
	ginkgo.It("should hold until every store is synced and enqueue nothing", func() {
		a, b := &syncFlag{}, &syncFlag{}
		a.synced.Store(true)
		src := Requires(a, b)
		Expect(src.Start(ctx, nil)).To(Succeed())
		short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		Expect(src.WaitForSync(short)).ToNot(Succeed(), "WaitForSync returned before every store synced")
		b.synced.Store(true)
		Expect(src.WaitForSync(ctx)).To(Succeed(), "WaitForSync once every store synced")
		nodeClaims, s, _ := newStores()
		Expect(Requires(nodeClaims, s).WaitForSync(ctx)).To(Succeed(), "WaitForSync for stores fed by a caller, which are synced from the start")
	})
})
