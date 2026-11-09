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
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Source is the store as a controller-runtime source: it enqueues a request for every entry the store holds, then for
// each key whose entry changes. The request names the object the key is: a key is a name, or a types.NamespacedName.
// A store keyed by anything else, such as the synthetic Cluster{}, is a source only under Wake or After(Wake), which
// enqueue one empty request. A controller that acts on part of an entry watches SourceOn.
func (r Reader[K, O, D, I]) Source() source.SyncingSource { return storeSource[K, O, D, I]{store: r} }

type storeSource[K comparable, O, D, I any] struct {
	store Reader[K, O, D, I]
	// on, if set, narrows the source to the changes of what it returns.
	on func(K, Entry[O, D, I]) any
}

func (src storeSource[K, O, D, I]) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	s := src.store
	var zero K
	if _, ok := request(zero); !ok {
		if _, waking := queue.(mappedQueue); !waking {
			return fmt.Errorf("store keyed by %T can't name a request; watch it under state.Wake", zero)
		}
	}
	enqueue := func(key K) {
		r, _ := request(key)
		queue.Add(r)
	}
	context.AfterFunc(ctx, s.Subscribe(func(c Change[K, O, D, I]) {
		if src.changed(c) {
			enqueue(c.Key)
		}
	}))
	for key := range s.List() {
		enqueue(key)
	}
	return nil
}

// SourceOn is the store as a source narrowed to what on returns of an entry: it enqueues every key the store holds,
// then each key that appears, disappears, or whose on(Old) and on(New) differ, compared semantically. on is pure and
// cheap; it runs twice on every change of the store.
func (r Reader[K, O, D, I]) SourceOn(on func(K, Entry[O, D, I]) any) source.SyncingSource {
	return storeSource[K, O, D, I]{store: r, on: on}
}

func (src storeSource[K, O, D, I]) changed(c Change[K, O, D, I]) bool {
	if src.on == nil || c.Existed != c.Exists {
		return true
	}
	return !equality.Semantic.DeepEqual(src.on(c.Key, c.Old), src.on(c.Key, c.New))
}

func request(key any) (reconcile.Request, bool) {
	switch k := key.(type) {
	case string:
		return reconcile.Request{NamespacedName: types.NamespacedName{Name: k}}, true
	case types.NamespacedName:
		return reconcile.Request{NamespacedName: k}, true
	}
	return reconcile.Request{}, false
}

// WaitForSync returns once the store is synced, so that a controller watching it never acts on partial fields.
// controller-runtime bounds the wait with CacheSyncTimeout, which a store meets, since its feeds sync once they apply
// their first List.
func (src storeSource[K, O, D, I]) WaitForSync(ctx context.Context) error {
	return wait.PollUntilContextCancel(ctx, 10*time.Millisecond, true, func(context.Context) (bool, error) {
		return src.store.Synced(), nil
	})
}

// Wake makes sources enqueue one empty request for every change, for a controller that reconciles the whole cluster
// at once, such as disruption. It replaces a singleton's poll: the controller runs when something changed, and the
// workqueue folds the changes made while it runs into one more run.
func Wake(sources ...source.SyncingSource) source.SyncingSource {
	return WakeCounting(nil, sources...)
}

// WakeCounting is Wake that calls count for each change before it enqueues the request, so that the run the change
// wakes sees it counted, for a controller that skips work while nothing it wakes on changed, as consolidation does.
// count must be cheap and never block.
func WakeCounting(count func(), sources ...source.SyncingSource) source.SyncingSource {
	return mapped{sources: sources, enqueue: func(q workqueue.TypedRateLimitingInterface[reconcile.Request], _ reconcile.Request) {
		if count != nil {
			count()
		}
		q.Add(reconcile.Request{})
	}}
}

// After makes sources enqueue each request no sooner than delay after its first change, and folds the changes in
// between into it, for a watcher that acts on a batch of changes rather than each one, such as metrics.
func After(delay time.Duration, sources ...source.SyncingSource) source.SyncingSource {
	return mapped{sources: sources, enqueue: func(q workqueue.TypedRateLimitingInterface[reconcile.Request], r reconcile.Request) {
		q.AddAfter(r, delay)
	}}
}

type mapped struct {
	sources []source.SyncingSource
	enqueue func(workqueue.TypedRateLimitingInterface[reconcile.Request], reconcile.Request)
}

func (m mapped) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	q := mappedQueue{TypedRateLimitingInterface: queue, enqueue: m.enqueue}
	for _, src := range m.sources {
		if err := src.Start(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (m mapped) WaitForSync(ctx context.Context) error {
	for _, src := range m.sources {
		if err := src.WaitForSync(ctx); err != nil {
			return err
		}
	}
	return nil
}

// mappedQueue is the queue a mapped source hands its sources, so that what they add goes through enqueue.
type mappedQueue struct {
	workqueue.TypedRateLimitingInterface[reconcile.Request]
	enqueue func(workqueue.TypedRateLimitingInterface[reconcile.Request], reconcile.Request)
}

func (q mappedQueue) Add(r reconcile.Request) { q.enqueue(q.TypedRateLimitingInterface, r) }

// An Accessor is also a controller-runtime source: a controller that watches it with WatchesRawSource reconciles an
// object whenever its derived fields change, and starts reconciling once the stores behind them are synced.
var _ source.SyncingSource = &Accessor[client.Object, client.ObjectList, any, any]{}

// Start enqueues each object whose derived fields change, until ctx is done, after enqueueing every object that has
// derived fields, since they may have changed before Start.
func (r *Accessor[O, L, D, I]) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	for _, src := range r.sources {
		if err := src.Start(ctx, queue); err != nil {
			return err
		}
	}
	return nil
}

// WaitForSync returns once every store the Accessor derives fields from is synced, so that a controller watching it
// reconciles with complete fields.
func (r *Accessor[O, L, D, I]) WaitForSync(ctx context.Context) error {
	for _, src := range r.sources {
		if err := src.WaitForSync(ctx); err != nil {
			return err
		}
	}
	return nil
}
