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

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Source is the store as a controller-runtime source: it enqueues a request for every key the store holds, then for
// each key whose fields change. The request names the object the key is: a key is a name, or a types.NamespacedName.
// A controller that needs other keys, or fewer changes, watches a store derived for it, whose keys and fields are
// exactly what it reconciles on. The store skips writes that leave fields equal, so a narrow derived store notifies
// only on the changes its consumer acts on.
func (s *Store[K, F]) Source() source.SyncingSource { return storeSource[K, F]{s} }

type storeSource[K comparable, F any] struct{ store *Store[K, F] }

func (src storeSource[K, F]) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	s := src.store
	var zero K
	if _, ok := request(zero); !ok {
		return fmt.Errorf("store keyed by %T can't name a request; derive a store keyed by name or types.NamespacedName", zero)
	}
	enqueue := func(key K) {
		r, _ := request(key)
		queue.Add(r)
	}
	context.AfterFunc(ctx, s.Subscribe(func(c Change[K, F]) { enqueue(c.Key) }))
	for key := range s.List() {
		enqueue(key)
	}
	return nil
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
func (src storeSource[K, F]) WaitForSync(ctx context.Context) error {
	return wait.PollUntilContextCancel(ctx, 10*time.Millisecond, true, func(context.Context) (bool, error) {
		return src.store.Synced(), nil
	})
}

// Wake makes sources enqueue one empty request for every change, for a controller that reconciles the whole cluster
// at once, such as disruption. It replaces a singleton's poll: the controller runs when something changed, and the
// workqueue folds the changes made while it runs into one more run.
func Wake(sources ...source.SyncingSource) source.SyncingSource {
	return mapped{sources: sources, enqueue: func(q workqueue.TypedRateLimitingInterface[reconcile.Request], _ reconcile.Request) {
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
var _ source.SyncingSource = &Accessor[client.Object, client.ObjectList, any]{}

// Start enqueues each object whose derived fields change, until ctx is done, after enqueueing every object that has
// derived fields, since they may have changed before Start.
func (r *Accessor[O, L, D]) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	for _, src := range r.sources {
		if err := src.Start(ctx, queue); err != nil {
			return err
		}
	}
	return nil
}

// WaitForSync returns once every store the Accessor derives fields from is synced, so that a controller watching it
// reconciles with complete fields.
func (r *Accessor[O, L, D]) WaitForSync(ctx context.Context) error {
	for _, src := range r.sources {
		if err := src.WaitForSync(ctx); err != nil {
			return err
		}
	}
	return nil
}
