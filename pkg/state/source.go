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
	"time"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Synced reports whether what feeds a store has caught up with the cluster, as a Contributor does once it has applied
// its first List.
type Synced interface{ Synced() bool }

// SyncedFunc is a Synced that calls itself, such as a cache's HasSynced.
type SyncedFunc func() bool

func (f SyncedFunc) Synced() bool { return f() }

// Watch is a controller-runtime source over a store: it enqueues the objects each changed key describes. Watch it with
// WatchesRawSource, or register it with NewAccessor so that the accessor's watchers get it.
//
// Its mapping and filter run on the goroutine that changed the store, after the store's lock is released, so they
// must not block: read other stores with Get, never the API server.
type Watch[K comparable, F any] struct {
	store   *Store[K, F]
	objects func(K, F) []client.ObjectKey
	changed func(old, new F) bool
	synced  []Synced
}

var _ source.SyncingSource = &Watch[string, int]{}

// From watches store, whose keys each describe the object that object names, such as ByName for a store keyed by
// name. The store is synced once each of synced is, such as the Contributors that feed it.
func From[K comparable, F any](store *Store[K, F], object func(K) client.ObjectKey, synced ...Synced) *Watch[K, F] {
	return FromEach(store, func(key K, _ F) []client.ObjectKey { return []client.ObjectKey{object(key)} }, synced...)
}

// FromEach watches store, whose keys each affect the objects that objects names from the key and its fields, such as
// a NodePool's NodeClaims, or the Node a providerID's fields hold. A change enqueues the objects of the fields both
// before and after it, so an object is enqueued when a key stops affecting it, or when the key goes away.
func FromEach[K comparable, F any](store *Store[K, F], objects func(K, F) []client.ObjectKey, synced ...Synced) *Watch[K, F] {
	return &Watch[K, F]{store: store, objects: objects, synced: synced}
}

// When returns w enqueueing only the changes for which changed reports true, given a key's fields before and after.
// Use it for a store whose fields change more often than the watcher acts on them, such as pod usage.
func (w *Watch[K, F]) When(changed func(old, new F) bool) *Watch[K, F] {
	filtered := *w
	filtered.changed = changed
	return &filtered
}

// ByName is the key of the cluster-scoped object named name, for a store keyed by name.
func ByName(name string) client.ObjectKey { return client.ObjectKey{Name: name} }

// Start enqueues the objects of each change until ctx is done, after enqueueing the objects of every key the store
// holds, since they may have changed before Start.
func (w *Watch[K, F]) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	enqueue := func(key K, fields F) {
		for _, object := range w.objects(key, fields) {
			queue.Add(reconcile.Request{NamespacedName: object})
		}
	}
	context.AfterFunc(ctx, w.store.Subscribe(func(key K, old, new F) {
		if w.changed == nil || w.changed(old, new) {
			enqueue(key, old)
			enqueue(key, new)
		}
	}))
	for key, fields := range w.store.List() {
		enqueue(key, fields)
	}
	return nil
}

// WaitForSync returns once every one of synced is, so that a controller watching w acts on complete fields.
func (w *Watch[K, F]) WaitForSync(ctx context.Context) error {
	return waitFor(ctx, w.synced)
}

func waitFor(ctx context.Context, synced []Synced) error {
	return wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(context.Context) (bool, error) {
		return lo.EveryBy(synced, func(s Synced) bool { return s.Synced() }), nil
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
