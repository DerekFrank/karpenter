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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ObjectOption configures an object store: WithKey and WithFilter.
type ObjectOption interface{ objectOption() }

type keyOption[K comparable] struct{ key func(types.NamespacedName) K }

func (keyOption[K]) objectOption() {}

type filterOption[S client.Object] struct{ in func(S) bool }

func (filterOption[S]) objectOption() {}

// WithKey keys each object by what key returns of its name, such as a cluster-scoped object's name (ByName). The key is
// a function of the name, since a removal has no object to key. A zero key leaves the object out of the store. Without
// it, a store is keyed by types.NamespacedName.
func WithKey[K comparable](key func(types.NamespacedName) K) ObjectOption {
	return keyOption[K]{key: key}
}

// WithFilter holds only the objects in accepts. A store holds every object of its kind, and a Derive's zero key does
// the filtering; a filter is for a kind whose objects cost memory to hold, measured first.
func WithFilter[S client.Object](in func(S) bool) ObjectOption { return filterOption[S]{in: in} }

// ByName keys a cluster-scoped object by its name.
func ByName(name types.NamespacedName) string { return name.Name }

// NewObjectStore returns the store of the objects of kind S that from holds, under the key WithKey returns of each
// one's name, or its namespaced name. It's level-triggered: on each key from notifies, it reads that object's newest
// version from from and holds it, or drops the entry once from no longer holds it, so a version from skips, repeats or
// delivers late never reaches it. A new UID under a name replaces the object, which drops its entry's intent. from is
// the overlay client's Source for the kind, under the manager and in suites. It's synced once from is and it has
// applied every key from held then; Start waits for that, as a manager runnable.
func NewObjectStore[K comparable, S client.Object, D, I any](from Newest, opts ...ObjectOption) *Store[K, S, D, I] {
	var key func(types.NamespacedName) K
	if k, ok := any(func(name types.NamespacedName) types.NamespacedName { return name }).(func(types.NamespacedName) K); ok {
		key = k
	}
	in := func(S) bool { return true }
	for _, opt := range opts {
		switch o := opt.(type) {
		case keyOption[K]:
			key = o.key
		case filterOption[S]:
			in = o.in
		default:
			panic(fmt.Sprintf("%T doesn't configure a store of %T keyed by %T", opt, *new(S), *new(K)))
		}
	}
	if key == nil {
		panic(fmt.Sprintf("a store keyed by %T needs WithKey", *new(K)))
	}
	c := newCore[K, S, D, I](objects, nil)
	n := &newestOf[K, S, D, I]{core: c, from: from, key: key, in: in}
	c.feed = n
	from.Subscribe(n.apply)
	if from.HasSynced() {
		n.sweep()
	}
	return &Store[K, S, D, I]{Reader[K, S, D, I]{c}}
}

// feeder feeds an object store from its kind's newest versions.
type feeder interface {
	start(ctx context.Context) error
	synced() bool
	// resync applies every object the source holds, for a store a test cleared.
	resync()
}

// Start waits for an object store's source to sync, then applies every object it holds, until ctx is done. It
// implements manager.Runnable.
func (s *Store[K, O, D, I]) Start(ctx context.Context) error {
	if s.s.feed == nil {
		<-ctx.Done()
		return nil
	}
	return s.s.feed.start(ctx)
}

// identity is an object's UID, or its namespaced name for an object that has none because it never went through the
// API server.
func identity(obj client.Object) types.UID {
	if uid := obj.GetUID(); uid != "" {
		return uid
	}
	return types.UID(client.ObjectKeyFromObject(obj).String())
}
