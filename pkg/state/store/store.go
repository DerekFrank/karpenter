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

// Package store keeps fields that Karpenter computes or sets about the objects in a cluster, keyed by any key such as
// a name or a providerID, up to date as the objects that contribute to them change.
package store

import (
	"context"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strconv"
	"sync"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/operator/overlay"
)

// Store holds fields F per key K. Contributors keep fields up to date from the objects of a kind, including the objects
// themselves, and Update sets fields Karpenter decides rather than derives.
//
// A key's fields never change once stored; a change stores new fields. So a reader takes no lock and can keep what Get
// and List return. Writes take one lock, under which Update functions and Contributors' apply functions run, so they
// must not block, and must return new field values rather than modifying values a reader may hold.
type Store[K comparable, F any] struct {
	entries sync.Map // K -> *F

	mu          sync.Mutex
	refs        map[K]int
	subscribers []func(key K, old, new F)
}

// New returns an empty store.
func New[K comparable, F any]() *Store[K, F] {
	return &Store[K, F]{refs: map[K]int{}}
}

// Get returns key's fields, and whether anything contributes to or has set them.
func (s *Store[K, F]) Get(key K) (F, bool) {
	v, ok := s.entries.Load(key)
	if !ok {
		var zero F
		return zero, false
	}
	return *v.(*F), true
}

// List returns every key and its fields. Each key's fields are from one moment, but different keys may be from
// different moments.
func (s *Store[K, F]) List() iter.Seq2[K, F] {
	return func(yield func(K, F) bool) {
		s.entries.Range(func(k, v any) bool {
			return yield(k.(K), *v.(*F))
		})
	}
}

// Update replaces key's fields with what update returns from its current fields. Update runs under the store's lock,
// so it sees the fields as every write before it left them.
func (s *Store[K, F]) Update(key K, update func(F) F) {
	s.mu.Lock()
	changed := s.updateLocked(key, update, 0)
	subscribers := s.subscribers
	s.mu.Unlock()
	notify(subscribers, changed)
}

// Subscribe calls fn with each key whose fields change, and its fields before and after. fn runs on the goroutine that
// made the change, after the store's lock is released, so it must not block.
func (s *Store[K, F]) Subscribe(fn func(key K, old, new F)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers = append(slices.Clip(s.subscribers), fn)
}

type change[K comparable, F any] struct {
	key      K
	old, new F
}

func (s *Store[K, F]) updateLocked(key K, update func(F) F, refs int) []change[K, F] {
	cur, _ := s.Get(key)
	next := update(cur)
	if s.refs[key] += refs; s.refs[key] == 0 {
		delete(s.refs, key)
	}
	if refs == 0 && reflect.DeepEqual(cur, next) {
		return nil
	}
	return s.storeLocked(key, cur, next)
}

// changeLocked updates the fields of a key whose contributions don't change in number, for a caller that knows the
// update changes them.
func (s *Store[K, F]) changeLocked(key K, update func(F) F) []change[K, F] {
	cur, _ := s.Get(key)
	return s.storeLocked(key, cur, update(cur))
}

func (s *Store[K, F]) storeLocked(key K, cur, next F) []change[K, F] {
	var zero F
	if s.refs[key] == 0 && reflect.DeepEqual(next, zero) {
		s.entries.Delete(key)
	} else {
		s.entries.Store(key, &next)
	}
	return []change[K, F]{{key: key, old: cur, new: next}}
}

func notify[K comparable, F any](subscribers []func(K, F, F), changed []change[K, F]) {
	for _, c := range changed {
		for _, subscriber := range subscribers {
			subscriber(c.key, c.old, c.new)
		}
	}
}

// feed orders an object's versions by UID and resourceVersion. A delete from a write leaves a tombstone until the
// informer's delete arrives, because the informer may still deliver older versions. Names are unique at any moment, so
// a newer object under a name another UID holds replaces that object, which leaves a tombstone too.
type feed struct {
	versions map[types.UID]version
	names    map[types.NamespacedName]types.UID
}

type version struct {
	rv      uint64
	deleted bool
}

func newFeed() feed {
	return feed{versions: map[types.UID]version{}, names: map[types.NamespacedName]types.UID{}}
}

// admit returns whether obj is a version to apply, whether the object is gone, and the UID of an object it replaces.
//
//nolint:gocyclo
func (f feed) admit(obj client.Object, deleted, fromInformer bool) (bool, bool, types.UID) {
	uid := identity(obj)
	name := client.ObjectKeyFromObject(obj)
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	cur, exists := f.versions[uid]
	switch {
	case deleted && fromInformer:
		if !exists {
			return false, false, ""
		}
		delete(f.versions, uid)
		if f.names[name] == uid {
			delete(f.names, name)
		}
		return true, true, ""
	case exists && cur.deleted:
		return false, false, ""
	case deleted:
	case rv == 0 && !fromInformer, exists && rv != 0 && rv <= cur.rv:
		return false, false, ""
	}
	var replaced types.UID
	if other, ok := f.names[name]; ok && other != uid {
		if v := f.versions[other]; !v.deleted && rv != 0 && v.rv < rv {
			f.versions[other] = version{rv: v.rv, deleted: true}
			replaced = other
		} else if !v.deleted {
			return false, false, ""
		}
	}
	f.versions[uid] = version{rv: max(rv, cur.rv), deleted: deleted}
	f.names[name] = uid
	return true, deleted, replaced
}

func (f feed) current(obj client.Object) bool {
	cur, exists := f.versions[identity(obj)]
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	return exists && !cur.deleted && rv == cur.rv
}

func (f feed) newer(obj client.Object) bool {
	cur, exists := f.versions[identity(obj)]
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	return !exists || (!cur.deleted && (rv == 0 || rv > cur.rv))
}

// identity is an object's UID, or its namespaced name for an object that has none because it never went through the
// API server.
func identity(obj client.Object) types.UID {
	if uid := obj.GetUID(); uid != "" {
		return uid
	}
	return types.UID(client.ObjectKeyFromObject(obj).String())
}

// forget removes the object named name. A name without a namespace also matches an object of the same name in any
// namespace, because a cluster-scoped kind is forgotten by name, and an object built without the API server, such as
// in a test, can carry a namespace anyway.
func (f feed) forget(name types.NamespacedName) (types.UID, bool) {
	uid, ok := f.names[name]
	if !ok && name.Namespace == "" {
		for n, u := range f.names {
			if n.Name == name.Name {
				name, uid, ok = n, u, true
				break
			}
		}
	}
	if !ok {
		return "", false
	}
	delete(f.versions, uid)
	delete(f.names, name)
	return uid, true
}

// follow subscribes apply to the kind's informer and the client's writes, then hands listed a List under the store's
// lock, so that events arriving meanwhile apply after it.
func follow[S client.Object, K comparable, F any](ctx context.Context, kubeClient client.Client, informers cache.Informers, list client.ObjectList, store *Store[K, F],
	apply func(obj S, deleted, fromInformer bool), listed func([]S) []change[K, F]) error {
	obj := reflect.New(reflect.TypeFor[S]().Elem()).Interface().(S)
	informer, err := informers.GetInformer(ctx, obj)
	if err != nil {
		return fmt.Errorf("getting informer, %w", err)
	}
	observe := func(raw any, deleted bool) {
		if tombstone, ok := raw.(toolscache.DeletedFinalStateUnknown); ok {
			raw = tombstone.Obj
		}
		if o, ok := raw.(S); ok {
			apply(o, deleted, true)
		}
	}
	registration, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { observe(o, false) },
		UpdateFunc: func(_, o any) { observe(o, false) },
		DeleteFunc: func(o any) { observe(o, true) },
	})
	if err != nil {
		return fmt.Errorf("adding event handler, %w", err)
	}
	defer func() { _ = informer.RemoveEventHandler(registration) }()
	if writer, ok := kubeClient.(*overlay.Client); ok {
		remove, err := writer.AddWriteHandler(obj, func(o client.Object, deleted bool) {
			if s, ok := o.(S); ok {
				apply(s, deleted, false)
			}
		})
		if err != nil {
			return fmt.Errorf("adding write handler, %w", err)
		}
		defer remove()
	}
	store.mu.Lock()
	l := list.DeepCopyObject().(client.ObjectList)
	if err := kubeClient.List(ctx, l, client.UnsafeDisableDeepCopy); err != nil {
		store.mu.Unlock()
		return fmt.Errorf("listing, %w", err)
	}
	items, err := meta.ExtractList(l)
	if err != nil {
		store.mu.Unlock()
		return fmt.Errorf("extracting list, %w", err)
	}
	changed := listed(lo.Map(items, func(item runtime.Object, _ int) S { return item.(S) }))
	subscribers := store.subscribers
	store.mu.Unlock()
	notify(subscribers, changed)
	<-ctx.Done()
	return nil
}
