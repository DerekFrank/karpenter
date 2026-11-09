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

// Package state keeps the objects in a cluster, the fields Karpenter derives about them and what Karpenter decided
// about them, in stores: maps from a key to an entry {Object, Derived, Intent}. It knows no object kinds.
package state

import (
	"iter"
	"reflect"
	"slices"
	"sync"
)

// Entry describes one object: the object itself, the fields Derives and Claims compute about it from other stores,
// and the intent Karpenter keeps about it.
type Entry[O, D, I any] struct {
	// Object is what the entry is about: an API object in an object store, a Go type in a synthetic store.
	Object O
	// Derived are the fields computed from other stores, written only by Derives and Claims.
	Derived D
	// Intent is what Karpenter decided about the object, written only through intent handles.
	Intent I
}

// Change is a change of one key's entry. Existed and Exists say whether the entry existed before and after.
type Change[K comparable, O, D, I any] struct {
	Key             K
	Old, New        Entry[O, D, I]
	Existed, Exists bool
}

// kind is where a store's objects come from.
type kind int

const (
	// objects are API objects, from the informer and the overlay's writes, or a caller's Observe.
	objects kind = iota
	// fed objects are reported by a feed.
	fed
	// built objects are built from the key while the entry has a contribution.
	built
)

// slot is what a store keeps under a key: the entry, and whether it exists. A key's Derived can wait in a slot for the
// object, such as the pods bound to a Node the store hasn't seen yet. A slot never changes once stored.
type slot[O, D, I any] struct {
	entry  Entry[O, D, I]
	exists bool
}

// core is a store's state, shared by its Reader and its Store.
type core[K comparable, O, D, I any] struct {
	kind  kind
	build func(K) O

	slots sync.Map // K -> *slot[O, D, I]

	mu sync.Mutex
	// refs counts each key's contributions and claims, under mu.
	refs        map[K]int
	subscribers []*func(Change[K, O, D, I])
	// pending are the changes made under mu, notified once it's released.
	pending []Change[K, O, D, I]
	// feed feeds an object store from its kind's objects.
	feed objectFeed
	// sources are the Derives into the store, by Derivation; it's synced once they are.
	sources map[any]func() bool
	// cleared drop what each Derive, Claims and expiring Intent of the store recorded, when the store is cleared.
	cleared map[any]func()
}

// Reader reads a store: Get, List, Subscribe, Synced, Source and SourceOn. pkg/cluster exports Readers, so that a
// store's entries are written only by the writers the model names.
type Reader[K comparable, O, D, I any] struct{ s *core[K, O, D, I] }

// Store is a store and its writes: the object feed (Observe, Forget, Resync, Feed), Touch and Clear. A Derive's
// target, a Claims' and an intent handle's take a Store.
type Store[K comparable, O, D, I any] struct{ Reader[K, O, D, I] }

func newCore[K comparable, O, D, I any](k kind, build func(K) O) *core[K, O, D, I] {
	return &core[K, O, D, I]{kind: k, build: build, refs: map[K]int{}, sources: map[any]func() bool{}, cleared: map[any]func(){}}
}

// NewSyntheticStore returns a store of objects built from their keys, such as the singleton Cluster{}: an entry exists
// while it has a contribution, from a Derive or a claim, and holds build(key) as its object.
func NewSyntheticStore[K comparable, O, D, I any](build func(K) O) *Store[K, O, D, I] {
	return &Store[K, O, D, I]{Reader[K, O, D, I]{newCore[K, O, D, I](built, build)}}
}

// NewFedStore returns a store of the objects a feed reports, such as a cloud provider's instance types: an entry exists
// while the feed reports its object. Feed writes them.
func NewFedStore[K comparable, O, D, I any]() *Store[K, O, D, I] {
	return &Store[K, O, D, I]{Reader[K, O, D, I]{newCore[K, O, D, I](fed, nil)}}
}

// Get returns key's entry, and whether it exists. An entry that doesn't exist can hold Derived contributions waiting
// for its object, which Get returns.
func (r Reader[K, O, D, I]) Get(key K) (Entry[O, D, I], bool) {
	v, ok := r.s.slots.Load(key)
	if !ok {
		return Entry[O, D, I]{}, false
	}
	sl := v.(*slot[O, D, I])
	return sl.entry, sl.exists
}

// List returns every entry that exists. Each entry is from one moment, but different entries may be from different
// moments.
func (r Reader[K, O, D, I]) List() iter.Seq2[K, Entry[O, D, I]] {
	return func(yield func(K, Entry[O, D, I]) bool) {
		r.s.slots.Range(func(k, v any) bool {
			sl := v.(*slot[O, D, I])
			return !sl.exists || yield(k.(K), sl.entry)
		})
	}
}

// Subscribe calls fn with each change of an entry that exists before or after it, and returns a func that stops it. fn
// runs on the goroutine that made the change, after the store's lock is released, so it must be cheap and never block:
// it updates a counter, enqueues or Touches.
func (r Reader[K, O, D, I]) Subscribe(fn func(Change[K, O, D, I])) func() {
	s := r.s
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := &fn
	s.subscribers = append(slices.Clip(s.subscribers), sub)
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.subscribers = slices.DeleteFunc(slices.Clone(s.subscribers), func(other *func(Change[K, O, D, I])) bool { return other == sub })
	}
}

// Synced returns whether the store holds what its sources held when it started: an object store following an informer
// has applied its first List, and every Derive into the store has applied its source's entries.
func (r Reader[K, O, D, I]) Synced() bool {
	s := r.s
	if s.feed != nil && !s.feed.synced() {
		return false
	}
	// A Derive can add a source while a caller polls; read the sources under the lock, and call them after it, since
	// each reads another store.
	s.mu.Lock()
	sources := make([]func() bool, 0, len(s.sources))
	for _, synced := range s.sources {
		sources = append(sources, synced)
	}
	s.mu.Unlock()
	for _, synced := range sources {
		if !synced() {
			return false
		}
	}
	return true
}

// Touch notifies key's subscribers without changing its entry, so that the Derives from the store recompute key's
// contribution, for a contribution that reads another store's entry that changed, such as a NodeClaim's, which reads
// its Node's.
func (s *Store[K, O, D, I]) Touch(key K) {
	e, ok := s.Get(key)
	if !ok {
		return
	}
	s.s.mu.Lock()
	subscribers := s.s.subscribers
	s.s.mu.Unlock()
	notify(subscribers, []Change[K, O, D, I]{{Key: key, Old: e, New: e, Existed: true, Exists: true}})
}

// Clear empties the store in place, notifying the removal of each entry, so that subscribers stay subscribed. An
// object store forgets the versions it has seen, and each Derive and Claims into the store forgets what it recorded.
func (s *Store[K, O, D, I]) Clear() {
	c := s.s
	c.mu.Lock()
	c.slots.Range(func(k, v any) bool {
		c.slots.Delete(k)
		if sl := v.(*slot[O, D, I]); sl.exists {
			c.pending = append(c.pending, Change[K, O, D, I]{Key: k.(K), Old: sl.entry, Existed: true})
		}
		return true
	})
	c.refs = map[K]int{}
	if c.feed != nil {
		c.feed.clearLocked()
	}
	for _, clear := range c.cleared {
		clear()
	}
	c.unlock()
}

// Feed sets or removes key's object in a fed store, from what update returns of the current object and whether it
// exists. A removed object drops its entry and its intent.
func (s *Store[K, O, D, I]) Feed(key K, update func(O, bool) (O, bool)) {
	c := s.s
	c.mu.Lock()
	cur, exists := s.Get(key)
	obj, ok := update(cur.Object, exists)
	c.feedLocked(key, obj, ok)
	c.unlock()
}

// FeedAll makes objects the objects of a fed store among the keys in accepts, in one write, such as one NodeClass's
// instance types: an entry in that set that objects leave out drops, and each of objects is set. Each entry is from
// one moment, so a reader sees an entry's old object or its new one.
func (s *Store[K, O, D, I]) FeedAll(in func(K) bool, objects map[K]O) {
	c := s.s
	c.mu.Lock()
	c.slots.Range(func(k, v any) bool {
		if _, ok := objects[k.(K)]; !ok && v.(*slot[O, D, I]).exists && in(k.(K)) {
			var zero O
			c.feedLocked(k.(K), zero, false)
		}
		return true
	})
	for k, obj := range objects {
		c.feedLocked(k, obj, true)
	}
	c.unlock()
}

// unlock releases the lock and notifies the changes made under it.
func (c *core[K, O, D, I]) unlock() {
	changed, subscribers := c.pending, c.subscribers
	c.pending = nil
	c.mu.Unlock()
	notify(subscribers, changed)
}

func (c *core[K, O, D, I]) load(key K) (*slot[O, D, I], bool) {
	v, ok := c.slots.Load(key)
	if !ok {
		return &slot[O, D, I]{}, false
	}
	return v.(*slot[O, D, I]), true
}

// derivedLocked replaces key's Derived with what update returns, and adds refs to the key's contributions. compare
// skips a write that leaves the entry equal, for a caller that doesn't know whether update changes it. A synthetic
// store's entry built from its key exists while it has a contribution.
func (c *core[K, O, D, I]) derivedLocked(key K, update func(D) D, refs int, compare bool) {
	cur, _ := c.load(key)
	next := *cur
	next.entry.Derived = update(cur.entry.Derived)
	if c.refs[key] += refs; c.refs[key] == 0 {
		delete(c.refs, key)
	}
	if c.kind == built {
		if exists := c.refs[key] != 0; exists != cur.exists {
			next.exists = exists
			var zero I
			next.entry.Intent = zero
			if exists {
				next.entry.Object = c.build(key)
			} else {
				var none O
				next.entry.Object = none
			}
		}
	}
	if compare && refs == 0 && reflect.DeepEqual(cur.entry.Derived, next.entry.Derived) {
		return
	}
	c.storeLocked(key, cur, &next)
}

// feedLocked sets key's object as objectLocked does, unless it holds an equal one.
func (c *core[K, O, D, I]) feedLocked(key K, obj O, ok bool) {
	if cur, _ := c.load(key); cur.exists == ok && (!ok || reflect.DeepEqual(cur.entry.Object, obj)) {
		return
	}
	c.objectLocked(key, obj, ok)
}

// objectLocked sets key's object, or removes it with ok false, which drops the entry and its intent; Derived waits for
// the next object.
func (c *core[K, O, D, I]) objectLocked(key K, obj O, ok bool) {
	cur, _ := c.load(key)
	next := *cur
	next.exists = ok
	next.entry.Object = obj
	if !ok {
		var zero I
		next.entry.Intent = zero
	}
	c.storeLocked(key, cur, &next)
}

// intentLocked replaces the Intent of key's entry with what update returns, if the entry exists. Intent about an
// object the store doesn't hold would outlive it, so it's dropped.
func (c *core[K, O, D, I]) intentLocked(key K, update func(I) I) bool {
	cur, _ := c.load(key)
	if !cur.exists {
		return false
	}
	next := *cur
	next.entry.Intent = update(cur.entry.Intent)
	if reflect.DeepEqual(cur.entry.Intent, next.entry.Intent) {
		return true
	}
	c.storeLocked(key, cur, &next)
	return true
}

// storeLocked stores next under key, or deletes the slot once its entry doesn't exist and nothing contributes to it,
// and records the change of an entry that existed before or after.
func (c *core[K, O, D, I]) storeLocked(key K, cur, next *slot[O, D, I]) {
	if next.exists || c.refs[key] != 0 {
		c.slots.Store(key, next)
	} else {
		c.slots.Delete(key)
	}
	if !cur.exists && !next.exists {
		return
	}
	change := Change[K, O, D, I]{Key: key, Existed: cur.exists, Exists: next.exists}
	if cur.exists {
		change.Old = cur.entry
	}
	if next.exists {
		change.New = next.entry
	}
	c.pending = append(c.pending, change)
}

func notify[K comparable, O, D, I any](subscribers []*func(Change[K, O, D, I]), changed []Change[K, O, D, I]) {
	for _, c := range changed {
		for _, subscriber := range subscribers {
			(*subscriber)(c)
		}
	}
}
