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

// Package state keeps the objects in a cluster, and the fields Karpenter derives or decides about them, in stores
// keyed by any key such as a name or a providerID.
package state

import (
	"iter"
	"reflect"
	"slices"
	"sync"
)

// Store holds fields F per key K. There are three kinds:
//   - an object store (NewObjectStore) holds the objects of one kind by name;
//   - a join store (NewJoinStore) holds objects of several kinds that are one thing under one key, or one kind with the
//     intent Karpenter keeps about each object;
//   - a derived store (NewStore) holds fields that Derive computes from another store, that Claims hold, or that Update
//     sets.
//
// A key's fields never change once stored; a change stores new fields. So a reader takes no lock and can keep what Get
// and List return. Writes take one lock, under which contributions and Update functions run, so they must not block,
// and must return new field values rather than modifying values a reader may hold.
type Store[K comparable, F any] struct {
	entries sync.Map // K -> *F

	mu          sync.Mutex
	refs        map[K]int
	subscribers []*func(Change[K, F])
	// parts feed an object or join store from its kinds' objects.
	parts []part[K, F]
	// sources are the Derives into this store; it's synced once they are.
	sources []func() bool
	// cleared drop the records of Derives into this store when it's cleared.
	cleared []func()
}

// Change is a change of one key's fields. Existed and Exists say whether the key was in the store before and after,
// which tells a key added or removed with zero fields apart from no change.
type Change[K comparable, F any] struct {
	Key             K
	Old, New        F
	Existed, Exists bool
}

// NewStore returns an empty derived store.
func NewStore[K comparable, F any]() *Store[K, F] {
	return &Store[K, F]{refs: map[K]int{}}
}

// Get returns key's fields, and whether the key is in the store.
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

// Update replaces key's fields with what update returns from its current fields, for fields Karpenter decides, such as
// a node's deletion mark. Update runs under the store's lock, so it sees the fields as every write before it left them.
func (s *Store[K, F]) Update(key K, update func(F) F) {
	s.mu.Lock()
	changed := s.updateLocked(key, update, 0)
	subscribers := s.subscribers
	s.mu.Unlock()
	notify(subscribers, changed)
}

// Touch notifies key's subscribers without changing its fields, so that the Derives from this store recompute key's
// contribution, for a contribution that reads something that changed while key's fields didn't.
func (s *Store[K, F]) Touch(key K) {
	f, ok := s.Get(key)
	if !ok {
		return
	}
	s.mu.Lock()
	subscribers := s.subscribers
	s.mu.Unlock()
	notify(subscribers, []Change[K, F]{{Key: key, Old: f, New: f, Existed: true, Exists: true}})
}

// Subscribe calls fn with each change of a key's fields, and returns a func that stops it. fn runs on the goroutine
// that made the change, after the store's lock is released, so it must not block.
func (s *Store[K, F]) Subscribe(fn func(Change[K, F])) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := &fn
	s.subscribers = append(slices.Clip(s.subscribers), sub)
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.subscribers = slices.DeleteFunc(slices.Clone(s.subscribers), func(other *func(Change[K, F])) bool { return other == sub })
	}
}

// Synced returns whether the store holds what its sources held when it started: every kind of an object or join store
// has applied its first List, and every Derive into a derived store has applied its source's keys.
func (s *Store[K, F]) Synced() bool {
	for _, p := range s.parts {
		if !p.synced() {
			return false
		}
	}
	// A Derive can add a source while a caller polls; read the sources under the lock, and call them after it, since
	// each reads another store.
	s.mu.Lock()
	sources := s.sources
	s.mu.Unlock()
	for _, synced := range sources {
		if !synced() {
			return false
		}
	}
	return true
}

// Clear empties the store in place, notifying the removal of each key, so that subscribers stay subscribed. An object
// or join store forgets the versions it has seen, and a Derive into the store forgets what it contributed.
func (s *Store[K, F]) Clear() {
	s.mu.Lock()
	var changed []Change[K, F]
	s.entries.Range(func(k, v any) bool {
		s.entries.Delete(k)
		changed = append(changed, Change[K, F]{Key: k.(K), Old: *v.(*F), Existed: true})
		return true
	})
	s.refs = map[K]int{}
	for _, p := range s.parts {
		p.clearLocked()
	}
	for _, clear := range s.cleared {
		clear()
	}
	subscribers := s.subscribers
	s.mu.Unlock()
	notify(subscribers, changed)
}

func (s *Store[K, F]) updateLocked(key K, update func(F) F, refs int) []Change[K, F] {
	cur, existed := s.Get(key)
	next := update(cur)
	if s.refs[key] += refs; s.refs[key] == 0 {
		delete(s.refs, key)
	}
	if refs == 0 && reflect.DeepEqual(cur, next) {
		return nil
	}
	return s.storeLocked(key, cur, existed, next)
}

// changeLocked updates the fields of a key whose contributions don't change in number, for a caller that knows the
// update changes them.
func (s *Store[K, F]) changeLocked(key K, update func(F) F) []Change[K, F] {
	cur, existed := s.Get(key)
	return s.storeLocked(key, cur, existed, update(cur))
}

func (s *Store[K, F]) storeLocked(key K, cur F, existed bool, next F) []Change[K, F] {
	var zero F
	exists := s.refs[key] != 0 || !reflect.DeepEqual(next, zero)
	if exists {
		s.entries.Store(key, &next)
	} else {
		s.entries.Delete(key)
	}
	if !existed && !exists {
		return nil
	}
	return []Change[K, F]{{Key: key, Old: cur, New: next, Existed: existed, Exists: exists}}
}

func notify[K comparable, F any](subscribers []*func(Change[K, F]), changed []Change[K, F]) {
	for _, c := range changed {
		for _, subscriber := range subscribers {
			(*subscriber)(c)
		}
	}
}
