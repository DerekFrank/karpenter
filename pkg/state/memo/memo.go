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

// Package memo caches values computed from the cache, per key, until an event invalidates them.
package memo

import (
	"context"
	"sync"
)

// Memo caches the value compute returns for each key until Invalidate or Reset drops it. compute reads the objects a
// value derives from, so the cache it reads is the source of truth and a Memo can be dropped at any time.
//
// Readers share the values Get returns, so neither compute's callers nor readers may modify them.
type Memo[K comparable, V any] struct {
	compute func(context.Context, K) (V, error)

	mu      sync.Mutex
	entries map[K]*entry[V]
}

type entry[V any] struct {
	value V
	valid bool
	// gen counts invalidations, so a computation that started before one doesn't cache what it read.
	gen uint64
	// computing counts computations in flight, which keep the entry, and so its gen, while it isn't valid.
	computing int
}

func New[K comparable, V any](compute func(context.Context, K) (V, error)) *Memo[K, V] {
	return &Memo[K, V]{compute: compute, entries: map[K]*entry[V]{}}
}

// Get returns key's cached value, or computes it. A value computed while key was invalidated is returned but not
// cached, since it may predate the change. Errors aren't cached.
func (m *Memo[K, V]) Get(ctx context.Context, key K) (V, error) {
	m.mu.Lock()
	e, ok := m.entries[key]
	if ok && e.valid {
		value := e.value
		m.mu.Unlock()
		return value, nil
	}
	if !ok {
		e = &entry[V]{}
		m.entries[key] = e
	}
	e.computing++
	gen := e.gen
	m.mu.Unlock()

	value, err := m.compute(ctx, key)

	m.mu.Lock()
	defer m.mu.Unlock()
	e.computing--
	if err == nil && e.gen == gen {
		e.value, e.valid = value, true
	} else if !e.valid && e.computing == 0 && m.entries[key] == e {
		delete(m.entries, key)
	}
	return value, err
}

// Invalidate drops the cached values of keys, so the next Get computes them again. Call it after every change that
// can change a value, once the change is readable through the cache compute reads.
func (m *Memo[K, V]) Invalidate(keys ...K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		if e, ok := m.entries[key]; ok {
			m.invalidateLocked(key, e)
		}
	}
}

// Reset drops every cached value, for a change whose keys the caller can't name.
func (m *Memo[K, V]) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, e := range m.entries {
		m.invalidateLocked(key, e)
	}
}

func (m *Memo[K, V]) invalidateLocked(key K, e *entry[V]) {
	if e.computing == 0 {
		delete(m.entries, key)
		return
	}
	e.gen++
	e.valid = false
	var zero V
	e.value = zero
}
