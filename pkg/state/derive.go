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
	"reflect"
	"sync/atomic"
)

// Contribution is what one key of a store contributes to a key of a derived store. Derive dispatches each change:
//   - new to a key: next.Update(fields, zero);
//   - same key, contribution changed: next.Update(fields, prev);
//   - same key, contribution equal: nothing;
//   - key changed: prev.Delete(fields) at the old key, then next.Update(fields, zero) at the new key;
//   - key became zero, or the source key gone: prev.Delete(fields) at the old key.
//
// Delete undoes Update from the zero contribution, and Update(fields, prev) equals prev.Delete followed by
// Update(fields, zero). Both return new fields and leave the ones they get intact.
type Contribution[F, C any] interface {
	// Update returns fields with the receiver contributing to them in place of prev, the zero C if the source is new
	// to the key.
	Update(fields F, prev C) F
	// Delete returns fields without the receiver, which is leaving the key. It runs once per stay.
	Delete(fields F) F
}

// Equaler is a Contribution that compares itself to another, for one that's costly to compare by reflection. Equal
// reports whether Update would leave fields holding either one the same. A Contribution that isn't an Equaler compares
// with reflect.DeepEqual.
type Equaler[C any] interface {
	Equal(C) bool
}

// Derivation keeps the fields of a derived store up to date from the fields of another store. Make one with Derive.
type Derivation[K1, K2 comparable, F1, F2 any, C Contribution[F2, C]] struct {
	from  *Store[K1, F1]
	to    *Store[K2, F2]
	key   func(K1, F1) K2
	value func(K1, F1) C

	// records and byKey are what each source key contributes, under to's lock.
	records map[K1]record[K2, C]
	byKey   map[K2]map[K1]struct{}
	// settle hands each change of a source key's contribution to the Claims against to, if any, under to's lock.
	settle func(source K1, value C, contributes bool) []Change[K2, F2]

	swept       atomic.Bool
	unsubscribe func()
}

type record[K comparable, C any] struct {
	key   K
	value C
}

// Derive keeps fields of to up to date from the fields of from, such as each NodeClaim's capacity summed onto its
// NodePool. Each key of from contributes value(key, fields) to the key that key(key, fields) returns in to, dispatched
// to the contribution's Update and Delete as Contribution describes. A zero key means a key contributes to nothing.
//
// Derive applies from's keys before it returns, and each change of from after from's lock is released, so to can
// briefly lag from. to is synced once from is.
func Derive[K1, K2 comparable, F1, F2 any, C Contribution[F2, C]](from *Store[K1, F1], to *Store[K2, F2], key func(K1, F1) K2, value func(K1, F1) C) *Derivation[K1, K2, F1, F2, C] {
	d := &Derivation[K1, K2, F1, F2, C]{
		from:    from,
		to:      to,
		key:     key,
		value:   value,
		records: map[K1]record[K2, C]{},
		byKey:   map[K2]map[K1]struct{}{},
	}
	to.mu.Lock()
	to.sources = append(to.sources, d.Synced)
	to.cleared = append(to.cleared, d.clearLocked)
	to.mu.Unlock()
	d.unsubscribe = from.Subscribe(func(c Change[K1, F1]) { d.apply(c.Key) })
	for k := range from.List() {
		d.apply(k)
	}
	d.swept.Store(true)
	return d
}

// Synced returns whether the derived store holds what from held once from synced.
func (d *Derivation[K1, K2, F1, F2, C]) Synced() bool { return d.swept.Load() && d.from.Synced() }

// Stop stops following from, leaving what it contributed.
func (d *Derivation[K1, K2, F1, F2, C]) Stop() { d.unsubscribe() }

// Sources returns the keys of from that contribute to key, such as the pods bound to a node.
func (d *Derivation[K1, K2, F1, F2, C]) Sources(key K2) []K1 {
	d.to.mu.Lock()
	defer d.to.mu.Unlock()
	sources := make([]K1, 0, len(d.byKey[key]))
	for k := range d.byKey[key] {
		sources = append(sources, k)
	}
	return sources
}

func (d *Derivation[K1, K2, F1, F2, C]) clearLocked() {
	d.records = map[K1]record[K2, C]{}
	d.byKey = map[K2]map[K1]struct{}{}
}

// apply applies k's latest fields. It computes the contribution without a lock, then commits it under to's lock only if
// from still holds the fields it computed from, and recomputes otherwise, so the last commit for k is from k's latest
// fields. A store's entries never change once stored, so the entry pointer identifies a version of the fields.
func (d *Derivation[K1, K2, F1, F2, C]) apply(k K1) {
	var zero K2
	for {
		read, _ := d.from.entries.Load(k)
		next := record[K2, C]{}
		contributes := false
		if read != nil {
			fields := *read.(*F1)
			next.key = d.key(k, fields)
			if contributes = next.key != zero; contributes {
				next.value = d.value(k, fields)
			}
		}
		d.to.mu.Lock()
		if current, _ := d.from.entries.Load(k); current != read {
			d.to.mu.Unlock()
			continue
		}
		changed := d.commitLocked(k, next, contributes)
		subscribers := d.to.subscribers
		d.to.mu.Unlock()
		notify(subscribers, changed)
		return
	}
}

func (d *Derivation[K1, K2, F1, F2, C]) commitLocked(k K1, next record[K2, C], contributes bool) []Change[K2, F2] {
	prev, had := d.records[k]
	if had && (!contributes || prev.key != next.key) {
		if delete(d.byKey[prev.key], k); len(d.byKey[prev.key]) == 0 {
			delete(d.byKey, prev.key)
		}
	}
	if contributes {
		d.records[k] = next
		if !had || prev.key != next.key {
			if d.byKey[next.key] == nil {
				d.byKey[next.key] = map[K1]struct{}{}
			}
			d.byKey[next.key][k] = struct{}{}
		}
	} else {
		delete(d.records, k)
	}
	changed := moveContribution(d.to, prev, had, next, contributes)
	if d.settle != nil {
		changed = append(changed, d.settle(k, next.value, contributes)...)
	}
	return changed
}

// moveContribution moves a source's contribution from prev to next, dispatching to Update and Delete as Contribution
// describes.
func moveContribution[K comparable, F any, C Contribution[F, C]](store *Store[K, F], prev record[K, C], had bool, next record[K, C], contributes bool) []Change[K, F] {
	if had && contributes && prev.key == next.key {
		if equal(prev.value, next.value) {
			return nil
		}
		// The contribution changed, so the fields did too; don't compare them again.
		return store.changeLocked(next.key, func(f F) F { return next.value.Update(f, prev.value) })
	}
	var changed []Change[K, F]
	if had {
		changed = append(changed, store.updateLocked(prev.key, prev.value.Delete, -1)...)
	}
	if contributes {
		var zero C
		changed = append(changed, store.updateLocked(next.key, func(f F) F { return next.value.Update(f, zero) }, 1)...)
	}
	return changed
}

func equal[C any](a, b C) bool {
	if e, ok := any(b).(Equaler[C]); ok {
		return e.Equal(a)
	}
	return reflect.DeepEqual(a, b)
}
