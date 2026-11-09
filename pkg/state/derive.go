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

// Contribution is what one entry of a store contributes to the Derived fields of an entry of another. Derive
// dispatches each change:
//   - new to a key: next.Update(fields, zero);
//   - same key, contribution changed: next.Update(fields, prev);
//   - same key, contribution equal: nothing;
//   - key changed: prev.Delete(fields) at the old key, then next.Update(fields, zero) at the new key;
//   - key became zero, or the source entry gone: prev.Delete(fields) at the old key.
//
// Delete undoes Update from the zero contribution, and Update(fields, prev) equals prev.Delete followed by
// Update(fields, zero). Both return new fields and leave the ones they get intact.
type Contribution[D, C any] interface {
	// Update returns fields with the receiver contributing to them in place of prev, the zero C if the source is new
	// to the key.
	Update(fields D, prev C) D
	// Delete returns fields without the receiver, which is leaving the key. It runs once per stay.
	Delete(fields D) D
}

// Equaler is a Contribution that compares itself to another, for one that's costly to compare by reflection. Equal
// reports whether Update would leave fields holding either one the same. A Contribution that isn't an Equaler compares
// with reflect.DeepEqual.
type Equaler[C any] interface {
	Equal(C) bool
}

// Derivation keeps the Derived fields of a store's entries up to date from the entries of another store. Make one with
// Derive.
type Derivation[K1, K2 comparable, D2 any, C Contribution[D2, C]] struct {
	// read computes a source key's contribution, and returns the version of its entry it computed from.
	read       func(K1) (version any, next record[K2, C], contributes bool)
	current    func(K1) any
	fromSynced func() bool
	to         target[K2, D2]

	// records and byKey are what each source key contributes, under to's lock.
	records map[K1]record[K2, C]
	byKey   map[K2]map[K1]struct{}
	// settles hand each change of a source key's contribution to each Claims against the Derivation, under to's lock.
	settles []func(source K1, value C, contributes bool)
	// removes end each Claims against the Derivation when it's removed, under to's lock.
	removes []func()
	// stops stop each Claims' expiry timer when the Derivation stops or is removed, under to's lock.
	stops []func()
	// removed is whether Remove took back what the Derivation contributed, under to's lock.
	removed bool

	swept       atomic.Bool
	unsubscribe func()
}

type record[K comparable, C any] struct {
	key   K
	value C
}

// target is the store a Derivation writes, under its lock.
type target[K comparable, D any] interface {
	lock()
	unlock()
	derivedLocked(key K, update func(D) D, refs int, compare bool)
	derivedOfLocked(key K) D
	addSourceLocked(id any, synced func() bool, cleared func())
	removeSourceLocked(id any)
}

func (c *core[K, O, D, I]) lock() { c.mu.Lock() }

func (c *core[K, O, D, I]) derivedOfLocked(key K) D {
	sl, _ := c.load(key)
	return sl.entry.Derived
}

func (c *core[K, O, D, I]) addSourceLocked(id any, synced func() bool, cleared func()) {
	if synced != nil {
		c.sources[id] = synced
	}
	if cleared != nil {
		c.cleared[id] = cleared
	}
}

func (c *core[K, O, D, I]) removeSourceLocked(id any) {
	delete(c.sources, id)
	delete(c.cleared, id)
}

// Derive keeps the Derived fields of to's entries up to date from from's entries, such as each NodeClaim's capacity
// summed onto its NodePool's entry. Each entry of from contributes value(key, entry) to the key that key(key, entry)
// returns in to, dispatched to the contribution's Update and Delete as Contribution describes. A zero key means an
// entry contributes to nothing. A join is a Derive onto an entry, such as a Node's name onto its NodeClaim's.
//
// key and value are pure: they read only the source entry, never block and do no I/O. A value that depends on another
// store's entry reads it, and a subscriber to that store Touches the source when it changes.
//
// Derive applies from's entries before it returns, and each change of from after from's lock is released, so to can
// briefly lag from. to is synced once from is.
func Derive[K1, K2 comparable, O1, D1, I1, O2, D2, I2 any, C Contribution[D2, C]](from Reader[K1, O1, D1, I1], to *Store[K2, O2, D2, I2],
	key func(K1, Entry[O1, D1, I1]) K2, value func(K1, Entry[O1, D1, I1]) C) *Derivation[K1, K2, D2, C] {
	return derive(from, to, key, value, nil)
}

// DeriveObject is Derive for a contribution computed from the source entry's object alone, such as a Node's NodePool
// label: it recomputes an entry's contribution only when its object changes, so a change of the entry's Derived or
// Intent, such as a pod binding to a Node, costs a comparison.
func DeriveObject[K1, K2 comparable, O1 comparable, D1, I1, O2, D2, I2 any, C Contribution[D2, C]](from Reader[K1, O1, D1, I1], to *Store[K2, O2, D2, I2],
	key func(K1, O1) K2, value func(K1, O1) C) *Derivation[K1, K2, D2, C] {
	return derive(from, to,
		func(k K1, e Entry[O1, D1, I1]) K2 { return key(k, e.Object) },
		func(k K1, e Entry[O1, D1, I1]) C { return value(k, e.Object) },
		func(c Change[K1, O1, D1, I1]) bool { return c.Existed && c.Exists && c.Old.Object == c.New.Object })
}

func derive[K1, K2 comparable, O1, D1, I1, O2, D2, I2 any, C Contribution[D2, C]](from Reader[K1, O1, D1, I1], to *Store[K2, O2, D2, I2],
	key func(K1, Entry[O1, D1, I1]) K2, value func(K1, Entry[O1, D1, I1]) C, unchanged func(Change[K1, O1, D1, I1]) bool) *Derivation[K1, K2, D2, C] {
	src := from.s
	var zero K2
	d := &Derivation[K1, K2, D2, C]{
		read: func(k K1) (any, record[K2, C], bool) {
			v, ok := src.slots.Load(k)
			if !ok {
				return nil, record[K2, C]{}, false
			}
			sl := v.(*slot[O1, D1, I1])
			if !sl.exists {
				return v, record[K2, C]{}, false
			}
			next := record[K2, C]{key: key(k, sl.entry)}
			if next.key == zero {
				return v, next, false
			}
			next.value = value(k, sl.entry)
			return v, next, true
		},
		current: func(k K1) any {
			v, _ := src.slots.Load(k)
			return v
		},
		fromSynced: from.Synced,
		to:         to.s,
		records:    map[K1]record[K2, C]{},
		byKey:      map[K2]map[K1]struct{}{},
	}
	if any(src) == any(to.s) {
		// A Derive from a store onto itself, such as a Node's repair matches onto its entry, is synced once its sweep is:
		// the store reports its own feed's sync.
		d.fromSynced = func() bool { return true }
	}
	to.s.lock()
	to.s.addSourceLocked(d, d.Synced, d.clearLocked)
	to.s.unlock()
	d.unsubscribe = from.Subscribe(func(c Change[K1, O1, D1, I1]) {
		if unchanged == nil || !unchanged(c) {
			d.apply(c.Key)
		}
	})
	for k := range from.List() {
		d.apply(k)
	}
	d.swept.Store(true)
	return d
}

// Synced returns whether the target holds what from held once from synced.
func (d *Derivation[K1, K2, D2, C]) Synced() bool { return d.swept.Load() && d.fromSynced() }

// Stop stops following from, leaving what it contributed, and stops the expiry timers of the Claims against it, whose
// claims then hold until they're bound and settled or released.
func (d *Derivation[K1, K2, D2, C]) Stop() {
	d.unsubscribe()
	d.to.lock()
	defer d.to.unlock()
	for _, stop := range d.stops {
		stop()
	}
}

// Remove stops following from and takes back everything the Derivation contributed, ending the claims against it, for
// a Derivation whose configuration is replaced, such as a CapacityQuota whose selector changed. The target forgets the
// Derivation and its Claims, so a Derivation built and removed leaves nothing behind. A Claims against a removed
// Derivation grants nothing.
func (d *Derivation[K1, K2, D2, C]) Remove() {
	d.unsubscribe()
	d.to.lock()
	d.removed = true
	for _, remove := range d.removes {
		remove()
	}
	for _, stop := range d.stops {
		stop()
	}
	for _, r := range d.records {
		d.to.derivedLocked(r.key, r.value.Delete, -1, false)
	}
	d.clearLocked()
	d.to.removeSourceLocked(d)
	d.to.unlock()
}

// Sources returns the keys of from that contribute to key, such as the pods bound to a Node.
func (d *Derivation[K1, K2, D2, C]) Sources(key K2) []K1 {
	d.to.lock()
	defer d.to.unlock()
	sources := make([]K1, 0, len(d.byKey[key]))
	for k := range d.byKey[key] {
		sources = append(sources, k)
	}
	return sources
}

func (d *Derivation[K1, K2, D2, C]) clearLocked() {
	d.records = map[K1]record[K2, C]{}
	d.byKey = map[K2]map[K1]struct{}{}
}

// apply applies k's latest entry. It computes the contribution without a lock, then commits it under to's lock only if
// from still holds the entry it computed from, and recomputes otherwise, so the last commit for k is from k's latest
// entry. A store's slots never change once stored, so the slot pointer identifies a version of the entry.
func (d *Derivation[K1, K2, D2, C]) apply(k K1) {
	for {
		read, next, contributes := d.read(k)
		if d.commit(k, read, next, contributes) {
			return
		}
	}
}

// commit commits a contribution of k computed from the entry read, and returns false, committing nothing, if from no
// longer holds that entry.
func (d *Derivation[K1, K2, D2, C]) commit(k K1, read any, next record[K2, C], contributes bool) bool {
	d.to.lock()
	defer d.to.unlock()
	if d.current(k) != read {
		return false
	}
	if !d.removed {
		d.commitLocked(k, next, contributes)
	}
	return true
}

func (d *Derivation[K1, K2, D2, C]) commitLocked(k K1, next record[K2, C], contributes bool) {
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
	moveContribution(d.to, prev, had, next, contributes)
	for _, settle := range d.settles {
		settle(k, next.value, contributes)
	}
}

// moveContribution moves a source's contribution from prev to next, dispatching to Update and Delete as Contribution
// describes.
func moveContribution[K comparable, D any, C Contribution[D, C]](to target[K, D], prev record[K, C], had bool, next record[K, C], contributes bool) {
	if had && contributes && prev.key == next.key {
		if equal(prev.value, next.value) {
			return
		}
		// The contribution changed, so the fields did too; don't compare them again.
		to.derivedLocked(next.key, func(f D) D { return next.value.Update(f, prev.value) }, 0, false)
		return
	}
	if had {
		to.derivedLocked(prev.key, prev.value.Delete, -1, false)
	}
	if contributes {
		var zero C
		to.derivedLocked(next.key, func(f D) D { return next.value.Update(f, zero) }, 1, false)
	}
}

func equal[C any](a, b C) bool {
	if e, ok := any(b).(Equaler[C]); ok {
		return e.Equal(a)
	}
	return reflect.DeepEqual(a, b)
}
