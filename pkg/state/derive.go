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
	// read computes a source key's contribution from its entry, and returns whether the entry exists.
	read       func(K1) (next record[K2, C], exists bool)
	fromSynced func() bool
	to         target[K2, D2]

	// records are what each source key whose entry exists contributes and the version of the entry it's from, plus, at
	// infinity, each source key that left while another apply was in flight, under to's lock. byKey are the
	// contributing source keys by the key they contribute to.
	records map[K1]record[K2, C]
	byKey   map[K2]map[K1]struct{}
	// buried are the keys of the records at infinity, which go once no apply is in flight.
	buried []K1 // bounded: source keys that left while an apply was in flight
	// inflight counts the applies between their read and their commit.
	inflight atomic.Int64
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
	// version is the version of the source entry the record is from.
	version entryVersion
	// contributes is whether the source contributes value to key: not when its key is zero, nor once it's buried.
	contributes bool
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
		read: func(k K1) (record[K2, C], bool) {
			// A source that's gone is at infinity in an incarnation as late as the store's count when it's read, which
			// beats every read of it from before it left, and loses to its next incarnation.
			gone := record[K2, C]{version: entryVersion{inc: src.count.Load(), n: infinity}}
			v, ok := src.slots.Load(k)
			if !ok {
				return gone, false
			}
			sl := v.(*slot[O1, D1, I1])
			if !sl.exists {
				return gone, false
			}
			next := record[K2, C]{key: key(k, sl.entry), version: sl.version}
			if next.key != zero {
				next.value, next.contributes = value(k, sl.entry), true
			}
			return next, true
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
		if r.contributes {
			d.to.derivedLocked(r.key, r.value.Delete, -1, false)
		}
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
	d.buried = nil
}

// apply applies k's latest entry. It computes the contribution without a lock, then commits it under to's lock only if
// it's from a newer version of k's entry than the record's, so an older recompute loses to a newer one and the last
// commit for k is from k's latest entry. A source that leaves is its record's incarnation at infinity, which beats
// every recompute from before it left and loses to the source's next incarnation.
func (d *Derivation[K1, K2, D2, C]) apply(k K1) {
	d.inflight.Add(1)
	next, exists := d.read(k)
	d.commit(k, next, exists)
}

// commit commits a record of k read from its entry, which exists or is gone, and ends the apply that read it.
func (d *Derivation[K1, K2, D2, C]) commit(k K1, next record[K2, C], exists bool) {
	d.to.lock()
	defer d.to.unlock()
	defer d.endLocked()
	if d.removed {
		return
	}
	prev, had := d.records[k]
	switch {
	case had && !prev.version.less(next.version), !exists && had && prev.version.n == infinity:
		return
	case !exists && !had && d.inflight.Load() == 1:
		return
	}
	if prev.contributes && (!next.contributes || prev.key != next.key) {
		if delete(d.byKey[prev.key], k); len(d.byKey[prev.key]) == 0 {
			delete(d.byKey, prev.key)
		}
	}
	if next.contributes && (!prev.contributes || prev.key != next.key) {
		if d.byKey[next.key] == nil {
			d.byKey[next.key] = map[K1]struct{}{}
		}
		d.byKey[next.key][k] = struct{}{}
	}
	switch {
	case exists:
		d.records[k] = next
	case d.inflight.Load() > 1:
		// Another apply may hold a read from before k left: bury k at infinity, so that read loses.
		d.records[k] = next
		d.buried = append(d.buried, k)
	default:
		delete(d.records, k)
	}
	moveContribution(d.to, prev, prev.contributes, next, next.contributes)
	for _, settle := range d.settles {
		settle(k, next.value, next.contributes)
	}
}

// endLocked ends an apply, and drops the buried records once no apply is in flight, since no read from before they
// left remains.
func (d *Derivation[K1, K2, D2, C]) endLocked() {
	if d.inflight.Add(-1) != 0 || len(d.buried) == 0 {
		return
	}
	for _, k := range d.buried {
		if r, ok := d.records[k]; ok && r.version.n == infinity {
			delete(d.records, k)
		}
	}
	d.buried = d.buried[:0]
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
