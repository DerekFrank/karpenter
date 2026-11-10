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
	"slices"
	"sync/atomic"
)

// EachDerivation keeps the Derived fields of a store's entries up to date from the entries of another store, each of
// which contributes to a set of keys. Make one with DeriveEach.
type EachDerivation[K1, K2 comparable, D2 any, C Contribution[D2, C]] struct {
	// read computes a source key's contributions, and returns the version of its entry it computed from.
	read       func(K1) (version any, next []record[K2, C])
	current    func(K1) any
	fromSynced func() bool
	to         target[K2, D2]

	// records and byKey are what each source key contributes, under to's lock.
	records map[K1][]record[K2, C]
	byKey   map[K2]map[K1]struct{}
	// removed is whether Remove took back what the EachDerivation contributed, under to's lock.
	removed bool

	swept       atomic.Bool
	unsubscribe func()
}

// DeriveEach is Derive for a source entry that contributes to several keys, such as a pod to each PVC it mounts: each
// entry of from contributes value(key, entry, k) to every key k that keys(key, entry) returns in to. Each key is
// dispatched to the contribution's Update and Delete as Contribution describes for a key that stays, and a key that
// joins or leaves the set is one the source is new to or leaving. Zero and repeated keys are dropped. The laws hold per
// key, and Sources(k) lists the sources whose set holds k.
//
// One change of a source applies all its keys under to's lock, so what serializes with the lock, such as Sources or
// a subscriber of to, sees all of a change or none of it. A Get of one key and then another doesn't take the lock,
// and can see one key changed and the other not yet, as with any two keys.
//
// keys and value are pure, as Derive's key and value are. A key set is small, such as the PVCs a pod mounts: a change
// compares the old set with the new in O(len(old)·len(new)).
func DeriveEach[K1, K2 comparable, O1, D1, I1, O2, D2, I2 any, C Contribution[D2, C]](from Reader[K1, O1, D1, I1], to *Store[K2, O2, D2, I2],
	keys func(K1, Entry[O1, D1, I1]) []K2, value func(K1, Entry[O1, D1, I1], K2) C) *EachDerivation[K1, K2, D2, C] {
	return deriveEach(from, to, keys, value, nil)
}

// DeriveEachObject is DeriveEach for contributions computed from the source entry's object alone, as DeriveObject is
// for Derive.
func DeriveEachObject[K1, K2 comparable, O1 comparable, D1, I1, O2, D2, I2 any, C Contribution[D2, C]](from Reader[K1, O1, D1, I1], to *Store[K2, O2, D2, I2],
	keys func(K1, O1) []K2, value func(K1, O1, K2) C) *EachDerivation[K1, K2, D2, C] {
	return deriveEach(from, to,
		func(k K1, e Entry[O1, D1, I1]) []K2 { return keys(k, e.Object) },
		func(k K1, e Entry[O1, D1, I1], to K2) C { return value(k, e.Object, to) },
		func(c Change[K1, O1, D1, I1]) bool { return c.Existed && c.Exists && c.Old.Object == c.New.Object })
}

func deriveEach[K1, K2 comparable, O1, D1, I1, O2, D2, I2 any, C Contribution[D2, C]](from Reader[K1, O1, D1, I1], to *Store[K2, O2, D2, I2],
	keys func(K1, Entry[O1, D1, I1]) []K2, value func(K1, Entry[O1, D1, I1], K2) C, unchanged func(Change[K1, O1, D1, I1]) bool) *EachDerivation[K1, K2, D2, C] {
	src := from.s
	var zero K2
	d := &EachDerivation[K1, K2, D2, C]{
		read: func(k K1) (any, []record[K2, C]) {
			v, ok := src.slots.Load(k)
			if !ok {
				return nil, nil
			}
			sl := v.(*slot[O1, D1, I1])
			if !sl.exists {
				return v, nil
			}
			var next []record[K2, C]
			for _, key := range keys(k, sl.entry) {
				if key == zero || slices.ContainsFunc(next, func(r record[K2, C]) bool { return r.key == key }) {
					continue
				}
				next = append(next, record[K2, C]{key: key, value: value(k, sl.entry, key)})
			}
			return v, next
		},
		current: func(k K1) any {
			v, _ := src.slots.Load(k)
			return v
		},
		fromSynced: from.Synced,
		to:         to.s,
		records:    map[K1][]record[K2, C]{},
		byKey:      map[K2]map[K1]struct{}{},
	}
	if any(src) == any(to.s) {
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
func (d *EachDerivation[K1, K2, D2, C]) Synced() bool { return d.swept.Load() && d.fromSynced() }

// Stop stops following from, leaving what it contributed.
func (d *EachDerivation[K1, K2, D2, C]) Stop() { d.unsubscribe() }

// Remove stops following from and takes back everything the EachDerivation contributed. The target forgets it, so an
// EachDerivation built and removed leaves nothing behind.
func (d *EachDerivation[K1, K2, D2, C]) Remove() {
	d.unsubscribe()
	d.to.lock()
	d.removed = true
	for _, rs := range d.records {
		for _, r := range rs {
			d.to.derivedLocked(r.key, r.value.Delete, -1, false)
		}
	}
	d.clearLocked()
	d.to.removeSourceLocked(d)
	d.to.unlock()
}

// Sources returns the keys of from whose key set holds key, such as the pods that mount a PVC.
func (d *EachDerivation[K1, K2, D2, C]) Sources(key K2) []K1 {
	d.to.lock()
	defer d.to.unlock()
	sources := make([]K1, 0, len(d.byKey[key]))
	for k := range d.byKey[key] {
		sources = append(sources, k)
	}
	return sources
}

func (d *EachDerivation[K1, K2, D2, C]) clearLocked() {
	d.records = map[K1][]record[K2, C]{}
	d.byKey = map[K2]map[K1]struct{}{}
}

// apply applies k's latest entry, committing it only if from still holds the entry it computed from, as Derive's does.
func (d *EachDerivation[K1, K2, D2, C]) apply(k K1) {
	for {
		read, next := d.read(k)
		d.to.lock()
		if d.current(k) != read {
			d.to.unlock()
			continue
		}
		if !d.removed {
			d.commitLocked(k, next)
		}
		d.to.unlock()
		return
	}
}

// commitLocked moves k's contributions from the set it recorded to next, key by key.
func (d *EachDerivation[K1, K2, D2, C]) commitLocked(k K1, next []record[K2, C]) {
	prev := d.records[k]
	for _, p := range prev {
		i := slices.IndexFunc(next, func(r record[K2, C]) bool { return r.key == p.key })
		moveContribution(d.to, p, true, recordAt(next, i), i >= 0)
		if i < 0 {
			if delete(d.byKey[p.key], k); len(d.byKey[p.key]) == 0 {
				delete(d.byKey, p.key)
			}
		}
	}
	for _, n := range next {
		if slices.ContainsFunc(prev, func(r record[K2, C]) bool { return r.key == n.key }) {
			continue
		}
		moveContribution(d.to, record[K2, C]{}, false, n, true)
		if d.byKey[n.key] == nil {
			d.byKey[n.key] = map[K1]struct{}{}
		}
		d.byKey[n.key][k] = struct{}{}
	}
	if len(next) == 0 {
		delete(d.records, k)
	} else {
		d.records[k] = next
	}
}

// recordAt returns rs[i], or the zero record for a negative i.
func recordAt[K comparable, C any](rs []record[K, C], i int) record[K, C] {
	if i < 0 {
		return record[K, C]{}
	}
	return rs[i]
}
