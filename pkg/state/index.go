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
	"fmt"
	"iter"
	"slices"
	"sync"
)

// index relates each value values returns of an entry to the keys of the entries that return it.
type index[K comparable, O, D, I any] struct {
	values func(K, Entry[O, D, I]) []string
	// of are each indexed key's values, sorted and unique, under the store's lock.
	of map[K][]string
	// keys are each value's keys, a []K replaced, never changed, on each change, so that Lookup reads without a lock.
	keys sync.Map // string -> []K
}

// Index indexes the store's entries by the values values returns of each, such as the pods by each PVC they mount, so
// that Lookup returns the keys of the entries that return a value. The store keeps it under its lock on every change of
// an entry: an entry that exists is indexed under each of its values, and one that stops existing leaves every value.
// Index indexes the entries the store holds before it returns. values runs under the store's lock, so it's pure, cheap
// and never blocks; it may return a value more than once. A name indexed twice panics.
func (s *Store[K, O, D, I]) Index(name string, values func(K, Entry[O, D, I]) []string) {
	c := s.s
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.indexByName.Load(name); ok {
		panic(fmt.Sprintf("index %q is already defined", name))
	}
	x := &index[K, O, D, I]{values: values, of: map[K][]string{}}
	c.slots.Range(func(k, v any) bool {
		x.updateLocked(k.(K), v.(*slot[O, D, I]))
		return true
	})
	c.indexes = append(c.indexes, x)
	c.indexByName.Store(name, x)
}

// Lookup returns the keys of the entries that exist and that the index name indexes under value, in no particular
// order. It takes no lock: the keys are from one moment, which may be just before or after another reader's. A name
// Index hasn't defined panics.
func (r Reader[K, O, D, I]) Lookup(name, value string) []K {
	return slices.Clone(r.s.index(name).lookup(value))
}

// ListMatching returns the entries that exist and that the index name indexes under value, such as the pods that mount
// a PVC: List served by the index, as client.MatchingFields{name: value} is served by a field index. Each entry is from
// one moment, as List's are, so an entry that changed since Lookup may no longer match. A name Index hasn't defined
// panics.
func (r Reader[K, O, D, I]) ListMatching(name, value string) iter.Seq2[K, Entry[O, D, I]] {
	keys := r.s.index(name).lookup(value)
	return func(yield func(K, Entry[O, D, I]) bool) {
		for _, k := range keys {
			if e, ok := r.Get(k); ok && !yield(k, e) {
				return
			}
		}
	}
}

func (c *core[K, O, D, I]) index(name string) *index[K, O, D, I] {
	x, ok := c.indexByName.Load(name)
	if !ok {
		panic(fmt.Sprintf("index %q isn't defined", name))
	}
	return x.(*index[K, O, D, I])
}

// lookup returns value's keys, which the caller doesn't change.
func (x *index[K, O, D, I]) lookup(value string) []K {
	keys, _ := x.keys.Load(value)
	ks, _ := keys.([]K)
	return ks
}

// indexLocked moves key in each index to the values of next, the slot about to be stored under it.
func (c *core[K, O, D, I]) indexLocked(key K, next *slot[O, D, I]) {
	for _, x := range c.indexes {
		x.updateLocked(key, next)
	}
}

// clearIndexesLocked empties each index, for a store Clear empties.
func (c *core[K, O, D, I]) clearIndexesLocked() {
	for _, x := range c.indexes {
		x.of = map[K][]string{}
		x.keys.Clear()
	}
}

// updateLocked moves key from the values it's indexed under to next's: none if next's entry doesn't exist.
func (x *index[K, O, D, I]) updateLocked(key K, next *slot[O, D, I]) {
	var values []string
	if next.exists {
		values = normalized(x.values(key, next.entry))
	}
	prev := x.of[key]
	if slices.Equal(prev, values) {
		return
	}
	for _, v := range prev {
		if _, ok := slices.BinarySearch(values, v); !ok {
			x.removeLocked(v, key)
		}
	}
	for _, v := range values {
		if _, ok := slices.BinarySearch(prev, v); !ok {
			x.addLocked(v, key)
		}
	}
	if len(values) == 0 {
		delete(x.of, key)
	} else {
		x.of[key] = values
	}
}

func (x *index[K, O, D, I]) addLocked(value string, key K) {
	// Clip makes append copy, so a reader's keys never change.
	x.keys.Store(value, append(slices.Clip(x.lookup(value)), key))
}

func (x *index[K, O, D, I]) removeLocked(value string, key K) {
	keys := x.lookup(value)
	i := slices.Index(keys, key)
	if i < 0 {
		return
	}
	if len(keys) == 1 {
		x.keys.Delete(value)
		return
	}
	x.keys.Store(value, slices.Delete(slices.Clone(keys), i, i+1))
}

// normalized returns values sorted and unique, copying them only if they aren't already, since they can be the entry's.
func normalized(values []string) []string {
	for i := 1; i < len(values); i++ {
		if values[i-1] >= values[i] {
			values = slices.Clone(values)
			slices.Sort(values)
			return slices.Compact(values)
		}
	}
	return values
}

// DependsOn keeps the Derives from target current with dep, for a contribution that reads dep's entries, such as a
// pod's volume usage, which reads the drivers of the PVCs it mounts: on each change of dep's entry under a key,
// including one that appears or drops, it touches each key of target that target's index named index returns for the
// dep key's string form, so that the Derives from target recompute it. The touch stores the entry in a new slot, so a
// recompute that read dep before it changed is refused at commit, and the recompute the touch triggers holds. A dep
// key's string form is the key itself for a string, and its String() for a fmt.Stringer, such as types.NamespacedName's
// "namespace/name"; any other key type panics. An index Index hasn't defined panics. It returns a func that stops it.
func DependsOn[K comparable, O, D, I any, K2 comparable, O2, D2, I2 any](target *Store[K, O, D, I], dep Reader[K2, O2, D2, I2], index string) func() {
	x := target.s.index(index)
	str := keyString[K2]()
	return dep.Subscribe(func(c Change[K2, O2, D2, I2]) {
		for _, key := range x.lookup(str(c.Key)) {
			target.touch(key)
		}
	})
}

// keyString returns how DependsOn turns a key of type K into an index value.
func keyString[K comparable]() func(K) string {
	var zero K
	switch any(zero).(type) {
	case string:
		return func(k K) string { return any(k).(string) }
	case fmt.Stringer:
		return func(k K) string { return any(k).(fmt.Stringer).String() }
	}
	panic(fmt.Sprintf("DependsOn on a store keyed by %T, which is neither a string nor a fmt.Stringer", zero))
}
