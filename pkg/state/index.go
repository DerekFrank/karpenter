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
	"reflect"
	"slices"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Index relates each value of type V that an entry of a store returns to the keys of the entries that return it, such
// as the pods that mount each PVC. Make one with NewIndex.
type Index[K comparable, O, D, I any, V comparable] struct {
	s      *core[K, O, D, I]
	values func(K, Entry[O, D, I]) []V
	// of are each indexed key's values, unique, under the store's lock.
	of map[K][]V
	// keys are each value's keys, a []K replaced, never changed, on each change, so that Lookup reads without a lock.
	keys sync.Map // V -> []K
}

// indexer is an Index of a store, whatever its value type, which the store keeps under its lock.
type indexer[K comparable, O, D, I any] interface {
	updateLocked(key K, next *slot[O, D, I])
	clearLocked()
}

// NewIndex indexes s's entries by the values values returns of each, such as the pods by each PVC they mount, so that
// Lookup returns the keys of the entries that return a value. The store keeps it under its lock on every change of an
// entry: an entry that exists is indexed under each of its values, and one that stops existing leaves every value.
// NewIndex indexes the entries s holds before it returns. values runs under the store's lock, so it's pure, cheap and
// never blocks; it may return a value more than once.
func NewIndex[K comparable, O, D, I any, V comparable](s *Store[K, O, D, I], values func(K, Entry[O, D, I]) []V) *Index[K, O, D, I, V] {
	c := s.s
	x := &Index[K, O, D, I, V]{s: c, values: values, of: map[K][]V{}}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slots.Range(func(k, v any) bool {
		x.updateLocked(k.(K), v.(*slot[O, D, I]))
		return true
	})
	c.indexes = append(c.indexes, x)
	return x
}

// Lookup returns the keys of the entries that exist and that return v, in no particular order. It takes no lock: the
// keys are from one moment, which may be just before or after another reader's.
func (x *Index[K, O, D, I, V]) Lookup(v V) []K {
	return slices.Clone(x.lookup(v))
}

// List returns the entries that exist and that return v, such as the pods that mount a PVC: matching a field is the
// index's List, where the client's would take client.MatchingFields. It's Lookup then Get, so it skips a key whose entry
// has since gone, and each entry is from one moment, as Reader.List's are; an entry that changed since Lookup may no
// longer return v. It takes the options listOptionsOf parses, and copies each object as Reader.List does.
func (x *Index[K, O, D, I, V]) List(v V, opts ...client.ListOption) iter.Seq2[K, Entry[O, D, I]] {
	o := listOptionsOf[O](opts)
	keys := x.lookup(v)
	r := Reader[K, O, D, I]{x.s}
	return func(yield func(K, Entry[O, D, I]) bool) {
		for _, k := range keys {
			e, ok := r.get(k)
			if !ok || !o.matches(e.Object) {
				continue
			}
			if !o.unsafeDisableDeepCopy {
				e.Object = deepCopy(e.Object)
			}
			if !yield(k, e) {
				return
			}
		}
	}
}

// listOptions are the options a store's List takes.
type listOptions struct {
	// selector filters by the object's labels, nil for every object.
	selector labels.Selector
	// unsafeDisableDeepCopy is client.UnsafeDisableDeepCopy: the read returns the store's own objects.
	unsafeDisableDeepCopy bool
}

// listOptionsOf parses the options a Reader's List and an Index's List take: client.UnsafeDisableDeepCopy, and a label
// selector, client.MatchingLabels or client.MatchingLabelsSelector, which needs O to be a metav1.Object. Any other
// option panics, naming it, such as client.MatchingFields, which an Index's List serves, and client.InNamespace.
func listOptionsOf[O any](opts []client.ListOption) listOptions {
	var parsed client.ListOptions
	for _, opt := range opts {
		switch opt.(type) {
		case client.MatchingLabels, client.MatchingLabelsSelector, client.UnsafeDisableDeepCopyOption:
			opt.ApplyToList(&parsed)
		case client.MatchingFields, client.MatchingFieldsSelector:
			panic(fmt.Sprintf("a store's List doesn't take a %T: match a field with an Index's List", opt))
		default:
			panic(fmt.Sprintf("a store's List doesn't take a %T", opt))
		}
	}
	o := listOptions{selector: parsed.LabelSelector, unsafeDisableDeepCopy: parsed.UnsafeDisableDeepCopy != nil && *parsed.UnsafeDisableDeepCopy}
	if o.selector != nil && !reflect.TypeFor[O]().Implements(reflect.TypeFor[metav1.Object]()) {
		panic(fmt.Sprintf("a label selector on a store of %s, which isn't a metav1.Object", reflect.TypeFor[O]()))
	}
	return o
}

// getOptionsOf parses the options a Reader's Get takes, client.UnsafeDisableDeepCopy, and returns whether it's set. Any
// other option panics, naming it. It reads the option's value rather than applying it to a client.GetOptions, which
// allocates, since a hot read such as a scrape's per-pod Node lookup takes it.
func getOptionsOf(opts []client.GetOption) (unsafeDisableDeepCopy bool) {
	for _, opt := range opts {
		o, ok := opt.(client.UnsafeDisableDeepCopyOption)
		if !ok {
			panic(fmt.Sprintf("a store's Get doesn't take a %T", opt))
		}
		unsafeDisableDeepCopy = bool(o)
	}
	return unsafeDisableDeepCopy
}

// matches returns whether obj is one the options select.
func (o listOptions) matches(obj any) bool {
	return o.selector == nil || o.selector.Matches(labels.Set(obj.(metav1.Object).GetLabels()))
}

// lookup returns v's keys, which the caller doesn't change.
func (x *Index[K, O, D, I, V]) lookup(v V) []K {
	keys, _ := x.keys.Load(v)
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
		x.clearLocked()
	}
}

func (x *Index[K, O, D, I, V]) clearLocked() {
	x.of = map[K][]V{}
	x.keys.Clear()
}

// updateLocked moves key from the values it's indexed under to next's: none if next's entry doesn't exist.
func (x *Index[K, O, D, I, V]) updateLocked(key K, next *slot[O, D, I]) {
	var values []V
	if next.exists {
		values = unique(x.values(key, next.entry))
	}
	prev := x.of[key]
	if slices.Equal(prev, values) {
		return
	}
	for _, v := range prev {
		if !slices.Contains(values, v) {
			x.removeLocked(v, key)
		}
	}
	for _, v := range values {
		if !slices.Contains(prev, v) {
			x.addLocked(v, key)
		}
	}
	if len(values) == 0 {
		delete(x.of, key)
	} else {
		x.of[key] = values
	}
}

func (x *Index[K, O, D, I, V]) addLocked(v V, key K) {
	// Clip makes append copy, so a reader's keys never change.
	x.keys.Store(v, append(slices.Clip(x.lookup(v)), key))
}

func (x *Index[K, O, D, I, V]) removeLocked(v V, key K) {
	keys := x.lookup(v)
	i := slices.Index(keys, key)
	if i < 0 {
		return
	}
	if len(keys) == 1 {
		x.keys.Delete(v)
		return
	}
	x.keys.Store(v, slices.Delete(slices.Clone(keys), i, i+1))
}

// unique returns values without repeats, in their order, copying them only if they repeat, since they can be the
// entry's. An entry returns few values, so it compares each with those before it.
func unique[V comparable](values []V) []V {
	for i := 1; i < len(values); i++ {
		if slices.Contains(values[:i], values[i]) {
			out := slices.Clone(values[:i])
			for _, v := range values[i+1:] {
				if !slices.Contains(out, v) {
					out = append(out, v)
				}
			}
			return out
		}
	}
	return values
}

// DependsOn keeps the Derives from target current with dep, for a contribution that reads dep's entries, such as a
// pod's volume usage, which reads the PVCs it mounts: on each change of dep's entry under a key, including one that
// appears or drops, it touches each key of target that idx, an index of target by dep's keys, returns for it, so that
// the Derives from target recompute it. The touch stores the entry in a new slot, so a recompute that read dep before
// it changed is refused at commit, and the recompute the touch triggers holds. An idx of another store panics. It
// returns a func that stops it.
func DependsOn[K comparable, O, D, I any, K2 comparable, O2, D2, I2 any](target *Store[K, O, D, I], dep Reader[K2, O2, D2, I2], idx *Index[K, O, D, I, K2]) func() {
	if idx.s != target.s {
		panic("DependsOn with an index of another store")
	}
	return dep.Subscribe(func(c Change[K2, O2, D2, I2]) {
		for _, key := range idx.lookup(c.Key) {
			target.touch(key)
		}
	})
}
