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
	"context"
	"fmt"

	fieldselector "k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// View is an object with the fields Karpenter derives about it and the intent it keeps about it. Object is a copy
// unless the reader opts out with client.UnsafeDisableDeepCopy, so it can be written, watched and recorded like any
// other. Derived and Intent are shared and immutable, so never modify them.
type View[O client.Object, D, I any] struct {
	Object  O
	Derived D
	Intent  I
}

// Accessor reads objects of kind O through the client with their entries' Derived and Intent: a state accessor for a
// kind whose readers need the client's own reads, such as NodePools. Read the objects that have an Accessor only
// through it, and use the client only to write them.
//
// The object is a version the client holds, and the fields are their store's at the time of the read, so they can lag
// the object.
type Accessor[O client.Object, L client.ObjectList, D, I any] struct {
	client  client.Reader
	newObj  func() O
	newList func() L
	items   func(L) []O
	entry   func(O) (D, I)
	sources []source.SyncingSource
}

// NewAccessor returns an Accessor that reads objects through kubeClient into new objects from newObj, lists them with a
// list from newList whose items are items, and reads their fields with entry, which reads the stores that keep them
// and must not block.
func NewAccessor[O client.Object, L client.ObjectList, D, I any](kubeClient client.Reader, newObj func() O, newList func() L, items func(L) []O,
	entry func(O) (D, I), sources ...source.SyncingSource) *Accessor[O, L, D, I] {
	return &Accessor[O, L, D, I]{client: kubeClient, newObj: newObj, newList: newList, items: items, entry: entry, sources: sources}
}

// Get returns the object named key with its fields.
func (r *Accessor[O, L, D, I]) Get(ctx context.Context, key client.ObjectKey, opts ...client.GetOption) (View[O, D, I], error) {
	obj := r.newObj()
	if err := r.client.Get(ctx, key, obj, opts...); err != nil {
		return View[O, D, I]{}, err
	}
	d, i := r.entry(obj)
	return View[O, D, I]{Object: obj, Derived: d, Intent: i}, nil
}

// List returns the objects that opts select, with their fields.
func (r *Accessor[O, L, D, I]) List(ctx context.Context, opts ...client.ListOption) ([]View[O, D, I], error) {
	list := r.newList()
	if err := r.client.List(ctx, list, opts...); err != nil {
		return nil, err
	}
	items := r.items(list)
	objects := make([]View[O, D, I], len(items))
	for i, obj := range items {
		d, in := r.entry(obj)
		objects[i] = View[O, D, I]{Object: obj, Derived: d, Intent: in}
	}
	return objects, nil
}

// StoreAccessor reads the entries of an object store as Views. Each read returns a copy of the object unless the
// caller opts out with client.UnsafeDisableDeepCopy, and must then leave the object intact. It serves List selectors
// and field indexes as the client does.
type StoreAccessor[K comparable, O client.Object, D, I any] struct {
	store   Reader[K, O, D, I]
	indexes map[string]func(string) []K
}

// indexed counts the keys whose entries index to one value. The keys themselves are the index Derive's sources, kept
// in place, because a set of them in the fields would be copied on every change.
type indexed int

func (i indexed) Update(f int, prev indexed) int { return f + int(i-prev) }
func (i indexed) Delete(f int) int               { return f - int(i) }

// NewStoreAccessor returns a StoreAccessor over store.
func NewStoreAccessor[K comparable, O client.Object, D, I any](store Reader[K, O, D, I]) *StoreAccessor[K, O, D, I] {
	return &StoreAccessor[K, O, D, I]{store: store, indexes: map[string]func(string) []K{}}
}

// IndexField indexes field, so that List with client.MatchingFields{field: value} reads only the keys whose entries
// index to value, as a field index does for the client's List. index returns a key's value, or "" for a key the index
// leaves out. Index every field before the accessor's first List.
func (a *StoreAccessor[K, O, D, I]) IndexField(field string, index func(K, Entry[O, D, I]) string) *StoreAccessor[K, O, D, I] {
	values := NewSyntheticStore[string, string, int, struct{}](func(v string) string { return v })
	a.indexes[field] = Derive(a.store, values, index, func(K, Entry[O, D, I]) indexed { return 1 }).Sources
	return a
}

// Get returns key's entry as a View, and whether it exists.
func (a *StoreAccessor[K, O, D, I]) Get(key K, opts ...client.GetOption) (View[O, D, I], bool) {
	e, ok := a.store.Get(key)
	if !ok {
		return View[O, D, I]{}, false
	}
	obj := e.Object
	if o := (&client.GetOptions{}).ApplyOptions(opts); o.UnsafeDisableDeepCopy == nil || !*o.UnsafeDisableDeepCopy {
		obj = obj.DeepCopyObject().(O)
	}
	return View[O, D, I]{Object: obj, Derived: e.Derived, Intent: e.Intent}, true
}

// List returns the entries that opts select, as Views. A field selector selects on indexed fields only, each
// requirement an equality, as the client's List does for a cache with field indexes.
func (a *StoreAccessor[K, O, D, I]) List(opts ...client.ListOption) ([]View[O, D, I], error) {
	o := (&client.ListOptions{}).ApplyOptions(opts)
	shared := o.UnsafeDisableDeepCopy != nil && *o.UnsafeDisableDeepCopy
	keys, err := a.selected(o.FieldSelector)
	if err != nil {
		return nil, err
	}
	var views []View[O, D, I]
	add := func(e Entry[O, D, I]) {
		obj := e.Object
		if o.LabelSelector != nil && !o.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
			return
		}
		if !shared {
			obj = obj.DeepCopyObject().(O)
		}
		views = append(views, View[O, D, I]{Object: obj, Derived: e.Derived, Intent: e.Intent})
	}
	if keys == nil {
		for _, e := range a.store.List() {
			add(e)
		}
		return views, nil
	}
	for k := range keys {
		if e, ok := a.store.Get(k); ok {
			add(e)
		}
	}
	return views, nil
}

// selected returns the keys selector selects through the indexes, or nil to select every key.
func (a *StoreAccessor[K, O, D, I]) selected(selector fieldselector.Selector) (map[K]struct{}, error) {
	if selector == nil || selector.Empty() {
		return nil, nil
	}
	var keys map[K]struct{}
	for i, r := range selector.Requirements() {
		sources, ok := a.indexes[r.Field]
		if !ok || r.Operator != selection.Equals && r.Operator != selection.DoubleEquals {
			return nil, fmt.Errorf("field selector %q isn't an equality on an indexed field", selector)
		}
		indexed := map[K]struct{}{}
		for _, k := range sources(r.Value) {
			if _, ok := keys[k]; i == 0 || ok {
				indexed[k] = struct{}{}
			}
		}
		keys = indexed
	}
	return keys, nil
}
