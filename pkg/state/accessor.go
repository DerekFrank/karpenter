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
	"maps"

	fieldselector "k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// View is an API object with the fields Karpenter derives about it. Object is the object as the client read it, so
// it can be written, watched and recorded like any other. Derived values are shared, so never modify them.
type View[O client.Object, D any] struct {
	Object  O
	Derived D
}

// Accessor reads objects of kind O with the fields D derived about them: a state accessor. Read the objects that have a
// Accessor only through it, and use the client only to write them.
//
// The object is a version the client holds, and each derived field is its store's at the time of the read, so a
// derived field can lag the object or other derived fields.
type Accessor[O client.Object, L client.ObjectList, D any] struct {
	client  client.Reader
	newObj  func() O
	newList func() L
	items   func(L) []O
	derive  func(O) D
}

// NewAccessor returns an Accessor that reads objects through kubeClient into new objects from newObj, lists them with a
// list from newList whose items are items, and derives their fields with derive, which reads the stores that keep them
// and must not block.
func NewAccessor[O client.Object, L client.ObjectList, D any](kubeClient client.Reader, newObj func() O, newList func() L, items func(L) []O, derive func(O) D) *Accessor[O, L, D] {
	return &Accessor[O, L, D]{client: kubeClient, newObj: newObj, newList: newList, items: items, derive: derive}
}

// Get returns the object named key with its derived fields.
func (r *Accessor[O, L, D]) Get(ctx context.Context, key client.ObjectKey, opts ...client.GetOption) (View[O, D], error) {
	obj := r.newObj()
	if err := r.client.Get(ctx, key, obj, opts...); err != nil {
		return View[O, D]{}, err
	}
	return View[O, D]{Object: obj, Derived: r.derive(obj)}, nil
}

// List returns the objects that opts select, with their derived fields.
func (r *Accessor[O, L, D]) List(ctx context.Context, opts ...client.ListOption) ([]View[O, D], error) {
	list := r.newList()
	if err := r.client.List(ctx, list, opts...); err != nil {
		return nil, err
	}
	items := r.items(list)
	objects := make([]View[O, D], len(items))
	for i, obj := range items {
		objects[i] = View[O, D]{Object: obj, Derived: r.derive(obj)}
	}
	return objects, nil
}

// JoinAccessor reads the objects of a join store, such as each node's Node, with the fields D derived about them. It
// reads the objects from the store, which holds the kubeClient's writes as soon as they return when it follows an
// overlay.Client. Each read returns a copy of the object unless the caller opts out with client.UnsafeDisableDeepCopy,
// and must then leave the object intact.
type JoinAccessor[K comparable, F any, O client.Object, D any] struct {
	store   *Store[K, F]
	view    func(K, F) (O, D, bool)
	indexes map[string]*Store[string, members[K]]
}

// members are the keys of a store whose fields index to one value.
type members[K comparable] map[K]struct{}

// member is a key's contribution to the value its fields index to: the key itself.
type member[K comparable] struct{ key K }

func (m member[K]) Update(f members[K], prev member[K]) members[K] {
	if _, ok := f[m.key]; ok && prev == m {
		return f
	}
	next := make(members[K], len(f)+1)
	for k := range f {
		next[k] = struct{}{}
	}
	delete(next, prev.key)
	next[m.key] = struct{}{}
	return next
}

func (m member[K]) Delete(f members[K]) members[K] {
	next := make(members[K], len(f))
	for k := range f {
		if k != m.key {
			next[k] = struct{}{}
		}
	}
	if len(next) == 0 {
		return nil
	}
	return next
}

// NewJoinAccessor returns a JoinAccessor over store. view returns the object of a key's fields and its derived fields,
// reading the stores that keep them without blocking, or false for fields without the object.
func NewJoinAccessor[K comparable, F any, O client.Object, D any](store *Store[K, F], view func(K, F) (O, D, bool)) *JoinAccessor[K, F, O, D] {
	return &JoinAccessor[K, F, O, D]{store: store, view: view, indexes: map[string]*Store[string, members[K]]{}}
}

// IndexField indexes field, so that List with client.MatchingFields{field: value} reads only the keys whose fields
// index to value, as a field index does for the client's List. index returns a key's value, or "" for a key the index
// leaves out. Index every field before the accessor's first List.
func (a *JoinAccessor[K, F, O, D]) IndexField(field string, index func(K, F) string) *JoinAccessor[K, F, O, D] {
	values := NewStore[string, members[K]]()
	Derive(a.store, values, index, func(k K, _ F) member[K] { return member[K]{key: k} })
	a.indexes[field] = values
	return a
}

// Get returns key's object with its derived fields, and whether it exists.
func (a *JoinAccessor[K, F, O, D]) Get(key K, opts ...client.GetOption) (View[O, D], bool) {
	f, ok := a.store.Get(key)
	if !ok {
		return View[O, D]{}, false
	}
	obj, derived, ok := a.view(key, f)
	if !ok {
		return View[O, D]{}, false
	}
	if o := (&client.GetOptions{}).ApplyOptions(opts); o.UnsafeDisableDeepCopy == nil || !*o.UnsafeDisableDeepCopy {
		obj = obj.DeepCopyObject().(O)
	}
	return View[O, D]{Object: obj, Derived: derived}, true
}

// List returns the objects that opts select, with their derived fields. A field selector selects on indexed fields
// only, each requirement an equality, as the client's List does for a cache with field indexes.
func (a *JoinAccessor[K, F, O, D]) List(opts ...client.ListOption) ([]View[O, D], error) {
	o := (&client.ListOptions{}).ApplyOptions(opts)
	shared := o.UnsafeDisableDeepCopy != nil && *o.UnsafeDisableDeepCopy
	keys, err := a.selected(o.FieldSelector)
	if err != nil {
		return nil, err
	}
	var views []View[O, D]
	add := func(k K, f F) {
		obj, derived, ok := a.view(k, f)
		if !ok || (o.LabelSelector != nil && !o.LabelSelector.Matches(labels.Set(obj.GetLabels()))) {
			return
		}
		if !shared {
			obj = obj.DeepCopyObject().(O)
		}
		views = append(views, View[O, D]{Object: obj, Derived: derived})
	}
	if keys == nil {
		for k, f := range a.store.List() {
			add(k, f)
		}
		return views, nil
	}
	for k := range keys {
		if f, ok := a.store.Get(k); ok {
			add(k, f)
		}
	}
	return views, nil
}

// selected returns the keys selector selects through the indexes, or nil to select every key.
func (a *JoinAccessor[K, F, O, D]) selected(selector fieldselector.Selector) (members[K], error) {
	if selector == nil || selector.Empty() {
		return nil, nil
	}
	var keys members[K]
	for i, r := range selector.Requirements() {
		index, ok := a.indexes[r.Field]
		if !ok || r.Operator != selection.Equals && r.Operator != selection.DoubleEquals {
			return nil, fmt.Errorf("field selector %q isn't an equality on an indexed field", selector)
		}
		indexed, _ := index.Get(r.Value)
		if i == 0 {
			keys = maps.Clone(indexed)
		} else {
			maps.DeleteFunc(keys, func(k K, _ struct{}) bool { _, ok := indexed[k]; return !ok })
		}
		if keys == nil {
			keys = members[K]{}
		}
	}
	return keys, nil
}
