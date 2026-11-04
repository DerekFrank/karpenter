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
