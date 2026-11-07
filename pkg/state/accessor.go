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

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
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
//
// An Accessor is also a controller-runtime source: a controller that watches it with WatchesRawSource reconciles an
// object whenever its derived fields change, and starts reconciling once the stores behind them are synced.
type Accessor[O client.Object, L client.ObjectList, D any] struct {
	client  client.Reader
	newObj  func() O
	newList func() L
	items   func(L) []O
	derive  func(O) D
	sources []source.SyncingSource
}

var _ source.SyncingSource = &Accessor[client.Object, client.ObjectList, any]{}

// NewAccessor returns an Accessor that reads objects through kubeClient into new objects from newObj, lists them with a
// list from newList whose items are items, and derives their fields with derive, which reads the stores that keep them
// and must not block. sources are the stores derive reads, made with From, so that a change to them reconciles the
// object.
func NewAccessor[O client.Object, L client.ObjectList, D any](kubeClient client.Reader, newObj func() O, newList func() L, items func(L) []O, derive func(O) D, sources ...source.SyncingSource) *Accessor[O, L, D] {
	return &Accessor[O, L, D]{client: kubeClient, newObj: newObj, newList: newList, items: items, derive: derive, sources: sources}
}

// Start enqueues each object whose derived fields change, until ctx is done, after enqueueing every object that has
// derived fields, since they may have changed before Start.
func (r *Accessor[O, L, D]) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	for _, src := range r.sources {
		if err := src.Start(ctx, queue); err != nil {
			return err
		}
	}
	return nil
}

// WaitForSync returns once every store the Accessor derives fields from is synced, so that a controller watching it
// reconciles with complete fields.
func (r *Accessor[O, L, D]) WaitForSync(ctx context.Context) error {
	for _, src := range r.sources {
		if err := src.WaitForSync(ctx); err != nil {
			return err
		}
	}
	return nil
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
