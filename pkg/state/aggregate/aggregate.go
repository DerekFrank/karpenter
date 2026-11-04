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

// Package aggregate keeps values computed over the objects of one kind, such as sums, up to date as the objects
// change, so that reading one doesn't List the objects.
package aggregate

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"sync"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/operator/overlay"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// Value is the state of one key. The aggregate calls Update with each version of an object that counts toward the key,
// and Delete with the object's UID once it stops counting. Update is idempotent: updating with an object the value already holds
// replaces that object's previous contribution. Update and Delete return whether the result changed. Result returns a
// result that later updates don't change, so a reader can keep it.
type Value[T client.Object, R any] interface {
	Update(obj T) bool
	Delete(uid types.UID) bool
	Result() R
}

// Resources returns a Value that sums the resource lists of the objects that count toward a key.
func Resources[T client.Object](value func(T) corev1.ResourceList) func() Value[T, corev1.ResourceList] {
	return func() Value[T, corev1.ResourceList] {
		return &resourceSum[T]{value: value, objects: map[types.UID]corev1.ResourceList{}, sum: corev1.ResourceList{}}
	}
}

type resourceSum[T client.Object] struct {
	value   func(T) corev1.ResourceList
	objects map[types.UID]corev1.ResourceList
	sum     corev1.ResourceList
}

func (r *resourceSum[T]) Update(obj T) bool {
	uid := obj.GetUID()
	next := r.value(obj)
	if prev, ok := r.objects[uid]; ok {
		if equality.Semantic.DeepEqual(prev, next) {
			return false
		}
		resources.SubtractFrom(r.sum, prev)
	}
	resources.MergeInto(r.sum, next)
	r.objects[uid] = next
	return true
}

func (r *resourceSum[T]) Delete(uid types.UID) bool {
	prev, ok := r.objects[uid]
	if !ok {
		return false
	}
	resources.SubtractFrom(r.sum, prev)
	delete(r.objects, uid)
	return true
}

func (r *resourceSum[T]) Result() corev1.ResourceList {
	return r.sum.DeepCopy()
}

// Aggregate keeps a Value per key over the objects of one kind. It follows the kind's informer and, when the client is
// an overlay.Client, the client's own writes as soon as they return, so a key's value covers the same versions of the
// objects that a List through the client returns. It hands a Value each object's newest version, ordered by
// resourceVersion, and tells it once the object is gone or moves to another key.
//
// Key must be a pure function of the object, and so must what a Value takes from it, because neither is recomputed
// unless the object changes. Key returns "" for an object the aggregate doesn't count.
type Aggregate[T client.Object, R any] struct {
	kubeClient client.Client
	informers  cache.Informers
	list       client.ObjectList
	key        func(T) string
	value      func() Value[T, R]

	mu          sync.Mutex
	synced      bool
	subscribers []func(key string)
	objects     map[types.UID]contribution
	names       map[types.NamespacedName]types.UID
	groups      map[string]*group[T, R]
}

type contribution struct {
	rv      uint64
	key     string
	deleted bool
}

type group[T client.Object, R any] struct {
	count int
	value Value[T, R]
}

// New returns an aggregate over the objects of list's kind, with a new value from value for each key.
func New[T client.Object, R any](kubeClient client.Client, informers cache.Informers, list client.ObjectList, key func(T) string, value func() Value[T, R]) *Aggregate[T, R] {
	return &Aggregate[T, R]{
		kubeClient: kubeClient,
		informers:  informers,
		list:       list,
		key:        key,
		value:      value,
		objects:    map[types.UID]contribution{},
		names:      map[types.NamespacedName]types.UID{},
		groups:     map[string]*group[T, R]{},
	}
}

// Start subscribes to the kind's informer and to the client's writes, applies the objects a List returns, and keeps
// the values up to date until ctx is done. It implements manager.Runnable.
func (a *Aggregate[T, R]) Start(ctx context.Context) error {
	obj := reflect.New(reflect.TypeFor[T]().Elem()).Interface().(T)
	informer, err := a.informers.GetInformer(ctx, obj)
	if err != nil {
		return fmt.Errorf("getting informer, %w", err)
	}
	registration, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { a.observe(o, false) },
		UpdateFunc: func(_, o any) { a.observe(o, false) },
		DeleteFunc: func(o any) { a.observe(o, true) },
	})
	if err != nil {
		return fmt.Errorf("adding event handler, %w", err)
	}
	defer func() { _ = informer.RemoveEventHandler(registration) }()
	if writer, ok := a.kubeClient.(*overlay.Client); ok {
		remove, err := writer.AddWriteHandler(obj, func(o client.Object, deleted bool) { a.apply(o, deleted, false) })
		if err != nil {
			return fmt.Errorf("adding write handler, %w", err)
		}
		defer remove()
	}
	a.mu.Lock()
	list := a.list.DeepCopyObject().(client.ObjectList)
	if err := a.kubeClient.List(ctx, list, client.UnsafeDisableDeepCopy); err != nil {
		a.mu.Unlock()
		return fmt.Errorf("listing, %w", err)
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		a.mu.Unlock()
		return fmt.Errorf("extracting list, %w", err)
	}
	for _, item := range items {
		a.applyLocked(item.(T), false, false)
	}
	a.synced = true
	keys := slices.Collect(maps.Keys(a.groups))
	subscribers := a.subscribers
	a.mu.Unlock()
	for _, key := range keys {
		for _, subscriber := range subscribers {
			subscriber(key)
		}
	}
	<-ctx.Done()
	return nil
}

// Subscribe calls fn with each key whose value changes once the aggregate has synced, and with every key when it
// syncs.
// fn runs on the goroutine that applied the change, so it must not block.
func (a *Aggregate[T, R]) Subscribe(fn func(key string)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.subscribers = append(slices.Clip(a.subscribers), fn)
}

// Synced returns whether Start has applied the objects a List returns.
func (a *Aggregate[T, R]) Synced() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.synced
}

// Get returns key's result and the number of objects that count toward it, and the zero R for a key no object counts
// toward.
func (a *Aggregate[T, R]) Get(key string) (R, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	g, ok := a.groups[key]
	if !ok {
		var zero R
		return zero, 0
	}
	return g.value.Result(), g.count
}

// Apply applies a version of an object that a caller saw outside the informer and the client's writes, such as the
// response to a write through a client that isn't an overlay.Client. Like the informer's versions, it applies only if
// it's newer than the one the aggregate holds, or has no resourceVersion to compare.
func (a *Aggregate[T, R]) Apply(obj T) {
	a.apply(obj, false, true)
}

// Forget removes the object named key, for a caller that learns an object is gone by its name alone, such as a
// reconciler that finds it missing from the cache.
func (a *Aggregate[T, R]) Forget(key types.NamespacedName) {
	a.mu.Lock()
	uid, ok := a.names[key]
	var changed []string
	if ok {
		changed = a.move(uid, *new(T), a.objects[uid], contribution{})
		delete(a.objects, uid)
		delete(a.names, key)
	}
	subscribers := lo.Ternary(a.synced, a.subscribers, nil)
	a.mu.Unlock()
	for _, key := range changed {
		for _, subscriber := range subscribers {
			subscriber(key)
		}
	}
}

func (a *Aggregate[T, R]) observe(raw any, deleted bool) {
	if tombstone, ok := raw.(toolscache.DeletedFinalStateUnknown); ok {
		raw = tombstone.Obj
	}
	if obj, ok := raw.(client.Object); ok {
		a.apply(obj, deleted, true)
	}
}

func (a *Aggregate[T, R]) apply(obj client.Object, deleted, fromInformer bool) {
	t, ok := obj.(T)
	if !ok {
		return
	}
	a.mu.Lock()
	changed := a.applyLocked(t, deleted, fromInformer)
	subscribers := lo.Ternary(a.synced, a.subscribers, nil)
	a.mu.Unlock()
	for _, key := range changed {
		for _, subscriber := range subscribers {
			subscriber(key)
		}
	}
}

//nolint:gocyclo
func (a *Aggregate[T, R]) applyLocked(obj T, deleted, fromInformer bool) []string {
	uid := obj.GetUID()
	name := client.ObjectKeyFromObject(obj)
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	cur, exists := a.objects[uid]
	switch {
	case deleted && fromInformer:
		if !exists {
			return nil
		}
		delete(a.objects, uid)
		if a.names[name] == uid {
			delete(a.names, name)
		}
		return a.move(uid, obj, cur, contribution{})
	case exists && cur.deleted:
		return nil
	case deleted:
	case rv == 0 && !fromInformer, exists && rv != 0 && rv <= cur.rv:
		return nil
	}
	next := contribution{rv: max(rv, cur.rv), deleted: deleted}
	if !deleted {
		next.key = a.key(obj)
	}
	a.objects[uid] = next
	a.names[name] = uid
	return a.move(uid, obj, cur, next)
}

func (a *Aggregate[T, R]) move(uid types.UID, obj T, from, to contribution) []string {
	fromKey := lo.Ternary(from.deleted, "", from.key)
	toKey := lo.Ternary(to.deleted, "", to.key)
	var changed []string
	if fromKey != "" && fromKey != toKey {
		g := a.groups[fromKey]
		g.count--
		g.value.Delete(uid)
		if g.count == 0 {
			delete(a.groups, fromKey)
		}
		changed = append(changed, fromKey)
	}
	if toKey != "" {
		g, ok := a.groups[toKey]
		if !ok {
			g = &group[T, R]{value: a.value()}
			a.groups[toKey] = g
		}
		if fromKey != toKey {
			g.count++
		}
		if g.value.Update(obj) || fromKey != toKey {
			changed = append(changed, toKey)
		}
	}
	return changed
}
