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
	"reflect"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Contribution is what one object contributes to its key's fields. A Contributor dispatches each change of an object:
//   - new to a key: next.Update(fields, zero);
//   - same key, contribution changed: next.Update(fields, prev);
//   - same key, contribution equal: nothing;
//   - key changed: prev.Delete(fields) at the old key, then next.Update(fields, zero) at the new key;
//   - key became zero, or object gone: prev.Delete(fields) at the old key.
//
// Delete undoes Update from the zero contribution, and Update(fields, prev) equals prev.Delete followed by
// Update(fields, zero). Both return new fields and leave the ones they get intact.
type Contribution[F, C any] interface {
	// Update returns fields with the receiver contributing to them in place of prev, the zero C if the object is new to
	// the key.
	Update(fields F, prev C) F
	// Delete returns fields without the receiver, which is leaving the key. It runs once per stay.
	Delete(fields F) F
}

// Equaler is a Contribution that compares itself to another, for one that's costly to compare by reflection. Equal
// reports whether Update would leave fields holding either one the same. A Contribution that isn't an Equaler compares
// with reflect.DeepEqual.
type Equaler[C any] interface {
	Equal(C) bool
}

// Contributor keeps a Store's fields up to date from the objects of a kind S. Each S contributes the value C that value
// returns to the key key returns, a zero key meaning it contributes to nothing.
//
// key must be a pure function of S, and so should value be, because a contribution is recomputed only when S changes.
type Contributor[K comparable, S client.Object, F any, C Contribution[F, C]] struct {
	store      *Store[K, F]
	kubeClient client.Client
	informers  cache.Informers

	key   func(S) K
	value func(context.Context, S) (C, error)

	sources       feed
	contributions map[types.UID]record[K, C]
	byTarget      map[K]map[types.UID]struct{}
	synced        bool
}

type record[K comparable, C any] struct {
	target K
	value  C
}

// Contribute returns a Contributor of S to store. Watch it to follow the informer and the client's writes.
func Contribute[K comparable, S client.Object, F any, C Contribution[F, C]](store *Store[K, F], key func(S) K, value func(context.Context, S) (C, error)) *Contributor[K, S, F, C] {
	return &Contributor[K, S, F, C]{
		store:         store,
		key:           key,
		value:         value,
		sources:       newFeed(),
		contributions: map[types.UID]record[K, C]{},
		byTarget:      map[K]map[types.UID]struct{}{},
	}
}

// Join returns a Contributor of S to store that contributes the object itself: set stores it in the key's fields, and
// gets a nil S once the object leaves the key.
func Join[K comparable, S client.Object, F any](store *Store[K, F], key func(S) K, set func(F, S) F) *Contributor[K, S, F, Joined[F, S]] {
	return Contribute(store, key, func(_ context.Context, obj S) (Joined[F, S], error) { return Joined[F, S]{obj: obj, set: set}, nil })
}

// Joined is the contribution of an object to a join: the object itself.
type Joined[F any, S client.Object] struct {
	obj S
	set func(F, S) F
}

func (j Joined[F, S]) Update(f F, _ Joined[F, S]) F { return j.set(f, j.obj) }

func (j Joined[F, S]) Delete(f F) F {
	var none S
	return j.set(f, none)
}

// Equal reports whether both are the same object, so a new version of the object always updates the join.
func (j Joined[F, S]) Equal(o Joined[F, S]) bool { return any(j.obj) == any(o.obj) }

// Watch makes Start follow S's informer from informers and kubeClient's writes, listing S through kubeClient.
func (c *Contributor[K, S, F, C]) Watch(kubeClient client.Client, informers cache.Informers) *Contributor[K, S, F, C] {
	c.kubeClient, c.informers = kubeClient, informers
	return c
}

// Start follows S's informer and the client's writes until ctx is done. It implements manager.Runnable.
func (c *Contributor[K, S, F, C]) Start(ctx context.Context) error {
	return follow(ctx, c.kubeClient, c.informers, c.store, func(obj S, deleted, fromInformer bool) {
		_ = c.change(ctx, obj, deleted, fromInformer)
	}, func(items []S) []change[K, F] {
		var changed []change[K, F]
		for _, item := range items {
			if next, contributes, err := c.contributionFor(ctx, item, false); err == nil {
				changed = append(changed, c.changeLocked(item, false, false, next, contributes)...)
			}
		}
		c.synced = true
		return changed
	})
}

// Synced returns whether Start has applied the objects a List returns.
func (c *Contributor[K, S, F, C]) Synced() bool {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	return c.synced
}

// Observe applies a version of an S that a caller saw outside the informer and the client's writes, such as from a
// reconciler or the response to a write through a client that isn't an overlay.Client.
func (c *Contributor[K, S, F, C]) Observe(ctx context.Context, obj S) error {
	return c.change(ctx, obj, false, true)
}

// Recompute observes obj like Observe, and recomputes its contribution when the store already holds its version, for a
// contribution that reads other objects that may have changed since, such as a pod's PVCs.
func (c *Contributor[K, S, F, C]) Recompute(ctx context.Context, obj S) error {
	next, contributes, err := c.contributionFor(ctx, obj, false)
	if err != nil {
		return err
	}
	c.store.mu.Lock()
	var changed []change[K, F]
	if c.sources.current(obj) {
		changed = c.move(identity(obj), next, contributes)
	} else {
		changed = c.changeLocked(obj, false, true, next, contributes)
	}
	subscribers := c.store.subscribers
	c.store.mu.Unlock()
	notify(subscribers, changed)
	return nil
}

// Forget removes the S named name, for a caller that learns an object is gone by its name alone, such as a reconciler
// that finds it missing from the cache.
func (c *Contributor[K, S, F, C]) Forget(name types.NamespacedName) {
	c.store.mu.Lock()
	var changed []change[K, F]
	if uid, ok := c.sources.forget(name); ok {
		changed = c.move(uid, record[K, C]{}, false)
	}
	subscribers := c.store.subscribers
	c.store.mu.Unlock()
	notify(subscribers, changed)
}

// Resync makes objs the S that contribute to target, for a caller that lists them, such as by an index of the
// objects pointing at target. It applies the objects newer than the versions the store holds, and removes the
// contributions to target of objects objs leaves out, which heals events the store missed.
func (c *Contributor[K, S, F, C]) Resync(ctx context.Context, target K, objs []S) error {
	c.store.mu.Lock()
	var stale []S
	for _, obj := range objs {
		if c.sources.newer(obj) {
			stale = append(stale, obj)
		}
	}
	c.store.mu.Unlock()
	type pending struct {
		obj         S
		next        record[K, C]
		contributes bool
	}
	var computed []pending
	for _, obj := range stale {
		next, contributes, err := c.contributionFor(ctx, obj, false)
		if err != nil {
			return err
		}
		computed = append(computed, pending{obj: obj, next: next, contributes: contributes})
	}
	listed := map[types.UID]struct{}{}
	for _, obj := range objs {
		listed[identity(obj)] = struct{}{}
	}
	c.store.mu.Lock()
	var changed []change[K, F]
	for _, p := range computed {
		changed = append(changed, c.changeLocked(p.obj, false, false, p.next, p.contributes)...)
	}
	for uid := range c.byTarget[target] {
		if _, ok := listed[uid]; !ok {
			changed = append(changed, c.move(uid, record[K, C]{}, false)...)
		}
	}
	subscribers := c.store.subscribers
	c.store.mu.Unlock()
	notify(subscribers, changed)
	return nil
}

func (c *Contributor[K, S, F, C]) change(ctx context.Context, obj S, deleted, fromInformer bool) error {
	next, contributes, err := c.contributionFor(ctx, obj, deleted)
	if err != nil {
		return err
	}
	c.store.mu.Lock()
	changed := c.changeLocked(obj, deleted, fromInformer, next, contributes)
	subscribers := c.store.subscribers
	c.store.mu.Unlock()
	notify(subscribers, changed)
	return nil
}

func (c *Contributor[K, S, F, C]) contributionFor(ctx context.Context, obj S, deleted bool) (record[K, C], bool, error) {
	var zero K
	if deleted {
		return record[K, C]{}, false, nil
	}
	next := record[K, C]{target: c.key(obj)}
	if next.target == zero {
		return next, false, nil
	}
	value, err := c.value(ctx, obj)
	if err != nil {
		return next, false, err
	}
	next.value = value
	return next, true, nil
}

func (c *Contributor[K, S, F, C]) changeLocked(obj S, deleted, fromInformer bool, next record[K, C], contributes bool) []change[K, F] {
	ok, gone, replaced := c.sources.admit(obj, deleted, fromInformer)
	if !ok {
		return nil
	}
	var changed []change[K, F]
	if replaced != "" {
		changed = c.move(replaced, record[K, C]{}, false)
	}
	if gone {
		return append(changed, c.move(identity(obj), record[K, C]{}, false)...)
	}
	return append(changed, c.move(identity(obj), next, contributes)...)
}

func (c *Contributor[K, S, F, C]) move(uid types.UID, next record[K, C], contributes bool) []change[K, F] {
	prev, had := c.contributions[uid]
	if had && (!contributes || prev.target != next.target) {
		if delete(c.byTarget[prev.target], uid); len(c.byTarget[prev.target]) == 0 {
			delete(c.byTarget, prev.target)
		}
	}
	if contributes {
		c.contributions[uid] = next
		if !had || prev.target != next.target {
			if c.byTarget[next.target] == nil {
				c.byTarget[next.target] = map[types.UID]struct{}{}
			}
			c.byTarget[next.target][uid] = struct{}{}
		}
	} else {
		delete(c.contributions, uid)
	}
	return moveContribution(c.store, prev, had, next, contributes)
}

// moveContribution moves a source's contribution from prev to next, dispatching to Update and Delete as Contribution
// describes.
func moveContribution[K comparable, F any, C Contribution[F, C]](store *Store[K, F], prev record[K, C], had bool, next record[K, C], contributes bool) []change[K, F] {
	if had && contributes && prev.target == next.target {
		if equal(prev.value, next.value) {
			return nil
		}
		// The contribution changed, so the fields did too; don't compare them again.
		return store.changeLocked(next.target, func(f F) F { return next.value.Update(f, prev.value) })
	}
	var changed []change[K, F]
	if had {
		changed = append(changed, store.updateLocked(prev.target, prev.value.Delete, -1)...)
	}
	if contributes {
		var zero C
		changed = append(changed, store.updateLocked(next.target, func(f F) F { return next.value.Update(f, zero) }, 1)...)
	}
	return changed
}

func equal[C any](a, b C) bool {
	if e, ok := any(b).(Equaler[C]); ok {
		return e.Equal(a)
	}
	return reflect.DeepEqual(a, b)
}
