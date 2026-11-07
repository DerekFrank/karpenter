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

// Contributor keeps a Store's fields up to date from the objects of a kind S. Each S contributes a value C to the key
// target returns, and apply folds a change of that contribution into the key's fields. An object can contribute itself,
// to keep a field holding the object.
//
// target must be a pure function of S, and so should contribution be, because a contribution is recomputed only when
// S changes. apply gets the previous and the next contribution, either of which is the zero C when S didn't or no
// longer contributes, and must return new fields without modifying the ones it gets.
type Contributor[K comparable, S client.Object, F, C any] struct {
	store      *Store[K, F]
	kubeClient client.Client
	informers  cache.Informers
	list       client.ObjectList

	target       func(S) K
	contribution func(context.Context, S) (C, error)
	apply        func(fields F, prev, next C) F
	equal        func(a, b C) bool

	sources       feed
	contributions map[types.UID]record[K, C]
	byTarget      map[K]map[types.UID]struct{}
	synced        bool
}

type record[K comparable, C any] struct {
	target K
	value  C
}

// Contribute returns a Contributor of list's kind to store. A zero key from target means S contributes to nothing.
func Contribute[K comparable, S client.Object, F, C any](store *Store[K, F], kubeClient client.Client, informers cache.Informers, list client.ObjectList,
	target func(S) K, contribution func(context.Context, S) (C, error), apply func(fields F, prev, next C) F) *Contributor[K, S, F, C] {
	return &Contributor[K, S, F, C]{
		store:         store,
		kubeClient:    kubeClient,
		informers:     informers,
		list:          list,
		target:        target,
		contribution:  contribution,
		apply:         apply,
		equal:         func(a, b C) bool { return reflect.DeepEqual(a, b) },
		sources:       newFeed(),
		contributions: map[types.UID]record[K, C]{},
		byTarget:      map[K]map[types.UID]struct{}{},
	}
}

// Start follows S's informer and the client's writes until ctx is done. It implements manager.Runnable.
func (c *Contributor[K, S, F, C]) Start(ctx context.Context) error {
	return follow(ctx, c.kubeClient, c.informers, c.list, c.store, func(obj S, deleted, fromInformer bool) {
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
	next := record[K, C]{target: c.target(obj)}
	if next.target == zero {
		return next, false, nil
	}
	value, err := c.contribution(ctx, obj)
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

// CompareWith makes the Contributor decide whether a source's contribution changed with equal instead of
// reflect.DeepEqual, for a contribution that's costly to compare by reflection. equal must report whether apply would
// fold the two the same way.
func (c *Contributor[K, S, F, C]) CompareWith(equal func(a, b C) bool) *Contributor[K, S, F, C] {
	c.equal = equal
	return c
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
	return moveContribution(c.store, c.apply, c.equal, prev, had, next, contributes)
}

// moveContribution moves a source's contribution from prev to next, which may target different keys.
func moveContribution[K comparable, F, C any](store *Store[K, F], apply func(F, C, C) F, equal func(a, b C) bool, prev record[K, C], had bool, next record[K, C], contributes bool) []change[K, F] {
	var zero C
	if had && contributes && prev.target == next.target {
		if equal(prev.value, next.value) {
			return nil
		}
		// The contribution changed, so the fields did too; don't compare them again.
		return store.changeLocked(next.target, func(f F) F { return apply(f, prev.value, next.value) })
	}
	var changed []change[K, F]
	if had {
		changed = append(changed, store.updateLocked(prev.target, func(f F) F { return apply(f, prev.value, zero) }, -1)...)
	}
	if contributes {
		changed = append(changed, store.updateLocked(next.target, func(f F) F { return apply(f, zero, next.value) }, 1)...)
	}
	return changed
}
