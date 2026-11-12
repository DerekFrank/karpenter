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
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Newest holds the newest version of each object of one kind, and notifies the key of each object whose newest
// version may have changed: overlay.Source, which combines the informer's store with the client's own writes, or
// Objects, which a suite fills.
type Newest interface {
	// Get returns the newest version of the object under key, without copying it, and whether it exists.
	Get(key types.NamespacedName) (client.Object, bool)
	// Keys returns the keys of the objects it holds.
	Keys() []types.NamespacedName
	// Subscribe calls fn with each key whose newest version may have changed, once Get returns the change, and
	// returns a func that stops it.
	Subscribe(fn func(types.NamespacedName)) func()
	// HasSynced returns whether it holds the objects the API server held when it started.
	HasSynced() bool
	// Holds returns whether it still holds the object with the UID under key, even if it hides it as deleted: until it
	// doesn't, an event of the object can still arrive.
	Holds(key types.NamespacedName, uid types.UID) bool
}

// NewLevelStore returns a store of the objects of kind S that from holds and in accepts, each under the key key
// returns for its name. It's level-triggered: on each key from notifies, it reads that object's newest version from from
// and holds it, or drops the entry once from no longer holds it, so a version from skips, repeats or delivers late never
// reaches it. A new UID under a name replaces the object, which drops its entry's intent. It's synced once from is and
// it has applied every key from held then.
func NewLevelStore[K comparable, S client.Object, D, I any](from Newest, key func(types.NamespacedName) K, in func(S) bool) *Store[K, S, D, I] {
	c := newCore[K, S, D, I](objects, nil)
	n := &newestOf[K, S, D, I]{core: c, from: from, key: key, in: in}
	c.feed = n
	from.Subscribe(n.apply)
	if from.HasSynced() {
		n.sweep()
	}
	return &Store[K, S, D, I]{Reader[K, S, D, I]{c}}
}

type newestOf[K comparable, S client.Object, D, I any] struct {
	core  *core[K, S, D, I]
	from  Newest
	key   func(types.NamespacedName) K
	in    func(S) bool
	swept atomic.Bool
}

func (n *newestOf[K, S, D, I]) synced() bool { return n.swept.Load() && n.from.HasSynced() }

// clearLocked empties a suite's Objects with the store, so that a later notification can't bring a cleared object back.
func (n *newestOf[K, S, D, I]) clearLocked() {
	if o, ok := n.from.(*Objects); ok {
		o.Clear()
	}
}

// start waits for from to sync, then applies every key it holds. Notifications meanwhile apply the newest version too,
// which is idempotent.
func (n *newestOf[K, S, D, I]) start(ctx context.Context) error {
	if !n.swept.Load() && toolscache.WaitForCacheSync(ctx.Done(), n.from.HasSynced) {
		n.sweep()
	}
	<-ctx.Done()
	return nil
}

func (n *newestOf[K, S, D, I]) sweep() {
	for _, name := range n.from.Keys() {
		n.apply(name)
	}
	n.swept.Store(true)
}

// apply holds the newest version of the object named name. It reads it without the store's lock, and commits it only if
// it's newer than the entry's, so an older read loses to a newer one.
func (n *newestOf[K, S, D, I]) apply(name types.NamespacedName) {
	var zero K
	k := n.key(name)
	if k == zero {
		return
	}
	var obj S
	o, ok := n.from.Get(name)
	if ok {
		obj, ok = o.(S)
		ok = ok && n.in(obj)
	}
	n.commit(name, k, obj, ok)
}

// commit holds obj under k, or removes k's object when ok is false. Within one object, an older resourceVersion loses,
// and one as new as the object held replaces it, so the store converges on the informer's own object for a version the
// overlay held a copy of. A removed object, and one another UID replaced, is at infinity: its UID stays in the slot's
// gone while from still holds it, so a stale read of it loses, and the next object under the name starts a new
// incarnation, which drops the entry's intent.
func (n *newestOf[K, S, D, I]) commit(name types.NamespacedName, k K, obj S, ok bool) {
	c := n.core
	c.mu.Lock()
	defer c.unlock()
	cur, _ := c.load(k)
	switch {
	case ok && slices.Contains(cur.gone, identity(obj)):
	case ok && cur.exists && identity(cur.entry.Object) == identity(obj):
		if any(cur.entry.Object) != any(obj) && (obj.GetResourceVersion() == "" || resourceVersion(obj) >= resourceVersion(cur.entry.Object)) {
			c.objectLocked(k, obj, true)
		}
	case ok && cur.exists && obj.GetResourceVersion() != "" && resourceVersion(obj) < resourceVersion(cur.entry.Object):
		// resourceVersions order every object of a kind, so an older UID's object is older than the one that replaced it.
	case ok:
		if cur.exists {
			n.buryLocked(k, cur)
		}
		c.objectLocked(k, obj, true)
	case cur.exists:
		n.buryLocked(k, cur)
	}
	n.pruneLocked(name, k)
}

// buryLocked removes k's object, which drops the entry's intent, and records its UID as gone.
func (n *newestOf[K, S, D, I]) buryLocked(k K, cur *slot[S, D, I]) {
	next := *cur
	next.exists = false
	var none S
	var zero I
	next.entry.Object, next.entry.Intent = none, zero
	next.gone = append(slices.Clip(cur.gone), identity(cur.entry.Object))
	n.core.storeLocked(k, cur, &next)
}

// pruneLocked drops the UIDs gone under k that from no longer holds: the informer's store has dropped them, so no event
// of theirs can arrive.
func (n *newestOf[K, S, D, I]) pruneLocked(name types.NamespacedName, k K) {
	cur, ok := n.core.load(k)
	if !ok || len(cur.gone) == 0 {
		return
	}
	gone := slices.DeleteFunc(slices.Clone(cur.gone), func(uid types.UID) bool { return !n.from.Holds(name, uid) })
	if len(gone) == len(cur.gone) {
		return
	}
	next := *cur
	next.gone = gone
	if !next.exists && len(gone) == 0 && n.core.refs[k] == 0 {
		n.core.slots.Delete(k)
		return
	}
	n.core.slots.Store(k, &next)
}

// Objects is a Newest that a caller fills, for a store without an informer, such as a suite's, which shows it each
// object as the informer would.
type Objects struct {
	mu          sync.RWMutex
	objects     map[types.NamespacedName]client.Object
	subscribers []*func(types.NamespacedName)
}

func NewObjects() *Objects {
	return &Objects{objects: map[types.NamespacedName]client.Object{}}
}

// Put holds obj, unless it's an older version of the object held under its name, and notifies its key. An object with
// another UID replaces the one held, and an object without a resourceVersion is always newest.
func (o *Objects) Put(obj client.Object) {
	key := client.ObjectKeyFromObject(obj)
	o.mu.Lock()
	cur, ok := o.objects[key]
	if ok && identity(cur) == identity(obj) && obj.GetResourceVersion() != "" && resourceVersion(obj) <= resourceVersion(cur) {
		o.mu.Unlock()
		return
	}
	o.objects[key] = obj
	subscribers := o.subscribers
	o.mu.Unlock()
	for _, fn := range subscribers {
		(*fn)(key)
	}
}

// Remove drops the object under key, and notifies its key.
func (o *Objects) Remove(key types.NamespacedName) {
	o.mu.Lock()
	delete(o.objects, key)
	subscribers := o.subscribers
	o.mu.Unlock()
	for _, fn := range subscribers {
		(*fn)(key)
	}
}

// Clear drops every object, without notifying.
func (o *Objects) Clear() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.objects = map[types.NamespacedName]client.Object{}
}

func (o *Objects) Get(key types.NamespacedName) (client.Object, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	obj, ok := o.objects[key]
	return obj, ok
}

func (o *Objects) Keys() []types.NamespacedName {
	o.mu.RLock()
	defer o.mu.RUnlock()
	keys := make([]types.NamespacedName, 0, len(o.objects))
	for k := range o.objects {
		keys = append(keys, k)
	}
	return keys
}

func (o *Objects) Subscribe(fn func(types.NamespacedName)) func() {
	o.mu.Lock()
	defer o.mu.Unlock()
	sub := &fn
	o.subscribers = append(slices.Clip(o.subscribers), sub)
	return func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.subscribers = slices.DeleteFunc(slices.Clone(o.subscribers), func(other *func(types.NamespacedName)) bool { return other == sub })
	}
}

func (o *Objects) HasSynced() bool { return true }

func resourceVersion(obj client.Object) uint64 {
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	return rv
}

func (o *Objects) Holds(key types.NamespacedName, uid types.UID) bool {
	obj, ok := o.Get(key)
	return ok && identity(obj) == uid
}
