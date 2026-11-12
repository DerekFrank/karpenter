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
// it's at least as new as the object held, so an older read loses to a newer one. A removal has no version, so after a
// commit it reads again, and applies the newer version it finds: a read from before a removal can't outlive it.
func (n *newestOf[K, S, D, I]) apply(name types.NamespacedName) {
	var zero K
	k := n.key(name)
	if k == zero {
		return
	}
	obj, ok := n.newest(name)
	for n.commit(k, obj, ok) {
		again, still := n.newest(name)
		if still == ok && (!ok || any(again) == any(obj)) {
			return
		}
		obj, ok = again, still
	}
}

func (n *newestOf[K, S, D, I]) newest(name types.NamespacedName) (S, bool) {
	var obj S
	o, ok := n.from.Get(name)
	if ok {
		obj, ok = o.(S)
	}
	return obj, ok && n.in(obj)
}

// commit holds obj under k, or drops k's entry when ok is false, and returns whether it changed the entry. An older
// version of the object held loses. A version as new as the one held replaces it, so the store converges on the
// informer's own object for a version the overlay held a copy of. An object with another UID replaces the one held,
// which drops its intent.
func (n *newestOf[K, S, D, I]) commit(k K, obj S, ok bool) bool {
	c := n.core
	c.mu.Lock()
	defer c.unlock()
	cur, _ := c.load(k)
	switch {
	case !ok && !cur.exists, ok && cur.exists && any(cur.entry.Object) == any(obj):
		return false
	case ok && cur.exists && obj.GetResourceVersion() != "" && resourceVersion(obj) < resourceVersion(cur.entry.Object):
		return false
	case ok && cur.exists && identity(cur.entry.Object) != identity(obj):
		var none S
		c.objectLocked(k, none, false)
	}
	c.objectLocked(k, obj, ok)
	return true
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
