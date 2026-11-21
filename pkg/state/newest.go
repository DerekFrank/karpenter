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
	"sync/atomic"

	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Newest holds the newest version of each object of one kind, and notifies the key of each object whose newest
// version may have changed: overlay.Source, which combines the informer's store with the client's own writes, or
// statetest.Objects, which a unit test fills as an informer would.
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

type newestOf[K comparable, S client.Object, D, I any] struct {
	core  *core[K, S, D, I]
	from  Newest
	key   func(types.NamespacedName) K
	in    func(S) bool
	swept atomic.Bool
}

func (n *newestOf[K, S, D, I]) synced() bool { return n.swept.Load() && n.from.HasSynced() }

// start waits for from to sync, then applies every key it holds. Notifications meanwhile apply the newest version too,
// which is idempotent.
func (n *newestOf[K, S, D, I]) start(ctx context.Context) error {
	if !n.swept.Load() && toolscache.WaitForCacheSync(ctx.Done(), n.from.HasSynced) {
		n.sweep()
	}
	<-ctx.Done()
	return nil
}

func (n *newestOf[K, S, D, I]) resync() {
	if n.from.HasSynced() {
		n.sweep()
	}
}

func (n *newestOf[K, S, D, I]) sweep() {
	for _, name := range n.from.Keys() {
		n.apply(name)
	}
	n.swept.Store(true)
}

// apply holds the newest version of the object named name. It reads it without the store's lock, and commits it only if
// it's newer than the entry's, so an older read loses to a newer one. A removal has no version, so a read from before a
// removal can commit after it once from has dropped the object's UID; apply reads again after each commit, and repeats
// while what it reads isn't what it committed, so the last commit is from a read after every earlier one.
func (n *newestOf[K, S, D, I]) apply(name types.NamespacedName) {
	var zero K
	k := n.key(name)
	if k == zero {
		return
	}
	obj, ok := n.read(name)
	for {
		n.commit(name, k, obj, ok)
		again, held := n.read(name)
		if held == ok && (!ok || any(again) == any(obj)) {
			return
		}
		obj, ok = again, held
	}
}

// read returns the newest version of the object named name that the store holds, if from holds one it accepts.
func (n *newestOf[K, S, D, I]) read(name types.NamespacedName) (S, bool) {
	var obj S
	o, ok := n.from.Get(name)
	if ok {
		obj, ok = o.(S)
		ok = ok && n.in(obj)
	}
	return obj, ok
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
			n.buryLocked(name, k, cur)
		}
		c.objectLocked(k, obj, true)
	case cur.exists && !n.serves(name, cur.entry.Object):
		// A read that found the object gone can commit after a newer read found it back under another UID, so the
		// removal commits only while from still doesn't serve the object the store holds.
		n.buryLocked(name, k, cur)
	}
	n.pruneLocked(name, k)
}

// serves returns whether from serves obj, by its UID, under name now, as an object the store accepts.
func (n *newestOf[K, S, D, I]) serves(name types.NamespacedName, obj S) bool {
	o, ok := n.read(name)
	return ok && identity(o) == identity(obj)
}

// buryLocked removes k's object, which drops the entry's intent, and records its UID as gone while from still holds it,
// so that a stale read of it is dropped. Once from doesn't hold the UID no event of it can arrive, which is what
// pruneLocked drops a record on, so recording one then would only be undone.
func (n *newestOf[K, S, D, I]) buryLocked(name types.NamespacedName, k K, cur *slot[S, D, I]) {
	next := *cur
	next.exists = false
	var none S
	var zero I
	next.entry.Object, next.entry.Intent = none, zero
	if uid := identity(cur.entry.Object); n.from.Holds(name, uid) {
		next.gone = append(slices.Clip(cur.gone), uid)
	}
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

func resourceVersion(obj client.Object) uint64 {
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	return rv
}
