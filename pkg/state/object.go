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
	"reflect"
	"strconv"
	"sync/atomic"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"sigs.k8s.io/karpenter/pkg/state/overlay"
)

// NewObjectStore returns a store of the objects of kind S by namespaced name. With a kubeClient and informers, Start
// lists them, then follows the informer and the kubeClient's writes, if it's an overlay.Client. Without, a caller feeds
// it with Observe, Forget and Resync. An entry exists while the store holds its object.
func NewObjectStore[S client.Object, D, I any](kubeClient client.Client, informers cache.Informers) *Store[types.NamespacedName, S, D, I] {
	return NewObjectStoreBy[types.NamespacedName, S, D, I](kubeClient, informers, func(obj S) types.NamespacedName { return client.ObjectKeyFromObject(obj) })
}

// NewObjectStoreBy returns a store of the objects of kind S under the key key returns, such as a cluster-scoped
// object's name. A zero key leaves the object out of the store. Otherwise it's NewObjectStore.
func NewObjectStoreBy[K comparable, S client.Object, D, I any](kubeClient client.Client, informers cache.Informers, key func(S) K) *Store[K, S, D, I] {
	c := newCore[K, S, D, I](objects, nil)
	c.feed = &objectsOf[K, S, D, I]{
		core:       c,
		kubeClient: kubeClient,
		informers:  informers,
		key:        key,
		typ:        reflect.TypeFor[S](),
		versions:   newFeed(),
		held:       map[types.UID]K{},
		watch:      kubeClient != nil,
	}
	return &Store[K, S, D, I]{Reader[K, S, D, I]{c}}
}

// objectFeed feeds an object store from its kind's objects, under the store's lock.
type objectFeed interface {
	kind() reflect.Type
	changeLocked(obj client.Object, deleted bool, from origin)
	forgetLocked(name types.NamespacedName)
	resyncLocked(objs []client.Object, scope []types.NamespacedName)
	start(ctx context.Context) error
	synced() bool
	watching() bool
	clearLocked()
}

type objectsOf[K comparable, S client.Object, D, I any] struct {
	core       *core[K, S, D, I]
	kubeClient client.Client
	informers  cache.Informers
	key        func(S) K
	typ        reflect.Type

	versions feed
	// held is the key each object is under, by UID.
	held   map[types.UID]K
	watch  bool
	listed atomic.Bool
}

func (p *objectsOf[K, S, D, I]) kind() reflect.Type { return p.typ }

func (p *objectsOf[K, S, D, I]) synced() bool { return !p.watch || p.listed.Load() }

func (p *objectsOf[K, S, D, I]) watching() bool { return p.watch }

func (p *objectsOf[K, S, D, I]) clearLocked() {
	p.versions = newFeed()
	p.held = map[types.UID]K{}
}

func (p *objectsOf[K, S, D, I]) changeLocked(o client.Object, deleted bool, from origin) {
	obj := o.(S)
	ok, gone, replaced := p.versions.admit(obj, deleted, from)
	if !ok {
		return
	}
	if replaced != "" {
		p.drop(replaced)
	}
	uid := identity(obj)
	var zero K
	key := p.key(obj)
	if gone || key == zero {
		p.drop(uid)
		return
	}
	if prev, had := p.held[uid]; had && prev != key {
		p.drop(uid)
	}
	p.held[uid] = key
	if cur, _ := p.core.load(key); cur.exists && any(cur.entry.Object) == any(obj) {
		return
	}
	p.core.objectLocked(key, obj, true)
}

// drop removes the object with the UID from the store, which drops its entry and its intent.
func (p *objectsOf[K, S, D, I]) drop(uid types.UID) {
	key, had := p.held[uid]
	if !had {
		return
	}
	delete(p.held, uid)
	var none S
	p.core.objectLocked(key, none, false)
}

func (p *objectsOf[K, S, D, I]) forgetLocked(name types.NamespacedName) {
	if uid, ok := p.versions.forget(name); ok {
		p.drop(uid)
	}
}

// resyncLocked applies the objects newer than the versions the store holds, and forgets the objects named in scope that
// objs leave out.
func (p *objectsOf[K, S, D, I]) resyncLocked(objs []client.Object, scope []types.NamespacedName) {
	listed := make(map[types.NamespacedName]struct{}, len(objs))
	for _, obj := range objs {
		listed[client.ObjectKeyFromObject(obj)] = struct{}{}
		if p.versions.newer(obj) {
			p.changeLocked(obj, false, copied)
		}
	}
	for _, name := range scope {
		if _, ok := listed[name]; !ok {
			p.forgetLocked(name)
		}
	}
}

// start subscribes to the kind's informer and the client's writes, then applies a List under the store's lock, so that
// events arriving meanwhile apply after it.
func (p *objectsOf[K, S, D, I]) start(ctx context.Context) error {
	s := p.core
	obj := reflect.New(p.typ.Elem()).Interface().(S)
	list, err := listFor(p.kubeClient.Scheme(), obj)
	if err != nil {
		return err
	}
	informer, err := p.informers.GetInformer(ctx, obj)
	if err != nil {
		return fmt.Errorf("getting informer, %w", err)
	}
	observe := func(raw any, deleted bool) {
		if tombstone, ok := raw.(toolscache.DeletedFinalStateUnknown); ok {
			raw = tombstone.Obj
		}
		if o, ok := raw.(S); ok {
			s.change(o, deleted, cached)
		}
	}
	registration, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { observe(o, false) },
		UpdateFunc: func(_, o any) { observe(o, false) },
		DeleteFunc: func(o any) { observe(o, true) },
	})
	if err != nil {
		return fmt.Errorf("adding event handler, %w", err)
	}
	defer func() { _ = informer.RemoveEventHandler(registration) }()
	if writer, ok := p.kubeClient.(*overlay.Client); ok {
		remove, err := writer.AddWriteHandler(obj, func(o client.Object, deleted bool) {
			if _, ok := o.(S); ok {
				s.change(o, deleted, copied)
			}
		})
		if err != nil {
			return fmt.Errorf("adding write handler, %w", err)
		}
		defer remove()
	}
	s.mu.Lock()
	if err := p.kubeClient.List(ctx, list, client.UnsafeDisableDeepCopy); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("listing, %w", err)
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("extracting list, %w", err)
	}
	for _, item := range items {
		p.changeLocked(item.(S), false, copied)
	}
	p.listed.Store(true)
	s.unlock()
	<-ctx.Done()
	return nil
}

// Start follows the kind of an object store made with a kubeClient until ctx is done. It implements manager.Runnable.
func (s *Store[K, O, D, I]) Start(ctx context.Context) error {
	if s.s.feed == nil || !s.s.feed.watching() {
		<-ctx.Done()
		return nil
	}
	return s.s.feed.start(ctx)
}

// Observe applies a version of an object that a caller saw outside the informer and the client's writes, such as from
// a reconciler. It ignores a version older than one the store has seen.
func (s *Store[K, O, D, I]) Observe(obj client.Object) {
	s.s.change(obj, false, observed)
}

// Forget removes the object named name from s, for a caller that learns an object is gone by its name alone, such as a
// reconciler that finds it missing from the cache.
func Forget[K comparable, O, D, I any](s *Store[K, O, D, I], name types.NamespacedName) {
	c := s.s
	c.mu.Lock()
	c.feed.forgetLocked(name)
	c.unlock()
}

// Resync makes objs the objects in s among those named in scope, for a caller that lists them, such as the pods by an
// index on their node. It applies the objects newer than the versions s holds, and forgets the objects in scope that
// objs leave out, which heals events s missed.
func Resync[K comparable, O client.Object, D, I any](s *Store[K, O, D, I], objs []O, scope []types.NamespacedName) {
	c := s.s
	c.mu.Lock()
	c.feed.resyncLocked(lo.Map(objs, func(obj O, _ int) client.Object { return obj }), scope)
	c.unlock()
}

func (c *core[K, O, D, I]) change(obj client.Object, deleted bool, from origin) {
	if c.feed == nil || c.feed.kind() != reflect.TypeOf(obj) {
		panic(fmt.Sprintf("store holds no %T", obj))
	}
	c.mu.Lock()
	c.feed.changeLocked(obj, deleted, from)
	c.unlock()
}

// feed orders an object's versions by UID and resourceVersion. A delete from a write leaves a tombstone until the
// informer's delete arrives, because the informer may still deliver older versions. Names are unique at any moment, so
// a newer object under a name another UID holds replaces that object, which leaves a tombstone too.
type feed struct {
	versions map[types.UID]version
	names    map[types.NamespacedName]types.UID
}

type version struct {
	rv      uint64
	deleted bool
	// cached is whether the store holds the informer's own object for the version, rather than a copy.
	cached bool
}

// origin is where a version of an object comes from.
type origin int

const (
	// copied is a copy of the object, from the client's write or a List.
	copied origin = iota
	// observed is a version a caller saw elsewhere, such as from a reconciler.
	observed
	// cached is the informer's own object.
	cached
)

func newFeed() feed {
	return feed{versions: map[types.UID]version{}, names: map[types.NamespacedName]types.UID{}}
}

// admit returns whether obj is a version to apply, whether the object is gone, and the UID of an object it replaces. The
// informer's own object for the version the store holds replaces a copy of it, so the store holds no copy of an object
// the informer's cache holds once the informer delivers it.
//
//nolint:gocyclo
func (f feed) admit(obj client.Object, deleted bool, from origin) (bool, bool, types.UID) {
	uid := identity(obj)
	name := client.ObjectKeyFromObject(obj)
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	cur, exists := f.versions[uid]
	fromInformer := from != copied
	switch {
	case exists && !cur.deleted && !deleted && from == cached && !cur.cached && rv != 0 && rv == cur.rv:
		f.versions[uid] = version{rv: rv, cached: true}
		return true, false, ""
	case deleted && fromInformer:
		if !exists {
			return false, false, ""
		}
		delete(f.versions, uid)
		if f.names[name] == uid {
			delete(f.names, name)
		}
		return true, true, ""
	case exists && cur.deleted:
		return false, false, ""
	case deleted:
	case rv == 0 && !fromInformer, exists && rv != 0 && rv <= cur.rv:
		return false, false, ""
	}
	var replaced types.UID
	if other, ok := f.names[name]; ok && other != uid {
		if v := f.versions[other]; !v.deleted && rv != 0 && v.rv < rv {
			f.versions[other] = version{rv: v.rv, deleted: true}
			replaced = other
		} else if !v.deleted {
			return false, false, ""
		}
	}
	f.versions[uid] = version{rv: max(rv, cur.rv), deleted: deleted, cached: from == cached}
	f.names[name] = uid
	return true, deleted, replaced
}

func (f feed) newer(obj client.Object) bool {
	cur, exists := f.versions[identity(obj)]
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	return !exists || (!cur.deleted && (rv == 0 || rv > cur.rv))
}

// identity is an object's UID, or its namespaced name for an object that has none because it never went through the
// API server.
func identity(obj client.Object) types.UID {
	if uid := obj.GetUID(); uid != "" {
		return uid
	}
	return types.UID(client.ObjectKeyFromObject(obj).String())
}

// forget removes the object named name. A name without a namespace also matches an object of the same name in any
// namespace, because a cluster-scoped kind is forgotten by name, and an object built without the API server, such as
// in a test, can carry a namespace anyway.
func (f feed) forget(name types.NamespacedName) (types.UID, bool) {
	uid, ok := f.names[name]
	if !ok && name.Namespace == "" {
		for n, u := range f.names {
			if n.Name == name.Name {
				name, uid, ok = n, u, true
				break
			}
		}
	}
	if !ok {
		return "", false
	}
	delete(f.versions, uid)
	delete(f.names, name)
	return uid, true
}

// listFor returns an empty list of obj's kind: the kind with a List suffix in scheme.
func listFor(scheme *runtime.Scheme, obj client.Object) (client.ObjectList, error) {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return nil, fmt.Errorf("finding kind, %w", err)
	}
	gvk.Kind += "List"
	l, err := scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("making list, %w", err)
	}
	list, ok := l.(client.ObjectList)
	if !ok {
		return nil, fmt.Errorf("%s isn't a list", gvk)
	}
	return list, nil
}
