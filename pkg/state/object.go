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
	"golang.org/x/sync/errgroup"
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
// it with Observe, Forget and Resync.
func NewObjectStore[S client.Object](kubeClient client.Client, informers cache.Informers) *Store[types.NamespacedName, S] {
	return NewJoinStore(kubeClient, informers, Join(func(obj S) types.NamespacedName { return client.ObjectKeyFromObject(obj) },
		func(_ S, obj S) S { return obj }))
}

// NewJoinStore returns a store that joins objects of several kinds that are one thing under one key, such as a Node and
// its NodeClaim by providerID, each kind fed as a Part. One store has one lock, so a join reads as one version. With a
// kubeClient and informers, Start follows every Part's kind as NewObjectStore does.
func NewJoinStore[K comparable, F any](kubeClient client.Client, informers cache.Informers, parts ...Part[K, F]) *Store[K, F] {
	s := NewStore[K, F]()
	for _, p := range parts {
		s.parts = append(s.parts, p.new(kubeClient, informers))
	}
	return s
}

// Part is one kind's place in a join store.
type Part[K comparable, F any] struct {
	new func(client.Client, cache.Informers) part[K, F]
}

// Join returns the Part of the objects of kind S in a join store: each S is stored in the fields of the key key
// returns, with set, which gets a nil S once the object leaves the key. A zero key means S is in no key.
func Join[K comparable, F any, S client.Object](key func(S) K, set func(F, S) F) Part[K, F] {
	return Part[K, F]{new: func(kubeClient client.Client, informers cache.Informers) part[K, F] {
		return &joinPart[K, F, S]{
			kubeClient: kubeClient,
			informers:  informers,
			key:        key,
			set:        set,
			typ:        reflect.TypeFor[S](),
			sources:    newFeed(),
			held:       map[types.UID]held[K, S]{},
			watch:      kubeClient != nil,
		}
	}}
}

// part is the part of a join store that one kind feeds.
type part[K comparable, F any] interface {
	kind() reflect.Type
	changeLocked(s *Store[K, F], obj client.Object, deleted, fromInformer bool) []Change[K, F]
	forgetLocked(s *Store[K, F], name types.NamespacedName) []Change[K, F]
	resyncLocked(s *Store[K, F], objs []client.Object, scope []types.NamespacedName) []Change[K, F]
	start(ctx context.Context, s *Store[K, F]) error
	synced() bool
	watching() bool
	clearLocked()
}

type joinPart[K comparable, F any, S client.Object] struct {
	kubeClient client.Client
	informers  cache.Informers
	key        func(S) K
	set        func(F, S) F
	typ        reflect.Type

	sources feed
	held    map[types.UID]held[K, S]
	watch   bool
	listed  atomic.Bool
}

type held[K comparable, S client.Object] struct {
	key K
	obj S
}

func (p *joinPart[K, F, S]) kind() reflect.Type { return p.typ }

func (p *joinPart[K, F, S]) synced() bool { return !p.watch || p.listed.Load() }

func (p *joinPart[K, F, S]) watching() bool { return p.watch }

func (p *joinPart[K, F, S]) clearLocked() {
	p.sources = newFeed()
	p.held = map[types.UID]held[K, S]{}
}

func (p *joinPart[K, F, S]) changeLocked(s *Store[K, F], o client.Object, deleted, fromInformer bool) []Change[K, F] {
	obj := o.(S)
	ok, gone, replaced := p.sources.admit(obj, deleted, fromInformer)
	if !ok {
		return nil
	}
	var changed []Change[K, F]
	if replaced != "" {
		changed = p.move(s, replaced, held[K, S]{}, false)
	}
	if gone {
		return append(changed, p.move(s, identity(obj), held[K, S]{}, false)...)
	}
	var zero K
	next := held[K, S]{key: p.key(obj), obj: obj}
	return append(changed, p.move(s, identity(obj), next, next.key != zero)...)
}

func (p *joinPart[K, F, S]) forgetLocked(s *Store[K, F], name types.NamespacedName) []Change[K, F] {
	if uid, ok := p.sources.forget(name); ok {
		return p.move(s, uid, held[K, S]{}, false)
	}
	return nil
}

// resyncLocked applies the objects newer than the versions the store holds, and forgets the objects named in scope that
// objs leave out.
func (p *joinPart[K, F, S]) resyncLocked(s *Store[K, F], objs []client.Object, scope []types.NamespacedName) []Change[K, F] {
	var changed []Change[K, F]
	listed := make(map[types.NamespacedName]struct{}, len(objs))
	for _, obj := range objs {
		listed[client.ObjectKeyFromObject(obj)] = struct{}{}
		if p.sources.newer(obj) {
			changed = append(changed, p.changeLocked(s, obj, false, false)...)
		}
	}
	for _, name := range scope {
		if _, ok := listed[name]; !ok {
			changed = append(changed, p.forgetLocked(s, name)...)
		}
	}
	return changed
}

// move moves an object's place in the store from prev to next, which may be under different keys.
func (p *joinPart[K, F, S]) move(s *Store[K, F], uid types.UID, next held[K, S], in bool) []Change[K, F] {
	prev, had := p.held[uid]
	if in {
		p.held[uid] = next
	} else {
		delete(p.held, uid)
	}
	if had && in && prev.key == next.key {
		if any(prev.obj) == any(next.obj) {
			return nil
		}
		return s.changeLocked(next.key, func(f F) F { return p.set(f, next.obj) })
	}
	var changed []Change[K, F]
	if had {
		var none S
		changed = append(changed, s.updateLocked(prev.key, func(f F) F { return p.set(f, none) }, -1)...)
	}
	if in {
		changed = append(changed, s.updateLocked(next.key, func(f F) F { return p.set(f, next.obj) }, 1)...)
	}
	return changed
}

// start subscribes to the kind's informer and the client's writes, then applies a List under the store's lock, so that
// events arriving meanwhile apply after it.
func (p *joinPart[K, F, S]) start(ctx context.Context, s *Store[K, F]) error {
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
			s.change(o, deleted, true)
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
				s.change(o, deleted, false)
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
	var changed []Change[K, F]
	for _, item := range items {
		changed = append(changed, p.changeLocked(s, item.(S), false, false)...)
	}
	p.listed.Store(true)
	subscribers := s.subscribers
	s.mu.Unlock()
	notify(subscribers, changed)
	<-ctx.Done()
	return nil
}

// Start follows every kind of an object or join store made with a kubeClient until ctx is done. It implements
// manager.Runnable.
func (s *Store[K, F]) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, p := range s.parts {
		if !p.watching() {
			continue
		}
		g.Go(func() error { return p.start(ctx, s) })
	}
	return g.Wait()
}

// Observe applies a version of an object that a caller saw outside the informer and the client's writes, such as from
// a reconciler, to the Part of its kind. It ignores a version older than one the store has seen.
func (s *Store[K, F]) Observe(obj client.Object) {
	s.change(obj, false, true)
}

// Forget removes the object of obj's kind named as obj is, for a caller that learns an object is gone by its name
// alone, such as a reconciler that finds it missing from the cache.
func (s *Store[K, F]) Forget(obj client.Object) {
	p := s.partFor(obj)
	s.mu.Lock()
	changed := p.forgetLocked(s, client.ObjectKeyFromObject(obj))
	subscribers := s.subscribers
	s.mu.Unlock()
	notify(subscribers, changed)
}

// Resync makes objs the objects of kind S in s among those named in scope, for a caller that lists them, such as the
// pods by an index on their node. It applies the objects newer than the versions s holds, and forgets the objects in
// scope that objs leave out, which heals events s missed.
func Resync[K comparable, F any, S client.Object](s *Store[K, F], objs []S, scope []types.NamespacedName) {
	p := s.partFor(reflect.New(reflect.TypeFor[S]().Elem()).Interface().(client.Object))
	s.mu.Lock()
	changed := p.resyncLocked(s, lo.Map(objs, func(obj S, _ int) client.Object { return obj }), scope)
	subscribers := s.subscribers
	s.mu.Unlock()
	notify(subscribers, changed)
}

func (s *Store[K, F]) change(obj client.Object, deleted, fromInformer bool) {
	p := s.partFor(obj)
	s.mu.Lock()
	changed := p.changeLocked(s, obj, deleted, fromInformer)
	subscribers := s.subscribers
	s.mu.Unlock()
	notify(subscribers, changed)
}

func (s *Store[K, F]) partFor(obj client.Object) part[K, F] {
	t := reflect.TypeOf(obj)
	for _, p := range s.parts {
		if p.kind() == t {
			return p
		}
	}
	panic(fmt.Sprintf("store has no part for %s", t))
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
}

func newFeed() feed {
	return feed{versions: map[types.UID]version{}, names: map[types.NamespacedName]types.UID{}}
}

// admit returns whether obj is a version to apply, whether the object is gone, and the UID of an object it replaces.
//
//nolint:gocyclo
func (f feed) admit(obj client.Object, deleted, fromInformer bool) (bool, bool, types.UID) {
	uid := identity(obj)
	name := client.ObjectKeyFromObject(obj)
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	cur, exists := f.versions[uid]
	switch {
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
	f.versions[uid] = version{rv: max(rv, cur.rv), deleted: deleted}
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
