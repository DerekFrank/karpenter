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

package statetest

import (
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Objects is a state.Newest that a unit test fills as an informer's store would, for a store built without an API
// server, such as in pkg/state's and pkg/cluster's own tests and benchmarks. A suite with an API server follows the
// overlay client's Source instead, and writes through the API server.
type Objects struct {
	mu          sync.RWMutex
	objects     map[types.NamespacedName]client.Object
	subscribers []*func(types.NamespacedName)
	unsynced    atomic.Bool
}

func NewObjects() *Objects {
	return &Objects{objects: map[types.NamespacedName]client.Object{}}
}

// NewUnsyncedObjects returns Objects that report they haven't synced until SetSynced, as an informer before its first
// List.
func NewUnsyncedObjects() *Objects {
	o := NewObjects()
	o.unsynced.Store(true)
	return o
}

// SetSynced reports that the Objects have synced.
func (o *Objects) SetSynced() { o.unsynced.Store(false) }

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

// Hold holds obj under its name without notifying, for a test of the versions a notification skips.
func (o *Objects) Hold(obj client.Object) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.objects[client.ObjectKeyFromObject(obj)] = obj
}

// Drop drops the object under key without notifying, for a test that notifies separately.
func (o *Objects) Drop(key types.NamespacedName) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.objects, key)
}

// Notify notifies key, as a source does once a change of its object is visible.
func (o *Objects) Notify(key types.NamespacedName) {
	o.mu.RLock()
	subscribers := o.subscribers
	o.mu.RUnlock()
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

func (o *Objects) HasSynced() bool { return !o.unsynced.Load() }

func (o *Objects) Holds(key types.NamespacedName, uid types.UID) bool {
	obj, ok := o.Get(key)
	return ok && identity(obj) == uid
}

// Sources are the Objects of each kind, for a test that builds cluster state without an API server, which reads each
// kind's newest versions from For.
type Sources struct {
	mu       sync.Mutex
	objects  map[reflect.Type]*Objects
	unsynced bool
}

func NewSources() *Sources { return &Sources{objects: map[reflect.Type]*Objects{}} }

// NewUnsyncedSources returns Sources whose Objects report they haven't synced until SetSynced.
func NewUnsyncedSources() *Sources {
	return &Sources{objects: map[reflect.Type]*Objects{}, unsynced: true}
}

// SetSynced reports that the Objects of every kind have synced.
func (s *Sources) SetSynced() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsynced = false
	for _, o := range s.objects {
		o.SetSynced()
	}
}

// For returns the Objects of obj's kind, which it makes on first use.
func (s *Sources) For(obj client.Object) *Objects {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[reflect.TypeOf(obj)]
	if !ok {
		o = NewObjects()
		if s.unsynced {
			o = NewUnsyncedObjects()
		}
		s.objects[reflect.TypeOf(obj)] = o
	}
	return o
}

// Put holds each object in the Objects of its kind, as Objects.Put does.
func (s *Sources) Put(objs ...client.Object) {
	for _, obj := range objs {
		s.For(obj).Put(obj)
	}
}

// Remove drops each object from the Objects of its kind, as Objects.Remove does.
func (s *Sources) Remove(objs ...client.Object) {
	for _, obj := range objs {
		s.For(obj).Remove(client.ObjectKeyFromObject(obj))
	}
}

func identity(obj client.Object) types.UID {
	if uid := obj.GetUID(); uid != "" {
		return uid
	}
	return types.UID(client.ObjectKeyFromObject(obj).String())
}

func resourceVersion(obj client.Object) uint64 {
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	return rv
}
