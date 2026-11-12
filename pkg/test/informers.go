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

package test

import (
	"context"
	"reflect"
	"sync"

	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"
)

// StoredInformer is a FakeInformer with a store, as the informer cache's informers have: Add, Update and Delete store
// the object, then deliver its event. The overlay reads the objects of a kind from its informer's store.
type StoredInformer struct {
	*controllertest.FakeInformer
	Store toolscache.Store
}

func (i *StoredInformer) GetStore() toolscache.Store { return i.Store }

func (i *StoredInformer) Add(obj metav1.Object) {
	lo.Must0(i.Store.Add(obj))
	i.FakeInformer.Add(obj)
}

func (i *StoredInformer) Update(oldObj, newObj metav1.Object) {
	lo.Must0(i.Store.Update(newObj))
	i.FakeInformer.Update(oldObj, newObj)
}

func (i *StoredInformer) Delete(obj metav1.Object) {
	lo.Must0(i.Store.Delete(obj))
	i.FakeInformer.Delete(obj)
}

// StoredInformers are FakeInformers that serve a StoredInformer for each kind Stored makes one for.
type StoredInformers struct {
	*informertest.FakeInformers

	mu     sync.Mutex
	stored map[reflect.Type]*StoredInformer
}

func NewStoredInformers(scheme *runtime.Scheme) *StoredInformers {
	return &StoredInformers{FakeInformers: &informertest.FakeInformers{Scheme: scheme}, stored: map[reflect.Type]*StoredInformer{}}
}

// Stored returns obj's kind's StoredInformer, which it makes on first use.
func (s *StoredInformers) Stored(ctx context.Context, obj client.Object) *StoredInformer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i, ok := s.stored[reflect.TypeOf(obj)]; ok {
		return i
	}
	i := &StoredInformer{FakeInformer: lo.Must(s.FakeInformerFor(ctx, obj)), Store: toolscache.NewStore(toolscache.MetaNamespaceKeyFunc)}
	s.stored[reflect.TypeOf(obj)] = i
	return i
}

func (s *StoredInformers) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	s.mu.Lock()
	i, ok := s.stored[reflect.TypeOf(obj)]
	s.mu.Unlock()
	if ok {
		return i, nil
	}
	return s.FakeInformers.GetInformer(ctx, obj, opts...)
}
