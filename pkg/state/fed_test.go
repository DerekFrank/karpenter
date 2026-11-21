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
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

// sources are the Objects each test object store follows, by its core, which a test fills as an informer would.
var sources sync.Map

// objectStore returns an object store that follows Objects the test fills with observe, forget and resync.
func objectStore[K comparable, S client.Object, D, I any](opts ...ObjectOption) *Store[K, S, D, I] {
	return objectStoreOf[K, S, D, I](statetest.NewObjects(), opts...)
}

func objectStoreOf[K comparable, S client.Object, D, I any](from *statetest.Objects, opts ...ObjectOption) *Store[K, S, D, I] {
	s := NewObjectStore[K, S, D, I](from, opts...)
	sources.Store(s.s, from)
	return s
}

func objectsOf[K comparable, O, D, I any](s *Store[K, O, D, I]) *statetest.Objects {
	o, _ := sources.Load(s.s)
	return o.(*statetest.Objects)
}

// observe shows the store a version of obj, as its informer would.
func observe[K comparable, O, D, I any](s *Store[K, O, D, I], obj client.Object) {
	objectsOf(s).Put(obj)
}

// forget shows the store that the object named name is gone, as its informer would.
func forget[K comparable, O, D, I any](s *Store[K, O, D, I], name types.NamespacedName) {
	objectsOf(s).Remove(name)
}

// resync makes objs the objects among those named in scope, as an informer's relist would.
func resync[K comparable, O client.Object, D, I any](s *Store[K, O, D, I], objs []O, scope []types.NamespacedName) {
	listed := map[types.NamespacedName]bool{}
	for _, obj := range objs {
		listed[client.ObjectKeyFromObject(obj)] = true
		observe(s, obj)
	}
	for _, name := range scope {
		if !listed[name] {
			forget(s, name)
		}
	}
}
