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

// Package state is a stand-in for pkg/state's builders.
package state

import "context"

type Entry[O, D, I any] struct {
	Object  O
	Derived D
	Intent  I
}

type Intended[V any] struct{ Value V }

type Reader[K comparable, O, D, I any] struct{}

func (Reader[K, O, D, I]) Subscribe(func()) func() { return func() {} }
func (Reader[K, O, D, I]) Source() any             { return nil }
func (Reader[K, O, D, I]) Get(K) (Entry[O, D, I], bool) {
	return Entry[O, D, I]{}, false
}

type Store[K comparable, O, D, I any] struct{ Reader[K, O, D, I] }

func (s *Store[K, O, D, I]) Start(context.Context) error { return nil }

func (s *Store[K, O, D, I]) Clear() {}

func NewSyntheticStore[K comparable, O, D, I any](func(K) O) *Store[K, O, D, I] {
	return &Store[K, O, D, I]{}
}

func NewObjectStore[K comparable, O, D, I any]() *Store[K, O, D, I] { return &Store[K, O, D, I]{} }

func NewFedStore[K comparable, O, D, I any]() *Store[K, O, D, I] { return &Store[K, O, D, I]{} }

type Derivation struct{}

func (Derivation) Sources(string) []string { return nil }

func Derive[K1, K2 comparable, O1, D1, I1, O2, D2, I2 any](Reader[K1, O1, D1, I1], *Store[K2, O2, D2, I2]) *Derivation {
	return &Derivation{}
}

type Intent[V any] struct{}

func NewIntent[K comparable, O, D, I, V any](*Store[K, O, D, I]) *Intent[V] { return &Intent[V]{} }
