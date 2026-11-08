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

// Package state stands in for pkg/state: it builds stores freely.
package state

import "context"

type Store[K comparable, F any] struct{}

func (s *Store[K, F]) Start(context.Context) error { return nil }

func NewStore[K comparable, F any]() *Store[K, F] { return &Store[K, F]{} }

func NewObjectStore[S any]() *Store[string, S] { return NewStore[string, S]() }

type Part[K comparable, F any] struct{}

func Join[K comparable, F any, S any](func(S) K, func(F, S) F) Part[K, F] { return Part[K, F]{} }

func NewJoinStore[K comparable, F any](...Part[K, F]) *Store[K, F] { return NewStore[K, F]() }

type Derivation struct{}

func Derive[K1, K2 comparable, F1, F2 any](*Store[K1, F1], *Store[K2, F2]) *Derivation {
	return &Derivation{}
}
