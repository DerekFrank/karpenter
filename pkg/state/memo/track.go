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

package memo

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/maphash"
	"reflect"
	"slices"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Tracker invalidates a Memo's keys from the objects of a kind S its values are computed from. For each object it
// remembers the key the object counts toward and a hash of its inputs, the fields of the object the value reads, and
// invalidates only when either changes. So observing an object is idempotent, and an update that changes none of the
// inputs, such as a condition, recomputes nothing.
//
// Observing never makes a value wrong, only stale until the next invalidation: a compute reads the current cache, so
// a version observed out of order costs at most a recompute.
type Tracker[S client.Object, K comparable, V any] struct {
	memo   *Memo[K, V]
	inputs func(context.Context, S) (K, uint64)

	mu      sync.Mutex
	tracked map[types.NamespacedName]tracked[K]
}

type tracked[K comparable] struct {
	key K
	// hash is the hash of the inputs last observed, or zero when they're unknown, which never matches.
	hash uint64
}

var seed = maphash.MakeSeed()

// Track returns a Tracker for memo. inputs returns the key an object counts toward and a struct of every field of the
// object that the key's value is computed from and that can change while the key stays the same. Track panics if I
// can't be hashed: it must hold exported fields of JSON-encodable values, with times as UnixNano and no interfaces.
func Track[S client.Object, K comparable, V, I any](memo *Memo[K, V], inputs func(context.Context, S) (K, I)) *Tracker[S, K, V] {
	if err := hashable(reflect.TypeFor[I](), "inputs"); err != nil {
		panic(fmt.Sprintf("memo: inputs %s can't be hashed, %s", reflect.TypeFor[I](), err))
	}
	return &Tracker[S, K, V]{
		memo: memo,
		inputs: func(ctx context.Context, obj S) (K, uint64) {
			key, in := inputs(ctx, obj)
			b, err := json.Marshal(in)
			if err != nil {
				// Unknown inputs never match, so a value that can't be encoded, such as NaN, always invalidates.
				return key, 0
			}
			return key, maphash.Bytes(seed, b)
		},
		tracked: map[types.NamespacedName]tracked[K]{},
	}
}

// Observe records obj's key and inputs, and invalidates its previous and current keys if either changed. It returns
// the key obj was tracked under before, if any.
func (t *Tracker[S, K, V]) Observe(ctx context.Context, obj S) (previous K, wasTracked bool) {
	key, hash := t.inputs(ctx, obj)
	name := client.ObjectKeyFromObject(obj)
	t.mu.Lock()
	defer t.mu.Unlock()
	old, ok := t.tracked[name]
	t.tracked[name] = tracked[K]{key: key, hash: hash}
	if !ok || old.key != key || old.hash != hash || hash == 0 {
		if ok {
			t.memo.Invalidate(old.key)
		}
		t.memo.Invalidate(key)
	}
	return old.key, ok
}

// Forget stops tracking the object, and invalidates the key it counted toward.
func (t *Tracker[S, K, V]) Forget(name types.NamespacedName) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, ok := t.tracked[name]; ok {
		delete(t.tracked, name)
		t.memo.Invalidate(old.key)
	}
}

// Claim is for a compute that finds obj counting toward key: it tracks obj under key if it isn't already, so that
// obj's delete, which only names it, invalidates key. It returns true if obj wasn't tracked under key, in which case
// a delete may have gone unnoticed before the claim, so the caller should check the object still exists, and Forget
// it if not.
func (t *Tracker[S, K, V]) Claim(key K, obj S) bool {
	name := client.ObjectKeyFromObject(obj)
	t.mu.Lock()
	defer t.mu.Unlock()
	old, ok := t.tracked[name]
	if ok && old.key == key {
		return false
	}
	if ok {
		t.memo.Invalidate(old.key)
	}
	t.tracked[name] = tracked[K]{key: key}
	return true
}

// Key returns the key the object counts toward, if it's tracked.
func (t *Tracker[S, K, V]) Key(name types.NamespacedName) (K, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	old, ok := t.tracked[name]
	return old.key, ok
}

// Reset stops tracking every object.
func (t *Tracker[S, K, V]) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tracked = map[types.NamespacedName]tracked[K]{}
}

var (
	jsonMarshaler = reflect.TypeFor[json.Marshaler]()
	timeTypes     = []reflect.Type{reflect.TypeFor[time.Time](), reflect.TypeFor[metav1.Time](), reflect.TypeFor[metav1.MicroTime]()}
)

// hashable returns why JSON can't encode every value of t exactly: it skips unexported fields, encodes times to the
// second, and can't be checked through an interface.
//
//nolint:gocyclo
func hashable(t reflect.Type, path string) error {
	if slices.Contains(timeTypes, t) {
		return fmt.Errorf("%s is a %s, which JSON encodes to the second; use its UnixNano", path, t)
	}
	if t.Implements(jsonMarshaler) || reflect.PointerTo(t).Implements(jsonMarshaler) {
		// Such as resource.Quantity, which encodes its value.
		return nil
	}
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.String:
		return nil
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return hashable(t.Elem(), path+"[]")
	case reflect.Map:
		if err := hashable(t.Key(), path+" key"); err != nil {
			return err
		}
		return hashable(t.Elem(), path+"[]")
	case reflect.Struct:
		for field := range t.Fields() {
			if !field.IsExported() {
				return fmt.Errorf("%s.%s is unexported, which JSON skips", path, field.Name)
			}
			if tag := field.Tag.Get("json"); tag == "-" {
				return fmt.Errorf("%s.%s has a json:\"-\" tag, which JSON skips", path, field.Name)
			}
			if err := hashable(field.Type, path+"."+field.Name); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%s is a %s, which can't be hashed", path, t.Kind())
	}
}
