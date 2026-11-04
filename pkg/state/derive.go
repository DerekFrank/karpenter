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
	"reflect"
	"sync"
)

// Derive keeps fields of to up to date from the fields of from, such as each node's capacity summed onto its
// NodePool. Each key of from contributes contribution(key, fields) to the key target(key, fields) returns in to, and
// apply folds a change of that contribution into to's fields, as a Contributor's apply does. A zero key from target
// means a key contributes to nothing.
//
// to follows from's changes after from's lock is released, so to can briefly lag from.
func Derive[K1, K2 comparable, F1, F2, C any](from *Store[K1, F1], to *Store[K2, F2], target func(K1, F1) K2, contribution func(K1, F1) C, apply func(fields F2, prev, next C) F2) {
	var mu sync.Mutex
	contributions := map[K1]record[K2, C]{}
	from.Subscribe(func(key K1, _, _ F1) {
		var zero K2
		mu.Lock()
		fields, exists := from.Get(key)
		next := record[K2, C]{}
		contributes := false
		if exists {
			next.target = target(key, fields)
			if contributes = next.target != zero; contributes {
				next.value = contribution(key, fields)
			}
		}
		to.mu.Lock()
		prev, had := contributions[key]
		if contributes {
			contributions[key] = next
		} else {
			delete(contributions, key)
		}
		changed := moveContribution(to, apply, func(a, b C) bool { return reflect.DeepEqual(a, b) }, prev, had, next, contributes)
		subscribers := to.subscribers
		to.mu.Unlock()
		mu.Unlock()
		notify(subscribers, changed)
	})
}
