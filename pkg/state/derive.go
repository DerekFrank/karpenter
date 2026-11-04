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
)

// Derive keeps fields of to up to date from the fields of from, such as each node's capacity summed onto its
// NodePool. Each key of from contributes value(key, fields) to the key that key(key, fields) returns in to, dispatched
// to the contribution's Update and Delete as a Contributor does. A zero key means a key contributes to nothing.
//
// to follows from's changes after from's lock is released, so to can briefly lag from.
func Derive[K1, K2 comparable, F1, F2 any, C Contribution[F2, C]](from *Store[K1, F1], to *Store[K2, F2], key func(K1, F1) K2, value func(K1, F1) C) {
	var mu sync.Mutex
	contributions := map[K1]record[K2, C]{}
	from.Subscribe(func(k K1, _, _ F1) {
		var zero K2
		mu.Lock()
		fields, exists := from.Get(k)
		next := record[K2, C]{}
		contributes := false
		if exists {
			next.target = key(k, fields)
			if contributes = next.target != zero; contributes {
				next.value = value(k, fields)
			}
		}
		to.mu.Lock()
		prev, had := contributions[k]
		if contributes {
			contributions[k] = next
		} else {
			delete(contributions, k)
		}
		changed := moveContribution(to, prev, had, next, contributes)
		subscribers := to.subscribers
		to.mu.Unlock()
		mu.Unlock()
		notify(subscribers, changed)
	})
}
