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
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

// BenchmarkDeriveOneKey changes the contribution of one of 1000 sources to one key, Derive's hot path.
func BenchmarkDeriveOneKey(b *testing.B) {
	nodeClaims, s, _ := newStores()
	versions := make([]uint64, 1000)
	for i := range versions {
		versions[i] = 1
		put(nodeClaims, nodeClaim(fmt.Sprintf("nc-%d", i), "x", 1, "1"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		n := i % len(versions)
		versions[n]++
		put(nodeClaims, nodeClaim(fmt.Sprintf("nc-%d", n), "x", versions[n], fmt.Sprint(1+versions[n]%2)))
	}
	b.StopTimer()
	if got := fields(s, "x").Count; got != len(versions) {
		b.Fatalf("x counts %d, want %d", got, len(versions))
	}
}

// BenchmarkFeedIndexed feeds one of 1000 pods a new object, with and without an index on its PVCs, the cost Index adds
// to a write. With moved, each pod alternates between two PVCs, each mounted by 10 pods; without, it alternates between
// two nodes and keeps its PVC.
func BenchmarkFeedIndexed(b *testing.B) {
	for _, c := range []struct{ indexed, moved bool }{{false, true}, {true, true}, {true, false}} {
		indexed, moved := c.indexed, c.moved
		b.Run(fmt.Sprintf("indexed=%t/moved=%t", indexed, moved), func(b *testing.B) {
			pods := NewFedStore[types.NamespacedName, testPod, struct{}, struct{}]()
			if indexed {
				NewIndex(pods, mountsPVC)
			}
			names := make([]types.NamespacedName, 1000)
			objects := [2][]testPod{make([]testPod, len(names)), make([]testPod, len(names))}
			for i := range names {
				names[i] = pod(fmt.Sprintf("p-%d", i))
				for v := range objects {
					if moved {
						objects[v][i] = testPod{PVCs: []string{fmt.Sprintf("pvc-%d", (i+v*50)%100)}}
					} else {
						objects[v][i] = testPod{Node: fmt.Sprint(v), PVCs: []string{fmt.Sprintf("pvc-%d", i%100)}}
					}
				}
				pods.Feed(names[i], objects[0][i])
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				n := i % len(names)
				pods.Feed(names[n], objects[(i/len(names)+1)%2][n])
			}
		})
	}
}
