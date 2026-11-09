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
)

// BenchmarkDeriveOneKey changes the contribution of one of 1000 sources to one key, Derive's hot path.
func BenchmarkDeriveOneKey(b *testing.B) {
	nodeClaims, s, _ := newStores()
	versions := make([]uint64, 1000)
	for i := range versions {
		versions[i] = 1
		observe(nodeClaims, nodeClaim(fmt.Sprintf("nc-%d", i), "x", 1, "1"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		n := i % len(versions)
		versions[n]++
		observe(nodeClaims, nodeClaim(fmt.Sprintf("nc-%d", n), "x", versions[n], fmt.Sprint(1+versions[n]%2)))
	}
	b.StopTimer()
	if got := fields(s, "x").Count; got != len(versions) {
		b.Fatalf("x counts %d, want %d", got, len(versions))
	}
}
