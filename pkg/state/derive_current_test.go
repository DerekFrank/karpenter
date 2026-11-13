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
	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
)

func pooled(s *pools) map[string]poolFields {
	got := map[string]poolFields{}
	for k, e := range s.List() {
		got[k] = e.Derived
	}
	return got
}

// staleRead reads key as an apply does, and returns whether the commit of that read, run later, commits.
func staleRead[K1, K2 comparable, D2 any, C Contribution[D2, C]](d *Derivation[K1, K2, D2, C], key K1) func() bool {
	read, next, contributes := d.read(key)
	return func() bool { return d.commit(key, read, next, contributes) }
}

var _ = ginkgo.Describe("Derive commit-if-current", func() {
	ginkgo.It("should not commit a recompute from an entry the source has replaced", func() {
		nodeClaims, s, d := newStores()
		key := types.NamespacedName{Name: "a"}
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		commit := staleRead(d, key)
		observe(nodeClaims, nodeClaim("a", "y", 2, "4"))
		Expect(commit()).To(BeFalse(), "a commit from the replaced entry, want it refused")
		Expect(pooled(s)).To(Equal(map[string]poolFields{"y": {CPU: 4, Count: 1}}))
	})
	ginkgo.It("should not commit a recompute from before the source's removal", func() {
		nodeClaims, s, d := newStores()
		key := types.NamespacedName{Name: "a"}
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		commit := staleRead(d, key)
		forget(nodeClaims, key)
		Expect(commit()).To(BeFalse(), "a commit from the removed entry, want it refused")
		Expect(pooled(s)).To(BeEmpty(), "want the removal to hold")
	})
	ginkgo.It("should apply a key that drops and reappears in a derived store, and refuse a recompute from before it dropped", func() {
		nodeClaims, pools, _ := newStores()
		totals := newPools()
		d := Derive(pools.Reader, totals, func(string, Entry[string, poolFields, struct{}]) string { return "all" },
			func(_ string, e Entry[string, poolFields, struct{}]) usage {
				return usage{CPU: e.Derived.CPU, Count: 1}
			})
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		commit := staleRead(d, "x")
		forget(nodeClaims, types.NamespacedName{Name: "a"})
		_, ok := pools.Get("x")
		Expect(ok).To(BeFalse(), "x once nothing contributes to it")
		observe(nodeClaims, nodeClaim("b", "x", 2, "5"))
		Expect(commit()).To(BeFalse(), "a commit from before x dropped, want it refused")
		Expect(pooled(totals)).To(Equal(map[string]poolFields{"all": {CPU: 5, Count: 1}}),
			"the totals once x reappears, want its new entry, not the recompute from before it dropped")
	})
})
