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

// staleRead reads key as an apply does, and returns the commit of that read, for a spec to run later.
func staleRead[K1, K2 comparable, D2 any, C Contribution[D2, C]](d *Derivation[K1, K2, D2, C], key K1) func() {
	d.inflight.Add(1)
	next, exists := d.read(key)
	return func() { d.commit(key, next, exists) }
}

var _ = ginkgo.Describe("Derive versions", func() {
	ginkgo.It("should not let a recompute from an older version of the source overwrite a newer one", func() {
		nodeClaims, s, d := newStores()
		key := types.NamespacedName{Name: "a"}
		nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
		commit := staleRead(d, key)
		nodeClaims.Observe(nodeClaim("a", "y", 2, "4"))
		commit()
		Expect(pooled(s)).To(Equal(map[string]poolFields{"y": {CPU: 4, Count: 1}}), "want the stale recompute to lose")
	})
	ginkgo.It("should not let a recompute from before the source's removal outlive it", func() {
		nodeClaims, s, d := newStores()
		key := types.NamespacedName{Name: "a"}
		nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
		commit := staleRead(d, key)
		Forget(nodeClaims, key)
		commit()
		Expect(pooled(s)).To(BeEmpty(), "want the removal to hold")
		Expect(d.records).To(BeEmpty(), "the records once no apply is in flight, want the removal unburied")
		Expect(d.buried).To(BeEmpty())
	})
	ginkgo.It("should apply a key that drops and reappears in a derived store, and drop a recompute from before it dropped", func() {
		nodeClaims, pools, _ := newStores()
		totals := newPools()
		d := Derive(pools.Reader, totals, func(string, Entry[string, poolFields, struct{}]) string { return "all" },
			func(_ string, e Entry[string, poolFields, struct{}]) usage {
				return usage{CPU: e.Derived.CPU, Count: 1}
			})
		nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
		commit := staleRead(d, "x")
		Forget(nodeClaims, types.NamespacedName{Name: "a"})
		_, ok := pools.Get("x")
		Expect(ok).To(BeFalse(), "x once nothing contributes to it")
		nodeClaims.Observe(nodeClaim("b", "x", 2, "5"))
		commit()
		Expect(pooled(totals)).To(Equal(map[string]poolFields{"all": {CPU: 5, Count: 1}}),
			"the totals once x reappears, want its new incarnation, not the recompute from before it dropped")
	})
})
