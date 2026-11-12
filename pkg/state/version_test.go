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

var _ = ginkgo.Describe("Derive versions", func() {
	ginkgo.It("should not let a recompute from an older version of the source overwrite a newer one", func() {
		nodeClaims, s, d := newStores()
		key := types.NamespacedName{Name: "a"}
		nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
		version, exists, next, contributes := d.read(key)
		nodeClaims.Observe(nodeClaim("a", "y", 2, "4"))
		d.commit(key, version, exists, next, contributes)
		Expect(pooled(s)).To(Equal(map[string]poolFields{"y": {CPU: 4, Count: 1}}), "want the stale recompute to lose")
	})
	ginkgo.It("should not let a recompute from before the source's removal outlive it", func() {
		nodeClaims, s, d := newStores()
		key := types.NamespacedName{Name: "a"}
		nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
		nodeClaims.Observe(nodeClaim("a", "x", 2, "4"))
		version, exists, next, contributes := d.read(key)
		Forget(nodeClaims, key)
		d.commit(key, version, exists, next, contributes)
		Expect(pooled(s)).To(BeEmpty(), "want the removal to hold")
	})
})
