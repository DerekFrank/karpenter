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
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

// poolsKey is the annotation that lists the NodePools a NodeClaim contributes to in the DeriveEach specs.
const poolsKey = "pools"

// spread returns a NodeClaim that contributes to each of pools.
func spread(name string, rv uint64, cpu string, pools ...string) *v1.NodeClaim {
	nc := nodeClaim(name, "", rv, cpu)
	nc.Annotations = map[string]string{poolsKey: strings.Join(pools, ",")}
	return nc
}

func poolsOf(_ types.NamespacedName, e nodeClaimEntry) []string {
	return strings.Split(e.Object.Annotations[poolsKey], ",")
}

func usageAt(k types.NamespacedName, e nodeClaimEntry, _ string) usage { return usageOf(k, e) }

func newEachStores() (*nodeClaimStore, *pools, *EachDerivation[types.NamespacedName, string, poolFields, usage]) {
	nodeClaims, s := newNodeClaims(), newPools()
	return nodeClaims, s, DeriveEach(nodeClaims.Reader, s, poolsOf, usageAt)
}

func names(keys []types.NamespacedName) []string {
	return lo.Map(keys, func(k types.NamespacedName, _ int) string { return k.Name })
}

var _ = ginkgo.Describe("DeriveEach", func() {
	ginkgo.It("should dispatch each key as the source joins, leaves or stays", func() {
		nodeClaims := newNodeClaims()
		s := NewSyntheticStore[string, string, dispatches, struct{}](func(k string) string { return k })
		DeriveEach(nodeClaims.Reader, s, poolsOf, func(_ types.NamespacedName, e nodeClaimEntry, _ string) counted {
			return counted{Name: e.Object.Name, CPU: e.Object.Status.Capacity.Cpu().Value()}
		})
		notes := map[string]int{}
		s.Subscribe(func(c Change[string, string, dispatches, struct{}]) { notes[c.Key]++ })
		nodeClaims.Observe(spread("a", 1, "2", "x", "y"))
		Expect(fields(s, "x")).To(Equal(dispatches{Updates: 1, CPU: 2}), "x, new to a's set: want one Update")
		Expect(fields(s, "y")).To(Equal(dispatches{Updates: 1, CPU: 2}), "y, new to a's set: want one Update")
		before := notes["y"]
		nodeClaims.Observe(spread("a", 2, "2", "y", "z"))
		_, ok := s.Get("x")
		Expect(ok).To(BeFalse(), "x once it left a's set, want its Delete to drop it")
		Expect(fields(s, "y")).To(Equal(dispatches{Updates: 1, CPU: 2}), "y, staying with an equal contribution: want no call")
		Expect(notes["y"]).To(Equal(before), "notes of y, staying with an equal contribution: want none")
		Expect(fields(s, "z")).To(Equal(dispatches{Updates: 1, CPU: 2}), "z, new to a's set: want one Update")
		nodeClaims.Observe(spread("a", 3, "4", "y", "z"))
		Expect(fields(s, "y")).To(Equal(dispatches{Updates: 2, CPU: 4}), "y, staying with a changed contribution: want an Update from prev")
		Expect(fields(s, "z")).To(Equal(dispatches{Updates: 2, CPU: 4}), "z, staying with a changed contribution: want an Update from prev")
		nodeClaims.Observe(spread("a", 4, "4", "y", "y", ""))
		Expect(fields(s, "y")).To(Equal(dispatches{Updates: 2, CPU: 4}), "y, repeated in a's set: want it counted once")
		_, ok = s.Get("z")
		Expect(ok).To(BeFalse(), "z once it left a's set")
		Forget(nodeClaims, types.NamespacedName{Name: "a"})
		_, ok = s.Get("y")
		Expect(ok).To(BeFalse(), "y once a was forgotten")
	})
	ginkgo.It("should list a source under each key of its set", func() {
		nodeClaims, s, d := newEachStores()
		nodeClaims.Observe(spread("a", 1, "2", "x", "y"))
		nodeClaims.Observe(spread("b", 2, "4", "y"))
		Expect(names(d.Sources("x"))).To(ConsistOf("a"), "sources of x")
		Expect(names(d.Sources("y"))).To(ConsistOf("a", "b"), "sources of y, want a under it as well as x")
		Expect(fields(s, "y")).To(cpuAndCount(6, 2), "y, want a's 2 and b's 4 cpu")
		nodeClaims.Observe(spread("a", 3, "2", "x"))
		Expect(names(d.Sources("y"))).To(ConsistOf("b"), "sources of y once it left a's set")
		Forget(nodeClaims, types.NamespacedName{Name: "a"})
		Expect(d.Sources("x")).To(BeEmpty(), "sources of x once a was forgotten")
	})
	ginkgo.It("should apply all the keys of a change under one lock", func() {
		nodeClaims, s, _ := newEachStores()
		var stop atomic.Bool
		var reads, torn atomic.Int64
		var wg sync.WaitGroup
		wg.Go(func() {
			for !stop.Load() {
				// A reader that serializes with the target's lock, as Sources and Claims do.
				s.s.mu.Lock()
				x, y := fields(s, "x"), fields(s, "y")
				s.s.mu.Unlock()
				if x != y {
					torn.Add(1)
				}
				reads.Add(1)
			}
		})
		for w := range 4 {
			wg.Go(func() {
				for i := range 500 {
					pools := lo.Ternary(i%3 == 0, []string{}, []string{"x", "y"})
					nodeClaims.s.change(spread(fmt.Sprintf("nc-%d", i%20), uint64(i*4+w+1), fmt.Sprint(1+i%5), pools...), false, copied) //nolint:gosec
				}
			})
		}
		Eventually(reads.Load).Should(BeNumerically(">", 100))
		stop.Store(true)
		wg.Wait()
		Expect(torn.Load()).To(BeZero(), "reads that saw x and y apart, want none: every change moves both")
		Expect(fields(s, "x")).To(Equal(fields(s, "y")), "x and y once the writers finished")
	})
	ginkgo.It("should notify a key's change once all of the change's keys are applied", func() {
		nodeClaims, s, d := newEachStores()
		var sawY []string
		s.Subscribe(func(c Change[string, string, poolFields, struct{}]) {
			if c.Key == "x" {
				sawY = names(d.Sources("y"))
			}
		})
		nodeClaims.Observe(spread("a", 1, "2", "x", "y"))
		Expect(sawY).To(ConsistOf("a"), "sources of y when x's change was notified")
	})
	ginkgo.It("should leave its contributions once stopped, and take them back once removed", func() {
		nodeClaims, s, d := newEachStores()
		nodeClaims.Observe(spread("a", 1, "2", "x", "y"))
		d.Stop()
		nodeClaims.Observe(spread("a", 2, "8", "x", "y"))
		Expect(fields(s, "x")).To(cpuAndCount(2, 1), "x once stopped, want what it held")
		for range 10 {
			DeriveEach(nodeClaims.Reader, s, poolsOf, usageAt).Remove()
		}
		Expect(fields(s, "y")).To(cpuAndCount(2, 1), "y once ten DeriveEachs were built and removed, want the stopped one's")
		d.Remove()
		_, ok := s.Get("x")
		Expect(ok).To(BeFalse(), "x once the stopped DeriveEach was removed")
		s.s.mu.Lock()
		defer s.s.mu.Unlock()
		Expect(s.s.sources).To(BeEmpty(), "sources once every DeriveEach was removed")
		Expect(s.s.refs).To(BeEmpty(), "refs once every DeriveEach was removed")
	})
	ginkgo.It("should apply what its source held before it", func() {
		nodeClaims := newNodeClaims()
		nodeClaims.Observe(spread("a", 1, "2", "x", "y"))
		s := newPools()
		d := DeriveEach(nodeClaims.Reader, s, poolsOf, usageAt)
		Expect(d.Synced()).To(BeTrue(), "synced once its sweep finished over a synced source")
		Expect(fields(s, "x")).To(cpuAndCount(2, 1), "x, want the NodeClaim the source held before DeriveEach")
		Expect(fields(s, "y")).To(cpuAndCount(2, 1), "y, want the NodeClaim the source held before DeriveEach")
	})
	ginkgo.It("should converge under concurrent changes to a recount", func() {
		nodeClaims, s, d := newEachStores()
		var wg sync.WaitGroup
		for w := range 4 {
			wg.Go(func() {
				for i := range 1000 {
					// Each NodeClaim's set is a run of the four pools that moves with i and w.
					pools := lo.Map(lo.Range(i%4), func(j, _ int) string { return fmt.Sprintf("pool-%d", (i+w+j)%4) })
					nodeClaims.s.change(spread(fmt.Sprintf("nc-%d", i%50), uint64(i*4+w+1), fmt.Sprint(1+w), pools...), false, lo.Ternary(w%2 == 0, cached, copied)) //nolint:gosec
				}
			})
		}
		wg.Wait()
		want, sources := map[string]poolFields{}, map[string][]string{}
		for k, e := range nodeClaims.List() {
			for _, pool := range lo.Uniq(lo.Compact(poolsOf(k, e))) {
				u := usageOf(k, e)
				want[pool] = poolFields{CPU: want[pool].CPU + u.CPU, Count: want[pool].Count + u.Count}
				sources[pool] = append(sources[pool], k.Name)
			}
		}
		for i := range 4 {
			pool := fmt.Sprintf("pool-%d", i)
			Expect(fields(s, pool)).To(Equal(want[pool]), "%s, want the recount of the NodeClaims' final sets", pool)
			Expect(names(d.Sources(pool))).To(ConsistOf(sources[pool]), "sources of %s, want the recount's", pool)
		}
	})
	ginkgo.It("should keep the laws per key", func() {
		statetest.ExpectContributionEach(ginkgo.GinkgoT(), poolFields{CPU: 3, Count: 1},
			map[string]usage{"x": {CPU: 2, Count: 1}, "y": {CPU: 2, Count: 1}},
			map[string]usage{"y": {CPU: 8, Count: 1}, "z": {CPU: 8, Count: 1}})
	})
})

// BenchmarkDeriveOneKey compares Derive with DeriveEach of a one-key set, for a NodeClaim whose capacity changes.
func BenchmarkDeriveOneKey(b *testing.B) {
	for _, each := range []bool{false, true} {
		b.Run(lo.Ternary(each, "DeriveEach", "Derive"), func(b *testing.B) {
			nodeClaims, s := newNodeClaims(), newPools()
			if each {
				DeriveEach(nodeClaims.Reader, s, func(k types.NamespacedName, e nodeClaimEntry) []string { return []string{poolOf(k, e)} }, usageAt)
			} else {
				Derive(nodeClaims.Reader, s, poolOf, usageOf)
			}
			ncs := make([]*v1.NodeClaim, 2000)
			for i := range ncs {
				ncs[i] = nodeClaim(fmt.Sprintf("nc-%d", i), fmt.Sprintf("pool-%d", i%10), 0, fmt.Sprint(1+i%2))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				nc := ncs[i%len(ncs)].DeepCopy()
				nc.ResourceVersion = fmt.Sprint(i + 1)
				nc.Status.Capacity[corev1.ResourceCPU] = resource.MustParse(fmt.Sprint(1 + i%3))
				nodeClaims.Observe(nc)
			}
		})
	}
}
