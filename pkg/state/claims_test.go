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
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

const cpuLimit = 40

// newCPUClaims holds the largest CPU a NodeClaim might launch with, until it launches with its actual CPU.
func newCPUClaims(clk *clocktesting.FakeClock) (*pools, *nodeClaimStore, *Claims[types.NamespacedName, string, poolFields, usage, heldCPU]) {
	nodeClaims, s, derivation := newStores()
	claims := NewClaims[types.NamespacedName, string, poolFields, usage, heldCPU](derivation,
		func(u usage) bool { return u.CPU > 0 },
		time.Minute, clk)
	return s, nodeClaims, claims
}

// heldCPU is CPU a claim holds against a NodePool.
type heldCPU int

func (h heldCPU) Update(f poolFields, prev heldCPU) poolFields { f.Reserved += int(h - prev); return f }
func (h heldCPU) Delete(f poolFields) poolFields               { f.Reserved -= int(h); return f }

func fitsCPU(f poolFields) bool { return f.CPU+int64(f.Reserved) <= cpuLimit }

func key(name string) types.NamespacedName { return types.NamespacedName{Name: name} }

// countedAndHeld matches poolFields with cpu counted and reserved held, whatever their count.
func countedAndHeld(cpu int64, reserved int) OmegaMatcher {
	return And(HaveField("CPU", cpu), HaveField("Reserved", reserved))
}

var _ = ginkgo.Describe("Claims", func() {
	ginkgo.It("should keep the contribution laws for held CPU", func() {
		statetest.ExpectContribution(ginkgo.GinkgoT(), poolFields{Reserved: 3}, heldCPU(2), heldCPU(5))
	})
	ginkgo.It("should admit up to the limit, counting what is held", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		observe(nodeClaims, nodeClaim("a", "x", 1, "16"))

		Expect(claims.Reserve("x", 10, 4, fitsCPU)).To(HaveLen(6), "claims of 4 granted against 24 free")
		Expect(claims.Reserve("x", 1, 1, fitsCPU)).To(BeEmpty(), "claims granted at the limit")
		Expect(fields(s, "x")).To(countedAndHeld(16, 24))
	})
	ginkgo.It("should settle to what the object counts", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		claim := claims.Reserve("x", 1, 4, fitsCPU)[0]
		claim.Bind(key("a"))

		// Created, not launched: the contribution doesn't count what the claim holds, so the claim keeps holding it.
		observe(nodeClaims, nodeClaim("a", "x", 1, "0"))
		Expect(fields(s, "x")).To(countedAndHeld(0, 4), "before launch")
		observe(nodeClaims, nodeClaim("a", "x", 2, "2"))
		Expect(fields(s, "x")).To(countedAndHeld(2, 0), "after launch")
	})
	ginkgo.It("should admit replacements against what the sources they replace contribute, once", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		observe(nodeClaims, nodeClaim("a", "x", 1, "30"))
		observe(nodeClaims, nodeClaim("b", "y", 1, "10"))
		Expect(claims.Reserve("x", 1, 20, fitsCPU)).To(BeEmpty(), "granted 20 against 30 of 40 counted")
		// b counts toward another key, and a named twice is credited once.
		Expect(claims.ReserveReplacing("x", 1, 40, []types.NamespacedName{key("a"), key("a"), key("b")}, fitsCPU)).To(HaveLen(1))
		Expect(claims.ReserveReplacing("x", 1, 1, []types.NamespacedName{key("a"), key("a")}, fitsCPU)).To(BeEmpty(), "granted over the limit")
		Expect(fields(s, "x")).To(countedAndHeld(30, 40), "the credit changed the fields")
	})
	ginkgo.It("should settle each Claims against one Derivation by its own counts", func() {
		clk := clocktesting.NewFakeClock(time.Now())
		s, nodeClaims, cpuClaims := newCPUClaims(clk)
		// A second Claims against the same Derivation, counted as soon as the object exists.
		countClaims := NewClaims[types.NamespacedName, string, poolFields, usage, heldCPU](cpuClaims.derivation,
			func(usage) bool { return true }, time.Minute, clk)
		cpuClaims.Reserve("x", 1, 4, fitsCPU)[0].Bind(key("a"))
		countClaims.Reserve("x", 1, 1, fitsCPU)[0].Bind(key("a"))
		Expect(fields(s, "x").Reserved).To(Equal(5))
		observe(nodeClaims, nodeClaim("a", "x", 1, "0"))
		Expect(fields(s, "x").Reserved).To(Equal(4), "held once created, before launch")
		observe(nodeClaims, nodeClaim("a", "x", 2, "2"))
		Expect(fields(s, "x")).To(countedAndHeld(2, 0), "after launch")
	})
	ginkgo.It("should release and grant nothing once their Derivation is removed", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		observe(nodeClaims, nodeClaim("a", "x", 1, "4"))
		claims.Reserve("x", 1, 4, fitsCPU)[0].Bind(key("b"))
		claims.Reserve("x", 1, 4, fitsCPU)
		claims.derivation.Remove()
		Expect(fields(s, "x")).To(countedAndHeld(0, 0), "fields once the Derivation is removed")
		_, exists := s.Get("x")
		Expect(exists).To(BeFalse(), "x is held once nothing contributes")
		Expect(claims.Reserve("x", 1, 4, fitsCPU)).To(BeEmpty(), "granted against a removed Derivation")
		observe(nodeClaims, nodeClaim("b", "x", 1, "4"))
		Expect(fields(s, "x")).To(countedAndHeld(0, 0), "a removed Derivation counted a new source")
	})
	ginkgo.It("should release at once when bound to an object already counted", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		claim := claims.Reserve("x", 1, 4, fitsCPU)[0]
		observe(nodeClaims, nodeClaim("a", "x", 1, "4"))
		Expect(fields(s, "x")).To(countedAndHeld(4, 4), "counted before bound")
		claim.Bind(key("a"))
		Expect(fields(s, "x").Reserved).To(Equal(0), "held once bound to a counted object")
	})
	// The ways an object goes away run in turn against the same stores, so each also starts from what the last left.
	ginkgo.It("should release when their object goes away before it counts", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		for name, gone := range map[string]func(){
			"a": func() { forget(nodeClaims, types.NamespacedName{Name: "a"}) },
			"b": func() {
				observe(nodeClaims, nodeClaim("b", "x", 2, "0"))
				forget(nodeClaims, types.NamespacedName{Name: "b"})
			},
			"c": func() { resync(nodeClaims, []*v1.NodeClaim(nil), claims.derivation.Sources("x")) },
		} {
			claim := claims.Reserve("x", 1, 4, fitsCPU)[0]
			claim.Bind(key(name))
			observe(nodeClaims, nodeClaim(name, "x", 1, "0"))
			gone()
			Expect(fields(s, "x").Reserved).To(Equal(0), "held once %s is gone before launch", name)
		}
	})
	ginkgo.It("should expire at its TTL without a read, and notify the derived store's subscribers", func() {
		clk := clocktesting.NewFakeClock(time.Now())
		s, _, claims := newCPUClaims(clk)
		reserved := make(chan int, 10)
		s.Subscribe(func(c Change[string, string, poolFields, struct{}]) { reserved <- c.New.Derived.Reserved })
		claims.Reserve("x", 1, 20, fitsCPU)
		Expect(reserved).To(Receive(Equal(20)))
		clk.Step(time.Minute)
		Eventually(reserved).Should(Receive(Equal(0)), "subscribers told of the expiry")
		Expect(fields(s, "x").Reserved).To(Equal(0))
	})
	ginkgo.It("should expire unbound, or bound to a source the Derivation never saw, at their TTL and say so", func() {
		clk := clocktesting.NewFakeClock(time.Now())
		s, _, claims := newCPUClaims(clk)
		var mu sync.Mutex
		var expired []heldCPU
		claims.OnExpire(func(_ string, held heldCPU) { mu.Lock(); defer mu.Unlock(); expired = append(expired, held) })
		claims.Reserve("x", 1, 20, fitsCPU)
		claims.Reserve("x", 1, 20, fitsCPU)[0].Bind(key("never-created"))
		Expect(claims.Reserve("x", 1, 1, fitsCPU)).To(BeEmpty(), "Reserve granted a claim at the limit")
		clk.Step(time.Minute - time.Nanosecond)
		Consistently(func() int { return fields(s, "x").Reserved }, 100*time.Millisecond).Should(Equal(40), "expired before their TTL")
		clk.Step(time.Nanosecond)
		Eventually(func() int { return fields(s, "x").Reserved }).Should(BeZero(), "held past their TTL")
		Expect(claims.Reserve("x", 2, 20, fitsCPU)).To(HaveLen(2), "claims granted once the others expired")
		mu.Lock()
		defer mu.Unlock()
		Expect(expired).To(Equal([]heldCPU{20, 20}))
	})
	ginkgo.It("should arm one timer at the earliest expiry, and stop it once nothing can expire or the Derivation stops", func() {
		clk := clocktesting.NewFakeClock(time.Now())
		_, _, claims := newCPUClaims(clk)
		first := claims.Reserve("x", 1, 1, fitsCPU)
		clk.Step(time.Second)
		second := claims.Reserve("x", 1, 1, fitsCPU)
		Expect(clk.Waiters()).To(Equal(1), "armed a timer per claim")
		first[0].Release()
		Expect(clk.HasWaiters()).To(BeTrue(), "stopped the timer while a claim can expire")
		second[0].Release()
		Expect(clk.HasWaiters()).To(BeFalse(), "kept a timer once no claim can expire")

		claims.Reserve("x", 1, 1, fitsCPU)
		claims.derivation.Stop()
		Expect(clk.HasWaiters()).To(BeFalse(), "kept a timer once the Derivation stopped")
		claims.Reserve("x", 1, 1, fitsCPU)
		Expect(clk.HasWaiters()).To(BeFalse(), "armed a timer after the Derivation stopped")
	})
	ginkgo.It("should never expire a claim bound to a source the Derivation sees, until the source counts it or goes", func() {
		clk := clocktesting.NewFakeClock(time.Now())
		s, nodeClaims, claims := newCPUClaims(clk)
		var mu sync.Mutex
		var expired []heldCPU
		claims.OnExpire(func(_ string, held heldCPU) { mu.Lock(); defer mu.Unlock(); expired = append(expired, held) })
		// Seen before it's bound, and bound before it's seen.
		observe(nodeClaims, nodeClaim("a", "x", 1, "0"))
		claims.Reserve("x", 1, 4, fitsCPU)[0].Bind(key("a"))
		claims.Reserve("x", 1, 6, fitsCPU)[0].Bind(key("b"))
		observe(nodeClaims, nodeClaim("b", "x", 1, "0"))
		// An unbound claim expires when the timer fires, which ends every claim due in one pass.
		claims.Reserve("x", 1, 1, fitsCPU)
		clk.Step(time.Hour)
		Eventually(func() []heldCPU { mu.Lock(); defer mu.Unlock(); return slices.Clone(expired) }).Should(Equal([]heldCPU{1}))
		Expect(fields(s, "x").Reserved).To(Equal(10), "expired claims whose sources haven't launched")

		observe(nodeClaims, nodeClaim("a", "x", 2, "2"))
		forget(nodeClaims, types.NamespacedName{Name: "b"})
		Expect(fields(s, "x")).To(countedAndHeld(2, 0), "once a launched and b went")
		mu.Lock()
		defer mu.Unlock()
		Expect(expired).To(Equal([]heldCPU{1}))
	})
	ginkgo.It("should release once", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		got := claims.Reserve("x", 2, 4, fitsCPU)
		got[0].Release()
		got[0].Release()
		got[0].Bind(key("a"))
		observe(nodeClaims, nodeClaim("a", "x", 1, "4"))
		// A second claim bound to the same object replaces the first.
		other := claims.Reserve("x", 1, 4, fitsCPU)[0]
		got[1].Bind(key("b"))
		other.Bind(key("b"))
		Expect(fields(s, "x").Reserved).To(Equal(4))
		got[1].Release()
		other.Release()
		Expect(fields(s, "x")).To(countedAndHeld(4, 0))
	})
	// Reservers that create, launch and delete NodeClaims of random sizes race against the limit, and after each step
	// the store matches a recount: what launched objects count plus what live claims hold, and never more than the limit.
	ginkgo.It("should never exceed the limit or count an object zero times", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		var mu sync.Mutex // serializes a step with its check
		held, launched := map[string]int64{}, map[string]int64{}
		var violations []string
		check := func(step string) {
			var want poolFields
			for _, cpu := range launched {
				want.CPU += cpu
			}
			for _, h := range held {
				want.Reserved += int(h)
			}
			got := fields(s, "x")
			if got.CPU != want.CPU || got.Reserved != want.Reserved || !fitsCPU(got) {
				violations = append(violations, fmt.Sprintf("after %s, fields = %d counted and %d held, want %d and %d within %d", step, got.CPU, got.Reserved, want.CPU, want.Reserved, cpuLimit))
			}
		}
		var wg sync.WaitGroup
		for w := range 16 {
			wg.Go(func() {
				r := rand.New(rand.NewPCG(uint64(w), 0)) //nolint:gosec // a seeded source makes failures reproducible
				for i := range 200 {
					name := fmt.Sprintf("%d-%d", w, i)
					mu.Lock()
					got := claims.Reserve("x", 1, 4, fitsCPU)
					if len(got) == 1 {
						held[name] = 4
					}
					check("reserve " + name)
					mu.Unlock()
					if len(got) == 0 {
						continue
					}
					mu.Lock()
					if r.IntN(5) == 0 {
						got[0].Release()
						delete(held, name)
						check("release " + name)
						mu.Unlock()
						continue
					}
					got[0].Bind(key(name))
					observe(nodeClaims, nodeClaim(name, "x", 1, "0"))
					check("create " + name)
					mu.Unlock()

					mu.Lock()
					cpu := int64(1 + r.IntN(4))
					observe(nodeClaims, nodeClaim(name, "x", 2, fmt.Sprint(cpu)))
					delete(held, name)
					launched[name] = cpu
					check("launch " + name)
					mu.Unlock()

					mu.Lock()
					forget(nodeClaims, types.NamespacedName{Name: name})
					delete(launched, name)
					check("delete " + name)
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		Expect(violations).To(BeEmpty())
	})
	// Reservers race without serializing, each counting its NodeClaim before binding its claim, as a create through the
	// overlay does, and what the objects count never exceeds the limit. What is counted and held together can, while an
	// object counts before its claim is bound.
	ginkgo.It("should admit atomically under concurrency", func() {
		s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
		var mu sync.Mutex
		var violations []string
		s.Subscribe(func(c Change[string, string, poolFields, struct{}]) {
			if f := c.New.Derived; f.CPU > cpuLimit {
				mu.Lock()
				defer mu.Unlock()
				violations = append(violations, fmt.Sprintf("%d counted, over %d", f.CPU, cpuLimit))
			}
		})
		var wg sync.WaitGroup
		for w := range 32 {
			wg.Go(func() {
				for i := range 100 {
					name := fmt.Sprintf("%d-%d", w, i)
					got := claims.Reserve("x", 1, 4, fitsCPU)
					if len(got) == 0 {
						continue
					}
					observe(nodeClaims, nodeClaim(name, "x", 1, "3"))
					got[0].Bind(key(name))
					forget(nodeClaims, types.NamespacedName{Name: name})
				}
			})
		}
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		Expect(violations).To(BeEmpty())
		Expect(fields(s, "x")).To(countedAndHeld(0, 0), "fields after every NodeClaim went away")
	})
})
