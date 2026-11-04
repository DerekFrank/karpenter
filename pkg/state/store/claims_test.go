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

package store

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

const cpuLimit = 40

// newCPUClaims holds the largest CPU a NodeClaim might launch with, until it launches with its actual CPU.
func newCPUClaims(clk *clocktesting.FakeClock) (*pools, *Contributor[string, *v1.NodeClaim, poolFields, usage], *Claims[string, *v1.NodeClaim, poolFields, usage, int64]) {
	s, _, nodeClaims := newStores()
	claims := NewClaims(nodeClaims,
		func(f poolFields, prev, next int64) poolFields { f.Reserved += int(next - prev); return f },
		func(u usage) bool { return u.CPU > 0 },
		time.Minute, clk)
	return s, nodeClaims, claims
}

func fitsCPU(f poolFields) bool { return f.CPU+int64(f.Reserved) <= cpuLimit }

func named(name string) types.NamespacedName { return types.NamespacedName{Name: name} }

func TestClaimsAdmitUpToTheLimitCountingWhatIsHeld(t *testing.T) {
	s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "16")))

	if got := claims.Reserve("x", 10, 4, fitsCPU); len(got) != 6 {
		t.Fatalf("Reserve granted %d claims of 4 against 24 free, want 6", len(got))
	}
	if got := claims.Reserve("x", 1, 1, fitsCPU); len(got) != 0 {
		t.Fatalf("Reserve granted %d claims at the limit, want 0", len(got))
	}
	if f := fields(s, "x"); f.CPU != 16 || f.Reserved != 24 {
		t.Fatalf("fields = %d counted and %d held, want 16 and 24", f.CPU, f.Reserved)
	}
}

func TestClaimsSettleToWhatTheObjectCounts(t *testing.T) {
	s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
	claim := claims.Reserve("x", 1, 4, fitsCPU)[0]
	claim.Bind(named("a"))

	// Created, not launched: the contribution doesn't count what the claim holds, so the claim keeps holding it.
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "0")))
	if f := fields(s, "x"); f.CPU != 0 || f.Reserved != 4 {
		t.Fatalf("before launch, fields = %d counted and %d held, want 0 and 4", f.CPU, f.Reserved)
	}
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 2, "2")))
	if f := fields(s, "x"); f.CPU != 2 || f.Reserved != 0 {
		t.Fatalf("after launch, fields = %d counted and %d held, want 2 and 0", f.CPU, f.Reserved)
	}
}

func TestClaimsReleaseAtOnceWhenBoundToAnObjectAlreadyCounted(t *testing.T) {
	s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
	claim := claims.Reserve("x", 1, 4, fitsCPU)[0]
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "4")))
	if f := fields(s, "x"); f.CPU != 4 || f.Reserved != 4 {
		t.Fatalf("counted before bound, fields = %d counted and %d held, want 4 and 4", f.CPU, f.Reserved)
	}
	claim.Bind(named("a"))
	if f := fields(s, "x"); f.Reserved != 0 {
		t.Fatalf("bound to a counted object, %d held, want 0", f.Reserved)
	}
}

func TestClaimsReleaseWhenTheirObjectGoesAwayBeforeItCounts(t *testing.T) {
	s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
	for name, gone := range map[string]func(){
		"a": func() { nodeClaims.Forget(named("a")) },
		"b": func() {
			must(t, nodeClaims.change(ctx, nodeClaim("b", "x", 2, "0"), true, true))
		},
		"c": func() { must(t, nodeClaims.Replace(ctx, "x", nil)) },
	} {
		claim := claims.Reserve("x", 1, 4, fitsCPU)[0]
		claim.Bind(named(name))
		must(t, nodeClaims.Apply(ctx, nodeClaim(name, "x", 1, "0")))
		gone()
		if f := fields(s, "x"); f.Reserved != 0 {
			t.Fatalf("%s gone before launch, %d held, want 0", name, f.Reserved)
		}
	}
}

func TestClaimsExpire(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	s, _, claims := newCPUClaims(clk)
	claims.Reserve("x", 1, 20, fitsCPU)
	claims.Reserve("x", 1, 20, fitsCPU)[0].Bind(named("never-created"))
	if got := claims.Reserve("x", 1, 1, fitsCPU); len(got) != 0 {
		t.Fatal("Reserve granted a claim at the limit")
	}
	clk.Step(time.Minute)
	if got := claims.Reserve("x", 2, 20, fitsCPU); len(got) != 2 {
		t.Fatalf("Reserve granted %d claims once the others expired, want 2", len(got))
	}
	if f := fields(s, "x"); f.Reserved != 40 {
		t.Fatalf("%d held, want 40", f.Reserved)
	}
}

func TestClaimsReleaseOnce(t *testing.T) {
	s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
	got := claims.Reserve("x", 2, 4, fitsCPU)
	got[0].Release()
	got[0].Release()
	got[0].Bind(named("a"))
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "4")))
	// A second claim bound to the same object replaces the first.
	other := claims.Reserve("x", 1, 4, fitsCPU)[0]
	got[1].Bind(named("b"))
	other.Bind(named("b"))
	if f := fields(s, "x"); f.Reserved != 4 {
		t.Fatalf("%d held, want 4", f.Reserved)
	}
	got[1].Release()
	other.Release()
	if f := fields(s, "x"); f.Reserved != 0 || f.CPU != 4 {
		t.Fatalf("fields = %d counted and %d held, want 4 and 0", f.CPU, f.Reserved)
	}
}

// TestClaimsNeverExceedTheLimitOrCountAnObjectZeroTimes races reservers that create, launch and delete NodeClaims of
// random sizes against the limit, and checks after each step that the store matches a recount: what launched objects
// count plus what live claims hold, and never more than the limit.
func TestClaimsNeverExceedTheLimitOrCountAnObjectZeroTimes(t *testing.T) {
	s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
	var mu sync.Mutex // serializes a step with its check
	held, launched := map[string]int64{}, map[string]int64{}
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
			t.Errorf("after %s, fields = %d counted and %d held, want %d and %d within %d", step, got.CPU, got.Reserved, want.CPU, want.Reserved, cpuLimit)
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
				got[0].Bind(named(name))
				must(t, nodeClaims.Apply(ctx, nodeClaim(name, "x", 1, "0")))
				check("create " + name)
				mu.Unlock()

				mu.Lock()
				cpu := int64(1 + r.IntN(4))
				must(t, nodeClaims.Apply(ctx, nodeClaim(name, "x", 2, fmt.Sprint(cpu))))
				delete(held, name)
				launched[name] = cpu
				check("launch " + name)
				mu.Unlock()

				mu.Lock()
				nodeClaims.Forget(named(name))
				delete(launched, name)
				check("delete " + name)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
}

// TestClaimsAdmitAtomicallyUnderConcurrency races reservers without serializing them, each counting its NodeClaim before
// binding its claim, as a create through the overlay does, and checks that what the objects count never exceeds the
// limit. What is counted and held together can, while an object counts before its claim is bound.
func TestClaimsAdmitAtomicallyUnderConcurrency(t *testing.T) {
	s, nodeClaims, claims := newCPUClaims(clocktesting.NewFakeClock(time.Now()))
	s.Subscribe(func(_ string, _, f poolFields) {
		if f.CPU > cpuLimit {
			t.Errorf("%d counted, over %d", f.CPU, cpuLimit)
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
				must(t, nodeClaims.Apply(ctx, nodeClaim(name, "x", 1, "3")))
				got[0].Bind(named(name))
				nodeClaims.Forget(named(name))
			}
		})
	}
	wg.Wait()
	if f := fields(s, "x"); f.CPU != 0 || f.Reserved != 0 {
		t.Fatalf("fields = %d counted and %d held after every NodeClaim went away, want 0 and 0", f.CPU, f.Reserved)
	}
}
