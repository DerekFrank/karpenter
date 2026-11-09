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
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	realclock "k8s.io/utils/clock"
	clock "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

type poolFields struct {
	CPU      int64
	Count    int
	Reserved int
}

// usage is a NodeClaim's usage of its NodePool.
type usage struct {
	CPU   int64
	Count int
}

func (u usage) Update(f poolFields, prev usage) poolFields {
	f.CPU += u.CPU - prev.CPU
	f.Count += u.Count - prev.Count
	return f
}

func (u usage) Delete(f poolFields) poolFields {
	f.CPU -= u.CPU
	f.Count -= u.Count
	return f
}

// nodeClaimIntent is what the tests decide about a NodeClaim.
type nodeClaimIntent struct {
	Deletion   Intended[bool]
	Nomination Intended[bool]
}

type (
	nodeClaimEntry = Entry[*v1.NodeClaim, struct{}, nodeClaimIntent]
	nodeClaimStore = Store[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent]
	pools          = Store[string, string, poolFields, struct{}]
)

func nodeClaim(name, pool string, rv uint64, cpu string) *v1.NodeClaim {
	return &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             types.UID(name),
			ResourceVersion: strconv.FormatUint(rv, 10),
			Labels:          map[string]string{v1.NodePoolLabelKey: pool},
		},
		Status: v1.NodeClaimStatus{Capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
	}
}

func poolOf(_ types.NamespacedName, e nodeClaimEntry) string {
	return e.Object.Labels[v1.NodePoolLabelKey]
}

func usageOf(_ types.NamespacedName, e nodeClaimEntry) usage {
	return usage{CPU: e.Object.Status.Capacity.Cpu().Value(), Count: 1}
}

func newNodeClaims() *nodeClaimStore {
	return objectStore[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent]()
}

func newPools() *pools {
	return NewSyntheticStore[string, string, poolFields, struct{}](func(k string) string { return k })
}

// newStores returns an object store of NodeClaims, and a synthetic store of their usage of each NodePool derived from
// it.
func newStores() (*nodeClaimStore, *pools, *Derivation[types.NamespacedName, string, poolFields, usage]) {
	nodeClaims, s := newNodeClaims(), newPools()
	return nodeClaims, s, Derive(nodeClaims.Reader, s, poolOf, usageOf)
}

func fields[K comparable, O, D, I any](s *Store[K, O, D, I], key K) D {
	e, _ := s.Get(key)
	return e.Derived
}

// cpuAndCount matches poolFields with cpu and count, whatever they hold.
func cpuAndCount(cpu int64, count int) OmegaMatcher {
	return And(HaveField("CPU", cpu), HaveField("Count", count))
}

// newZeroStores returns NodeClaims that contribute zero fields to their NodePools.
func newZeroStores() (*nodeClaimStore, *pools) {
	nodeClaims, s := newNodeClaims(), newPools()
	Derive(nodeClaims.Reader, s, poolOf, func(types.NamespacedName, nodeClaimEntry) usage { return usage{} })
	return nodeClaims, s
}

type clusterFields struct{ CPU int64 }

type poolCPU int64

func (c poolCPU) Update(f clusterFields, prev poolCPU) clusterFields {
	f.CPU += int64(c - prev)
	return f
}
func (c poolCPU) Delete(f clusterFields) clusterFields { f.CPU -= int64(c); return f }

// counted counts its Update and Delete calls, to check how Derive dispatches.
type counted struct {
	Name string
	CPU  int64
}

type dispatches struct{ Updates, Deletes, CPU int64 }

func (c counted) Update(f dispatches, prev counted) dispatches {
	f.Updates++
	f.CPU += c.CPU - prev.CPU
	return f
}

func (c counted) Delete(f dispatches) dispatches {
	f.Deletes++
	f.CPU -= c.CPU
	return f
}

var _ = ginkgo.Describe("ObjectStore", func() {
	ginkgo.It("should hold the newest version by name", func() {
		nodeClaims, _, _ := newStores()
		observe(nodeClaims, nodeClaim("a", "x", 2, "2"))
		observe(nodeClaims, nodeClaim("a", "x", 1, "8"))
		e, _ := nodeClaims.Get(types.NamespacedName{Name: "a"})
		Expect(e.Object.ResourceVersion).To(Equal("2"), "a's version after an older version")
		forget(nodeClaims, types.NamespacedName{Name: "a"})
		_, ok := nodeClaims.Get(types.NamespacedName{Name: "a"})
		Expect(ok).To(BeFalse(), "a is still there once forgotten")
	})
	ginkgo.It("should replace an object recreated under its name when its delete is missed", func() {
		nodeClaims, s, _ := newStores()
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		recreated := nodeClaim("a", "y", 5, "4")
		recreated.UID = "a-2"
		observe(nodeClaims, recreated)
		_, ok := s.Get("x")
		Expect(ok).To(BeFalse(), "the replaced object still contributes to x")
		Expect(fields(s, "y")).To(cpuAndCount(4, 1), "y, want the recreated object's")
		observe(nodeClaims, nodeClaim("a", "x", 3, "2"))
		_, ok = s.Get("x")
		Expect(ok).To(BeFalse(), "a late version of the replaced object contributed again")
	})
	ginkgo.It("should hold Derived for a key until its object arrives", func() {
		nodeClaims := newNodeClaims()
		byName := objectStore[types.NamespacedName, *v1.NodeClaim, poolFields, struct{}]()
		Derive(nodeClaims.Reader, byName, func(_ types.NamespacedName, e nodeClaimEntry) types.NamespacedName {
			return types.NamespacedName{Name: e.Object.Labels[v1.NodePoolLabelKey]}
		}, usageOf)
		var notes int
		byName.Subscribe(func(Change[types.NamespacedName, *v1.NodeClaim, poolFields, struct{}]) { notes++ })
		observe(nodeClaims, nodeClaim("a", "b", 1, "2"))
		_, ok := byName.Get(types.NamespacedName{Name: "b"})
		Expect(ok).To(BeFalse(), "b's entry exists before its object")
		Expect(byName.List()).To(BeEmpty(), "List returned an entry without its object")
		Expect(notes).To(BeZero(), "an entry that doesn't exist notified")
		observe(byName, nodeClaim("b", "", 2, "8"))
		e, ok := byName.Get(types.NamespacedName{Name: "b"})
		Expect(ok).To(BeTrue(), "b's entry once its object arrived")
		Expect(e.Derived).To(cpuAndCount(2, 1), "b's Derived once its object arrived, want what waited for it")
		Expect(notes).To(Equal(1), "notes once the object arrived")
	})
})

var _ = ginkgo.Describe("Join", func() {
	ginkgo.It("should put a Node's name on its NodeClaim's entry", func() {
		nodeClaims := objectStore[string, *v1.NodeClaim, linked, struct{}](WithKey(ByName))
		nodes := objectStore[string, *corev1.Node, struct{}, struct{}](WithKey(ByName))
		Derive(nodes.Reader, nodeClaims, func(_ string, e Entry[*corev1.Node, struct{}, struct{}]) string {
			if len(e.Object.OwnerReferences) == 0 {
				return ""
			}
			return e.Object.OwnerReferences[0].Name
		}, func(name string, _ Entry[*corev1.Node, struct{}, struct{}]) linkedNode { return linkedNode(name) })
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-a", ResourceVersion: "3"}}
		observe(nodes, node)
		Expect(fields(nodeClaims, "a").Node).To(BeEmpty(), "a Node without an owner reference linked")
		node = node.DeepCopy()
		node.ResourceVersion, node.OwnerReferences = "4", []metav1.OwnerReference{{Kind: "NodeClaim", Name: "a"}}
		observe(nodes, node)
		Expect(fields(nodeClaims, "a").Node).To(Equal("node-a"), "the NodeClaim's linked Node")
		forget(nodes, types.NamespacedName{Name: "node-a"})
		Expect(fields(nodeClaims, "a").Node).To(BeEmpty(), "the NodeClaim's linked Node once the Node is gone")
	})
})

// linked is a NodeClaim's linked Node.
type linked struct{ Node string }

// linkedNode is a Node's contribution to the NodeClaim that owns it: its name.
type linkedNode string

func (n linkedNode) Update(f linked, _ linkedNode) linked {
	f.Node = string(n)
	return f
}

func (n linkedNode) Delete(f linked) linked {
	f.Node = ""
	return f
}

var _ = ginkgo.Describe("Store", func() {
	ginkgo.It("should drop a key once nothing contributes", func() {
		nodeClaims, s := newZeroStores()
		observe(nodeClaims, nodeClaim("a", "x", 1, "0"))
		observe(nodeClaims, nodeClaim("b", "x", 2, "0"))
		forget(nodeClaims, types.NamespacedName{Name: "a"})
		_, ok := s.Get("x")
		Expect(ok).To(BeTrue(), "dropped the key while a NodeClaim still contributes to it, even though its fields are zero")
		forget(nodeClaims, types.NamespacedName{Name: "b"})
		_, ok = s.Get("x")
		Expect(ok).To(BeFalse(), "fields once nothing is left, want none")
	})
	ginkgo.It("should not change entries once read", func() {
		nodeClaims, s, _ := newStores()
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		read, _ := s.Get("x")
		observe(nodeClaims, nodeClaim("b", "x", 2, "4"))
		Expect(read.Derived).To(Equal(poolFields{CPU: 2, Count: 1}), "fields read before later changes")
	})
	ginkgo.It("should notify on change", func() {
		nodeClaims, s, _ := newStores()
		type note struct {
			key      string
			old, new int64
		}
		var notes []note
		s.Subscribe(func(c Change[string, string, poolFields, struct{}]) {
			notes = append(notes, note{c.Key, c.Old.Derived.CPU, c.New.Derived.CPU})
		})
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		observe(nodeClaims, nodeClaim("a", "x", 2, "2"))
		observe(nodeClaims, nodeClaim("a", "y", 3, "2"))
		Expect(notes).To(Equal([]note{{"x", 0, 2}, {"x", 2, 0}, {"y", 0, 2}}), "want x, then x and y for the move, and nothing for changes that keep the fields")
	})
	ginkgo.It("should notify an entry added or removed with zero fields", func() {
		nodeClaims, s := newZeroStores()
		var changes []Change[string, string, poolFields, struct{}]
		unsubscribe := s.Subscribe(func(c Change[string, string, poolFields, struct{}]) { changes = append(changes, c) })
		observe(nodeClaims, nodeClaim("a", "x", 1, "0"))
		forget(nodeClaims, types.NamespacedName{Name: "a"})
		want := []Change[string, string, poolFields, struct{}]{
			{Key: "x", New: Entry[string, poolFields, struct{}]{Object: "x"}, Exists: true},
			{Key: "x", Old: Entry[string, poolFields, struct{}]{Object: "x"}, Existed: true},
		}
		Expect(changes).To(Equal(want))
		unsubscribe()
		observe(nodeClaims, nodeClaim("b", "x", 2, "1"))
		Expect(changes).To(HaveLen(len(want)), "notified after unsubscribing")
	})
	ginkgo.It("should apply what its source holds and follow it on Start", func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		from := statetest.NewUnsyncedObjects()
		from.Put(nodeClaim("a", "x", 1, "2"))
		from.Put(nodeClaim("b", "x", 2, "4"))
		nodeClaims := objectStoreOf[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent](from)
		s := newPools()
		Derive(nodeClaims.Reader, s, poolOf, usageOf)
		var mu sync.Mutex
		var changed []string
		s.Subscribe(func(c Change[string, string, poolFields, struct{}]) {
			mu.Lock()
			defer mu.Unlock()
			changed = append(changed, c.Key)
		})
		Expect(s.Synced()).To(BeFalse(), "the derived store is synced before its source synced")
		go func() { _ = nodeClaims.Start(ctx) }()
		from.SetSynced()
		Eventually(s.Synced).WithTimeout(10*time.Second).WithPolling(10*time.Millisecond).Should(BeTrue(), "the store didn't sync")
		Expect(fields(s, "x")).To(cpuAndCount(6, 2), "x after Start")
		from.Remove(types.NamespacedName{Name: "a"})
		Expect(fields(s, "x")).To(cpuAndCount(4, 1), "x after the source dropped a")
		mu.Lock()
		defer mu.Unlock()
		Expect(changed).To(Equal([]string{"x", "x", "x"}), "want x for each NodeClaim the source held and x for the removal")
	})
	ginkgo.It("should report Synced safely while a Derive is added", func() {
		nodeClaims, s := newNodeClaims(), newPools()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range 1000 {
				_ = s.Synced()
			}
		}()
		for range 50 {
			Derive(nodeClaims.Reader, s, poolOf, usageOf)
		}
		<-done
	})
})

var _ = ginkgo.Describe("FedStore", func() {
	ginkgo.It("should replace a set of objects in one write", func() {
		type instanceType struct{ Class, Name string }
		s := NewFedStore[instanceType, string, struct{}, struct{}]()
		var notes []Change[instanceType, string, struct{}, struct{}]
		s.Subscribe(func(c Change[instanceType, string, struct{}, struct{}]) { notes = append(notes, c) })
		ofA := func(k instanceType) bool { return k.Class == "a" }
		s.FeedAll(ofA, map[instanceType]string{{"a", "m5"}: "1", {"a", "c5"}: "2"})
		s.FeedAll(func(k instanceType) bool { return k.Class == "b" }, map[instanceType]string{{"b", "m5"}: "3"})
		s.FeedAll(ofA, map[instanceType]string{{"a", "m5"}: "4"})
		var held []string
		for k, e := range s.List() {
			held = append(held, k.Class+"/"+k.Name+"="+e.Object)
		}
		Expect(held).To(ConsistOf("a/m5=4", "b/m5=3"), "the feed's objects once a's set was replaced")
		s.Feed(instanceType{"b", "m5"}, "")
		_, ok := s.Get(instanceType{"b", "m5"})
		Expect(ok).To(BeFalse(), "an object the feed stopped reporting is still there")
		Expect(notes).To(HaveLen(6), "changes: three added, one changed, one dropped by the replace, one by the feed")
	})
})

var _ = ginkgo.Describe("Intent", func() {
	ginkgo.It("should set, clear and drop with its entry", func() {
		nodeClaims := newNodeClaims()
		deletion := NewIntent(nodeClaims, func(i *nodeClaimIntent) *Intended[bool] { return &i.Deletion })
		key := types.NamespacedName{Name: "a"}
		Expect(deletion.Set(key, true)).To(BeFalse(), "an intent set on an entry that doesn't exist")
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		Expect(deletion.Set(key, true)).To(BeTrue(), "an intent set on an entry that exists")
		e, _ := nodeClaims.Get(key)
		Expect(e.Intent.Deletion.Value).To(BeTrue(), "the entry's intent once set")
		deletion.Clear(key)
		Expect(lo.Must(deletion.Get(key))).To(BeFalse(), "the intent once cleared")
		deletion.Set(key, true)
		recreated := nodeClaim("a", "x", 2, "2")
		recreated.UID = "a-2"
		observe(nodeClaims, recreated)
		Expect(lo.Must(deletion.Get(key))).To(BeFalse(), "the intent once a new object replaced its object under the name")
		deletion.Set(key, true)
		forget(nodeClaims, key)
		observe(nodeClaims, nodeClaim("a", "x", 3, "2"))
		e, _ = nodeClaims.Get(key)
		Expect(e.Intent.Deletion.Value).To(BeFalse(), "the intent outlived its entry")
	})
	// lapsingNominations returns an intent made WithExpiry on a fake clock over a store holding "a", and the number of
	// lapses of its value the store notified.
	lapsingNominations := func(clk realclock.Clock, ttl time.Duration) (*Store[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent],
		*Intent[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent, bool], func() int) {
		nodeClaims := newNodeClaims()
		nomination := NewIntent(nodeClaims, func(i *nodeClaimIntent) *Intended[bool] { return &i.Nomination }).WithExpiry(clk, ttl)
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		var mu sync.Mutex
		var lapsed int
		nodeClaims.Subscribe(func(c Change[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent]) {
			if c.Old.Intent.Nomination.Value && !c.New.Intent.Nomination.Value {
				mu.Lock()
				defer mu.Unlock()
				lapsed++
			}
		})
		return nodeClaims, nomination, func() int { mu.Lock(); defer mu.Unlock(); return lapsed }
	}
	ginkgo.It("should lapse WithExpiry once its clock steps past the TTL, and notify the lapse", func() {
		clk := clock.NewFakeClock(time.Now())
		nodeClaims, nomination, lapsed := lapsingNominations(clk, time.Minute)
		key := types.NamespacedName{Name: "a"}
		nomination.Set(key, true)
		Expect(clk.Waiters()).To(Equal(1), "the lapse timer doesn't wait on the intent's clock")
		clk.Step(59 * time.Second)
		Consistently(lapsed, 100*time.Millisecond).Should(BeZero(), "lapsed before its TTL")
		clk.Step(time.Second)
		Eventually(lapsed).Should(Equal(1), "the lapse didn't notify")
		e, _ := nodeClaims.Get(key)
		Expect(e.Intent.Nomination).To(BeZero(), "the lapsed intent is still in the entry")
		Expect(lo.Must(nomination.Get(key))).To(BeFalse(), "a nomination held past its TTL")
		Expect(clk.HasWaiters()).To(BeFalse(), "kept a timer once nothing can lapse")
	})
	ginkgo.It("should re-arm WithExpiry when set again before the TTL", func() {
		clk := clock.NewFakeClock(time.Now())
		_, nomination, lapsed := lapsingNominations(clk, time.Minute)
		key := types.NamespacedName{Name: "a"}
		nomination.Set(key, true)
		clk.Step(30 * time.Second)
		nomination.Set(key, true)
		clk.Step(45 * time.Second)
		Eventually(clk.Waiters).Should(Equal(1), "didn't re-arm the timer at the extended lapse")
		Consistently(lapsed, 100*time.Millisecond).Should(BeZero(), "lapsed at the first TTL once extended")
		v, ok := nomination.Get(key)
		Expect(v && ok).To(BeTrue(), "a nomination extended before it lapsed")
		clk.Step(15 * time.Second)
		Eventually(lapsed).Should(Equal(1), "the extended lapse didn't notify")
		Expect(clk.HasWaiters()).To(BeFalse(), "kept a timer once nothing can lapse")
	})
	ginkgo.It("should stop its lapse timer once cleared, or once its entry drops", func() {
		clk := clock.NewFakeClock(time.Now())
		nodeClaims, nomination, lapsed := lapsingNominations(clk, time.Minute)
		key := types.NamespacedName{Name: "a"}
		nomination.Set(key, true)
		Expect(clk.HasWaiters()).To(BeTrue(), "didn't arm a timer for the lapse")
		nomination.Clear(key)
		Expect(clk.HasWaiters()).To(BeFalse(), "kept a timer once the intent cleared")

		nomination.Set(key, true)
		forget(nodeClaims, key)
		observe(nodeClaims, nodeClaim("a", "x", 2, "2"))
		nomination.Set(key, true)
		Expect(clk.Waiters()).To(Equal(1), "armed a timer per lapse")
		dropped := lapsed()
		clk.Step(time.Minute)
		Eventually(lapsed).Should(Equal(dropped+1), "the lapse after the entry dropped didn't notify")
		Expect(clk.HasWaiters()).To(BeFalse(), "kept a timer once nothing can lapse")
	})
})

var _ = ginkgo.Describe("Derive", func() {
	ginkgo.It("should move contributions between keys", func() {
		nodeClaims, s, _ := newStores()
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		observe(nodeClaims, nodeClaim("b", "x", 2, "4"))
		Expect(fields(s, "x")).To(cpuAndCount(6, 2), "x, want 6 cpu over 2 NodeClaims")
		observe(nodeClaims, nodeClaim("a", "y", 3, "2"))
		Expect(fields(s, "x")).To(cpuAndCount(4, 1), "x after a moved")
		Expect(fields(s, "y")).To(cpuAndCount(2, 1), "y after a moved")
		observe(nodeClaims, nodeClaim("a", "", 4, "2"))
		_, ok := s.Get("y")
		Expect(ok).To(BeFalse(), "y still has fields once its only NodeClaim stopped contributing")
	})
	ginkgo.It("should follow a synthetic store", func() {
		nodeClaims, s, _ := newStores()
		total := NewSyntheticStore[string, string, clusterFields, struct{}](func(k string) string { return k })
		Derive(s.Reader, total, func(string, Entry[string, poolFields, struct{}]) string { return "all" },
			func(_ string, e Entry[string, poolFields, struct{}]) poolCPU { return poolCPU(e.Derived.CPU) })
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		observe(nodeClaims, nodeClaim("b", "y", 2, "4"))
		observe(nodeClaims, nodeClaim("a", "x", 3, "8"))
		Expect(fields(total, "all").CPU).To(Equal(int64(12)), "total cpu")
		forget(nodeClaims, types.NamespacedName{Name: "a"})
		forget(nodeClaims, types.NamespacedName{Name: "b"})
		_, ok := total.Get("all")
		Expect(ok).To(BeFalse(), "total still has fields once nothing contributes")
	})
	ginkgo.It("should apply what its source held before it", func() {
		nodeClaims := newNodeClaims()
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		s := newPools()
		Derive(nodeClaims.Reader, s, poolOf, usageOf)
		Expect(fields(s, "x")).To(cpuAndCount(2, 1), "x, want the NodeClaim the source held before Derive")
	})
	ginkgo.It("should converge under concurrent changes", func() {
		nodeClaims, s, _ := newStores()
		var wg sync.WaitGroup
		for w := range 4 {
			wg.Go(func() {
				for i := range 1000 {
					observe(nodeClaims, nodeClaim(fmt.Sprintf("nc-%d", i%50), fmt.Sprintf("pool-%d", (i+w)%3), uint64(i*4+w+1), "1")) //nolint:gosec
				}
			})
		}
		wg.Wait()
		total, cpu := 0, int64(0)
		for i := range 3 {
			f := fields(s, fmt.Sprintf("pool-%d", i))
			total += f.Count
			cpu += f.CPU
		}
		Expect(total).To(Equal(50), "NodeClaims the NodePools count, want each of the 50 counted once")
		Expect(cpu).To(Equal(int64(50)), "cpu the NodePools count, want each of the 50 counted once")
	})
	ginkgo.It("should recompute a contribution that reads another store on a touch", func() {
		nodeClaims, s := newNodeClaims(), newPools()
		cpu := int64(2)
		Derive(nodeClaims.Reader, s, poolOf, func(types.NamespacedName, nodeClaimEntry) usage { return usage{CPU: cpu, Count: 1} })
		observe(nodeClaims, nodeClaim("a", "x", 1, "0"))
		cpu = 6
		observe(nodeClaims, nodeClaim("a", "x", 1, "0"))
		Expect(fields(s, "x").CPU).To(Equal(int64(2)), "x's cpu after observing the version it holds")
		nodeClaims.touch(types.NamespacedName{Name: "a"})
		Expect(fields(s, "x")).To(cpuAndCount(6, 1), "x after a touch, want the recomputed cpu")
	})
	ginkgo.It("should dispatch Update and Delete", func() {
		nodeClaims := newNodeClaims()
		s := NewSyntheticStore[string, string, dispatches, struct{}](func(k string) string { return k })
		Derive(nodeClaims.Reader, s, poolOf, func(_ types.NamespacedName, e nodeClaimEntry) counted {
			return counted{Name: e.Object.Name, CPU: e.Object.Status.Capacity.Cpu().Value()}
		})
		var notes int
		s.Subscribe(func(Change[string, string, dispatches, struct{}]) { notes++ })
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		Expect(fields(s, "x")).To(Equal(dispatches{Updates: 1, CPU: 2}), "x for a new object, want one Update")
		observe(nodeClaims, nodeClaim("a", "x", 2, "4"))
		Expect(fields(s, "x")).To(Equal(dispatches{Updates: 2, CPU: 4}), "x once its contribution changed, want a second Update")
		before := notes
		observe(nodeClaims, nodeClaim("a", "x", 3, "4"))
		Expect(fields(s, "x")).To(Equal(dispatches{Updates: 2, CPU: 4}), "x for an equal contribution, want no call")
		Expect(notes).To(Equal(before), "notes for an equal contribution, want none")
		observe(nodeClaims, nodeClaim("a", "y", 4, "4"))
		Expect(fields(s, "y")).To(Equal(dispatches{Updates: 1, CPU: 4}), "y after a moved, want an Update")
		observe(nodeClaims, nodeClaim("b", "x", 5, "1"))
		Expect(fields(s, "x")).To(Equal(dispatches{Updates: 1, CPU: 1}), "x once a left and b arrived: a's Delete dropped the entry, b's Update rebuilt it")
		observe(nodeClaims, nodeClaim("a", "", 6, "4"))
		_, ok := s.Get("y")
		Expect(ok).To(BeFalse(), "y once a's key became zero, want its Delete to drop it")
		forget(nodeClaims, types.NamespacedName{Name: "b"})
		forget(nodeClaims, types.NamespacedName{Name: "b"})
		_, ok = s.Get("x")
		Expect(ok).To(BeFalse(), "x once b was forgotten twice")
	})
	ginkgo.It("should leave nothing behind in its target once removed", func() {
		nodeClaims, s, _ := newStores()
		for range 10 {
			d := Derive(nodeClaims.Reader, s, poolOf, usageOf)
			NewClaims[types.NamespacedName, string, poolFields, usage, heldCPU](d, func(usage) bool { return true }, time.Minute, clock.NewFakeClock(time.Now()))
			d.Remove()
		}
		s.s.mu.Lock()
		defer s.s.mu.Unlock()
		Expect(s.s.sources).To(HaveLen(1), "sources once ten Derives were built and removed, want only the one that stays")
	})
	// A source key deleted while a late Derive's sweep computes its contribution: the derived store reports unsynced
	// until the sweep ends and doesn't resurrect the deleted key from the sweep's stale read.
	ginkgo.It("should stay unsynced during its sweep and drop a key deleted during it", func() {
		nodeClaims := newNodeClaims()
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		observe(nodeClaims, nodeClaim("b", "x", 2, "4"))
		s := newPools()
		deleted, syncedDuringSweep := false, false
		Derive(nodeClaims.Reader, s, poolOf, func(k types.NamespacedName, e nodeClaimEntry) usage {
			syncedDuringSweep = syncedDuringSweep || s.Synced()
			if k.Name == "a" && !deleted {
				deleted = true
				forget(nodeClaims, types.NamespacedName{Name: "a"})
			}
			return usageOf(k, e)
		})
		Expect(syncedDuringSweep).To(BeFalse(), "the derived store reported synced before its sweep finished")
		Expect(s.Synced()).To(BeTrue(), "the derived store isn't synced once its sweep finished over a synced source")
		Expect(fields(s, "x")).To(cpuAndCount(4, 1), "x, want only b's 4 cpu: a was deleted while the sweep computed it")
	})
})

var _ = ginkgo.Describe("Resync", func() {
	ginkgo.It("should heal missed events for a key", func() {
		nodeClaims, s, usage := newStores()
		observe(nodeClaims, nodeClaim("a", "x", 1, "2"))
		observe(nodeClaims, nodeClaim("b", "x", 2, "4"))
		observe(nodeClaims, nodeClaim("z", "y", 3, "1"))
		resync(nodeClaims, []*v1.NodeClaim{nodeClaim("b", "x", 2, "4"), nodeClaim("c", "x", 3, "8")}, usage.Sources("x"))
		Expect(fields(s, "x")).To(cpuAndCount(12, 2), "x, want b's 4 and c's 8 cpu once a is left out of the listing")
		resync(nodeClaims, []*v1.NodeClaim(nil), usage.Sources("x"))
		_, ok := s.Get("x")
		Expect(ok).To(BeFalse(), "x still has fields once the listing is empty")
		Expect(fields(s, "y").Count).To(Equal(1), "y, want z, which is out of the resync's scope")
	})
})

var _ = ginkgo.Describe("Contributions", func() {
	ginkgo.It("should keep their laws", func() {
		statetest.ExpectContribution(ginkgo.GinkgoT(), poolFields{CPU: 3, Count: 1}, usage{CPU: 2, Count: 1}, usage{CPU: 8, Count: 1})
		statetest.ExpectContribution(ginkgo.GinkgoT(), clusterFields{CPU: 5}, poolCPU(2), poolCPU(7))
		statetest.ExpectContribution(ginkgo.GinkgoT(), 3, indexed(1), indexed(1))
		statetest.ExpectContribution(ginkgo.GinkgoT(), linked{}, linkedNode("a"), linkedNode("b"))
	})
})
