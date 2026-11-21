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
	"sync"
	"sync/atomic"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// testPod is a pod the tests feed: the node it's bound to and the PVCs it mounts, in its namespace.
type testPod struct {
	Node string
	PVCs []string
}

type (
	testPodEntry = Entry[testPod, struct{}, struct{}]
	testPods     = Store[types.NamespacedName, testPod, struct{}, struct{}]
	// testPVCs hold each PVC's size, which a pod's contribution to its node sums.
	testPVCs = Store[types.NamespacedName, int64, struct{}, struct{}]
)

// podIndex indexes pods by the PVCs they mount.
type podIndex = Index[types.NamespacedName, testPod, struct{}, struct{}, types.NamespacedName]

func mountsPVC(k types.NamespacedName, e testPodEntry) []types.NamespacedName {
	values := make([]types.NamespacedName, 0, len(e.Object.PVCs))
	for _, name := range e.Object.PVCs {
		values = append(values, types.NamespacedName{Namespace: k.Namespace, Name: name})
	}
	return values
}

func pod(name string) types.NamespacedName { return types.NamespacedName{Namespace: "ns", Name: name} }

func pvc(name string) types.NamespacedName { return types.NamespacedName{Namespace: "ns", Name: name} }

func newTestPods() (*testPods, *podIndex) {
	pods := NewFedStore[types.NamespacedName, testPod, struct{}, struct{}]()
	return pods, NewIndex(pods, mountsPVC)
}

// volumeStores returns pods indexed by the PVCs they mount, PVCs, and nodes each pod contributes the size of its PVCs
// to, read from the PVC store, kept current with DependsOn. recomputes counts each pod's contribution computed.
func volumeStores() (pods *testPods, pvcs *testPVCs, nodes *pools, d *Derivation[types.NamespacedName, string, poolFields, usage], recomputes *sync.Map) {
	pods, mounts := newTestPods()
	pvcs, nodes, recomputes = NewFedStore[types.NamespacedName, int64, struct{}, struct{}](), newPools(), &sync.Map{}
	d = Derive(pods.Reader, nodes, func(_ types.NamespacedName, e testPodEntry) string { return e.Object.Node },
		func(k types.NamespacedName, e testPodEntry) usage {
			n, _ := recomputes.LoadOrStore(k, new(atomic.Int64))
			n.(*atomic.Int64).Add(1)
			return volumeUsage(pvcs, k, e.Object)
		})
	DependsOn(pods, pvcs.Reader, mounts)
	return pods, pvcs, nodes, d, recomputes
}

func volumeUsage(pvcs *testPVCs, k types.NamespacedName, p testPod) usage {
	u := usage{Count: 1}
	for _, name := range p.PVCs {
		e, _ := pvcs.Get(types.NamespacedName{Namespace: k.Namespace, Name: name})
		u.CPU += e.Object
	}
	return u
}

func recomputed(recomputes *sync.Map, k types.NamespacedName) int64 {
	n, ok := recomputes.Load(k)
	if !ok {
		return 0
	}
	return n.(*atomic.Int64).Load()
}

var _ = ginkgo.Describe("Index", func() {
	ginkgo.It("should follow Feed, an entry moving between values, and its removal", func() {
		pods, mounts := newTestPods()
		pods.Feed(pod("a"), testPod{PVCs: []string{"x", "y"}})
		pods.Feed(pod("b"), testPod{PVCs: []string{"y"}})
		Expect(mounts.Lookup(pvc("x"))).To(ConsistOf(pod("a")))
		Expect(mounts.Lookup(pvc("y"))).To(ConsistOf(pod("a"), pod("b")))
		pods.Feed(pod("a"), testPod{PVCs: []string{"z"}})
		Expect(mounts.Lookup(pvc("x"))).To(BeEmpty(), "x once a moved off it")
		Expect(mounts.Lookup(pvc("y"))).To(ConsistOf(pod("b")), "y once a moved off it")
		Expect(mounts.Lookup(pvc("z"))).To(ConsistOf(pod("a")), "z once a moved onto it")
		pods.Feed(pod("a"), testPod{})
		pods.Feed(pod("b"), testPod{})
		Expect(mounts.Lookup(pvc("y"))).To(BeEmpty(), "y once its pods are removed")
		Expect(mounts.Lookup(pvc("z"))).To(BeEmpty(), "z once its pods are removed")
	})
	ginkgo.It("should index a key once under a value its entry returns twice", func() {
		pods, mounts := newTestPods()
		pods.Feed(pod("a"), testPod{PVCs: []string{"y", "x", "y"}})
		Expect(mounts.Lookup(pvc("y"))).To(Equal([]types.NamespacedName{pod("a")}))
		pods.Feed(pod("a"), testPod{PVCs: []string{"x"}})
		Expect(mounts.Lookup(pvc("y"))).To(BeEmpty(), "y once a's entry no longer returns it")
		Expect(mounts.Lookup(pvc("x"))).To(Equal([]types.NamespacedName{pod("a")}))
	})
	ginkgo.It("should follow an object store's objects and their removal", func() {
		nodeClaims := newNodeClaims()
		pool := NewIndex(nodeClaims, func(_ types.NamespacedName, e nodeClaimEntry) []string {
			return []string{e.Object.Labels[v1.NodePoolLabelKey]}
		})
		put(nodeClaims, nodeClaim("a", "x", 1, "1"))
		Expect(pool.Lookup("x")).To(ConsistOf(types.NamespacedName{Name: "a"}))
		put(nodeClaims, nodeClaim("a", "y", 2, "1"))
		Expect(pool.Lookup("x")).To(BeEmpty(), "x once a moved to y")
		Expect(pool.Lookup("y")).To(ConsistOf(types.NamespacedName{Name: "a"}))
		remove(nodeClaims, types.NamespacedName{Name: "a"})
		Expect(pool.Lookup("y")).To(BeEmpty(), "y once a is gone")
	})
	ginkgo.It("should follow Derived changes, and drop an entry that stops existing", func() {
		nodeClaims, pools, _ := newStores()
		cpu := NewIndex(pools, func(_ string, e Entry[string, poolFields, struct{}]) []int64 {
			return []int64{e.Derived.CPU}
		})
		put(nodeClaims, nodeClaim("a", "x", 1, "2"))
		Expect(cpu.Lookup(2)).To(ConsistOf("x"))
		put(nodeClaims, nodeClaim("a", "x", 2, "3"))
		Expect(cpu.Lookup(2)).To(BeEmpty(), "2 once x's cpu changed")
		Expect(cpu.Lookup(3)).To(ConsistOf("x"))
		remove(nodeClaims, types.NamespacedName{Name: "a"})
		Expect(cpu.Lookup(0)).To(BeEmpty(), "an entry that stopped existing, want it in no value")
		Expect(cpu.Lookup(3)).To(BeEmpty())
	})
	ginkgo.It("should not index Derived waiting for an object", func() {
		nodeClaims := newNodeClaims()
		nodes := objectStore[types.NamespacedName, *v1.NodeClaim, poolFields, struct{}]()
		cpu := NewIndex(nodes, func(_ types.NamespacedName, e Entry[*v1.NodeClaim, poolFields, struct{}]) []int64 {
			return []int64{e.Derived.CPU}
		})
		Derive(nodeClaims.Reader, nodes, func(types.NamespacedName, nodeClaimEntry) types.NamespacedName {
			return types.NamespacedName{Name: "n"}
		}, usageOf)
		put(nodeClaims, nodeClaim("a", "x", 1, "2"))
		Expect(cpu.Lookup(2)).To(BeEmpty(), "Derived waiting for its object, want it unindexed")
		put(nodes, nodeClaim("n", "x", 1, "0"))
		Expect(cpu.Lookup(2)).To(ConsistOf(types.NamespacedName{Name: "n"}), "once its object arrives")
	})
	ginkgo.It("should index the entries a store holds when it's added", func() {
		pods := NewFedStore[types.NamespacedName, testPod, struct{}, struct{}]()
		pods.Feed(pod("a"), testPod{PVCs: []string{"x"}})
		pods.Feed(pod("b"), testPod{PVCs: []string{"x"}})
		pods.Feed(pod("c"), testPod{PVCs: []string{"x"}})
		pods.Feed(pod("c"), testPod{})
		mounts := NewIndex(pods, mountsPVC)
		Expect(mounts.Lookup(pvc("x"))).To(ConsistOf(pod("a"), pod("b")))
		pods.Feed(pod("a"), testPod{})
		Expect(mounts.Lookup(pvc("x"))).To(ConsistOf(pod("b")), "once a is removed")
	})
	ginkgo.It("should List the entries that return a value, filtered by a label selector", func() {
		nodeClaims := newNodeClaims()
		cpu := NewIndex(nodeClaims, func(_ types.NamespacedName, e nodeClaimEntry) []int64 {
			return []int64{e.Object.Status.Capacity.Cpu().Value()}
		})
		put(nodeClaims, nodeClaim("a", "x", 1, "1"))
		put(nodeClaims, nodeClaim("b", "y", 1, "1"))
		put(nodeClaims, nodeClaim("c", "x", 1, "2"))
		list := func(opts ...client.ListOption) []string {
			var names []string
			for k, e := range cpu.List(1, opts...) {
				Expect(e.Object.Name).To(Equal(k.Name))
				names = append(names, k.Name)
			}
			return names
		}
		Expect(list()).To(ConsistOf("a", "b"))
		Expect(list(client.UnsafeDisableDeepCopy)).To(ConsistOf("a", "b"))
		Expect(list(client.MatchingLabels{v1.NodePoolLabelKey: "x"})).To(ConsistOf("a"))
		Expect(list(client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(labels.Set{v1.NodePoolLabelKey: "y"})},
			client.UnsafeDisableDeepCopy)).To(ConsistOf("b"))
		remove(nodeClaims, types.NamespacedName{Name: "a"})
		Expect(list()).To(ConsistOf("b"), "once a is gone")
	})
	ginkgo.It("should panic on a List option it doesn't take", func() {
		nodeClaims, pools, _ := newStores()
		byPool := NewIndex(nodeClaims, func(_ types.NamespacedName, e nodeClaimEntry) []string {
			return []string{poolOf(types.NamespacedName{}, e)}
		})
		Expect(func() { byPool.List("x", client.MatchingFields{"pool": "x"}) }).
			To(PanicWith(ContainSubstring("doesn't take a client.MatchingFields")))
		Expect(func() { byPool.List("x", client.InNamespace("ns")) }).To(PanicWith(ContainSubstring("doesn't take a client.InNamespace")))
		byCPU := NewIndex(pools, func(_ string, e Entry[string, poolFields, struct{}]) []int64 { return []int64{e.Derived.CPU} })
		Expect(func() { byCPU.List(1, client.MatchingLabels{"a": "b"}) }).
			To(PanicWith(ContainSubstring("a label selector on a store of string")))
	})
	ginkgo.It("should not let a Lookup's caller change the index", func() {
		pods, mounts := newTestPods()
		pods.Feed(pod("a"), testPod{PVCs: []string{"x"}})
		keys := mounts.Lookup(pvc("x"))
		keys[0] = pod("z")
		Expect(mounts.Lookup(pvc("x"))).To(ConsistOf(pod("a")))
	})
	ginkgo.It("should match a recount under concurrent writers and readers", func() {
		pods, mounts := newTestPods()
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for range 2 {
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						for i := range 5 {
							for _, k := range mounts.Lookup(pvc(fmt.Sprintf("pvc-%d", i))) {
								pods.Get(k)
							}
						}
					}
				}
			})
		}
		var writers sync.WaitGroup
		for w := range 4 {
			writers.Go(func() {
				for i := range 2000 {
					var p testPod
					if (i+w)%7 != 0 {
						p.PVCs = []string{fmt.Sprintf("pvc-%d", (i+w)%5), fmt.Sprintf("pvc-%d", (i*3+w)%5)}
					}
					pods.Feed(pod(fmt.Sprintf("p-%d", i%40)), p)
				}
			})
		}
		writers.Wait()
		close(stop)
		wg.Wait()
		want := map[types.NamespacedName]sets.Set[types.NamespacedName]{}
		for k, e := range pods.List() {
			for _, v := range mountsPVC(k, e) {
				if want[v] == nil {
					want[v] = sets.New[types.NamespacedName]()
				}
				want[v].Insert(k)
			}
		}
		for i := range 5 {
			v := pvc(fmt.Sprintf("pvc-%d", i))
			Expect(mounts.Lookup(v)).To(ConsistOf(want[v].UnsortedList()), "%s's keys, want the recount from List", v)
		}
	})
})

var _ = ginkgo.Describe("DependsOn", func() {
	ginkgo.It("should recompute exactly the pods that mount a PVC that changed", func() {
		pods, pvcs, nodes, _, recomputes := volumeStores()
		pvcs.Feed(pvc("x"), 2)
		pvcs.Feed(pvc("y"), 3)
		pods.Feed(pod("a"), testPod{Node: "n", PVCs: []string{"x"}})
		pods.Feed(pod("b"), testPod{Node: "n", PVCs: []string{"x", "y"}})
		pods.Feed(pod("c"), testPod{Node: "m", PVCs: []string{"y"}})
		pods.Feed(pod("d"), testPod{Node: "m"})
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 7, Count: 2}, "m": {CPU: 3, Count: 2}}))
		before := map[string]int64{}
		for _, name := range []string{"a", "b", "c", "d"} {
			before[name] = recomputed(recomputes, pod(name))
		}
		pvcs.Feed(pvc("x"), 5)
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 13, Count: 2}, "m": {CPU: 3, Count: 2}}), "once x grew")
		for name, want := range map[string]int64{"a": 1, "b": 1, "c": 0, "d": 0} {
			Expect(recomputed(recomputes, pod(name))-before[name]).To(Equal(want), "%s's recomputes on x's change", name)
		}
		pvcs.Feed(pvc("y"), 0)
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 10, Count: 2}, "m": {CPU: 0, Count: 2}}), "once y is gone")
		pvcs.Feed(pvc("y"), 1)
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 11, Count: 2}, "m": {CPU: 1, Count: 2}}), "once y is back")
	})
	ginkgo.It("should not commit a recompute that read the dependency from before it changed", func() {
		pods, pvcs, nodes, d, _ := volumeStores()
		pvcs.Feed(pvc("x"), 2)
		pods.Feed(pod("a"), testPod{Node: "n", PVCs: []string{"x"}})
		commit := staleRead(d, pod("a"))
		pvcs.Feed(pvc("x"), 6)
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 6, Count: 1}}), "the recompute the dependency's change triggers")
		Expect(commit()).To(BeFalse(), "a commit that read the dependency before it changed, want it refused")
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 6, Count: 1}}), "want the recompute from the new dependency to hold")
	})
	ginkgo.It("should do nothing on a change of a key no entry indexes", func() {
		pods, pvcs, nodes, _, recomputes := volumeStores()
		pods.Feed(pod("a"), testPod{Node: "n", PVCs: []string{"x"}})
		before := recomputed(recomputes, pod("a"))
		var changes atomic.Int64
		pods.Subscribe(func(Change[types.NamespacedName, testPod, struct{}, struct{}]) { changes.Add(1) })
		pvcs.Feed(pvc("other"), 4)
		pvcs.Feed(types.NamespacedName{Namespace: "elsewhere", Name: "x"}, 4)
		Expect(recomputed(recomputes, pod("a"))).To(Equal(before), "a's recomputes")
		Expect(changes.Load()).To(BeZero(), "the pods' notifications")
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 0, Count: 1}}))
	})
	ginkgo.It("should stop", func() {
		pods, mounts := newTestPods()
		pvcs, nodes := NewFedStore[types.NamespacedName, int64, struct{}, struct{}](), newPools()
		Derive(pods.Reader, nodes, func(_ types.NamespacedName, e testPodEntry) string { return e.Object.Node },
			func(k types.NamespacedName, e testPodEntry) usage { return volumeUsage(pvcs, k, e.Object) })
		stop := DependsOn(pods, pvcs.Reader, mounts)
		pods.Feed(pod("a"), testPod{Node: "n", PVCs: []string{"x"}})
		pvcs.Feed(pvc("x"), 2)
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 2, Count: 1}}))
		stop()
		pvcs.Feed(pvc("x"), 5)
		Expect(pooled(nodes)).To(Equal(map[string]poolFields{"n": {CPU: 2, Count: 1}}), "once stopped, want no recompute")
	})
	ginkgo.It("should panic on an index of another store", func() {
		pods, _ := newTestPods()
		_, others := newTestPods()
		Expect(func() { DependsOn(pods, NewFedStore[types.NamespacedName, int64, struct{}, struct{}]().Reader, others) }).
			To(PanicWith(ContainSubstring("an index of another store")))
	})
	ginkgo.It("should converge on a recount under concurrent dependency and target churn", func() {
		pods, pvcs, nodes, _, _ := volumeStores()
		var wg sync.WaitGroup
		for w := range 3 {
			wg.Go(func() {
				for i := range 2000 {
					var p testPod
					if (i+w)%9 != 0 {
						p = testPod{Node: fmt.Sprintf("n-%d", (i+w)%3), PVCs: []string{fmt.Sprintf("pvc-%d", (i+w)%6), fmt.Sprintf("pvc-%d", (i*5+w)%6)}}
					}
					pods.Feed(pod(fmt.Sprintf("p-%d", i%30)), p)
				}
			})
		}
		for w := range 3 {
			wg.Go(func() {
				for i := range 2000 {
					pvcs.Feed(pvc(fmt.Sprintf("pvc-%d", (i+w)%6)), int64((i*7+w)%4)) //nolint:gosec
				}
			})
		}
		wg.Wait()
		want := map[string]poolFields{}
		for k, e := range pods.List() {
			u := volumeUsage(pvcs, k, e.Object)
			f := want[e.Object.Node]
			f.CPU += u.CPU
			f.Count += u.Count
			want[e.Object.Node] = f
		}
		Expect(pooled(nodes)).To(Equal(want), "the nodes' fields, want the recount from the pods and PVCs")
	})
})
