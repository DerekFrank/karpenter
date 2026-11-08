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
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

var ctx = context.Background()

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

type (
	nodeClaimStore = Store[types.NamespacedName, *v1.NodeClaim]
	pools          = Store[string, poolFields]
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

func poolOf(_ types.NamespacedName, nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] }

func usageOf(_ types.NamespacedName, nc *v1.NodeClaim) usage {
	return usage{CPU: nc.Status.Capacity.Cpu().Value(), Count: 1}
}

// newStores returns an object store of NodeClaims, and a store of their usage of each NodePool derived from it.
func newStores() (*nodeClaimStore, *pools, *Derivation[types.NamespacedName, string, *v1.NodeClaim, poolFields, usage]) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	s := NewStore[string, poolFields]()
	return nodeClaims, s, Derive(nodeClaims, s, poolOf, usageOf)
}

func named(name string) *v1.NodeClaim {
	return &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func fields[K comparable, F any](s *Store[K, F], key K) F {
	f, _ := s.Get(key)
	return f
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestObjectStoreHoldsTheNewestVersionByName(t *testing.T) {
	nodeClaims, _, _ := newStores()
	nodeClaims.Observe(nodeClaim("a", "x", 2, "2"))
	nodeClaims.Observe(nodeClaim("a", "x", 1, "8"))
	if got := fields(nodeClaims, types.NamespacedName{Name: "a"}); got.ResourceVersion != "2" {
		t.Fatalf("a = version %s after an older version, want 2", got.ResourceVersion)
	}
	nodeClaims.Forget(named("a"))
	if _, ok := nodeClaims.Get(types.NamespacedName{Name: "a"}); ok {
		t.Fatal("a is still there once forgotten")
	}
}

func TestJoinStoreJoinsKindsOnAKeyOtherThanTheirNames(t *testing.T) {
	type joined struct {
		NodeClaim *v1.NodeClaim
		Node      *corev1.Node
	}
	s := NewJoinStore(nil, nil,
		Join(func(nc *v1.NodeClaim) string { return nc.Status.ProviderID }, func(f joined, nc *v1.NodeClaim) joined { f.NodeClaim = nc; return f }),
		Join(func(n *corev1.Node) string { return n.Spec.ProviderID }, func(f joined, n *corev1.Node) joined { f.Node = n; return f }))
	nc := nodeClaim("a", "x", 1, "2")
	s.Observe(nc)
	for range s.List() {
		t.Fatal("a NodeClaim without a providerID joined something")
	}
	nc = nc.DeepCopy()
	nc.ResourceVersion, nc.Status.ProviderID = "2", "fake://a"
	s.Observe(nc)
	s.Observe(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-a", ResourceVersion: "3"}, Spec: corev1.NodeSpec{ProviderID: "fake://a"}})
	if f := fields(s, "fake://a"); f.NodeClaim == nil || f.Node == nil || f.Node.Name != "node-a" {
		t.Fatalf("joined = %+v, want the NodeClaim and its Node", f)
	}
	s.Forget(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	if f := fields(s, "fake://a"); f.NodeClaim == nil || f.Node != nil {
		t.Fatalf("joined = %+v once the Node is gone, want only the NodeClaim", f)
	}
	s.Forget(named("a"))
	if _, ok := s.Get("fake://a"); ok {
		t.Fatal("the join is still there once both sides are gone")
	}
}

// newZeroStores returns NodeClaims that contribute zero fields to their NodePools.
func newZeroStores() (*nodeClaimStore, *pools) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	s := NewStore[string, poolFields]()
	Derive(nodeClaims, s, poolOf, func(types.NamespacedName, *v1.NodeClaim) usage { return usage{} })
	return nodeClaims, s
}

func TestStoreDropsAKeyOnceNothingContributesAndItsFieldsAreZero(t *testing.T) {
	nodeClaims, s := newZeroStores()
	nodeClaims.Observe(nodeClaim("a", "x", 1, "0"))
	nodeClaims.Observe(nodeClaim("b", "x", 2, "0"))
	nodeClaims.Forget(named("a"))
	if _, ok := s.Get("x"); !ok {
		t.Fatal("dropped the key while a NodeClaim still contributes to it, even though its fields are zero")
	}
	nodeClaims.Forget(named("b"))
	if f, ok := s.Get("x"); ok {
		t.Fatalf("fields = %+v once nothing is left, want none", f)
	}
}

func TestStoreFieldsDoNotChangeOnceRead(t *testing.T) {
	nodeClaims, s, _ := newStores()
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	read, _ := s.Get("x")
	nodeClaims.Observe(nodeClaim("b", "x", 2, "4"))
	s.Update("x", func(f poolFields) poolFields { f.Reserved = 3; return f })
	if read != (poolFields{CPU: 2, Count: 1}) {
		t.Fatalf("fields read before later changes became %+v", read)
	}
}

func TestStoreUpdatesAtomically(t *testing.T) {
	s := NewStore[string, poolFields]()
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() { s.Update("x", func(f poolFields) poolFields { f.Reserved++; return f }) })
	}
	wg.Wait()
	if got := fields(s, "x").Reserved; got != 100 {
		t.Fatalf("reserved = %d after 100 concurrent updates, want 100", got)
	}
}

func TestDeriveMovesContributionsBetweenKeys(t *testing.T) {
	nodeClaims, s, _ := newStores()
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Observe(nodeClaim("b", "x", 2, "4"))
	if got := fields(s, "x"); got.CPU != 6 || got.Count != 2 {
		t.Fatalf("x = %+v, want 6 cpu over 2 NodeClaims", got)
	}
	nodeClaims.Observe(nodeClaim("a", "y", 3, "2"))
	if got := fields(s, "x"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("x = %+v after a moved, want 4 cpu over 1", got)
	}
	if got := fields(s, "y"); got.CPU != 2 || got.Count != 1 {
		t.Fatalf("y = %+v after a moved, want 2 cpu over 1", got)
	}
	nodeClaims.Observe(nodeClaim("a", "", 4, "2"))
	if _, ok := s.Get("y"); ok {
		t.Fatal("y still has fields once its only NodeClaim stopped contributing")
	}
}

func TestObjectStoreKeepsTheNewestVersionAcrossWritesAndTheInformer(t *testing.T) {
	nodeClaims, s, _ := newStores()
	nodeClaims.change(nodeClaim("a", "x", 5, "8"), false, false)
	nodeClaims.change(nodeClaim("a", "x", 3, "2"), false, true)
	if got := fields(s, "x").CPU; got != 8 {
		t.Fatalf("x = %d cpu after the informer delivered an older version than the write, want 8", got)
	}
	nodeClaims.change(nodeClaim("a", "x", 6, "4"), false, true)
	if got := fields(s, "x"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("x = %+v after another writer's newer version, want 4 cpu over 1", got)
	}
}

func TestObjectStoreIgnoresOlderVersionsOfAnObjectItDeleted(t *testing.T) {
	nodeClaims, s, _ := newStores()
	nodeClaims.change(nodeClaim("a", "x", 1, "2"), false, true)
	nodeClaims.change(nodeClaim("a", "x", 1, "2"), true, false)
	nodeClaims.change(nodeClaim("a", "x", 2, "2"), false, true)
	if got := fields(s, "x").Count; got != 0 {
		t.Fatalf("x counts %d NodeClaims after the informer delivered an update from before the delete, want 0", got)
	}
	nodeClaims.change(nodeClaim("a", "x", 2, "2"), true, true)
	p := nodeClaims.parts[0].(*joinPart[types.NamespacedName, *v1.NodeClaim, *v1.NodeClaim])
	if len(p.sources.versions) != 0 || len(p.held) != 0 {
		t.Fatalf("versions %v, held %v once the informer delivered the delete, want none", p.sources.versions, p.held)
	}
}

func TestObjectStoreCountsAnObjectRecreatedWithTheSameNameAsANewObject(t *testing.T) {
	nodeClaims, s, _ := newStores()
	nodeClaims.change(nodeClaim("a", "x", 1, "2"), false, true)
	nodeClaims.change(nodeClaim("a", "x", 1, "2"), true, false)
	recreated := nodeClaim("a", "x", 7, "4")
	recreated.UID = "a-2"
	nodeClaims.Observe(recreated)
	nodeClaims.change(nodeClaim("a", "x", 2, "2"), true, true)
	if got := fields(s, "x"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("x = %+v after the old object's delete, want the recreated object's 4 cpu over 1", got)
	}
	nodeClaims.Forget(named("a"))
	if got := fields(s, "x"); got.Count != 0 {
		t.Fatalf("x = %+v once a is forgotten, want nothing", got)
	}
}

func TestObjectStoreReplacesAnObjectRecreatedUnderItsNameWhenItsDeleteIsMissed(t *testing.T) {
	nodeClaims, s, _ := newStores()
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	recreated := nodeClaim("a", "y", 5, "4")
	recreated.UID = "a-2"
	nodeClaims.Observe(recreated)
	if _, ok := s.Get("x"); ok {
		t.Fatal("the replaced object still contributes to x")
	}
	if got := fields(s, "y"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("y = %+v, want the recreated object's 4 cpu over 1", got)
	}
	nodeClaims.Observe(nodeClaim("a", "x", 3, "2"))
	if _, ok := s.Get("x"); ok {
		t.Fatal("a late version of the replaced object contributed again")
	}
}

func TestResyncHealsMissedEventsForAKey(t *testing.T) {
	nodeClaims, s, usage := newStores()
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Observe(nodeClaim("b", "x", 2, "4"))
	nodeClaims.Observe(nodeClaim("z", "y", 3, "1"))
	Resync(nodeClaims, []*v1.NodeClaim{nodeClaim("b", "x", 2, "4"), nodeClaim("c", "x", 3, "8")}, usage.Sources("x"))
	if got := fields(s, "x"); got.CPU != 12 || got.Count != 2 {
		t.Fatalf("x = %+v, want b's 4 and c's 8 cpu over 2 once a is left out of the listing", got)
	}
	Resync(nodeClaims, []*v1.NodeClaim(nil), usage.Sources("x"))
	if _, ok := s.Get("x"); ok {
		t.Fatal("x still has fields once the listing is empty")
	}
	if got := fields(s, "y"); got.Count != 1 {
		t.Fatalf("y = %+v, want z, which is out of the resync's scope", got)
	}
}

func TestStoreNotifiesOnChange(t *testing.T) {
	nodeClaims, s, _ := newStores()
	type note struct {
		key      string
		old, new int64
	}
	var notes []note
	s.Subscribe(func(c Change[string, poolFields]) { notes = append(notes, note{c.Key, c.Old.CPU, c.New.CPU}) })
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Observe(nodeClaim("a", "x", 2, "2"))
	nodeClaims.Observe(nodeClaim("a", "y", 3, "2"))
	s.Update("y", func(f poolFields) poolFields { return f })
	if !slices.Equal(notes, []note{{"x", 0, 2}, {"x", 2, 0}, {"y", 0, 2}}) {
		t.Fatalf("notified %v, want x, then x and y for the move, and nothing for changes that keep the fields", notes)
	}
}

func TestStoreNotifiesAKeyAddedOrRemovedWithZeroFields(t *testing.T) {
	nodeClaims, s := newZeroStores()
	var changes []Change[string, poolFields]
	unsubscribe := s.Subscribe(func(c Change[string, poolFields]) { changes = append(changes, c) })
	nodeClaims.Observe(nodeClaim("a", "x", 1, "0"))
	nodeClaims.Forget(named("a"))
	want := []Change[string, poolFields]{{Key: "x", Exists: true}, {Key: "x", Existed: true}}
	if !slices.Equal(changes, want) {
		t.Fatalf("notified %+v, want %+v", changes, want)
	}
	unsubscribe()
	nodeClaims.Observe(nodeClaim("b", "x", 2, "1"))
	if len(changes) != len(want) {
		t.Fatal("notified after unsubscribing")
	}
}

type clusterFields struct{ CPU int64 }

type poolCPU int64

func (c poolCPU) Update(f clusterFields, prev poolCPU) clusterFields {
	f.CPU += int64(c - prev)
	return f
}
func (c poolCPU) Delete(f clusterFields) clusterFields { f.CPU -= int64(c); return f }

func TestDeriveFollowsADerivedStore(t *testing.T) {
	nodeClaims, s, _ := newStores()
	total := NewStore[string, clusterFields]()
	Derive(s, total, func(string, poolFields) string { return "all" }, func(_ string, f poolFields) poolCPU { return poolCPU(f.CPU) })
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Observe(nodeClaim("b", "y", 2, "4"))
	nodeClaims.Observe(nodeClaim("a", "x", 3, "8"))
	if got := fields(total, "all").CPU; got != 12 {
		t.Fatalf("total = %d cpu, want 12", got)
	}
	nodeClaims.Forget(named("a"))
	nodeClaims.Forget(named("b"))
	if _, ok := total.Get("all"); ok {
		t.Fatal("total still has fields once nothing contributes")
	}
}

func TestDeriveAppliesWhatItsSourceHeldBeforeIt(t *testing.T) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	s := NewStore[string, poolFields]()
	Derive(nodeClaims, s, poolOf, usageOf)
	if got := fields(s, "x"); got.CPU != 2 || got.Count != 1 {
		t.Fatalf("x = %+v, want the NodeClaim the source held before Derive", got)
	}
}

func TestStoreStartAppliesTheListAndFollowsTheInformer(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	a0, b0 := nodeClaim("a", "x", 0, "2"), nodeClaim("b", "x", 0, "4")
	a0.ResourceVersion, b0.ResourceVersion = "", ""
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(a0, b0).Build()
	informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
	nodeClaims := NewObjectStore[*v1.NodeClaim](kubeClient, informers)
	s := NewStore[string, poolFields]()
	Derive(nodeClaims, s, poolOf, usageOf)
	var mu sync.Mutex
	var changed []string
	s.Subscribe(func(c Change[string, poolFields]) { mu.Lock(); defer mu.Unlock(); changed = append(changed, c.Key) })
	if s.Synced() {
		t.Fatal("the derived store is synced before its source listed")
	}
	go func() { _ = nodeClaims.Start(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for !s.Synced() {
		if time.Now().After(deadline) {
			t.Fatal("the store didn't sync")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := fields(s, "x"); got.CPU != 6 || got.Count != 2 {
		t.Fatalf("x = %+v after Start, want 6 cpu over 2", got)
	}
	informer, err := informers.FakeInformerFor(ctx, &v1.NodeClaim{})
	must(t, err)
	listed := &v1.NodeClaim{}
	must(t, kubeClient.Get(ctx, client.ObjectKey{Name: "a"}, listed))
	informer.Delete(listed)
	if got := fields(s, "x"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("x = %+v after the informer deleted a, want 4 cpu over 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(changed, []string{"x", "x", "x"}) {
		t.Fatalf("notified %v, want x for each listed NodeClaim and x for the delete", changed)
	}
}

func TestDeriveConcurrentChangesConverge(t *testing.T) {
	nodeClaims, s, _ := newStores()
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 1000 {
				nodeClaims.change(nodeClaim(fmt.Sprintf("nc-%d", i%50), fmt.Sprintf("pool-%d", (i+w)%3), uint64(i*4+w+1), "1"), false, w%2 == 0) //nolint:gosec
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
	if total != 50 || cpu != 50 {
		t.Fatalf("the NodePools count %d NodeClaims with %d cpu, want each of the 50 counted once", total, cpu)
	}
}

func TestTouchRecomputesAContributionThatReadsOtherFields(t *testing.T) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	s := NewStore[string, poolFields]()
	cpu := int64(2)
	Derive(nodeClaims, s, poolOf, func(types.NamespacedName, *v1.NodeClaim) usage { return usage{CPU: cpu, Count: 1} })
	nodeClaims.Observe(nodeClaim("a", "x", 1, "0"))
	cpu = 6
	nodeClaims.Observe(nodeClaim("a", "x", 1, "0"))
	if got := fields(s, "x").CPU; got != 2 {
		t.Fatalf("x = %d cpu after observing the version it holds, want 2", got)
	}
	nodeClaims.Touch(types.NamespacedName{Name: "a"})
	if got := fields(s, "x"); got.CPU != 6 || got.Count != 1 {
		t.Fatalf("x = %+v after a touch, want the recomputed 6 cpu over 1", got)
	}
}

func TestClearEmptiesInPlaceAndKeepsSubscriptions(t *testing.T) {
	nodeClaims, s, _ := newStores()
	var removed, added int
	s.Subscribe(func(c Change[string, poolFields]) {
		if c.Existed && !c.Exists {
			removed++
		}
		if !c.Existed && c.Exists {
			added++
		}
	})
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Clear()
	if _, ok := s.Get("x"); ok || removed != 1 {
		t.Fatalf("x is still there, or %d removals notified, once its source cleared; want gone with 1", removed)
	}
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	s.Clear()
	nodeClaims.Observe(nodeClaim("b", "x", 2, "4"))
	if got := fields(s, "x"); got.CPU != 4 || added != 3 {
		t.Fatalf("x = %+v after %d additions, want b's 4 cpu after 3: the cleared store drops a's contribution and keeps notifying", got, added)
	}
	nodeClaims.Forget(named("a"))
	if got := fields(s, "x"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("x = %+v once a, which the cleared store dropped, is forgotten; want b's 4 cpu over 1", got)
	}
}

func TestContributionsKeepTheirLaws(t *testing.T) {
	statetest.ExpectContribution(t, poolFields{CPU: 3, Count: 1}, usage{CPU: 2, Count: 1}, usage{CPU: 8, Count: 1})
	statetest.ExpectContribution(t, clusterFields{CPU: 5}, poolCPU(2), poolCPU(7))
	statetest.ExpectContribution(t, 3, indexed(1), indexed(1))
}

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

func TestDeriveDispatchesUpdateAndDelete(t *testing.T) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	s := NewStore[string, dispatches]()
	Derive(nodeClaims, s, poolOf, func(_ types.NamespacedName, nc *v1.NodeClaim) counted {
		return counted{Name: nc.Name, CPU: nc.Status.Capacity.Cpu().Value()}
	})
	var notes int
	s.Subscribe(func(Change[string, dispatches]) { notes++ })
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	if got := fields(s, "x"); got != (dispatches{Updates: 1, CPU: 2}) {
		t.Fatalf("x = %+v for a new object, want one Update", got)
	}
	nodeClaims.Observe(nodeClaim("a", "x", 2, "4"))
	if got := fields(s, "x"); got != (dispatches{Updates: 2, CPU: 4}) {
		t.Fatalf("x = %+v once its contribution changed, want a second Update", got)
	}
	before := notes
	nodeClaims.Observe(nodeClaim("a", "x", 3, "4"))
	if got := fields(s, "x"); got != (dispatches{Updates: 2, CPU: 4}) || notes != before {
		t.Fatalf("x = %+v after %d notes for an equal contribution, want no call and no note", got, notes-before)
	}
	nodeClaims.Observe(nodeClaim("a", "y", 4, "4"))
	if x, y := fields(s, "x"), fields(s, "y"); x != (dispatches{Updates: 2, Deletes: 1}) || y != (dispatches{Updates: 1, CPU: 4}) {
		t.Fatalf("x = %+v, y = %+v after a moved, want a Delete at x and an Update at y", x, y)
	}
	nodeClaims.Observe(nodeClaim("a", "", 5, "4"))
	if y := fields(s, "y"); y != (dispatches{Updates: 1, Deletes: 1}) {
		t.Fatalf("y = %+v once a's key became zero, want a Delete", y)
	}
	nodeClaims.Observe(nodeClaim("b", "x", 6, "1"))
	nodeClaims.Forget(named("b"))
	nodeClaims.Forget(named("b"))
	if x := fields(s, "x"); x != (dispatches{Updates: 3, Deletes: 2}) {
		t.Fatalf("x = %+v once b was forgotten twice, want one Delete", x)
	}
}

func TestJoinAccessorCopiesUnlessTheCallerOptsOut(t *testing.T) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Observe(nodeClaim("b", "y", 2, "2"))
	accessor := NewJoinAccessor(nodeClaims, func(_ types.NamespacedName, nc *v1.NodeClaim) (*v1.NodeClaim, int, bool) { return nc, 1, nc != nil })
	key := types.NamespacedName{Name: "a"}
	got, ok := accessor.Get(key)
	held, _ := nodeClaims.Get(key)
	if !ok || got.Object == held || got.Object.Name != "a" || got.Derived != 1 {
		t.Fatalf("Get = %+v, want a copy of a with its derived field", got)
	}
	if shared, _ := accessor.Get(key, client.UnsafeDisableDeepCopy); shared.Object != held {
		t.Fatal("Get with UnsafeDisableDeepCopy copied")
	}
	if views, err := accessor.List(client.MatchingLabels{v1.NodePoolLabelKey: "y"}); err != nil || len(views) != 1 || views[0].Object.Name != "b" {
		t.Fatalf("List = %v, %v, want only b", views, err)
	}
}

func TestJoinAccessorListsByAnIndexedField(t *testing.T) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	accessor := NewJoinAccessor(nodeClaims, func(_ types.NamespacedName, nc *v1.NodeClaim) (*v1.NodeClaim, struct{}, bool) {
		return nc, struct{}{}, nc != nil
	}).
		IndexField("pool", func(_ types.NamespacedName, nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] })
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Observe(nodeClaim("b", "y", 2, "2"))
	nodeClaims.Observe(nodeClaim("c", "x", 3, "2"))
	names := func(views []View[*v1.NodeClaim, struct{}]) []string {
		var out []string
		for _, v := range views {
			out = append(out, v.Object.Name)
		}
		slices.Sort(out)
		return out
	}
	views, err := accessor.List(client.MatchingFields{"pool": "x"})
	if err != nil || !slices.Equal(names(views), []string{"a", "c"}) {
		t.Fatalf("List = %v, %v, want a and c", names(views), err)
	}
	nodeClaims.Observe(nodeClaim("c", "y", 4, "2"))
	if views, _ := accessor.List(client.MatchingFields{"pool": "x"}); !slices.Equal(names(views), []string{"a"}) {
		t.Fatalf("List = %v once c moved, want a", names(views))
	}
	if views, err := accessor.List(client.MatchingFields{"pool": "none"}); err != nil || len(views) != 0 {
		t.Fatalf("List = %v, %v for a value nothing indexes to, want nothing", names(views), err)
	}
	if _, err := accessor.List(client.MatchingFields{"unindexed": "x"}); err == nil {
		t.Fatal("List by an unindexed field succeeded, want an error as the client returns")
	}
}
