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
)

var ctx = context.Background()

type poolFields struct {
	NodePool *v1.NodePool
	CPU      int64
	Count    int
	Reserved int
}

type usage struct {
	CPU   int64
	Count int
}

type pools = Store[string, poolFields]

func nodePool(rv uint64) *v1.NodePool {
	return &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "x", UID: "x", ResourceVersion: strconv.FormatUint(rv, 10)}}
}

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

func usageOf(_ context.Context, nc *v1.NodeClaim) (usage, error) {
	return usage{CPU: nc.Status.Capacity.Cpu().Value(), Count: 1}, nil
}

func addUsage(f poolFields, prev, next usage) poolFields {
	f.CPU += next.CPU - prev.CPU
	f.Count += next.Count - prev.Count
	return f
}

func newStores() (*pools, *Contributor[string, *v1.NodePool, poolFields, *v1.NodePool], *Contributor[string, *v1.NodeClaim, poolFields, usage]) {
	s := New[string, poolFields]()
	nodePools := Contribute(s, nil, nil, &v1.NodePoolList{},
		func(np *v1.NodePool) string { return np.Name },
		func(_ context.Context, np *v1.NodePool) (*v1.NodePool, error) { return np, nil },
		func(f poolFields, _, next *v1.NodePool) poolFields { f.NodePool = next; return f })
	nodeClaims := Contribute(s, nil, nil, &v1.NodeClaimList{},
		func(nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] }, usageOf, addUsage)
	return s, nodePools, nodeClaims
}

func fields[F any](s *Store[string, F], key string) F {
	f, _ := s.Get(key)
	return f
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreKeepsFieldsWhenItsObjectIsReplaced(t *testing.T) {
	s, nodePools, nodeClaims := newStores()
	must(t, nodePools.Apply(ctx, nodePool(1)))
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")))
	must(t, nodePools.Apply(ctx, nodePool(2)))
	if f := fields(s, "x"); f.NodePool.ResourceVersion != "2" || f.CPU != 2 {
		t.Fatalf("fields = %+v after the NodePool changed, want the new NodePool with the NodeClaim's 2 cpu", f)
	}
}

func TestStoreHoldsFieldsBeforeTheirObjectArrives(t *testing.T) {
	s, nodePools, nodeClaims := newStores()
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")))
	if f, ok := s.Get("x"); !ok || f.NodePool != nil || f.CPU != 2 {
		t.Fatalf("fields = %+v, want the NodeClaim's 2 cpu without a NodePool", f)
	}
	must(t, nodePools.Apply(ctx, nodePool(1)))
	if f := fields(s, "x"); f.NodePool == nil || f.CPU != 2 {
		t.Fatalf("fields = %+v once the NodePool arrived, want it with the NodeClaim's 2 cpu", f)
	}
}

func TestStoreJoinsKindsOnAKeyOtherThanTheirNames(t *testing.T) {
	type joined struct {
		NodeClaim *v1.NodeClaim
		Node      *corev1.Node
	}
	s := New[string, joined]()
	nodeClaims := Contribute(s, nil, nil, &v1.NodeClaimList{},
		func(nc *v1.NodeClaim) string { return nc.Status.ProviderID },
		func(_ context.Context, nc *v1.NodeClaim) (*v1.NodeClaim, error) { return nc, nil },
		func(f joined, _, next *v1.NodeClaim) joined { f.NodeClaim = next; return f })
	nodes := Contribute(s, nil, nil, &corev1.NodeList{},
		func(n *corev1.Node) string { return n.Spec.ProviderID },
		func(_ context.Context, n *corev1.Node) (*corev1.Node, error) { return n, nil },
		func(f joined, _, next *corev1.Node) joined { f.Node = next; return f })
	nc := nodeClaim("a", "x", 1, "2")
	must(t, nodeClaims.Apply(ctx, nc))
	for range s.List() {
		t.Fatal("a NodeClaim without a providerID joined something")
	}
	nc = nc.DeepCopy()
	nc.ResourceVersion, nc.Status.ProviderID = "2", "fake://a"
	must(t, nodeClaims.Apply(ctx, nc))
	must(t, nodes.Apply(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-a", ResourceVersion: "3"}, Spec: corev1.NodeSpec{ProviderID: "fake://a"}}))
	if f := fields(s, "fake://a"); f.NodeClaim == nil || f.Node == nil || f.Node.Name != "node-a" {
		t.Fatalf("joined = %+v, want the NodeClaim and its Node", f)
	}
	nodes.Forget(types.NamespacedName{Name: "node-a"})
	nodeClaims.Forget(types.NamespacedName{Name: "a"})
	if _, ok := s.Get("fake://a"); ok {
		t.Fatal("the join is still there once both sides are gone")
	}
}

func TestStoreDropsAKeyOnceNothingContributesAndItsFieldsAreZero(t *testing.T) {
	s, nodePools, nodeClaims := newStores()
	must(t, nodePools.Apply(ctx, nodePool(1)))
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "0")))
	nodePools.Forget(types.NamespacedName{Name: "x"})
	if _, ok := s.Get("x"); !ok {
		t.Fatal("dropped the key while a NodeClaim still contributes to it, even though its fields are zero")
	}
	nodeClaims.Forget(types.NamespacedName{Name: "a"})
	if f, ok := s.Get("x"); ok {
		t.Fatalf("fields = %+v once nothing is left, want none", f)
	}
}

func TestStoreFieldsDoNotChangeOnceRead(t *testing.T) {
	s, _, nodeClaims := newStores()
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")))
	read, _ := s.Get("x")
	must(t, nodeClaims.Apply(ctx, nodeClaim("b", "x", 2, "4")))
	s.Update("x", func(f poolFields) poolFields { f.Reserved = 3; return f })
	if read != (poolFields{CPU: 2, Count: 1}) {
		t.Fatalf("fields read before later changes became %+v", read)
	}
}

func TestStoreUpdatesAtomically(t *testing.T) {
	s, _, _ := newStores()
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() { s.Update("x", func(f poolFields) poolFields { f.Reserved++; return f }) })
	}
	wg.Wait()
	if got := fields(s, "x").Reserved; got != 100 {
		t.Fatalf("reserved = %d after 100 concurrent updates, want 100", got)
	}
}

func TestContributorMovesContributionsBetweenKeys(t *testing.T) {
	s, _, nodeClaims := newStores()
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")))
	must(t, nodeClaims.Apply(ctx, nodeClaim("b", "x", 2, "4")))
	if got := fields(s, "x"); got.CPU != 6 || got.Count != 2 {
		t.Fatalf("x = %+v, want 6 cpu over 2 NodeClaims", got)
	}
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "y", 3, "2")))
	if got := fields(s, "x"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("x = %+v after a moved, want 4 cpu over 1", got)
	}
	if got := fields(s, "y"); got.CPU != 2 || got.Count != 1 {
		t.Fatalf("y = %+v after a moved, want 2 cpu over 1", got)
	}
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "", 4, "2")))
	if _, ok := s.Get("y"); ok {
		t.Fatal("y still has fields once its only NodeClaim stopped contributing")
	}
}

func TestContributorKeepsTheNewestVersionAcrossWritesAndTheInformer(t *testing.T) {
	s, _, nodeClaims := newStores()
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 5, "8"), false, false))
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 3, "2"), false, true))
	if got := fields(s, "x").CPU; got != 8 {
		t.Fatalf("x = %d cpu after the informer delivered an older version than the write, want 8", got)
	}
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 6, "4"), false, true))
	if got := fields(s, "x"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("x = %+v after another writer's newer version, want 4 cpu over 1", got)
	}
}

func TestContributorIgnoresOlderVersionsOfAnObjectItDeleted(t *testing.T) {
	s, _, nodeClaims := newStores()
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 1, "2"), false, true))
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 1, "2"), true, false))
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 2, "2"), false, true))
	if got := fields(s, "x").Count; got != 0 {
		t.Fatalf("x counts %d NodeClaims after the informer delivered an update from before the delete, want 0", got)
	}
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 2, "2"), true, true))
	if len(nodeClaims.sources.versions) != 0 || len(nodeClaims.contributions) != 0 {
		t.Fatalf("versions %v, contributions %v once the informer delivered the delete, want none", nodeClaims.sources.versions, nodeClaims.contributions)
	}
}

func TestContributorCountsAnObjectRecreatedWithTheSameNameAsANewObject(t *testing.T) {
	s, _, nodeClaims := newStores()
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 1, "2"), false, true))
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 1, "2"), true, false))
	recreated := nodeClaim("a", "x", 7, "4")
	recreated.UID = "a-2"
	must(t, nodeClaims.Apply(ctx, recreated))
	must(t, nodeClaims.change(ctx, nodeClaim("a", "x", 2, "2"), true, true))
	if got := fields(s, "x"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("x = %+v after the old object's delete, want the recreated object's 4 cpu over 1", got)
	}
	nodeClaims.Forget(types.NamespacedName{Name: "a"})
	if got := fields(s, "x"); got.Count != 0 {
		t.Fatalf("x = %+v once a is forgotten, want nothing", got)
	}
}

func TestContributorReturnsContributionErrorsWithoutApplying(t *testing.T) {
	s := New[string, poolFields]()
	nodeClaims := Contribute(s, nil, nil, &v1.NodeClaimList{},
		func(nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] },
		func(context.Context, *v1.NodeClaim) (usage, error) { return usage{}, fmt.Errorf("volumes unavailable") },
		addUsage)
	if err := nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")); err == nil {
		t.Fatal("want the contribution's error")
	}
	if _, ok := s.Get("x"); ok || len(nodeClaims.sources.versions) != 0 {
		t.Fatal("applied a version whose contribution failed, so a retry with the same version would be dropped")
	}
}

func TestStoreNotifiesOnChange(t *testing.T) {
	s, _, nodeClaims := newStores()
	type note struct {
		key      string
		old, new int64
	}
	var notes []note
	s.Subscribe(func(key string, old, new poolFields) { notes = append(notes, note{key, old.CPU, new.CPU}) })
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")))
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 2, "2")))
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "y", 3, "2")))
	s.Update("y", func(f poolFields) poolFields { return f })
	if !slices.Equal(notes, []note{{"x", 0, 2}, {"x", 2, 0}, {"y", 0, 2}}) {
		t.Fatalf("notified %v, want x, then x and y for the move, and nothing for changes that keep the fields", notes)
	}
}

func TestDeriveFollowsAnotherStore(t *testing.T) {
	s, _, nodeClaims := newStores()
	type cluster struct{ CPU int64 }
	total := New[string, cluster]()
	Derive(s, total, func(string, poolFields) string { return "all" }, func(_ string, f poolFields) int64 { return f.CPU },
		func(f cluster, prev, next int64) cluster { f.CPU += next - prev; return f })
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")))
	must(t, nodeClaims.Apply(ctx, nodeClaim("b", "y", 2, "4")))
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 3, "8")))
	if got := fields(total, "all").CPU; got != 12 {
		t.Fatalf("total = %d cpu, want 12", got)
	}
	nodeClaims.Forget(types.NamespacedName{Name: "a"})
	nodeClaims.Forget(types.NamespacedName{Name: "b"})
	if _, ok := total.Get("all"); ok {
		t.Fatal("total still has fields once nothing contributes")
	}
}

func TestContributorStartAppliesTheListAndFollowsTheInformer(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	a0, b0 := nodeClaim("a", "x", 0, "2"), nodeClaim("b", "x", 0, "4")
	a0.ResourceVersion, b0.ResourceVersion = "", ""
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(a0, b0).Build()
	informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
	s := New[string, poolFields]()
	nodeClaims := Contribute(s, kubeClient, informers, &v1.NodeClaimList{},
		func(nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] }, usageOf, addUsage)
	var mu sync.Mutex
	var changed []string
	s.Subscribe(func(key string, _, _ poolFields) { mu.Lock(); defer mu.Unlock(); changed = append(changed, key) })
	go func() { _ = nodeClaims.Start(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for !nodeClaims.Synced() {
		if time.Now().After(deadline) {
			t.Fatal("the contributor didn't sync")
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

func TestContributorConcurrentChangesConverge(t *testing.T) {
	s, _, nodeClaims := newStores()
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 1000 {
				_ = nodeClaims.change(ctx, nodeClaim(fmt.Sprintf("nc-%d", i%50), fmt.Sprintf("pool-%d", (i+w)%3), uint64(i*4+w+1), "1"), false, w%2 == 0) //nolint:gosec
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

func TestContributorReplacesAnObjectRecreatedUnderItsNameWhenItsDeleteIsMissed(t *testing.T) {
	s, _, nodeClaims := newStores()
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")))
	recreated := nodeClaim("a", "y", 5, "4")
	recreated.UID = "a-2"
	must(t, nodeClaims.Apply(ctx, recreated))
	if _, ok := s.Get("x"); ok {
		t.Fatal("the replaced object still contributes to x")
	}
	if got := fields(s, "y"); got.CPU != 4 || got.Count != 1 {
		t.Fatalf("y = %+v, want the recreated object's 4 cpu over 1", got)
	}
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 3, "2")))
	if _, ok := s.Get("x"); ok {
		t.Fatal("a late version of the replaced object contributed again")
	}
}

func TestContributorReplaceHealsMissedEventsForATarget(t *testing.T) {
	s, _, nodeClaims := newStores()
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "2")))
	must(t, nodeClaims.Apply(ctx, nodeClaim("b", "x", 2, "4")))
	must(t, nodeClaims.Replace(ctx, "x", []*v1.NodeClaim{nodeClaim("b", "x", 2, "4"), nodeClaim("c", "x", 3, "8")}))
	if got := fields(s, "x"); got.CPU != 12 || got.Count != 2 {
		t.Fatalf("x = %+v, want b's 4 and c's 8 cpu over 2 once a is left out of the listing", got)
	}
	must(t, nodeClaims.Replace(ctx, "x", nil))
	if _, ok := s.Get("x"); ok {
		t.Fatal("x still has fields once the listing is empty")
	}
}

func TestContributorRefreshRecomputesTheVersionItHolds(t *testing.T) {
	s := New[string, poolFields]()
	cpu := int64(2)
	nodeClaims := Contribute(s, nil, nil, &v1.NodeClaimList{},
		func(nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] },
		func(context.Context, *v1.NodeClaim) (usage, error) { return usage{CPU: cpu, Count: 1}, nil },
		addUsage)
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "0")))
	cpu = 6
	must(t, nodeClaims.Apply(ctx, nodeClaim("a", "x", 1, "0")))
	if got := fields(s, "x").CPU; got != 2 {
		t.Fatalf("x = %d cpu after applying the version it holds, want 2", got)
	}
	must(t, nodeClaims.Refresh(ctx, nodeClaim("a", "x", 1, "0")))
	if got := fields(s, "x"); got.CPU != 6 || got.Count != 1 {
		t.Fatalf("x = %+v after a refresh, want the recomputed 6 cpu over 1", got)
	}
	must(t, nodeClaims.Refresh(ctx, nodeClaim("a", "x", 0, "0")))
	if got := fields(s, "x").CPU; got != 6 {
		t.Fatalf("x = %d cpu after refreshing an older version, want 6", got)
	}
}
