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

package aggregate

import (
	"fmt"
	"maps"
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

func nodeClaim(name, nodePool string, rv uint64, cpu string) *v1.NodeClaim {
	return &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             types.UID(name),
			ResourceVersion: strconv.FormatUint(rv, 10),
			Labels:          map[string]string{v1.NodePoolLabelKey: nodePool},
		},
		Status: v1.NodeClaimStatus{Capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
	}
}

func nodePoolKey(nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] }

func capacity(nc *v1.NodeClaim) corev1.ResourceList { return nc.Status.Capacity }

func newAggregate() *Aggregate[*v1.NodeClaim, corev1.ResourceList] {
	return New(nil, nil, &v1.NodeClaimList{}, nodePoolKey, Resources(capacity))
}

func cpu(a *Aggregate[*v1.NodeClaim, corev1.ResourceList], key string) (int64, int) {
	sum, count := a.Get(key)
	return sum.Cpu().Value(), count
}

type maxCPU struct{ cpu map[types.UID]int64 }

func (m *maxCPU) Update(nc *v1.NodeClaim) bool {
	next := nc.Status.Capacity.Cpu().Value()
	prev, ok := m.cpu[nc.UID]
	m.cpu[nc.UID] = next
	return !ok || prev != next
}

func (m *maxCPU) Delete(uid types.UID) bool {
	_, ok := m.cpu[uid]
	delete(m.cpu, uid)
	return ok
}

func (m *maxCPU) Result() int64 {
	return slices.Max(slices.Collect(maps.Values(m.cpu)))
}

func TestAggregateSumsAndMovesObjectsBetweenKeys(t *testing.T) {
	a := newAggregate()
	a.apply(nodeClaim("a", "x", 1, "2"), false, true)
	a.apply(nodeClaim("b", "x", 2, "4"), false, true)
	if got, n := cpu(a, "x"); got != 6 || n != 2 {
		t.Fatalf("x = %d cpu over %d objects, want 6 over 2", got, n)
	}
	a.apply(nodeClaim("a", "y", 3, "2"), false, true)
	if got, n := cpu(a, "x"); got != 4 || n != 1 {
		t.Fatalf("x = %d cpu over %d objects after a moved, want 4 over 1", got, n)
	}
	if got, n := cpu(a, "y"); got != 2 || n != 1 {
		t.Fatalf("y = %d cpu over %d objects after a moved, want 2 over 1", got, n)
	}
	a.apply(nodeClaim("a", "y", 4, "8"), false, true)
	if got, n := cpu(a, "y"); got != 8 || n != 1 {
		t.Fatalf("y = %d cpu over %d objects after a grew, want 8 over 1", got, n)
	}
	a.apply(nodeClaim("a", "y", 5, "8"), true, true)
	if _, n := cpu(a, "y"); n != 0 {
		t.Fatalf("y counts %d objects after the informer deleted a, want 0", n)
	}
}

func TestAggregateDoesNotCountObjectsWithoutAKey(t *testing.T) {
	a := newAggregate()
	a.apply(nodeClaim("a", "", 1, "2"), false, true)
	if len(a.groups) != 0 {
		t.Fatalf("groups = %v, want none for an object without a key", a.groups)
	}
}

func TestAggregateKeepsTheNewestVersionAcrossWritesAndTheInformer(t *testing.T) {
	a := newAggregate()
	a.apply(nodeClaim("a", "x", 5, "8"), false, false)
	a.apply(nodeClaim("a", "x", 3, "2"), false, true)
	if got, _ := cpu(a, "x"); got != 8 {
		t.Fatalf("x = %d after the informer delivered an older version than the write, want 8", got)
	}
	a.apply(nodeClaim("a", "x", 5, "8"), false, true)
	if got, n := cpu(a, "x"); got != 8 || n != 1 {
		t.Fatalf("x = %d over %d objects after the informer delivered the write, want 8 over 1", got, n)
	}
	a.apply(nodeClaim("a", "x", 6, "4"), false, true)
	if got, _ := cpu(a, "x"); got != 4 {
		t.Fatalf("x = %d after another writer's newer version, want 4", got)
	}
}

func TestAggregateIgnoresOlderVersionsOfAnObjectItDeleted(t *testing.T) {
	a := newAggregate()
	a.apply(nodeClaim("a", "x", 1, "2"), false, true)
	a.apply(nodeClaim("a", "x", 1, "2"), true, false)
	if _, n := cpu(a, "x"); n != 0 {
		t.Fatalf("x counts %d objects once a write deleted a, want 0", n)
	}
	a.apply(nodeClaim("a", "x", 2, "2"), false, true)
	if _, n := cpu(a, "x"); n != 0 {
		t.Fatalf("x counts %d objects after the informer delivered an update from before the delete, want 0", n)
	}
	a.apply(nodeClaim("a", "x", 2, "2"), true, true)
	if len(a.objects) != 0 {
		t.Fatalf("objects = %v once the informer delivered the delete, want none", a.objects)
	}
}

func TestAggregateCountsAnObjectRecreatedWithTheSameNameAsANewObject(t *testing.T) {
	a := newAggregate()
	a.apply(nodeClaim("a", "x", 1, "2"), false, true)
	a.apply(nodeClaim("a", "x", 1, "2"), true, false)
	recreated := nodeClaim("a", "x", 7, "4")
	recreated.UID = "a-2"
	a.apply(recreated, false, false)
	a.apply(nodeClaim("a", "x", 2, "2"), true, true)
	if got, n := cpu(a, "x"); got != 4 || n != 1 {
		t.Fatalf("x = %d over %d objects after the old object's delete, want the recreated object's 4 over 1", got, n)
	}
}

func TestAggregateForgetsAnObjectByName(t *testing.T) {
	a := newAggregate()
	a.Apply(nodeClaim("a", "x", 1, "2"))
	a.Apply(nodeClaim("b", "x", 2, "4"))
	a.Forget(types.NamespacedName{Name: "a"})
	if got, n := cpu(a, "x"); got != 4 || n != 1 {
		t.Fatalf("x = %d over %d objects once a is forgotten, want 4 over 1", got, n)
	}
	a.Forget(types.NamespacedName{Name: "missing"})
	recreated := nodeClaim("b", "x", 3, "8")
	recreated.UID = "b-2"
	a.Apply(recreated)
	a.apply(nodeClaim("b", "x", 4, "4"), true, true)
	if got, n := cpu(a, "x"); got != 8 || n != 1 {
		t.Fatalf("x = %d over %d objects after the old b's delete, want the recreated b's 8 over 1", got, n)
	}
	a.Forget(types.NamespacedName{Name: "b"})
	if len(a.objects) != 0 || len(a.names) != 0 || len(a.groups) != 0 {
		t.Fatalf("objects %v, names %v, groups %v once everything is forgotten, want none", a.objects, a.names, a.groups)
	}
}

func TestAggregateNotifiesChangedKeysOnceSynced(t *testing.T) {
	var changed []string
	a := newAggregate()
	a.Subscribe(func(key string) { changed = append(changed, key) })
	a.apply(nodeClaim("a", "x", 1, "2"), false, true)
	if len(changed) != 0 {
		t.Fatalf("notified %v before the aggregate synced", changed)
	}
	a.synced = true
	a.apply(nodeClaim("a", "y", 2, "2"), false, true)
	a.apply(nodeClaim("a", "y", 3, "2"), false, true)
	if !slices.Equal(changed, []string{"x", "y"}) {
		t.Fatalf("notified %v, want [x y] for the move and nothing for a version that keeps the value", changed)
	}
}

func TestAggregateKeepsAnyValue(t *testing.T) {
	a := New(nil, nil, &v1.NodeClaimList{}, nodePoolKey, func() Value[*v1.NodeClaim, int64] { return &maxCPU{cpu: map[types.UID]int64{}} })
	a.apply(nodeClaim("a", "x", 1, "2"), false, true)
	a.apply(nodeClaim("b", "x", 2, "8"), false, true)
	if got, _ := a.Get("x"); got != 8 {
		t.Fatalf("max cpu in x = %d, want 8", got)
	}
	a.apply(nodeClaim("b", "x", 3, "8"), true, true)
	if got, _ := a.Get("x"); got != 2 {
		t.Fatalf("max cpu in x = %d once b is gone, want 2", got)
	}
	if got, n := a.Get("y"); got != 0 || n != 0 {
		t.Fatalf("y = %d over %d objects, want the zero value for a key nothing counts toward", got, n)
	}
}

func TestAggregateResultsDoNotChangeOnceRead(t *testing.T) {
	a := newAggregate()
	a.apply(nodeClaim("a", "x", 1, "2"), false, true)
	read, _ := a.Get("x")
	a.apply(nodeClaim("b", "x", 2, "4"), false, true)
	a.apply(nodeClaim("a", "x", 3, "8"), false, true)
	if got := read.Cpu().Value(); got != 2 {
		t.Fatalf("a sum read before later changes became %d, want it to stay 2", got)
	}
}

func TestAggregateStartAppliesTheListAndFollowsTheInformer(t *testing.T) {
	ctx := t.Context()
	a0, b0 := nodeClaim("a", "x", 0, "2"), nodeClaim("b", "x", 0, "4")
	a0.ResourceVersion, b0.ResourceVersion = "", ""
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(a0, b0).Build()
	informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
	var mu sync.Mutex
	var changed []string
	a := New(kubeClient, informers, &v1.NodeClaimList{}, nodePoolKey, Resources(capacity))
	a.Subscribe(func(key string) { mu.Lock(); defer mu.Unlock(); changed = append(changed, key) })
	go func() { _ = a.Start(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for !a.Synced() {
		if time.Now().After(deadline) {
			t.Fatal("the aggregate didn't sync")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, n := cpu(a, "x"); got != 6 || n != 2 {
		t.Fatalf("x = %d over %d objects after Start, want 6 over 2", got, n)
	}
	informer, err := informers.FakeInformerFor(ctx, &v1.NodeClaim{})
	if err != nil {
		t.Fatal(err)
	}
	listed := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: "a"}, listed); err != nil {
		t.Fatal(err)
	}
	informer.Delete(listed)
	if got, n := cpu(a, "x"); got != 4 || n != 1 {
		t.Fatalf("x = %d over %d objects after the informer deleted a, want 4 over 1", got, n)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(changed, []string{"x", "x"}) {
		t.Fatalf("notified %v, want x on sync and x on the delete", changed)
	}
}

func TestAggregateConcurrentAppliesConverge(t *testing.T) {
	a := newAggregate()
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 1000 {
				name := fmt.Sprintf("nc-%d", i%50)
				pool := fmt.Sprintf("pool-%d", (i+w)%3)
				a.apply(nodeClaim(name, pool, uint64(i*4+w+1), "1"), false, w%2 == 0) //nolint:gosec
			}
		})
	}
	wg.Wait()
	total, cpus := 0, int64(0)
	for key := range a.groups {
		sum, n := a.Get(key)
		total += n
		cpus += sum.Cpu().Value()
	}
	if total != 50 || cpus != 50 {
		t.Fatalf("the groups count %d objects with %d cpu, want each of the 50 objects counted once", total, cpus)
	}
}
