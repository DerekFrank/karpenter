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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func TestStoreSourceEnqueuesItsKeysAndTheirChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	counts := NewStore[types.NamespacedName, int]()
	a, b := types.NamespacedName{Namespace: "ns", Name: "a"}, types.NamespacedName{Namespace: "ns", Name: "b"}
	counts.Update(a, func(int) int { return 1 })
	queue := newQueue(t)
	must(t, counts.Source().Start(ctx, queue))
	counts.Update(b, func(int) int { return 1 })
	counts.Update(b, func(int) int { return 1 })
	counts.Update(a, func(int) int { return 0 })
	req1, _ := queue.Get()
	req2, _ := queue.Get()
	if req1.NamespacedName != a || req2.NamespacedName != b || queue.Len() != 0 {
		t.Fatalf("queued %v, %v and %d more, want ns/a (held at Start, then gone) and ns/b once, as its second write changed nothing", req1, req2, queue.Len())
	}
}

func TestStoreSourceRejectsKeysThatCantNameARequest(t *testing.T) {
	if err := NewStore[int, int]().Source().Start(ctx, newQueue(t)); err == nil {
		t.Fatal("started a source over a store keyed by int")
	}
}

// A consumer that cares about part of a store's fields watches a store derived with only that part: the derived store
// keeps its fields when the rest changes, so it notifies only on the changes the consumer acts on.
func TestADerivedStoreNotifiesOnlyWhenItsNarrowerFieldsChange(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type usage struct{ Pods, CPU int }
	nodes := NewStore[string, usage]()
	pods := NewStore[string, int]()
	Derive(nodes, pods, func(key string, _ usage) string { return key }, func(_ string, f usage) int { return f.Pods },
		func(_ int, _, next int) int { return next })
	queue := newQueue(t)
	must(t, pods.Source().Start(ctx, queue))
	nodes.Update("a", func(usage) usage { return usage{Pods: 1, CPU: 1} })
	nodes.Update("a", func(usage) usage { return usage{Pods: 1, CPU: 2} })
	if got := drain(queue); len(got) != 1 || got[0] != "a" {
		t.Fatalf("queued %v, want a once for its pod, and not for its CPU changing", got)
	}
	nodes.Update("a", func(usage) usage { return usage{} })
	if got := drain(queue); len(got) != 1 || got[0] != "a" {
		t.Fatalf("queued %v, want a as its pods went away", got)
	}
}

func TestAStoreFedFromElsewhereIsSynced(t *testing.T) {
	s, _, _ := newStores()
	if !s.Synced() {
		t.Fatal("a store whose Contributors have no client, so are fed through Observe by whatever follows their kind, isn't synced")
	}
}

func TestStoreIsSyncedOnceItsFeedsAre(t *testing.T) {
	from, to := NewStore[string, int](), NewStore[string, int]()
	s := &synced{}
	from.feedBy(s)
	Derive(from, to, func(key string, _ int) string { return key }, func(_ string, f int) int { return f },
		func(_ int, _, next int) int { return next })
	if !NewStore[string, int]().Synced() {
		t.Fatal("a store nothing feeds isn't synced")
	}
	if from.Synced() || to.Synced() {
		t.Fatal("a store, or one derived from it, is synced before its feed")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := to.Source().WaitForSync(waitCtx); err == nil {
		t.Fatal("WaitForSync returned before the store synced")
	}
	s.Store(true)
	if !from.Synced() || !to.Synced() {
		t.Fatal("a store, or one derived from it, isn't synced once its feed is")
	}
	must(t, to.Source().WaitForSync(ctx))
}

func TestWakeFoldsEveryChangeIntoOneRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	a, b := NewStore[string, int](), NewStore[string, string]()
	a.Update("x", func(int) int { return 1 })
	queue := newQueue(t)
	must(t, Wake(a.Source(), b.Source()).Start(ctx, queue))
	a.Update("y", func(int) int { return 1 })
	b.Update("z", func(string) string { return "set" })
	if got := drain(queue); len(got) != 1 || got[0] != "" {
		t.Fatalf("queued %q, want one empty request for the existing key and both changes", got)
	}
}

func TestAfterDelaysAndFoldsChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	counts := NewStore[string, int]()
	queue := newQueue(t)
	must(t, After(100*time.Millisecond, counts.Source()).Start(ctx, queue))
	for i := 1; i <= 5; i++ {
		counts.Update("a", func(int) int { return i })
	}
	if queue.Len() != 0 {
		t.Fatal("queued a change before its delay")
	}
	time.Sleep(300 * time.Millisecond)
	if got := drain(queue); len(got) != 1 || got[0] != "a" {
		t.Fatalf("queued %v, want a once for its five changes", got)
	}
}
