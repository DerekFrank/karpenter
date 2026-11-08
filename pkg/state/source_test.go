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
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func newQueue(t *testing.T) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	t.Helper()
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(queue.ShutDown)
	return queue
}

// drain returns the names queued, once each, in the order they were first queued.
func drain(queue workqueue.TypedRateLimitingInterface[reconcile.Request]) []string {
	var names []string
	for queue.Len() > 0 {
		req, _ := queue.Get()
		names = append(names, req.Name)
		queue.Done(req)
		queue.Forget(req)
	}
	return names
}

// latest is a key's contribution of a value that replaces the key's fields.
type latest int

func (l latest) Update(int, latest) int { return int(l) }
func (l latest) Delete(int) int         { return 0 }

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
	Derive(nodes, pods, func(key string, _ usage) string { return key }, func(_ string, f usage) latest { return latest(f.Pods) })
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

func TestStoreSourceWaitsForItsStoreToSync(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	nodeClaims := NewObjectStore[*v1.NodeClaim](kubeClient, &informertest.FakeInformers{Scheme: scheme.Scheme})
	pools := NewStore[string, poolFields]()
	Derive(nodeClaims, pools, poolOf, usageOf)
	waitCtx, waitCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer waitCancel()
	if err := pools.Source().WaitForSync(waitCtx); err == nil {
		t.Fatal("WaitForSync returned before the store its fields derive from listed")
	}
	go func() { _ = nodeClaims.Start(ctx) }()
	must(t, pools.Source().WaitForSync(ctx))
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

func TestAccessorEnqueuesObjectsWhoseDerivedFieldsChange(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	accessor, counts, _ := nodePoolAccessor(t, "a", "b", "c")
	counts.Update("a", func(int) int { return 1 })
	queue := newQueue(t)
	must(t, accessor.Start(ctx, queue))
	if got := drain(queue); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Start queued %v, want a, the one NodePool with derived fields", got)
	}
	counts.Update("b", func(int) int { return 2 })
	counts.Update("b", func(int) int { return 3 })
	counts.Update("a", func(int) int { return 0 })
	counts.Update("c", func(int) int { return 0 })
	if got := drain(queue); len(got) != 2 || got[0] != "b" || got[1] != "a" {
		t.Fatalf("queued %v, want b once for its two changes, then a for its fields going away, and not c, which didn't change", got)
	}
}

func TestAccessorStopsEnqueueingOnceItsContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	accessor, counts, _ := nodePoolAccessor(t, "a")
	queue := newQueue(t)
	must(t, accessor.Start(ctx, queue))
	cancel()
	deadline := time.Now().Add(10 * time.Second)
	for {
		counts.Update("a", func(n int) int { return n + 1 })
		if drain(queue) == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the accessor still queues changes after its context is done")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
