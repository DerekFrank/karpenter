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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func newQueue() workqueue.TypedRateLimitingInterface[reconcile.Request] {
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	ginkgo.DeferCleanup(queue.ShutDown)
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

func set[K comparable, O any](s *Store[K, O, struct{}, struct{}], key K, obj O) {
	s.Feed(key, func(O, bool) (O, bool) { return obj, true })
}

func unset[K comparable, O any](s *Store[K, O, struct{}, struct{}], key K) {
	s.Feed(key, func(o O, _ bool) (O, bool) { return o, false })
}

// latest is a key's contribution of a value that replaces the key's fields.
type latest int

func (l latest) Update(int, latest) int { return int(l) }
func (l latest) Delete(int) int         { return 0 }

var _ = ginkgo.Describe("Store Source", func() {
	ginkgo.It("should enqueue its keys and their changes", func() {
		// Wake: a store keyed by something other than a name enqueues one empty request.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		counts := NewFedStore[types.NamespacedName, int, struct{}, struct{}]()
		a, b := types.NamespacedName{Namespace: "ns", Name: "a"}, types.NamespacedName{Namespace: "ns", Name: "b"}
		set(counts, a, 1)
		queue := newQueue()
		Expect(counts.Source().Start(ctx, queue)).To(Succeed())
		set(counts, b, 1)
		set(counts, b, 1)
		unset(counts, a)
		req1, _ := queue.Get()
		req2, _ := queue.Get()
		Expect(req1.NamespacedName).To(Equal(a), "want ns/a first, held at Start, then gone")
		Expect(req2.NamespacedName).To(Equal(b), "want ns/b second")
		Expect(queue.Len()).To(Equal(0), "want ns/b once, as its second write changed nothing")
	})
	ginkgo.It("should enqueue only keys that appear, disappear, or whose narrowed fields change, with SourceOn", func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		type fields struct{ Narrow, Other int }
		store := NewFedStore[string, fields, struct{}, struct{}]()
		set(store, "held", fields{Narrow: 1})
		queue := newQueue()
		Expect(store.SourceOn(func(_ string, e Entry[fields, struct{}, struct{}]) any { return e.Object.Narrow }).Start(ctx, queue)).To(Succeed())
		Expect(drain(queue)).To(Equal([]string{"held"}), "want every key held at Start")

		set(store, "held", fields{Narrow: 1, Other: 2})
		Expect(drain(queue)).To(BeEmpty(), "want nothing for a change outside the narrowed fields")
		set(store, "held", fields{Narrow: 3, Other: 2})
		Expect(drain(queue)).To(Equal([]string{"held"}), "want the key whose narrowed fields changed")
		set(store, "new", fields{Other: 1})
		Expect(drain(queue)).To(Equal([]string{"new"}), "want a key that appears, whatever its narrowed fields")
		unset(store, "new")
		Expect(drain(queue)).To(Equal([]string{"new"}), "want a key that disappears")
	})
	ginkgo.It("should reject keys that can't name a request", func() {
		Expect(NewFedStore[int, int, struct{}, struct{}]().Source().Start(ctx, newQueue())).ToNot(Succeed(), "started a source over a store keyed by int")
	})
	// A Derive's target notifies only when what's contributed changes.
	ginkgo.It("should notify from a synthetic store only when its contributions change", func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		type usage struct{ Pods, CPU int }
		nodes := NewFedStore[string, usage, struct{}, struct{}]()
		pods := NewSyntheticStore[string, string, int, struct{}](func(k string) string { return k })
		Derive(nodes.Reader, pods, func(key string, e Entry[usage, struct{}, struct{}]) string {
			return lo.Ternary(e.Object.Pods > 0, key, "")
		},
			func(_ string, e Entry[usage, struct{}, struct{}]) latest { return latest(e.Object.Pods) })
		queue := newQueue()
		Expect(pods.Source().Start(ctx, queue)).To(Succeed())
		set(nodes, "a", usage{Pods: 1, CPU: 1})
		set(nodes, "a", usage{Pods: 1, CPU: 2})
		Expect(drain(queue)).To(Equal([]string{"a"}), "want a once for its pod, and not for its CPU changing")
		unset(nodes, "a")
		Expect(drain(queue)).To(Equal([]string{"a"}), "want a as its pods went away")
	})
	ginkgo.It("should wait for its store to sync", func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
		nodeClaims := NewObjectStore[*v1.NodeClaim, struct{}, nodeClaimIntent](kubeClient, &informertest.FakeInformers{Scheme: scheme.Scheme})
		pools := newPools()
		Derive(nodeClaims.Reader, pools, poolOf, usageOf)
		waitCtx, waitCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer waitCancel()
		Expect(pools.Source().WaitForSync(waitCtx)).ToNot(Succeed(), "WaitForSync returned before the store its fields derive from listed")
		go func() { _ = nodeClaims.Start(ctx) }()
		Expect(pools.Source().WaitForSync(ctx)).To(Succeed())
	})
})

var _ = ginkgo.Describe("Wake", func() {
	ginkgo.It("should fold every change into one request", func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		a, b := NewFedStore[string, int, struct{}, struct{}](), NewFedStore[string, string, struct{}, struct{}]()
		set(a, "x", 1)
		queue := newQueue()
		Expect(Wake(a.Source(), b.Source()).Start(ctx, queue)).To(Succeed())
		set(a, "y", 1)
		set(b, "z", "set")
		Expect(drain(queue)).To(Equal([]string{""}), "want one empty request for the existing key and both changes")
	})
})

var _ = ginkgo.Describe("After", func() {
	ginkgo.It("should delay and fold changes", func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		counts := NewFedStore[string, int, struct{}, struct{}]()
		queue := newQueue()
		Expect(After(100*time.Millisecond, counts.Source()).Start(ctx, queue)).To(Succeed())
		for i := 1; i <= 5; i++ {
			set(counts, "a", i)
		}
		Expect(queue.Len()).To(Equal(0), "queued a change before its delay")
		time.Sleep(300 * time.Millisecond)
		Expect(drain(queue)).To(Equal([]string{"a"}), "want a once for its five changes")
	})
})

var _ = ginkgo.Describe("Accessor Source", func() {
	ginkgo.It("should enqueue objects whose derived fields change", func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		accessor, counts, _ := nodePoolAccessor("a", "b", "c")
		set(counts, "a", 1)
		queue := newQueue()
		Expect(accessor.Start(ctx, queue)).To(Succeed())
		Expect(drain(queue)).To(Equal([]string{"a"}), "Start queued, want a, the one NodePool with derived fields")
		set(counts, "b", 2)
		set(counts, "b", 3)
		unset(counts, "a")
		unset(counts, "c")
		Expect(drain(queue)).To(Equal([]string{"b", "a"}), "want b once for its two changes, then a for its fields going away, and not c, which didn't change")
	})
	ginkgo.It("should stop enqueueing once its context is done", func() {
		ctx, cancel := context.WithCancel(ctx)
		accessor, counts, _ := nodePoolAccessor("a")
		queue := newQueue()
		Expect(accessor.Start(ctx, queue)).To(Succeed())
		cancel()
		Eventually(func() []string {
			counts.Feed("a", func(n int, _ bool) (int, bool) { return n + 1, true })
			return drain(queue)
		}).WithTimeout(10*time.Second).WithPolling(10*time.Millisecond).Should(BeNil(), "the accessor still queues changes after its context is done")
	})
})
