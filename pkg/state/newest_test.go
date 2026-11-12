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
	"math/rand/v2"
	"sync"
	"sync/atomic"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state/overlay"
)

type levelNodeClaims = Store[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent]

func newLevelNodeClaims(from Newest) *levelNodeClaims {
	return NewLevelStore[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent](from,
		func(name types.NamespacedName) types.NamespacedName { return name }, func(*v1.NodeClaim) bool { return true })
}

// recount returns the usage of each NodePool counted from the objects the store holds.
func recount(nodeClaims *levelNodeClaims) map[string]poolFields {
	want := map[string]poolFields{}
	for k, e := range nodeClaims.List() {
		u := usageOf(k, e)
		f := want[poolOf(k, e)]
		want[poolOf(k, e)] = u.Update(f, usage{})
	}
	return want
}

var _ = ginkgo.Describe("LevelStore", func() {
	var objects *Objects
	var nodeClaims *levelNodeClaims
	var s *pools
	ginkgo.BeforeEach(func() {
		objects = NewObjects()
		nodeClaims = newLevelNodeClaims(objects)
		s = newPools()
		Derive(nodeClaims.Reader, s, poolOf, usageOf)
	})

	ginkgo.It("should apply the newest version on each notification, and drop the entry once it's gone", func() {
		objects.Put(nodeClaim("a", "x", 1, "2"))
		Expect(fields(s, "x")).To(cpuAndCount(2, 1))
		objects.Put(nodeClaim("a", "y", 3, "4"))
		Expect(fields(s, "x")).To(cpuAndCount(0, 0))
		Expect(fields(s, "y")).To(cpuAndCount(4, 1))
		objects.Remove(types.NamespacedName{Name: "a"})
		_, ok := nodeClaims.Get(types.NamespacedName{Name: "a"})
		Expect(ok).To(BeFalse())
		Expect(pooled(s)).To(BeEmpty())
	})
	ginkgo.It("should do nothing for a duplicate delivery", func() {
		var changes, dispatched int
		nodeClaims.Subscribe(func(Change[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent]) { changes++ })
		counts := NewSyntheticStore[string, string, dispatches, struct{}](func(k string) string { return k })
		Derive(nodeClaims.Reader, counts, poolOf, func(_ types.NamespacedName, e nodeClaimEntry) counted {
			dispatched++
			return counted{Name: e.Object.Name, CPU: e.Object.Status.Capacity.Cpu().Value()}
		})
		nc := nodeClaim("a", "x", 1, "2")
		objects.Put(nc)
		Expect(changes).To(Equal(1))
		before := dispatched
		objects.Put(nc.DeepCopy())
		nodeClaims.s.feed.(*newestOf[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent]).apply(types.NamespacedName{Name: "a"})
		Expect(changes).To(Equal(1), "changes for a version the store holds, want none")
		Expect(dispatched).To(Equal(before), "contributions computed for a version the store holds, want none")
		Expect(fields(counts, "x")).To(Equal(dispatches{Updates: 1, CPU: 2}))
	})
	ginkgo.It("should end equal to a recount when it skips intermediate versions", func() {
		feed := nodeClaims.s.feed.(*newestOf[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent])
		// Versions the notifications never see: the source moves on before each is applied.
		objects.mu.Lock()
		objects.objects[types.NamespacedName{Name: "a"}] = nodeClaim("a", "x", 1, "1")
		objects.objects[types.NamespacedName{Name: "a"}] = nodeClaim("a", "y", 2, "2")
		objects.objects[types.NamespacedName{Name: "b"}] = nodeClaim("b", "y", 3, "3")
		objects.objects[types.NamespacedName{Name: "a"}] = nodeClaim("a", "z", 4, "5")
		objects.mu.Unlock()
		feed.apply(types.NamespacedName{Name: "a"})
		feed.apply(types.NamespacedName{Name: "b"})
		Expect(pooled(s)).To(Equal(recount(nodeClaims)))
		Expect(fields(s, "z")).To(cpuAndCount(5, 1))
		Expect(fields(s, "y")).To(cpuAndCount(3, 1))
	})
	ginkgo.It("should drop an object a read from before its removal commits after the removal", func() {
		key := types.NamespacedName{Name: "a"}
		late := &readsOnce{Objects: NewObjects(), first: nodeClaim("a", "x", 1, "1")}
		store := newLevelNodeClaims(late)
		feed := store.s.feed.(*newestOf[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent])
		feed.apply(key)
		_, ok := store.Get(key)
		Expect(ok).To(BeFalse(), "the store once the read that followed the late commit found the object gone")
	})
	ginkgo.It("should keep the newer version when an older read commits late", func() {
		feed := nodeClaims.s.feed.(*newestOf[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent])
		key := types.NamespacedName{Name: "a"}
		stale := nodeClaim("a", "x", 1, "1")
		objects.Put(nodeClaim("a", "y", 2, "2"))
		Expect(feed.commit(key, stale, true)).To(BeFalse(), "an older version, want it to lose")
		e, _ := nodeClaims.Get(key)
		Expect(e.Object.ResourceVersion).To(Equal("2"))
		Expect(pooled(s)).To(Equal(recount(nodeClaims)))
	})
	ginkgo.It("should drop the old object's intent and contributions when a new UID replaces it", func() {
		key := types.NamespacedName{Name: "a"}
		objects.Put(nodeClaim("a", "x", 1, "2"))
		deletion := NewIntent(nodeClaims, func(i *nodeClaimIntent) *Intended[bool] { return &i.Deletion })
		deletion.Set(key, true)
		replacement := nodeClaim("a", "y", 5, "4")
		replacement.UID = "a-2"
		objects.Put(replacement)
		e, ok := nodeClaims.Get(key)
		Expect(ok).To(BeTrue())
		Expect(e.Object.UID).To(Equal(types.UID("a-2")))
		Expect(e.Intent.Deletion.Value).To(BeFalse(), "the replacement's intent, want none")
		Expect(pooled(s)).To(Equal(map[string]poolFields{"y": {CPU: 4, Count: 1}}))
	})
	ginkgo.It("should converge on the newest version under concurrent notifications for one key", func() {
		feed := nodeClaims.s.feed.(*newestOf[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent])
		key := types.NamespacedName{Name: "a"}
		var rv atomic.Uint64
		var wg sync.WaitGroup
		for w := range 8 {
			wg.Go(func() {
				for i := range 500 {
					objects.Put(nodeClaim("a", fmt.Sprintf("pool-%d", (i+w)%3), rv.Add(1), "1"))
					feed.apply(key)
				}
			})
		}
		wg.Wait()
		newest, _ := objects.Get(key)
		e, _ := nodeClaims.Get(key)
		Expect(e.Object).To(BeIdenticalTo(newest))
		Expect(pooled(s)).To(Equal(recount(nodeClaims)))
	})
	ginkgo.It("should converge on a recount under concurrent writes, removals and replacements", func() {
		var rv atomic.Uint64
		var wg sync.WaitGroup
		for w := range 4 {
			wg.Go(func() {
				r := rand.New(rand.NewPCG(uint64(w), 7)) //nolint:gosec
				for range 2000 {
					name := fmt.Sprintf("nc-%d", r.IntN(20))
					switch r.IntN(6) {
					case 0:
						objects.Remove(types.NamespacedName{Name: name})
					case 1:
						nc := nodeClaim(name, fmt.Sprintf("pool-%d", r.IntN(3)), rv.Add(1), fmt.Sprint(1+r.IntN(4)))
						nc.UID = types.UID(fmt.Sprintf("%s-%s", name, nc.ResourceVersion))
						objects.Put(nc)
					default:
						objects.Put(nodeClaim(name, fmt.Sprintf("pool-%d", r.IntN(3)), rv.Add(1), fmt.Sprint(1+r.IntN(4))))
					}
				}
			})
		}
		wg.Wait()
		for _, k := range objects.Keys() {
			o, _ := objects.Get(k)
			e, ok := nodeClaims.Get(k)
			Expect(ok).To(BeTrue(), "%s, want the store to hold it", k)
			Expect(e.Object).To(BeIdenticalTo(o), "%s, want the newest version", k)
		}
		held := 0
		for range nodeClaims.List() {
			held++
		}
		Expect(held).To(Equal(len(objects.Keys())))
		Expect(pooled(s)).To(Equal(recount(nodeClaims)))
	})
})

// storedInformer is a FakeInformer with a store, as the informer cache's informers have, which the overlay reads.
type storedInformer struct {
	*controllertest.FakeInformer
	store toolscache.Store
}

func (i *storedInformer) GetStore() toolscache.Store { return i.store }

// deliver stores obj and delivers its event, as the informer does with a version it watched.
func (i *storedInformer) deliver(obj client.Object) {
	obj = obj.DeepCopyObject().(client.Object)
	old, exists := lo.Must2(i.store.Get(obj))
	lo.Must0(i.store.Update(obj))
	if exists {
		i.Update(old.(metav1.Object), obj)
	} else {
		i.Add(obj)
	}
}

func (i *storedInformer) remove(obj client.Object) {
	lo.Must0(i.store.Delete(obj))
	i.Delete(obj)
}

type storedInformers struct {
	*informertest.FakeInformers
	nodeClaims *storedInformer
}

func (s *storedInformers) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	if _, ok := obj.(*v1.NodeClaim); ok {
		return s.nodeClaims, nil
	}
	return s.FakeInformers.GetInformer(ctx, obj, opts...)
}

var _ = ginkgo.Describe("LevelStore over the overlay", func() {
	var informer *storedInformer
	var kubeClient *overlay.Client
	var nodeClaims *levelNodeClaims
	var s *pools
	var nc *v1.NodeClaim
	ginkgo.BeforeEach(func() {
		informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
		informer = &storedInformer{FakeInformer: lo.Must(informers.FakeInformerFor(ctx, &v1.NodeClaim{})), store: toolscache.NewStore(toolscache.MetaNamespaceKeyFunc)}
		kubeClient = lo.Must(overlay.New(ctx, fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(), &storedInformers{FakeInformers: informers, nodeClaims: informer}, &v1.NodeClaim{}))
		nodeClaims = newLevelNodeClaims(lo.Must(kubeClient.Source(&v1.NodeClaim{})))
		s = newPools()
		Derive(nodeClaims.Reader, s, poolOf, func(_ types.NamespacedName, e nodeClaimEntry) usage {
			return usage{CPU: lo.Ternary[int64](e.Object.DeletionTimestamp.IsZero(), 0, 1), Count: 1}
		})
		nc = nodeClaim("a", "x", 0, "1")
		nc.ResourceVersion, nc.UID = "", ""
		Expect(kubeClient.Create(ctx, nc)).To(Succeed())
		informer.deliver(nc)
	})
	held := func() (*v1.NodeClaim, bool) {
		e, ok := nodeClaims.Get(client.ObjectKeyFromObject(nc))
		return e.Object, ok
	}

	ginkgo.It("should keep its write when the informer delivers an older version late", func() {
		stale := nc.DeepCopy()
		stored := nc.DeepCopy()
		nc.Labels[v1.NodePoolLabelKey] = "y"
		Expect(kubeClient.Patch(ctx, nc, client.MergeFrom(stored))).To(Succeed())
		Expect(fields(s, "y")).To(cpuAndCount(0, 1), "y once the patch returns")
		informer.deliver(stale)
		got, _ := held()
		Expect(got.ResourceVersion).To(Equal(nc.ResourceVersion), "the store's version after a late delivery of an older one")
		Expect(pooled(s)).To(Equal(map[string]poolFields{"y": {Count: 1}}))
		informer.deliver(nc)
		cached, _ := lo.Must2(informer.store.Get(nc))
		got, _ = held()
		Expect(got).To(BeIdenticalTo(cached), "the store's object once the informer delivers the version, want the informer's own")
	})
	ginkgo.It("should do nothing when the informer delivers the version it holds again", func() {
		var changes int
		nodeClaims.Subscribe(func(Change[types.NamespacedName, *v1.NodeClaim, struct{}, nodeClaimIntent]) { changes++ })
		cached, _ := lo.Must2(informer.store.Get(nc))
		informer.Update(cached.(metav1.Object), cached.(metav1.Object))
		Expect(changes).To(Equal(0))
	})
	ginkgo.It("should hide what it deleted from reads and the store once the delete returns, even if the informer delivers it again", func() {
		stale := nc.DeepCopy()
		Expect(kubeClient.Delete(ctx, nc)).To(Succeed())
		_, ok := held()
		Expect(ok).To(BeFalse(), "the store once the delete returns")
		Expect(pooled(s)).To(BeEmpty(), "the contributions once the delete returns")
		informer.deliver(stale)
		_, ok = held()
		Expect(ok).To(BeFalse(), "the store once the informer delivers the deleted object again")
		Expect(pooled(s)).To(BeEmpty())
		err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nc), &v1.NodeClaim{})
		Expect(err).To(HaveOccurred())
		list := &v1.NodeClaimList{}
		Expect(kubeClient.List(ctx, list)).To(Succeed())
		Expect(list.Items).To(BeEmpty())

		informer.remove(stale)
		replacement := nodeClaim("a", "z", 0, "1")
		replacement.ResourceVersion, replacement.UID = "", "a-2"
		// The fake client numbers each object's versions from 1, below what the informer has delivered, so the informer delivers
		// the new object rather than the overlay holding it.
		Expect(kubeClient.Create(ctx, replacement)).To(Succeed())
		informer.deliver(replacement)
		got, ok := held()
		Expect(ok).To(BeTrue(), "a new object under the name")
		Expect(got.UID).To(Equal(types.UID("a-2")))
		Expect(pooled(s)).To(Equal(map[string]poolFields{"z": {Count: 1}}))
	})
	ginkgo.It("should hold an object a finalizer keeps as deleting, and drop it once the finalizer goes", func() {
		stored := nc.DeepCopy()
		nc.Finalizers = []string{v1.TerminationFinalizer}
		Expect(kubeClient.Patch(ctx, nc, client.MergeFrom(stored))).To(Succeed())
		informer.deliver(nc)

		Expect(kubeClient.Delete(ctx, nc)).To(Succeed())
		got, ok := held()
		Expect(ok).To(BeTrue(), "the store once the delete returns, want the terminating NodeClaim")
		Expect(got.DeletionTimestamp.IsZero()).To(BeFalse())
		Expect(pooled(s)).To(Equal(map[string]poolFields{"x": {CPU: 1, Count: 1}}), "x once the delete returns, want the NodeClaim counted as deleting")
		read := &v1.NodeClaim{}
		Expect(kubeClient.Get(ctx, client.ObjectKeyFromObject(nc), read)).To(Succeed())
		Expect(read.DeletionTimestamp.IsZero()).To(BeFalse())
		list := &v1.NodeClaimList{}
		Expect(kubeClient.List(ctx, list)).To(Succeed())
		Expect(list.Items).To(ConsistOf(HaveField("DeletionTimestamp", Not(BeNil()))))

		terminating := nc.DeepCopy()
		stored = nc.DeepCopy()
		nc.Finalizers = nil
		Expect(kubeClient.Patch(ctx, nc, client.MergeFrom(stored))).To(Succeed())
		_, ok = held()
		Expect(ok).To(BeFalse(), "the store once the finalizer's removal returns")
		Expect(pooled(s)).To(BeEmpty())
		informer.deliver(terminating)
		_, ok = held()
		Expect(ok).To(BeFalse(), "the store once the informer delivers the terminating NodeClaim late")
		Expect(kubeClient.List(ctx, list)).To(Succeed())
		Expect(list.Items).To(BeEmpty())
	})
})

// readsOnce returns first from its first Get, a read from before the object's removal, and Objects' after.
type readsOnce struct {
	*Objects
	first client.Object
}

func (r *readsOnce) Get(key types.NamespacedName) (client.Object, bool) {
	if first := r.first; first != nil {
		r.first = nil
		return first, true
	}
	return r.Objects.Get(key)
}
