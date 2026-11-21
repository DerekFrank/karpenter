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

package overlay

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("overlay", func() {
	var o *overlay
	BeforeEach(func() {
		o = newOverlay()
	})

	It("should keep entries for Lists that pinned them", func() {
		o.record(versionOf("a", 10), nil, true)
		w0 := o.begin()
		for rv := uint64(10); rv < 20; rv++ {
			o.observe(versionOf("other", rv))
		}
		Expect(resourceVersions(merge([]runtime.Object{versionOf("a", 5)}, o.visible(w0), matchEverything))).To(HaveKeyWithValue("a", uint64(10)), "a List that pinned the watermark before the eviction should see a at 10")
		o.end(w0)
		Expect(o.len()).To(Equal(1), "the overlay deleted an entry a pinned List could see")
		w1 := o.begin()
		Expect(resourceVersions(merge([]runtime.Object{versionOf("a", 10)}, o.visible(w1), matchEverything))).To(HaveKeyWithValue("a", uint64(10)))
		o.end(w1)
		o.observe(versionOf("other", 21))
		Expect(o.len()).To(Equal(0), "len once no List pins the entry")
	})
	It("should hide evicted entries from later reads", func() {
		o.record(versionOf("a", 10), nil, true)
		pinned := o.begin()
		o.observe(versionOf("a", 11))
		w0 := o.begin()
		Expect(resourceVersions(merge(nil, o.visible(w0), matchEverything))).NotTo(HaveKey("a"), "a List that started after the eviction added the deleted object back")
		o.end(w0)
		_, ok := o.get(types.NamespacedName{Name: "a"})
		Expect(ok).To(BeFalse(), "a Get that started after the eviction added the deleted object back")
		Expect(resourceVersions(merge(nil, o.visible(pinned), matchEverything))).To(HaveKeyWithValue("a", uint64(10)), "the List that started before the eviction lost a")
		o.end(pinned)
	})
	It("should hide entries an older List keeps stored", func() {
		const entries = 1000
		for i := range entries {
			o.record(versionOf(fmt.Sprintf("o%d", i), uint64(100+i)), nil, true)
		}
		w0 := o.begin()
		DeferCleanup(func() { o.end(w0) })
		Expect(resourceVersions(merge(nil, o.visible(w0), matchEverything))).To(HaveLen(entries))
		o.observe(versionOf("other", uint64(100+entries*3/4)))
		Expect(o.len()).To(Equal(entries), "len while an older List pins every entry")
		w1 := o.begin()
		DeferCleanup(func() { o.end(w1) })
		Expect(resourceVersions(merge(nil, o.visible(w1), matchEverything))).To(HaveLen(entries/4-1), "merged entries after the informer passed most of them")
		Expect(resourceVersions(merge([]runtime.Object{versionOf("o0", 50)}, o.visible(w1), matchEverything))).To(HaveKeyWithValue("o0", uint64(50)), "a List should merge the cache's copy of o0 after the informer passed it")
	})
	It("should index entries by field", func() {
		const field = "field"
		indexers := map[string]client.IndexerFunc{field: func(obj client.Object) []string { return []string{obj.GetLabels()[field]} }}
		withValue := func(name string, rv uint64, value string) client.Object {
			obj := versionOf(name, rv)
			obj.Labels = map[string]string{field: value}
			return obj
		}
		byValue := func(value string) fields.Selector { return fields.OneTermEqualSelector(field, value) }
		o.record(withValue("a", 10, "one"), indexers, true)
		o.record(withValue("b", 11, "two"), indexers, true)
		o.record(withValue("a", 12, "two"), indexers, true)

		w0 := o.begin()
		for value, want := range map[string]int{"one": 0, "two": 2, "three": 0} {
			listOpts := &client.ListOptions{FieldSelector: byValue(value)}
			k, ok := exactIndexedField(listOpts, indexers)
			Expect(ok).To(BeTrue(), "%q isn't an exact match on an indexed field", value)
			Expect(resourceVersions(merge(nil, o.visibleWith(w0, k), matcher(listOpts, indexers)))).To(HaveLen(want), "a List by %s=%s", field, value)
		}
		o.end(w0)
		o.record(withValue("a", 14, "two"), indexers, true)
		o.observe(versionOf("other", 13))
		w1 := o.begin()
		k, _ := exactIndexedField(&client.ListOptions{FieldSelector: byValue("two")}, indexers)
		Expect(resourceVersions(merge(nil, o.visibleWith(w1, k), matchEverything))).To(HaveKeyWithValue("a", uint64(14)), "a List by %s=two once an older version of a was deleted", field)
		o.end(w1)
		o.observe(versionOf("other", 20))
		o.indexMu.RLock()
		defer o.indexMu.RUnlock()
		Expect(o.indexed).To(BeEmpty(), "the index once every entry is deleted")
	})
	It("should keep the newer response", func() {
		o.record(versionOf("a", 20), nil, true)
		o.record(versionOf("a", 15), nil, true)
		e, ok := o.get(types.NamespacedName{Name: "a"})
		Expect(ok).To(BeTrue())
		Expect(e.rv).To(Equal(uint64(20)))
	})
	It("should only let created entries stand in for missing objects", func() {
		inStore := map[string]bool{}
		o.cached = func(key types.NamespacedName) bool { return inStore[key.Name] }
		o.record(versionOf("updated", 10), nil, false)
		o.record(versionOf("created", 11), nil, true)
		o.record(versionOf("created", 12), nil, false)
		w0 := o.begin()
		Expect(resourceVersions(merge(nil, o.visible(w0), matchEverything))).To(Equal(map[string]uint64{"created": 12}), "want only created at 12")
		inStore["updated"] = true
		Expect(resourceVersions(merge(nil, o.visible(w0), matchEverything))).To(HaveKeyWithValue("updated", uint64(10)), "want updated at 10 once the store holds it")
		o.end(w0)
	})
	It("should forget deleted objects", func() {
		const field = "field"
		indexers := map[string]client.IndexerFunc{field: func(client.Object) []string { return []string{"value"} }}
		o.record(versionOf("a", 10), indexers, true)
		o.forget(types.NamespacedName{Name: "a"})
		_, ok := o.get(types.NamespacedName{Name: "a"})
		Expect(ok).To(BeFalse(), "got a after forgetting it")
		Expect(o.len()).To(Equal(0))
		Expect(o.indexedKeys(indexKey{field: field, value: "value"})).To(BeEmpty())
		o.observe(versionOf("other", 11))
	})
	It("should hide an object the client deleted from a read that copied it before the informer dropped it", func() {
		// A read of the informer's objects copies them, then hides the ones the client deleted. The informer can drop a
		// deleted object, and the tombstone with it, while the read holds its copy, so a read hides what the client had
		// deleted when it started.
		a := versionOf("a", 10)
		store := map[types.NamespacedName]client.Object{{Name: "a"}: a}
		o.lookup = func(key types.NamespacedName) (client.Object, bool) {
			obj, ok := store[key]
			return obj, ok
		}
		o.bury(a)
		Expect(o.hidden(a)).To(BeTrue(), "a is hidden while the informer still holds it")
		// The read starts here, and copies a.
		buried := o.buriedUIDs()
		// The informer drops a, which prunes its tombstone, before the read hides what it copied.
		delete(store, types.NamespacedName{Name: "a"})
		o.prune(types.NamespacedName{Name: "a"})
		Expect(o.hidden(a)).To(BeFalse(), "the tombstone is pruned once the informer drops the object")
		Expect(o.hiddenSince(buried, a)).To(BeTrue(), "the read shows a, which the client had deleted when it started")
	})
	It("should observe tombstones", func() {
		o.record(versionOf("a", 10), nil, true)
		o.observe(toolscache.DeletedFinalStateUnknown{Key: "a", Obj: versionOf("a", 10)})
		_, ok := o.get(types.NamespacedName{Name: "a"})
		Expect(ok).To(BeFalse(), "a is visible after the informer delivered its tombstone")
		Expect(o.len()).To(Equal(0), "a is stored after the informer delivered its tombstone")
	})
	//nolint:gocyclo
	It("should stay consistent under concurrency", func() {
		var (
			apiMu            sync.Mutex
			rv               uint64 = 1000
			events                  = make(chan *metav1.PartialObjectMetadata, 1<<16)
			cacheMu          sync.RWMutex
			cache            = map[string]*metav1.PartialObjectMetadata{}
			writtenMu        sync.Mutex
			written          = map[string]uint64{}
			deliveredMu      sync.Mutex
			deliveredDeletes = map[string]uint64{}
			deliveries       atomic.Uint64
			stop             = make(chan struct{})
			wg               sync.WaitGroup
			failure          atomic.Value
		)
		const deletedAnnotation = "deleted"
		o.cached = func(key types.NamespacedName) bool {
			cacheMu.RLock()
			defer cacheMu.RUnlock()
			_, ok := cache[key.Name]
			return ok
		}
		for i := range 20 {
			obj := versionOf(fmt.Sprintf("u%d", i), uint64(i+1))
			cache[obj.Name] = obj
			o.observe(obj)
		}
		// send delivers an event unless the test is stopping, so a writer blocked on a full channel can't outlive the
		// consumer, which returns on stop.
		send := func(obj *metav1.PartialObjectMetadata) bool {
			select {
			case events <- obj:
				return true
			case <-stop:
				return false
			}
		}
		for writer := range 4 {
			wg.Go(func() {
				r := rand.New(rand.NewPCG(uint64(writer), 9)) //nolint:gosec
				created := 0
				for {
					select {
					case <-stop:
						return
					default:
					}
					name := fmt.Sprintf("u%d", r.IntN(20))
					if r.IntN(3) == 0 {
						created++
						name = fmt.Sprintf("c-%d-%d", writer, created)
					}
					apiMu.Lock()
					rv++
					wrote := rv
					obj := versionOf(name, wrote)
					if !send(obj) {
						apiMu.Unlock()
						return
					}
					apiMu.Unlock()
					o.record(obj, nil, name[0] == 'c')
					writtenMu.Lock()
					written[name] = max(written[name], wrote)
					writtenMu.Unlock()
					if name[0] == 'c' {
						apiMu.Lock()
						rv++
						deleted := versionOf(name, rv)
						deleted.Annotations = map[string]string{deletedAnnotation: "true"}
						if !send(deleted) {
							apiMu.Unlock()
							return
						}
						apiMu.Unlock()
					}
					if r.IntN(4) == 0 {
						time.Sleep(time.Duration(r.IntN(50)) * time.Microsecond)
					}
				}
			})
		}
		wg.Go(func() {
			r := rand.New(rand.NewPCG(5, 5)) //nolint:gosec
			for {
				var event *metav1.PartialObjectMetadata
				select {
				case <-stop:
					return
				case event = <-events:
				}
				if r.IntN(50) == 0 {
					time.Sleep(time.Duration(r.IntN(200)) * time.Microsecond)
				}
				deleted := event.Annotations[deletedAnnotation] == "true"
				cacheMu.Lock()
				if deleted {
					delete(cache, event.Name)
				} else {
					cache[event.Name] = event
				}
				cacheMu.Unlock()
				o.observe(event)
				if deleted {
					deliveredMu.Lock()
					deliveredDeletes[event.Name] = deliveries.Add(1)
					deliveredMu.Unlock()
				}
			}
		})
		for range 8 {
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					started := deliveries.Load()
					writtenMu.Lock()
					want := map[string]uint64{}
					for i := range 20 {
						want[fmt.Sprintf("u%d", i)] = written[fmt.Sprintf("u%d", i)]
					}
					writtenMu.Unlock()
					w0 := o.begin()
					cacheMu.RLock()
					items := make([]runtime.Object, 0, len(cache))
					for _, obj := range cache {
						items = append(items, obj)
					}
					cacheMu.RUnlock()
					got := resourceVersions(merge(items, o.visible(w0), matchEverything))
					o.end(w0)
					for name, rv := range want {
						if rv != 0 && got[name] < rv {
							failure.Store(fmt.Sprintf("a List lost %s: got it at %d, its write at %d completed before the List started", name, got[name], rv))
						}
					}
					deliveredMu.Lock()
					for name := range got {
						if at, ok := deliveredDeletes[name]; ok && at <= started {
							failure.Store(fmt.Sprintf("a List added back %s, whose delete was delivered before the List started", name))
						}
					}
					deliveredMu.Unlock()
				}
			})
		}
		time.Sleep(time.Second)
		close(stop)
		wg.Wait()
		Expect(failure.Load()).To(BeNil())
	})
})

func versionOf(name string, rv uint64) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: strconv.FormatUint(rv, 10)}}
}

func resourceVersions(objs []runtime.Object) map[string]uint64 {
	out := map[string]uint64{}
	for _, obj := range objs {
		out[obj.(client.Object).GetName()] = parseResourceVersion(obj.(client.Object).GetResourceVersion())
	}
	return out
}

func matchEverything(client.Object) match { return matchYes }
