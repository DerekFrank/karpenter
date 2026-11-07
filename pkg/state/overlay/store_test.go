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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestOverlayKeepsEntriesForListsThatPinnedThem(t *testing.T) {
	o := newOverlay()
	o.record(versionOf("a", 10), nil, true)
	w0 := o.begin()
	for rv := uint64(10); rv < 20; rv++ {
		o.observe(versionOf("other", rv))
	}
	if got := resourceVersions(merge([]runtime.Object{versionOf("a", 5)}, o.visible(w0), matchEverything)); got["a"] != 10 {
		t.Fatalf("a List that pinned the watermark before the eviction saw a at %d, want 10", got["a"])
	}
	o.end(w0)
	if o.len() != 1 {
		t.Fatalf("the overlay deleted an entry a pinned List could see, len %d, want 1", o.len())
	}
	w1 := o.begin()
	if got := resourceVersions(merge([]runtime.Object{versionOf("a", 10)}, o.visible(w1), matchEverything)); got["a"] != 10 {
		t.Fatalf("got %v", got)
	}
	o.end(w1)
	o.observe(versionOf("other", 21))
	if o.len() != 0 {
		t.Fatalf("len %d once no List pins the entry, want 0", o.len())
	}
}

func TestOverlayHidesEvictedEntriesFromLaterReads(t *testing.T) {
	o := newOverlay()
	o.record(versionOf("a", 10), nil, true)
	pinned := o.begin()
	o.observe(versionOf("a", 11))
	w0 := o.begin()
	if got := resourceVersions(merge(nil, o.visible(w0), matchEverything)); got["a"] != 0 {
		t.Fatalf("a List that started after the eviction added the deleted object back: %v", got)
	}
	o.end(w0)
	if _, ok := o.get(types.NamespacedName{Name: "a"}); ok {
		t.Fatal("a Get that started after the eviction added the deleted object back")
	}
	if got := resourceVersions(merge(nil, o.visible(pinned), matchEverything)); got["a"] != 10 {
		t.Fatalf("the List that started before the eviction lost a: %v", got)
	}
	o.end(pinned)
}

func TestOverlayHidesEntriesAnOlderListKeepsStored(t *testing.T) {
	const entries = 1000
	o := newOverlay()
	for i := range entries {
		o.record(versionOf(fmt.Sprintf("o%d", i), uint64(100+i)), nil, true)
	}
	w0 := o.begin()
	defer o.end(w0)
	if got := resourceVersions(merge(nil, o.visible(w0), matchEverything)); len(got) != entries {
		t.Fatalf("merged %d entries, want %d", len(got), entries)
	}
	o.observe(versionOf("other", uint64(100+entries*3/4)))
	if o.len() != entries {
		t.Fatalf("len %d while an older List pins every entry, want %d", o.len(), entries)
	}
	w1 := o.begin()
	defer o.end(w1)
	if got := resourceVersions(merge(nil, o.visible(w1), matchEverything)); len(got) != entries/4-1 {
		t.Fatalf("merged %d entries after the informer passed most of them, want %d", len(got), entries/4-1)
	}
	if got := resourceVersions(merge([]runtime.Object{versionOf("o0", 50)}, o.visible(w1), matchEverything)); got["o0"] != 50 {
		t.Fatalf("a List merged o0 at %d after the informer passed it, want the cache's copy at 50", got["o0"])
	}
}

func TestOverlayIndexesEntriesByField(t *testing.T) {
	const field = "field"
	indexers := map[string]client.IndexerFunc{field: func(obj client.Object) []string { return []string{obj.GetLabels()[field]} }}
	withValue := func(name string, rv uint64, value string) client.Object {
		obj := versionOf(name, rv)
		obj.Labels = map[string]string{field: value}
		return obj
	}
	byValue := func(value string) fields.Selector { return fields.OneTermEqualSelector(field, value) }
	o := newOverlay()
	o.record(withValue("a", 10, "one"), indexers, true)
	o.record(withValue("b", 11, "two"), indexers, true)
	o.record(withValue("a", 12, "two"), indexers, true)

	w0 := o.begin()
	for value, want := range map[string]int{"one": 0, "two": 2, "three": 0} {
		listOpts := &client.ListOptions{FieldSelector: byValue(value)}
		k, ok := exactIndexedField(listOpts, indexers)
		if !ok {
			t.Fatalf("%q isn't an exact match on an indexed field", value)
		}
		if got := resourceVersions(merge(nil, o.visibleWith(w0, k), matcher(listOpts, indexers))); len(got) != want {
			t.Fatalf("a List by %s=%s merged %v, want %d entries", field, value, got, want)
		}
	}
	o.end(w0)
	o.record(withValue("a", 14, "two"), indexers, true)
	o.observe(versionOf("other", 13))
	w1 := o.begin()
	k, _ := exactIndexedField(&client.ListOptions{FieldSelector: byValue("two")}, indexers)
	if got := resourceVersions(merge(nil, o.visibleWith(w1, k), matchEverything)); got["a"] != 14 {
		t.Fatalf("a List by %s=two merged %v once an older version of a was deleted, want a at 14", field, got)
	}
	o.end(w1)
	o.observe(versionOf("other", 20))
	o.indexMu.RLock()
	defer o.indexMu.RUnlock()
	if len(o.indexed) != 0 {
		t.Fatalf("the index holds %v once every entry is deleted", o.indexed)
	}
}

func TestOverlayKeepsTheNewerResponse(t *testing.T) {
	o := newOverlay()
	o.record(versionOf("a", 20), nil, true)
	o.record(versionOf("a", 15), nil, true)
	e, ok := o.get(types.NamespacedName{Name: "a"})
	if !ok || e.rv != 20 {
		t.Fatalf("got %v, %t, want a at 20", e, ok)
	}
}

func TestOverlayOnlyCreatedEntriesStandInForMissingObjects(t *testing.T) {
	o := newOverlay()
	inStore := map[string]bool{}
	o.cached = func(key types.NamespacedName) bool { return inStore[key.Name] }
	o.record(versionOf("updated", 10), nil, false)
	o.record(versionOf("created", 11), nil, true)
	o.record(versionOf("created", 12), nil, false)
	w0 := o.begin()
	if got := resourceVersions(merge(nil, o.visible(w0), matchEverything)); len(got) != 1 || got["created"] != 12 {
		t.Fatalf("merged %v, want only created at 12", got)
	}
	inStore["updated"] = true
	if got := resourceVersions(merge(nil, o.visible(w0), matchEverything)); got["updated"] != 10 {
		t.Fatalf("merged %v, want updated at 10 once the store holds it", got)
	}
	o.end(w0)
}

func TestOverlayForgetsDeletedObjects(t *testing.T) {
	const field = "field"
	indexers := map[string]client.IndexerFunc{field: func(client.Object) []string { return []string{"value"} }}
	o := newOverlay()
	o.record(versionOf("a", 10), indexers, true)
	o.forget(types.NamespacedName{Name: "a"})
	if _, ok := o.get(types.NamespacedName{Name: "a"}); ok {
		t.Fatal("got a after forgetting it")
	}
	if o.len() != 0 || len(o.indexedKeys(indexKey{field: field, value: "value"})) != 0 {
		t.Fatalf("len %d, index %v after forgetting a", o.len(), o.indexed)
	}
	o.observe(versionOf("other", 11))
}

func TestOverlayObservesTombstones(t *testing.T) {
	o := newOverlay()
	o.record(versionOf("a", 10), nil, true)
	o.observe(toolscache.DeletedFinalStateUnknown{Key: "a", Obj: versionOf("a", 10)})
	if _, ok := o.get(types.NamespacedName{Name: "a"}); ok || o.len() != 0 {
		t.Fatalf("a is visible or stored (len %d) after the informer delivered its tombstone", o.len())
	}
}

//nolint:gocyclo
func TestOverlayUnderConcurrency(t *testing.T) {
	o := newOverlay()
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
				events <- obj
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
					events <- deleted
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
	if f := failure.Load(); f != nil {
		t.Fatal(f)
	}
}

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
