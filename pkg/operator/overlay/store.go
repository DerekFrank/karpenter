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
	"container/heap"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type overlay struct {
	entries sync.Map
	w       atomic.Uint64
	newest  atomic.Uint64

	heapMu sync.Mutex
	byRV   rvHeap

	pinMu  sync.Mutex
	pinned map[uint64]int

	indexMu sync.RWMutex
	indexed map[indexKey]map[types.NamespacedName]struct{}

	cached func(types.NamespacedName) bool
}

type entry struct {
	obj       client.Object
	rv        uint64
	indexKeys []indexKey
	created   bool
}

type indexKey struct {
	field, value string
}

func (e *entry) newerThan(cached client.Object) bool {
	rv := parseResourceVersion(cached.GetResourceVersion())
	return rv != 0 && e.rv > rv
}

func parseResourceVersion(resourceVersion string) uint64 {
	rv, err := strconv.ParseUint(resourceVersion, 10, 64)
	if err != nil {
		return 0
	}
	return rv
}

func newOverlay() *overlay {
	return &overlay{pinned: map[uint64]int{}, indexed: map[indexKey]map[types.NamespacedName]struct{}{}}
}

func (o *overlay) standsIn(key types.NamespacedName, e *entry) bool {
	return e.created || (o.cached != nil && o.cached(key))
}

//nolint:gocyclo
func (o *overlay) record(obj client.Object, indexers map[string]client.IndexerFunc, created bool) {
	rv := parseResourceVersion(obj.GetResourceVersion())
	if rv == 0 || rv <= o.w.Load() {
		return
	}
	key := client.ObjectKeyFromObject(obj)
	e := &entry{obj: obj, rv: rv, created: created}
	for field, indexer := range indexers {
		for _, value := range indexer(obj) {
			e.indexKeys = append(e.indexKeys, indexKey{field: field, value: value})
		}
	}
	for {
		cur, loaded := o.entries.LoadOrStore(key, e)
		if !loaded {
			break
		}
		if cur.(*entry).rv >= rv {
			return
		}
		e.created = created || cur.(*entry).created
		if o.entries.CompareAndSwap(key, cur, e) {
			break
		}
	}
	storeMax(&o.newest, rv)
	o.indexMu.Lock()
	for _, k := range e.indexKeys {
		if o.indexed[k] == nil {
			o.indexed[k] = map[types.NamespacedName]struct{}{}
		}
		o.indexed[k][key] = struct{}{}
	}
	o.indexMu.Unlock()
	o.heapMu.Lock()
	heap.Push(&o.byRV, rvItem{rv: rv, key: key, entry: e})
	o.heapMu.Unlock()
}

func (o *overlay) unindex(key types.NamespacedName, e *entry) {
	o.indexMu.Lock()
	defer o.indexMu.Unlock()
	var current []indexKey
	if v, ok := o.entries.Load(key); ok {
		current = v.(*entry).indexKeys
	}
	for _, k := range e.indexKeys {
		if slices.Contains(current, k) {
			continue
		}
		delete(o.indexed[k], key)
		if len(o.indexed[k]) == 0 {
			delete(o.indexed, k)
		}
	}
}

func (o *overlay) indexedKeys(k indexKey) []types.NamespacedName {
	o.indexMu.RLock()
	defer o.indexMu.RUnlock()
	return slices.Collect(maps.Keys(o.indexed[k]))
}

func (o *overlay) observe(raw any) {
	if tombstone, ok := raw.(toolscache.DeletedFinalStateUnknown); ok {
		raw = tombstone.Obj
	}
	obj, ok := raw.(client.Object)
	if !ok {
		return
	}
	if rv := parseResourceVersion(obj.GetResourceVersion()); rv != 0 {
		storeMax(&o.w, rv)
	}
	o.pinMu.Lock()
	safe := o.w.Load()
	for w0 := range o.pinned {
		safe = min(safe, w0)
	}
	o.pinMu.Unlock()
	o.heapMu.Lock()
	defer o.heapMu.Unlock()
	for len(o.byRV) > 0 && o.byRV[0].rv <= safe {
		item := heap.Pop(&o.byRV).(rvItem)
		o.entries.CompareAndDelete(item.key, item.entry)
		o.unindex(item.key, item.entry)
	}
}

func (o *overlay) forget(key types.NamespacedName) {
	v, ok := o.entries.Load(key)
	if !ok || !o.entries.CompareAndDelete(key, v) {
		return
	}
	o.unindex(key, v.(*entry))
}

func (o *overlay) get(key types.NamespacedName) (*entry, bool) {
	v, ok := o.entries.Load(key)
	if !ok || v.(*entry).rv <= o.w.Load() {
		return nil, false
	}
	return v.(*entry), true
}

func (o *overlay) begin() uint64 {
	o.pinMu.Lock()
	defer o.pinMu.Unlock()
	w0 := o.w.Load()
	o.pinned[w0]++
	return w0
}

func (o *overlay) end(w0 uint64) {
	o.pinMu.Lock()
	defer o.pinMu.Unlock()
	if o.pinned[w0]--; o.pinned[w0] == 0 {
		delete(o.pinned, w0)
	}
}

func (o *overlay) visible(w0 uint64) *pinnedEntries {
	return &pinnedEntries{overlay: o, w0: w0}
}

func (o *overlay) visibleWith(w0 uint64, k indexKey) *pinnedEntries {
	keys := o.indexedKeys(k)
	return &pinnedEntries{overlay: o, w0: w0, keys: &keys}
}

type pinnedEntries struct {
	overlay *overlay
	w0      uint64
	keys    *[]types.NamespacedName
}

func (p *pinnedEntries) get(key types.NamespacedName) (*entry, bool) {
	v, ok := p.overlay.entries.Load(key)
	if !ok || v.(*entry).rv <= p.w0 {
		return nil, false
	}
	return v.(*entry), true
}

func storeMax(v *atomic.Uint64, rv uint64) {
	for {
		cur := v.Load()
		if rv <= cur || v.CompareAndSwap(cur, rv) {
			return
		}
	}
}

type rvItem struct {
	rv    uint64
	key   types.NamespacedName
	entry *entry
}

type rvHeap []rvItem

func (h rvHeap) Len() int           { return len(h) }
func (h rvHeap) Less(i, j int) bool { return h[i].rv < h[j].rv }
func (h rvHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rvHeap) Push(x any)        { *h = append(*h, x.(rvItem)) }
func (h *rvHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

func (o *overlay) len() int {
	n := 0
	o.entries.Range(func(any, any) bool { n++; return true })
	return n
}
