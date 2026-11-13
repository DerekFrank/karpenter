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

// Package overlay provides a client whose cache reads include the client's own writes before the informer delivers
// them, without waiting for the informer, and hide the objects it deleted until the informer delivers the deletes.
//
// A Delete that leaves the object in storage, such as one a finalizer holds, is a write like any other: the overlay
// serves the terminating object it returned. Only a Delete that removes the object hides it. Telling the two apart
// depends on the controller-runtime fork go.mod replaces controller-runtime with (DerekFrank/controller-runtime,
// "Populate obj with the remaining object on Delete"), which writes the remaining object into the Delete's obj, until
// upstream controller-runtime returns the delete response (a delete-response-v0.25 branch is ready for v0.25).
package overlay

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/samber/lo"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Client is a cache-backed client whose reads of the configured kinds include the objects its own writes returned,
// until the informer delivers them. A write's response is held in an overlay, and a Get or List returns the overlay's
// copy of an object when its resourceVersion is newer than the cache's. Reads never wait for the informer, and the
// informer's store and event handlers never see the overlay.
//
// An object's copy is at least as new as the cache's, but a List isn't a snapshot of the server at one
// resourceVersion: it combines the cache with the latest versions of the objects this client wrote, which carry other
// writers' changes to those objects too. Don't use it for decisions that rely on several objects being observed at
// the same resourceVersion.
//
// resourceVersions are compared as etcd revisions (KEP-5504). A write whose resourceVersion isn't one isn't overlaid,
// and the cache's copy is served.
type Client struct {
	client.Client

	overlays map[schema.GroupVersionKind]*overlay

	mu       sync.RWMutex
	indexers map[schema.GroupVersionKind]map[string]client.IndexerFunc
}

// New overlays writes of the kinds, each of which must be one the cache already has an informer for, since the overlay
// listens to it. Writes and reads of other kinds go straight to the client.
func New(ctx context.Context, c client.Client, informers cache.Informers, kinds ...client.Object) (*Client, error) {
	o := &Client{
		Client:   c,
		overlays: map[schema.GroupVersionKind]*overlay{},
		indexers: map[schema.GroupVersionKind]map[string]client.IndexerFunc{},
	}
	for _, obj := range kinds {
		gvk, err := apiutil.GVKForObject(obj, c.Scheme())
		if err != nil {
			return nil, err
		}
		informer, err := informers.GetInformer(ctx, obj, cache.BlockUntilSynced(false))
		if err != nil {
			return nil, fmt.Errorf("getting informer for %s, %w", gvk, err)
		}
		ov := newOverlay()
		ov.informer = informer
		if s, ok := informer.(interface{ GetStore() toolscache.Store }); ok {
			store := s.GetStore()
			ov.store = store
			ov.lookup = func(key types.NamespacedName) (client.Object, bool) {
				obj, exists, err := store.GetByKey(toolscache.NewObjectName(key.Namespace, key.Name).String())
				if err != nil || !exists {
					return nil, false
				}
				o, ok := obj.(client.Object)
				return o, ok
			}
			ov.cached = func(key types.NamespacedName) bool {
				_, ok := ov.cachedVisible(key)
				return ok
			}
		}
		if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			AddFunc:    ov.observe,
			UpdateFunc: func(_, obj any) { ov.observe(obj) },
			DeleteFunc: ov.observe,
		}); err != nil {
			return nil, fmt.Errorf("adding overlay event handler for %s, %w", gvk, err)
		}
		o.overlays[gvk] = ov
	}
	return o, nil
}

// IndexField records a field index so that Lists with a field selector on it include overlaid objects. Register the
// same index with the cache's FieldIndexer, which FieldIndexer does.
func (c *Client) IndexField(obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	indexers := maps.Clone(c.indexers[gvk])
	if indexers == nil {
		indexers = map[string]client.IndexerFunc{}
	}
	indexers[field] = extractValue
	c.indexers[gvk] = indexers
	return nil
}

// FieldIndexer registers indexes with both the cache and the overlay
func (c *Client) FieldIndexer(cacheIndexer client.FieldIndexer) client.FieldIndexer {
	return fieldIndexer{FieldIndexer: cacheIndexer, overlay: c}
}

type fieldIndexer struct {
	client.FieldIndexer
	overlay *Client
}

func (f fieldIndexer) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	if err := f.FieldIndexer.IndexField(ctx, obj, field, extractValue); err != nil {
		return err
	}
	return f.overlay.IndexField(obj, field, extractValue)
}

func (c *Client) overlayFor(obj runtime.Object) (*overlay, schema.GroupVersionKind, error) {
	switch obj.(type) {
	case runtime.Unstructured, *metav1.PartialObjectMetadata, *metav1.PartialObjectMetadataList:
		return nil, schema.GroupVersionKind{}, nil
	}
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return nil, gvk, err
	}
	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	return c.overlays[gvk], gvk, nil
}

func (c *Client) record(obj client.Object, created bool) error {
	o, gvk, err := c.overlayFor(obj)
	if err != nil || o == nil {
		return err
	}
	if removed(obj) {
		o.remove(obj)
		return nil
	}
	stored := obj.DeepCopyObject().(client.Object)
	o.record(stored, c.indexersFor(gvk), created)
	o.notifyKeys(client.ObjectKeyFromObject(stored))
	return nil
}

// removed returns whether a write's response is an object the server removed: one being deleted without a grace period
// whose last finalizer the write removed.
func removed(obj client.Object) bool {
	grace := obj.GetDeletionGracePeriodSeconds()
	return obj.GetDeletionTimestamp() != nil && len(obj.GetFinalizers()) == 0 && (grace == nil || *grace == 0)
}

func (c *Client) indexersFor(gvk schema.GroupVersionKind) map[string]client.IndexerFunc {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.indexers[gvk]
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if len((&client.CreateOptions{}).ApplyOptions(opts).DryRun) > 0 {
		return nil
	}
	return c.record(obj, true)
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.Client.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if len((&client.UpdateOptions{}).ApplyOptions(opts).DryRun) > 0 {
		return nil
	}
	return c.record(obj, false)
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if err := c.Client.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	if len((&client.PatchOptions{}).ApplyOptions(opts).DryRun) > 0 {
		return nil
	}
	return c.record(obj, false)
}

// Delete records the object the server returned if the client updated obj with it, which it does when the object
// remains in storage, and otherwise forgets the client's writes of the object and hides it until the informer's store
// drops it. Telling the two apart relies on the controller-runtime fork go.mod replaces controller-runtime with
// (DerekFrank/controller-runtime, "Populate obj with the remaining object on Delete"), until upstream returns the delete
// response.
func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	rv := obj.GetResourceVersion()
	if err := c.Client.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	if len((&client.DeleteOptions{}).ApplyOptions(opts).DryRun) > 0 {
		return nil
	}
	o, _, err := c.overlayFor(obj)
	if err != nil || o == nil {
		return err
	}
	if obj.GetResourceVersion() != rv {
		return c.record(obj, false)
	}
	o.remove(obj)
	return nil
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	o, gvk, err := c.overlayFor(obj)
	if err != nil {
		return err
	}
	if o == nil {
		return c.Client.Get(ctx, key, obj, opts...)
	}
	e, ok := o.get(key)
	buriedUID, wasBuried := o.buriedUID(key)
	err = c.Client.Get(ctx, key, obj, opts...)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil && (o.hidden(obj) || (wasBuried && buriedUID == obj.GetUID())) {
		err = apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, key.Name)
	}
	if !ok || (err == nil && !e.newerThan(obj)) {
		return err
	}
	if err != nil && !o.standsIn(key, e) {
		return err
	}
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(e.obj.DeepCopyObject()).Elem())
	return nil
}

func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	o, gvk, err := c.overlayFor(list)
	if err != nil {
		return err
	}
	if o == nil {
		return c.Client.List(ctx, list, opts...)
	}
	w0 := o.begin()
	defer o.end(w0)
	wasBuried := o.buriedUIDs()
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	buried := len(wasBuried) > 0 || o.buried()
	if o.newest.Load() <= w0 && !buried {
		return nil
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	if buried {
		items = slices.DeleteFunc(items, func(item runtime.Object) bool { return o.hiddenSince(wasBuried, item.(client.Object)) })
		if o.newest.Load() <= w0 {
			return meta.SetList(list, items)
		}
	}
	listOpts := (&client.ListOptions{}).ApplyOptions(opts)
	indexers := c.indexersFor(gvk)
	entries := o.visible(w0)
	if k, ok := exactIndexedField(listOpts, indexers); ok {
		entries = o.visibleWith(w0, k)
	}
	return meta.SetList(list, merge(items, entries, matcher(listOpts, indexers)))
}

func exactIndexedField(listOpts *client.ListOptions, indexers map[string]client.IndexerFunc) (indexKey, bool) {
	if listOpts.FieldSelector == nil {
		return indexKey{}, false
	}
	for _, r := range listOpts.FieldSelector.Requirements() {
		if _, ok := indexers[r.Field]; ok && (r.Operator == selection.Equals || r.Operator == selection.DoubleEquals) {
			return indexKey{field: r.Field, value: r.Value}, true
		}
	}
	return indexKey{}, false
}

//nolint:gocyclo
func merge(items []runtime.Object, entries *pinnedEntries, matches func(client.Object) match) []runtime.Object {
	merged := make([]runtime.Object, 0, len(items))
	hit := map[types.NamespacedName]struct{}{}
	for _, raw := range items {
		cached := raw.(client.Object)
		key := client.ObjectKeyFromObject(cached)
		e, ok := entries.get(key)
		if !ok {
			merged = append(merged, raw)
			continue
		}
		hit[key] = struct{}{}
		switch {
		case !e.newerThan(cached):
			merged = append(merged, raw)
		case matches(e.obj) != matchNo:
			merged = append(merged, e.obj.DeepCopyObject())
		}
	}
	add := func(key types.NamespacedName, e *entry) {
		if _, ok := hit[key]; !ok && matches(e.obj) == matchYes && entries.overlay.standsIn(key, e) {
			merged = append(merged, e.obj.DeepCopyObject())
		}
	}
	if entries.keys != nil {
		for _, key := range *entries.keys {
			if e, ok := entries.get(key); ok {
				add(key, e)
			}
		}
		return merged
	}
	entries.overlay.entries.Range(func(key, v any) bool {
		if e := v.(*entry); e.rv > entries.w0 {
			add(key.(types.NamespacedName), e)
		}
		return true
	})
	return merged
}

type match int

const (
	matchNo match = iota
	matchYes
	matchUnknown
)

func matcher(listOpts *client.ListOptions, indexers map[string]client.IndexerFunc) func(client.Object) match {
	labelSelector := listOpts.LabelSelector
	if labelSelector == nil {
		labelSelector = labels.Everything()
	}
	var requirements fields.Requirements
	if listOpts.FieldSelector != nil {
		requirements = listOpts.FieldSelector.Requirements()
	}
	return func(obj client.Object) match {
		if listOpts.Namespace != "" && obj.GetNamespace() != listOpts.Namespace {
			return matchNo
		}
		if !labelSelector.Matches(labels.Set(obj.GetLabels())) {
			return matchNo
		}
		for _, r := range requirements {
			indexer, ok := indexers[r.Field]
			if !ok || (r.Operator != selection.Equals && r.Operator != selection.DoubleEquals) {
				return matchUnknown
			}
			if !lo.Contains(indexer(obj), r.Value) {
				return matchNo
			}
		}
		return matchYes
	}
}

func (c *Client) Status() client.SubResourceWriter {
	return c.SubResource("status")
}

func (c *Client) SubResource(subResource string) client.SubResourceClient {
	return &subResourceClient{SubResourceClient: c.Client.SubResource(subResource), parent: c}
}

type subResourceClient struct {
	client.SubResourceClient
	parent *Client
}

func (s *subResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if err := s.SubResourceClient.Update(ctx, obj, opts...); err != nil {
		return err
	}
	updateOpts := (&client.SubResourceUpdateOptions{}).ApplyOptions(opts)
	if updateOpts.SubResourceBody != nil || len(updateOpts.DryRun) > 0 {
		return nil
	}
	return s.parent.record(obj, false)
}

func (s *subResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if err := s.SubResourceClient.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	patchOpts := (&client.SubResourcePatchOptions{}).ApplyOptions(opts)
	if patchOpts.SubResourceBody != nil || len(patchOpts.DryRun) > 0 {
		return nil
	}
	return s.parent.record(obj, false)
}

// Source is the newest version of each object of one kind that a Client knows: its own write when it's newer than the
// informer's copy, or the informer's copy, unless the client deleted the object. It's where a store that applies each
// object's newest version reads it.
type Source struct{ o *overlay }

// Source returns the newest versions of the objects of obj's kind, which the client must overlay and read from an
// informer's store.
func (c *Client) Source(obj client.Object) (*Source, error) {
	o, gvk, err := c.overlayFor(obj)
	if err != nil {
		return nil, err
	}
	if o == nil || o.store == nil {
		return nil, fmt.Errorf("%s isn't overlaid on an informer's store", gvk)
	}
	return &Source{o: o}, nil
}

// Get returns the newest version of the object under key, and whether it exists. The object is the informer's or the
// overlay's own, so it's read-only.
func (s *Source) Get(key types.NamespacedName) (client.Object, bool) { return s.o.newestOf(key) }

// Keys returns the keys of the objects the informer's store holds and the client created since.
func (s *Source) Keys() []types.NamespacedName {
	var keys []types.NamespacedName
	seen := map[types.NamespacedName]struct{}{}
	for _, k := range s.o.store.ListKeys() {
		namespace, name, err := toolscache.SplitMetaNamespaceKey(k)
		if err != nil {
			continue
		}
		key := types.NamespacedName{Namespace: namespace, Name: name}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	s.o.entries.Range(func(k, _ any) bool {
		if _, ok := seen[k.(types.NamespacedName)]; !ok {
			keys = append(keys, k.(types.NamespacedName))
		}
		return true
	})
	return keys
}

// Subscribe calls fn with the key of each object whose newest version may have changed: on each of the informer's
// events, each of the client's writes and deletes, and each write the overlay stops holding. fn runs on the goroutine of
// the change, after it's visible to Get, so it must not block. It returns a func that stops it.
func (s *Source) Subscribe(fn func(types.NamespacedName)) func() {
	h := &fn
	s.o.handlersMu.Lock()
	defer s.o.handlersMu.Unlock()
	s.o.keyHandlers[h] = struct{}{}
	return func() {
		s.o.handlersMu.Lock()
		defer s.o.handlersMu.Unlock()
		delete(s.o.keyHandlers, h)
	}
}

// HasSynced returns whether the informer has synced.
func (s *Source) HasSynced() bool { return s.o.informer.HasSynced() }

// Holds returns whether the informer's store still holds the object with the UID under key, even one the client
// deleted: until it drops it, the informer can still deliver it.
func (s *Source) Holds(key types.NamespacedName, uid types.UID) bool {
	obj, ok := s.o.lookupCache(key)
	return ok && obj.GetUID() == uid
}

// Delivered returns whether the Source serves obj's version or a newer one: obj's resourceVersion or newer under its
// key, another object under its name, or nothing once the informer has passed obj's version. Once it does, it notifies
// obj's key to every subscriber before it returns true, so every store following the Source holds that version or a
// newer one when it returns. It's for a suite that writes obj through another client, as a third party does, and waits
// until cluster state has seen the write.
func (s *Source) Delivered(obj client.Object) bool {
	key := client.ObjectKeyFromObject(obj)
	rv := parseResourceVersion(obj.GetResourceVersion())
	newest, ok := s.o.newestOf(key)
	switch {
	case ok && newest.GetUID() == obj.GetUID() && parseResourceVersion(newest.GetResourceVersion()) >= rv:
	case ok && newest.GetUID() != obj.GetUID() && parseResourceVersion(newest.GetResourceVersion()) > rv:
	case !ok && s.o.w.Load() >= rv:
	default:
		return false
	}
	s.o.notifyKeys(key)
	return true
}

// Removed returns whether the Source no longer serves obj, by its UID, or any object under obj's key for an obj
// without a UID. Once it doesn't, it notifies obj's key to every subscriber before it returns true, so no store
// following the Source holds obj when it returns. It's for a suite that deletes obj through another client and waits
// until cluster state has seen the removal.
func (s *Source) Removed(obj client.Object) bool {
	key := client.ObjectKeyFromObject(obj)
	is := func(o client.Object) bool { return obj.GetUID() == "" || o.GetUID() == obj.GetUID() }
	if newest, ok := s.o.newestOf(key); ok && is(newest) {
		return false
	}
	if cached, ok := s.o.lookupCache(key); ok && is(cached) && !s.o.hidden(cached) {
		return false
	}
	s.o.notifyKeys(key)
	return true
}
