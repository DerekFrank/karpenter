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
// them, without waiting for the informer.
package overlay

import (
	"context"
	"fmt"
	"maps"
	"reflect"
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
		if s, ok := informer.(interface{ GetStore() toolscache.Store }); ok {
			store := s.GetStore()
			ov.cached = func(key types.NamespacedName) bool {
				_, exists, err := store.GetByKey(toolscache.NewObjectName(key.Namespace, key.Name).String())
				return err == nil && exists
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
	o.record(obj.DeepCopyObject().(client.Object), c.indexersFor(gvk), created)
	return nil
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
// remains in storage, and otherwise forgets the client's writes of the object.
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
	o.forget(client.ObjectKeyFromObject(obj))
	return nil
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	o, _, err := c.overlayFor(obj)
	if err != nil {
		return err
	}
	if o == nil {
		return c.Client.Get(ctx, key, obj, opts...)
	}
	e, ok := o.get(key)
	err = c.Client.Get(ctx, key, obj, opts...)
	if !ok || (err != nil && !apierrors.IsNotFound(err)) {
		return err
	}
	if err == nil && !e.newerThan(obj) {
		return nil
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
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if o.newest.Load() <= w0 {
		return nil
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
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
