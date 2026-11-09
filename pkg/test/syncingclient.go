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

package test

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"sigs.k8s.io/karpenter/pkg/state/overlay"
)

// syncTimeout bounds a write's wait for the cache and cluster state to see it. A write the cache never sees fails the
// write, so a spec never runs on a cache or a store that hasn't seen what it wrote.
const syncTimeout = 30 * time.Second

// SyncingClient is a suite's client: the third party the controller under test shares the cluster with. It writes to
// the API server directly, not through the overlay, so its writes reach the cache and cluster state through the
// informer, as another writer's do. Each write returns once the cache holds it and, for a kind the overlay follows,
// once every store following the overlay holds it, or its removal: the overlay's Source has delivered the write's
// resourceVersion, or the object's removal, to every store. It reads through the overlay client, as the controller
// under test does.
type SyncingClient struct {
	// Client is the overlay client, which the SyncingClient reads through.
	*overlay.Client
	// writer writes to the API server without the overlay recording the write.
	writer client.Client
	// direct reads the API server, for a write that returns no object, such as a Binding.
	direct client.Reader
}

// NewSyncingClient returns a client that reads through overlaid, and writes through writer, which shares overlaid's
// cache but not its overlay, then waits for overlaid's cache and Sources to see each write.
func NewSyncingClient(overlaid *overlay.Client, writer client.Client, direct client.Reader) *SyncingClient {
	return &SyncingClient{Client: overlaid, writer: writer, direct: direct}
}

// await polls done until it returns true, backing off from 1ms to 20ms, or fails once ctx is done.
func await(ctx context.Context, what string, obj client.Object, done func(context.Context) (bool, error)) error {
	for interval := time.Millisecond; ; interval = min(interval*3/2, 20*time.Millisecond) {
		ok, err := done(ctx)
		if err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s of %T %s, %w", what, obj, client.ObjectKeyFromObject(obj), ctx.Err())
		case <-time.After(interval):
		}
	}
}

// synced waits until the cache and cluster state hold obj's version, or a newer one.
func (c *SyncingClient) synced(ctx context.Context, obj client.Object) error {
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()
	if src, err := c.Source(obj); err == nil {
		return await(ctx, "cluster state", obj, func(context.Context) (bool, error) { return src.Delivered(obj), nil })
	}
	return await(ctx, "the cache", obj, func(ctx context.Context) (bool, error) {
		cached := obj.DeepCopyObject().(client.Object)
		switch err := c.Client.Get(ctx, client.ObjectKeyFromObject(obj), cached); {
		case apierrors.IsNotFound(err):
			return false, nil
		case err != nil:
			// A kind the cache can't watch has nothing to wait for.
			return true, nil //nolint:nilerr
		}
		return cached.GetUID() != obj.GetUID() || resourceVersion(cached) >= resourceVersion(obj), nil
	})
}

// removed waits until neither the cache nor cluster state holds obj.
func (c *SyncingClient) removed(ctx context.Context, obj client.Object) error {
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()
	if src, err := c.Source(obj); err == nil {
		return await(ctx, "cluster state to drop", obj, func(context.Context) (bool, error) { return src.Removed(obj), nil })
	}
	return await(ctx, "the cache to drop", obj, func(ctx context.Context) (bool, error) {
		cached := obj.DeepCopyObject().(client.Object)
		err := c.Client.Get(ctx, client.ObjectKeyFromObject(obj), cached)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return err == nil && obj.GetUID() != "" && cached.GetUID() != obj.GetUID(), client.IgnoreNotFound(err)
	})
}

// refreshed waits for the cache and cluster state to see the object under obj's key as the API server has it now, for
// a write that returns no object, such as a Binding or an eviction.
func (c *SyncingClient) refreshed(ctx context.Context, obj client.Object) error {
	current := obj.DeepCopyObject().(client.Object)
	if err := c.direct.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
		if apierrors.IsNotFound(err) {
			return c.removed(ctx, obj)
		}
		return err
	}
	return c.synced(ctx, current)
}

func (c *SyncingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	// A Binding's create returns no object: wait for the pod it binds, as the API server has it.
	key := client.ObjectKeyFromObject(obj)
	if err := c.writer.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if _, ok := obj.(*corev1.Binding); ok {
		return c.refreshed(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}})
	}
	return c.synced(ctx, obj)
}

func (c *SyncingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.writer.Update(ctx, obj, opts...); err != nil {
		return err
	}
	return c.written(ctx, obj)
}

func (c *SyncingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if err := c.writer.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	return c.written(ctx, obj)
}

// written waits for a write that returned obj: the object as stored, or one the write removed, such as an update that
// removes the last finalizer of an object being deleted.
func (c *SyncingClient) written(ctx context.Context, obj client.Object) error {
	if obj.GetDeletionTimestamp() != nil && len(obj.GetFinalizers()) == 0 {
		return c.removed(ctx, obj)
	}
	return c.synced(ctx, obj)
}

// Delete waits for a delete: the object as the delete left it, which the API server returns when a finalizer holds it,
// or its removal.
func (c *SyncingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	rv := obj.GetResourceVersion()
	if err := c.writer.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	if obj.GetResourceVersion() != rv && obj.GetDeletionTimestamp() != nil && len(obj.GetFinalizers()) > 0 {
		return c.synced(ctx, obj)
	}
	return c.refreshed(ctx, obj)
}

// DeleteAllOf waits for the removal of each object the delete removed, and for each one a finalizer holds as deleting.
func (c *SyncingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	options := (&client.DeleteAllOfOptions{}).ApplyOptions(opts)
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return err
	}
	list, err := c.Scheme().New(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	if err != nil {
		return err
	}
	if err := c.direct.List(ctx, list.(client.ObjectList), &options.ListOptions); err != nil {
		return err
	}
	if err := c.writer.DeleteAllOf(ctx, obj, opts...); err != nil {
		return err
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := c.refreshed(ctx, item.(client.Object)); err != nil {
			return err
		}
	}
	return nil
}

func (c *SyncingClient) Status() client.SubResourceWriter { return c.SubResource("status") }

func (c *SyncingClient) SubResource(subResource string) client.SubResourceClient {
	return &syncingSubResourceClient{SubResourceClient: c.writer.SubResource(subResource), parent: c}
}

type syncingSubResourceClient struct {
	client.SubResourceClient
	parent *SyncingClient
}

// Create waits for the object the subresource belongs to, such as an evicted pod, as the API server has it.
func (s *syncingSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	if err := s.SubResourceClient.Create(ctx, obj, subResource, opts...); err != nil {
		return err
	}
	return s.parent.refreshed(ctx, obj)
}

func (s *syncingSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if err := s.SubResourceClient.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if (&client.SubResourceUpdateOptions{}).ApplyOptions(opts).SubResourceBody != nil {
		return s.parent.refreshed(ctx, obj)
	}
	return s.parent.written(ctx, obj)
}

func (s *syncingSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if err := s.SubResourceClient.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	if (&client.SubResourcePatchOptions{}).ApplyOptions(opts).SubResourceBody != nil {
		return s.parent.refreshed(ctx, obj)
	}
	return s.parent.written(ctx, obj)
}

func (s *syncingSubResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return s.SubResourceClient.Apply(ctx, obj, opts...)
}

func resourceVersion(obj client.Object) uint64 {
	rv, _ := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	return rv
}
