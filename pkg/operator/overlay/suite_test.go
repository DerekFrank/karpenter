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
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/client-go/kubernetes/scheme"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var (
	ctx context.Context
	env *test.Environment
)

func TestOverlay(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Overlay")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed())
})

const providerIDField = "status.providerID"

type heldInformers struct {
	cache.Informers

	mu       sync.Mutex
	view     map[client.ObjectKey]client.Object
	handlers []toolscache.ResourceEventHandler
	queue    []func()
	informed []string
}

func (h *heldInformers) GetInformer(_ context.Context, obj client.Object, _ ...cache.InformerGetOption) (cache.Informer, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.informed = append(h.informed, fmt.Sprintf("%T", obj))
	return &heldInformer{parent: h}, nil
}

type heldInformer struct {
	cache.Informer
	parent *heldInformers
}

func (i *heldInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	i.parent.mu.Lock()
	defer i.parent.mu.Unlock()
	i.parent.handlers = append(i.parent.handlers, handler)
	return nil, nil
}

func (i *heldInformer) GetStore() toolscache.Store {
	return heldStore{parent: i.parent}
}

type heldStore struct {
	toolscache.Store
	parent *heldInformers
}

func (s heldStore) GetByKey(key string) (any, bool, error) {
	namespace, name, err := toolscache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, false, err
	}
	s.parent.mu.Lock()
	defer s.parent.mu.Unlock()
	obj, ok := s.parent.view[client.ObjectKey{Namespace: namespace, Name: name}]
	return obj, ok, nil
}

func (h *heldInformers) enqueue(obj client.Object, deleted bool) {
	obj = obj.DeepCopyObject().(client.Object)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queue = append(h.queue, func() {
		key := client.ObjectKeyFromObject(obj)
		old, existed := h.view[key]
		switch {
		case deleted:
			delete(h.view, key)
		default:
			h.view[key] = obj
		}
		for _, handler := range h.handlers {
			switch {
			case deleted:
				handler.OnDelete(obj)
			case existed:
				handler.OnUpdate(old, obj)
			default:
				handler.OnAdd(obj, false)
			}
		}
	})
}

func (h *heldInformers) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, deliver := range h.queue {
		deliver()
	}
	h.queue = nil
}

func (h *heldInformers) get(key client.ObjectKey, into client.Object) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	obj, ok := h.view[key]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", into)}, key.Name)
	}
	reflect.ValueOf(into).Elem().Set(reflect.ValueOf(obj.DeepCopyObject()).Elem())
	return nil
}

var _ = Describe("Overlay", func() {
	var informers *heldInformers
	var overlayClient *Client
	var nodeClaims, pods *overlay
	var nodeClaim *v1.NodeClaim
	var afterCacheRead func()

	BeforeEach(func() {
		informers = &heldInformers{view: map[client.ObjectKey]client.Object{}}
		afterCacheRead = func() {}
		overlaid := func(obj client.Object) bool {
			switch obj.(type) {
			case *v1.NodeClaim, *corev1.Pod:
				return true
			}
			return false
		}
		upstream := interceptor.NewClient(lo.Must(client.NewWithWatch(env.Config, client.Options{Scheme: scheme.Scheme})), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if overlaid(obj) {
					defer afterCacheRead()
					return informers.get(key, obj)
				}
				return c.Get(ctx, key, obj, opts...)
			},
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				switch list.(type) {
				case *v1.NodeClaimList, *corev1.PodList:
					defer afterCacheRead()
					informers.mu.Lock()
					defer informers.mu.Unlock()
					listOpts := (&client.ListOptions{}).ApplyOptions(opts)
					selector := listOpts.LabelSelector
					if selector == nil {
						selector = labels.Everything()
					}
					var items []runtime.Object
					for _, obj := range informers.view {
						if reflect.TypeOf(list).Elem().Name() != reflect.TypeOf(obj).Elem().Name()+"List" || (listOpts.Namespace != "" && obj.GetNamespace() != listOpts.Namespace) || !selector.Matches(labels.Set(obj.GetLabels())) {
							continue
						}
						if listOpts.FieldSelector != nil && !listOpts.FieldSelector.Empty() {
							value, _ := listOpts.FieldSelector.RequiresExactMatch(providerIDField)
							if nc, ok := obj.(*v1.NodeClaim); !ok || nc.Status.ProviderID != value {
								continue
							}
						}
						items = append(items, obj.DeepCopyObject())
					}
					return meta.SetList(list, items)
				}
				return c.List(ctx, list, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := c.Create(ctx, obj, opts...); err != nil {
					return err
				}
				informers.enqueue(obj, false)
				return nil
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if err := c.Update(ctx, obj, opts...); err != nil {
					return err
				}
				informers.enqueue(obj, false)
				return nil
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if err := c.Patch(ctx, obj, patch, opts...); err != nil {
					return err
				}
				informers.enqueue(obj, false)
				return nil
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if err := c.Delete(ctx, obj, opts...); err != nil {
					return err
				}
				remaining := obj.DeepCopyObject().(client.Object)
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), remaining); err != nil {
					informers.enqueue(obj, true)
					return client.IgnoreNotFound(err)
				}
				informers.enqueue(remaining, false)
				return nil
			},
			SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if err := c.SubResource(subResource).Patch(ctx, obj, patch, opts...); err != nil {
					return err
				}
				informers.enqueue(obj, false)
				return nil
			},
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if err := c.SubResource(subResource).Update(ctx, obj, opts...); err != nil {
					return err
				}
				informers.enqueue(obj, false)
				return nil
			},
		})
		var err error
		overlayClient, err = New(ctx, upstream, informers, &v1.NodeClaim{}, &corev1.Pod{})
		Expect(err).ToNot(HaveOccurred())
		Expect(overlayClient.IndexField(&v1.NodeClaim{}, providerIDField, func(o client.Object) []string { return []string{o.(*v1.NodeClaim).Status.ProviderID} })).To(Succeed())
		nodeClaims, _, err = overlayClient.overlayFor(&v1.NodeClaim{})
		Expect(err).ToNot(HaveOccurred())
		pods, _, err = overlayClient.overlayFor(&corev1.Pod{})
		Expect(err).ToNot(HaveOccurred())

		nodeClaim = test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: test.RandomName()}}})
		Expect(overlayClient.Create(ctx, nodeClaim)).To(Succeed())
		informers.release()
		DeferCleanup(func() {
			stored := &v1.NodeClaim{}
			if err := env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), stored); err == nil {
				stored.Finalizers = nil
				_ = env.Client.Update(ctx, stored)
				_ = env.Client.Delete(ctx, stored)
			}
		})
	})

	DescribeTable("reads its own writes before the informer delivers them",
		func(_, _ string, write func(c client.Client, nc *v1.NodeClaim), read func(c client.Client, nc *v1.NodeClaim) *v1.NodeClaim) {
			written := nodeClaim.DeepCopy()
			write(overlayClient, written)
			Expect(nodeClaims.len()).To(Equal(1))
			for _, delivered := range []bool{false, true} {
				if delivered {
					informers.release()
					Expect(nodeClaims.len()).To(Equal(0))
				}
				got := read(overlayClient, written)
				cmp, err := resourceversion.CompareResourceVersion(got.ResourceVersion, written.ResourceVersion)
				Expect(err).ToNot(HaveOccurred())
				Expect(cmp).To(BeNumerically(">=", 0), "delivered: %t, read %s, wrote %s", delivered, got.ResourceVersion, written.ResourceVersion)
			}
		},
		func(write, read string, _, _ any) string { return read + " after " + write },
		Entry(nil, "Patch", "Get", patchNodeClaim, getNodeClaim),
		Entry(nil, "Patch", "List", patchNodeClaim, listNodeClaims),
		Entry(nil, "Update", "Get", updateNodeClaim, getNodeClaim),
		Entry(nil, "Update", "List", updateNodeClaim, listNodeClaims),
		Entry(nil, "status Patch", "Get", patchNodeClaimStatus, getNodeClaim),
		Entry(nil, "status Patch", "List", patchNodeClaimStatus, listNodeClaims),
		Entry(nil, "status Patch", "List by field", patchNodeClaimStatus, listNodeClaimsByProviderID),
		Entry(nil, "status Update", "Get", updateNodeClaimStatus, getNodeClaim),
		Entry(nil, "status Update", "List", updateNodeClaimStatus, listNodeClaims),
		Entry(nil, "status Update", "List by field", updateNodeClaimStatus, listNodeClaimsByProviderID),
	)

	DescribeTable("reads its own write when the informer delivers it between the cache read and the merge",
		func(read func(c client.Client, nc *v1.NodeClaim) *v1.NodeClaim) {
			written := nodeClaim.DeepCopy()
			patchNodeClaim(overlayClient, written)
			afterCacheRead = func() {
				afterCacheRead = func() {}
				informers.release()
			}
			got := read(overlayClient, written)
			Expect(got.ResourceVersion).To(Equal(written.ResourceVersion))
		},
		Entry("Get", getNodeClaim),
		Entry("List", listNodeClaims),
	)
	It("should read an object it created before the informer delivers it", func() {
		created := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: nodeClaim.Labels}})
		Expect(overlayClient.Create(ctx, created)).To(Succeed())
		DeferCleanup(func() { _ = env.Client.Delete(ctx, created) })

		Expect(overlayClient.Get(ctx, client.ObjectKeyFromObject(created), &v1.NodeClaim{})).To(Succeed())
		list := &v1.NodeClaimList{}
		Expect(overlayClient.List(ctx, list, client.MatchingLabels(nodeClaim.Labels))).To(Succeed())
		Expect(list.Items).To(ConsistOf(HaveField("Name", nodeClaim.Name), HaveField("Name", created.Name)))
		informers.release()
		Expect(nodeClaims.len()).To(Equal(0))
	})
	It("should only add an object it wrote to a List by field when the field matches", func() {
		created := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: nodeClaim.Labels}})
		Expect(overlayClient.Create(ctx, created)).To(Succeed())
		DeferCleanup(func() { _ = env.Client.Delete(ctx, created) })
		stored := created.DeepCopy()
		created.Status.ProviderID = test.RandomProviderID()
		Expect(overlayClient.Status().Patch(ctx, created, client.MergeFrom(stored))).To(Succeed())

		list := &v1.NodeClaimList{}
		Expect(overlayClient.List(ctx, list, client.MatchingFields{providerIDField: created.Status.ProviderID})).To(Succeed())
		Expect(list.Items).To(ConsistOf(HaveField("Name", created.Name)))
		Expect(overlayClient.List(ctx, list, client.MatchingFields{providerIDField: test.RandomProviderID()})).To(Succeed())
		Expect(list.Items).To(BeEmpty())
	})
	It("should hide an object it deleted until the informer delivers the delete", func() {
		pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
		Expect(overlayClient.Create(ctx, pod)).To(Succeed())
		informers.release()

		Expect(overlayClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
		informers.release()
		Expect(pods.len()).To(Equal(0))
		err := overlayClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "got %v", err)
	})
	It("should read an object a finalizer keeps as terminating once it deletes it", func() {
		stored := nodeClaim.DeepCopy()
		nodeClaim.Finalizers = []string{v1.TerminationFinalizer}
		Expect(overlayClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		informers.release()

		Expect(overlayClient.Delete(ctx, nodeClaim)).To(Succeed())
		for _, delivered := range []bool{false, true} {
			if delivered {
				informers.release()
				Expect(nodeClaims.len()).To(Equal(0))
			}
			got := &v1.NodeClaim{}
			Expect(overlayClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), got)).To(Succeed())
			Expect(got.DeletionTimestamp.IsZero()).To(BeFalse(), "delivered: %t", delivered)
			list := &v1.NodeClaimList{}
			Expect(overlayClient.List(ctx, list, client.MatchingLabels(nodeClaim.Labels))).To(Succeed())
			Expect(list.Items).To(ConsistOf(HaveField("DeletionTimestamp", Not(BeNil()))), "delivered: %t", delivered)
		}
	})
	It("should forget an object it created once it deletes it", func() {
		pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}})
		Expect(overlayClient.Create(ctx, pod)).To(Succeed())
		Expect(overlayClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})).To(Succeed())

		Expect(overlayClient.Delete(ctx, pod)).To(Succeed())
		Expect(pods.len()).To(Equal(0))
		err := overlayClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "got %v", err)
		list := &corev1.PodList{}
		Expect(overlayClient.List(ctx, list, client.InNamespace("default"))).To(Succeed())
		Expect(list.Items).ToNot(ContainElement(HaveField("Name", pod.Name)))
	})
	It("should forget an object it created once a delete of its terminating copy removes it", func() {
		pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}, NodeName: test.RandomName()})
		Expect(overlayClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = env.Client.Delete(ctx, pod, client.GracePeriodSeconds(0)) })

		Expect(overlayClient.Delete(ctx, pod)).To(Succeed())
		got := &corev1.Pod{}
		Expect(overlayClient.Get(ctx, client.ObjectKeyFromObject(pod), got)).To(Succeed())
		Expect(got.DeletionTimestamp.IsZero()).To(BeFalse())

		Expect(overlayClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
		err := overlayClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "got %v", err)
	})
	It("should not add back an object it updated once the informer's store drops it", func() {
		stored := nodeClaim.DeepCopy()
		nodeClaim.Labels["patched"] = "true"
		Expect(overlayClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		Expect(env.Client.Delete(ctx, nodeClaim)).To(Succeed())
		informers.mu.Lock()
		delete(informers.view, client.ObjectKeyFromObject(nodeClaim))
		informers.mu.Unlock()

		err := overlayClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), &v1.NodeClaim{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "got %v", err)
		list := &v1.NodeClaimList{}
		Expect(overlayClient.List(ctx, list, client.MatchingLabels{v1.NodePoolLabelKey: nodeClaim.Labels[v1.NodePoolLabelKey]})).To(Succeed())
		Expect(list.Items).To(BeEmpty())
	})
	It("should add an object it updated into the selector to a List", func() {
		other := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: "other"}}})
		Expect(overlayClient.Create(ctx, other)).To(Succeed())
		DeferCleanup(func() { _ = env.Client.Delete(ctx, other) })
		informers.release()
		stored := other.DeepCopy()
		other.Labels[v1.NodePoolLabelKey] = nodeClaim.Labels[v1.NodePoolLabelKey]
		Expect(overlayClient.Patch(ctx, other, client.MergeFrom(stored))).To(Succeed())

		list := &v1.NodeClaimList{}
		Expect(overlayClient.List(ctx, list, client.MatchingLabels{v1.NodePoolLabelKey: nodeClaim.Labels[v1.NodePoolLabelKey]})).To(Succeed())
		Expect(list.Items).To(ConsistOf(HaveField("Name", nodeClaim.Name), HaveField("Name", other.Name)))
	})
	It("should forget an object the informer relists past without ever delivering", func() {
		ghost := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: nodeClaim.Labels}})
		Expect(overlayClient.Create(ctx, ghost)).To(Succeed())
		Expect(env.Client.Delete(ctx, ghost)).To(Succeed())
		informers.mu.Lock()
		informers.queue = nil
		informers.mu.Unlock()
		Expect(overlayClient.Get(ctx, client.ObjectKeyFromObject(ghost), &v1.NodeClaim{})).To(Succeed())

		stored := nodeClaim.DeepCopy()
		nodeClaim.Labels["delivered"] = "true"
		Expect(env.Client.Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		informers.enqueue(nodeClaim, false)
		informers.release()
		Expect(nodeClaims.len()).To(Equal(0))
		err := overlayClient.Get(ctx, client.ObjectKeyFromObject(ghost), &v1.NodeClaim{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "got %v", err)
		list := &v1.NodeClaimList{}
		Expect(overlayClient.List(ctx, list, client.MatchingLabels{v1.NodePoolLabelKey: nodeClaim.Labels[v1.NodePoolLabelKey]})).To(Succeed())
		Expect(list.Items).To(ConsistOf(HaveField("Name", nodeClaim.Name)))
	})
	It("should not hold a write response the informer has already delivered", func() {
		stored := nodeClaim.DeepCopy()
		nodeClaim.Labels["value"] = "first"
		Expect(overlayClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		late := nodeClaim.DeepCopy()
		stored = nodeClaim.DeepCopy()
		nodeClaim.Labels["value"] = "second"
		Expect(overlayClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		informers.release()

		Expect(overlayClient.record(late, false)).To(Succeed())
		Expect(nodeClaims.len()).To(Equal(0))
		got := &v1.NodeClaim{}
		Expect(overlayClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), got)).To(Succeed())
		Expect(got.Labels["value"]).To(Equal("second"))
	})
	It("should serve a newer version written by another writer", func() {
		stored := nodeClaim.DeepCopy()
		nodeClaim.Labels["owner"] = "ours"
		Expect(overlayClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		stored = nodeClaim.DeepCopy()
		nodeClaim.Labels["owner"] = "theirs"
		Expect(env.Client.Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		informers.enqueue(nodeClaim, false)
		informers.release()

		got := &v1.NodeClaim{}
		Expect(overlayClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), got)).To(Succeed())
		Expect(got.Labels["owner"]).To(Equal("theirs"))
	})
	It("should drop an object from a List once its write moves it out of the selector", func() {
		selector := client.MatchingLabels{v1.NodePoolLabelKey: nodeClaim.Labels[v1.NodePoolLabelKey]}
		stored := nodeClaim.DeepCopy()
		nodeClaim.Labels[v1.NodePoolLabelKey] = "other"
		Expect(overlayClient.Patch(ctx, nodeClaim, client.MergeFrom(stored))).To(Succeed())
		list := &v1.NodeClaimList{}
		Expect(overlayClient.List(ctx, list, selector)).To(Succeed())
		Expect(list.Items).To(BeEmpty())
	})
	It("should pass through kinds it doesn't overlay", func() {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: test.RandomName(), Namespace: "default"}}
		Expect(overlayClient.Create(ctx, cm)).To(Succeed())
		DeferCleanup(func() { _ = env.Client.Delete(ctx, cm) })
		Expect(overlayClient.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})).To(Succeed())
		Expect(informers.informed).To(ConsistOf("*v1.NodeClaim", "*v1.Pod"))
	})
})

var patchNodeClaim = func(c client.Client, nc *v1.NodeClaim) {
	stored := nc.DeepCopy()
	nc.Labels["patched"] = "true"
	Expect(c.Patch(ctx, nc, client.MergeFrom(stored))).To(Succeed())
}

var updateNodeClaim = func(c client.Client, nc *v1.NodeClaim) {
	nc.Labels["updated"] = "true"
	Expect(c.Update(ctx, nc)).To(Succeed())
}

var patchNodeClaimStatus = func(c client.Client, nc *v1.NodeClaim) {
	stored := nc.DeepCopy()
	nc.Status.ProviderID = test.RandomProviderID()
	Expect(c.Status().Patch(ctx, nc, client.MergeFrom(stored))).To(Succeed())
}

var updateNodeClaimStatus = func(c client.Client, nc *v1.NodeClaim) {
	nc.Status.ProviderID = test.RandomProviderID()
	Expect(c.Status().Update(ctx, nc)).To(Succeed())
}

var getNodeClaim = func(c client.Client, nc *v1.NodeClaim) *v1.NodeClaim {
	got := &v1.NodeClaim{}
	Expect(c.Get(ctx, client.ObjectKeyFromObject(nc), got)).To(Succeed())
	return got
}

var listNodeClaims = func(c client.Client, nc *v1.NodeClaim) *v1.NodeClaim {
	list := &v1.NodeClaimList{}
	Expect(c.List(ctx, list, client.MatchingLabels{v1.NodePoolLabelKey: nc.Labels[v1.NodePoolLabelKey]})).To(Succeed())
	Expect(list.Items).To(HaveLen(1))
	return &list.Items[0]
}

var listNodeClaimsByProviderID = func(c client.Client, nc *v1.NodeClaim) *v1.NodeClaim {
	list := &v1.NodeClaimList{}
	Expect(c.List(ctx, list, client.MatchingFields{providerIDField: nc.Status.ProviderID})).To(Succeed())
	Expect(list.Items).To(HaveLen(1))
	return &list.Items[0]
}
