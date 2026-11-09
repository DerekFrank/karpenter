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
	"slices"

	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

type nodePoolDerived struct{ NodeClaims int }

type nodePoolIntent struct{ Registered Intended[bool] }

func nodePoolAccessor(nodePools ...string) (*Accessor[*v1.NodePool, *v1.NodePoolList, nodePoolDerived, nodePoolIntent], *Store[string, int, struct{}, struct{}], client.Client) {
	objs := lo.Map(nodePools, func(name string, _ int) client.Object {
		return &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}}
	})
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).WithStatusSubresource(&v1.NodePool{}).Build()
	counts := NewFedStore[string, int, struct{}, struct{}]()
	accessor := NewAccessor(kubeClient,
		func() *v1.NodePool { return &v1.NodePool{} },
		func() *v1.NodePoolList { return &v1.NodePoolList{} },
		func(l *v1.NodePoolList) []*v1.NodePool {
			return lo.Map(l.Items, func(_ v1.NodePool, i int) *v1.NodePool { return &l.Items[i] })
		},
		func(np *v1.NodePool) (nodePoolDerived, nodePoolIntent) {
			n, _ := counts.Get(np.Name)
			return nodePoolDerived{NodeClaims: n.Object}, nodePoolIntent{}
		}, counts.Source())
	return accessor, counts, kubeClient
}

var _ = ginkgo.Describe("Accessor", func() {
	ginkgo.It("should return objects with derived fields", func() {
		accessor, counts, _ := nodePoolAccessor("a", "b")
		counts.Feed("a", func(int, bool) (int, bool) { return 3, true })

		np, err := accessor.Get(ctx, client.ObjectKey{Name: "a"})
		Expect(err).ToNot(HaveOccurred())
		Expect(np.Object.Name).To(Equal("a"))
		Expect(np.Derived.NodeClaims).To(Equal(3), "Get a's NodeClaims")
		nodePools, err := accessor.List(ctx)
		Expect(err).ToNot(HaveOccurred())
		got := map[string]int{}
		for _, np := range nodePools {
			got[np.Object.Name] = np.Derived.NodeClaims
		}
		Expect(got).To(Equal(map[string]int{"a": 3, "b": 0}), "List, want a with 3 and b with 0")
		_, err = accessor.Get(ctx, client.ObjectKey{Name: "missing"})
		Expect(errors.IsNotFound(err)).To(BeTrue(), "Get of a missing NodePool returned %v, want NotFound", err)
	})
	ginkgo.It("should return objects that are the API objects", func() {
		accessor, counts, kubeClient := nodePoolAccessor("a")
		counts.Feed("a", func(int, bool) (int, bool) { return 3, true })
		np, err := accessor.Get(ctx, client.ObjectKey{Name: "a"})
		Expect(err).ToNot(HaveOccurred())

		stored := np.Object.DeepCopy()
		np.Object.Status.Nodes = new(int64(np.Derived.NodeClaims))
		Expect(kubeClient.Status().Patch(ctx, np.Object, client.MergeFrom(stored))).To(Succeed())
		written, err := accessor.Get(ctx, client.ObjectKey{Name: "a"})
		Expect(err).ToNot(HaveOccurred())
		Expect(lo.FromPtr(written.Object.Status.Nodes)).To(Equal(int64(3)), "status.nodes written")
	})
})

var _ = ginkgo.Describe("StoreAccessor", func() {
	ginkgo.It("should copy unless the caller opts out", func() {
		nodeClaims := newNodeClaims()
		nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
		nodeClaims.Observe(nodeClaim("b", "y", 2, "2"))
		deletion := NewIntent(nodeClaims, func(i *nodeClaimIntent) *Intended[bool] { return &i.Deletion })
		accessor := NewStoreAccessor(nodeClaims.Reader)
		key := types.NamespacedName{Name: "a"}
		got, ok := accessor.Get(key)
		e, _ := nodeClaims.Get(key)
		held := e.Object
		Expect(ok).To(BeTrue())
		Expect(got.Object).ToNot(BeIdenticalTo(held), "Get, want a copy of a")
		Expect(got.Object.Name).To(Equal("a"))
		deletion.Set(key, true)
		got, _ = accessor.Get(key)
		Expect(got.Intent.Deletion.Value).To(BeTrue(), "Get, want a with its intent")
		shared, _ := accessor.Get(key, client.UnsafeDisableDeepCopy)
		Expect(shared.Object).To(BeIdenticalTo(held), "Get with UnsafeDisableDeepCopy copied")
		views, err := accessor.List(client.MatchingLabels{v1.NodePoolLabelKey: "y"})
		Expect(err).ToNot(HaveOccurred())
		Expect(views).To(HaveLen(1), "List, want only b")
		Expect(views[0].Object.Name).To(Equal("b"), "List, want only b")
	})
	ginkgo.It("should list by an indexed field", func() {
		nodeClaims := newNodeClaims()
		accessor := NewStoreAccessor(nodeClaims.Reader).IndexField("pool", poolOf)
		nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
		nodeClaims.Observe(nodeClaim("b", "y", 2, "2"))
		nodeClaims.Observe(nodeClaim("c", "x", 3, "2"))
		names := func(views []View[*v1.NodeClaim, struct{}, nodeClaimIntent]) []string {
			var out []string
			for _, v := range views {
				out = append(out, v.Object.Name)
			}
			slices.Sort(out)
			return out
		}
		views, err := accessor.List(client.MatchingFields{"pool": "x"})
		Expect(err).ToNot(HaveOccurred())
		Expect(names(views)).To(Equal([]string{"a", "c"}))
		nodeClaims.Observe(nodeClaim("c", "y", 4, "2"))
		views, _ = accessor.List(client.MatchingFields{"pool": "x"})
		Expect(names(views)).To(Equal([]string{"a"}), "List once c moved")
		views, err = accessor.List(client.MatchingFields{"pool": "none"})
		Expect(err).ToNot(HaveOccurred())
		Expect(views).To(BeEmpty(), "List for a value nothing indexes to, want nothing")
		_, err = accessor.List(client.MatchingFields{"unindexed": "x"})
		Expect(err).To(HaveOccurred(), "List by an unindexed field succeeded, want an error as the client returns")
	})
})
