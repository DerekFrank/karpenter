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
	"testing"

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

func nodePoolAccessor(t *testing.T, nodePools ...string) (*Accessor[*v1.NodePool, *v1.NodePoolList, nodePoolDerived], *Store[string, int], client.Client) {
	t.Helper()
	objs := lo.Map(nodePools, func(name string, _ int) client.Object {
		return &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}}
	})
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).WithStatusSubresource(&v1.NodePool{}).Build()
	counts := NewStore[string, int]()
	accessor := NewAccessor(kubeClient,
		func() *v1.NodePool { return &v1.NodePool{} },
		func() *v1.NodePoolList { return &v1.NodePoolList{} },
		func(l *v1.NodePoolList) []*v1.NodePool {
			return lo.Map(l.Items, func(_ v1.NodePool, i int) *v1.NodePool { return &l.Items[i] })
		},
		func(np *v1.NodePool) nodePoolDerived {
			n, _ := counts.Get(np.Name)
			return nodePoolDerived{NodeClaims: n}
		}, counts.Source())
	return accessor, counts, kubeClient
}

func TestReaderReturnsObjectsWithDerivedFields(t *testing.T) {
	accessor, counts, _ := nodePoolAccessor(t, "a", "b")
	counts.Update("a", func(int) int { return 3 })

	np, err := accessor.Get(ctx, client.ObjectKey{Name: "a"})
	must(t, err)
	if np.Object.Name != "a" || np.Derived.NodeClaims != 3 {
		t.Fatalf("Get = %s with %d NodeClaims, want a with 3", np.Object.Name, np.Derived.NodeClaims)
	}
	nodePools, err := accessor.List(ctx)
	must(t, err)
	got := map[string]int{}
	for _, np := range nodePools {
		got[np.Object.Name] = np.Derived.NodeClaims
	}
	if len(got) != 2 || got["a"] != 3 || got["b"] != 0 {
		t.Fatalf("List = %v, want a with 3 and b with 0", got)
	}
	if _, err := accessor.Get(ctx, client.ObjectKey{Name: "missing"}); !errors.IsNotFound(err) {
		t.Fatalf("Get of a missing NodePool returned %v, want NotFound", err)
	}
}

func TestReaderObjectsAreTheAPIObjects(t *testing.T) {
	accessor, counts, kubeClient := nodePoolAccessor(t, "a")
	counts.Update("a", func(int) int { return 3 })
	np, err := accessor.Get(ctx, client.ObjectKey{Name: "a"})
	must(t, err)

	stored := np.Object.DeepCopy()
	np.Object.Status.Nodes = new(int64(np.Derived.NodeClaims))
	must(t, kubeClient.Status().Patch(ctx, np.Object, client.MergeFrom(stored)))
	written, err := accessor.Get(ctx, client.ObjectKey{Name: "a"})
	must(t, err)
	if lo.FromPtr(written.Object.Status.Nodes) != 3 {
		t.Fatalf("wrote status.nodes %d, want 3", lo.FromPtr(written.Object.Status.Nodes))
	}
}

func TestJoinAccessorCopiesUnlessTheCallerOptsOut(t *testing.T) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Observe(nodeClaim("b", "y", 2, "2"))
	accessor := NewJoinAccessor(nodeClaims, func(_ types.NamespacedName, nc *v1.NodeClaim) (*v1.NodeClaim, int, bool) { return nc, 1, nc != nil })
	key := types.NamespacedName{Name: "a"}
	got, ok := accessor.Get(key)
	held, _ := nodeClaims.Get(key)
	if !ok || got.Object == held || got.Object.Name != "a" || got.Derived != 1 {
		t.Fatalf("Get = %+v, want a copy of a with its derived field", got)
	}
	if shared, _ := accessor.Get(key, client.UnsafeDisableDeepCopy); shared.Object != held {
		t.Fatal("Get with UnsafeDisableDeepCopy copied")
	}
	if views, err := accessor.List(client.MatchingLabels{v1.NodePoolLabelKey: "y"}); err != nil || len(views) != 1 || views[0].Object.Name != "b" {
		t.Fatalf("List = %v, %v, want only b", views, err)
	}
}

func TestJoinAccessorListsByAnIndexedField(t *testing.T) {
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	accessor := NewJoinAccessor(nodeClaims, func(_ types.NamespacedName, nc *v1.NodeClaim) (*v1.NodeClaim, struct{}, bool) {
		return nc, struct{}{}, nc != nil
	}).
		IndexField("pool", func(_ types.NamespacedName, nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] })
	nodeClaims.Observe(nodeClaim("a", "x", 1, "2"))
	nodeClaims.Observe(nodeClaim("b", "y", 2, "2"))
	nodeClaims.Observe(nodeClaim("c", "x", 3, "2"))
	names := func(views []View[*v1.NodeClaim, struct{}]) []string {
		var out []string
		for _, v := range views {
			out = append(out, v.Object.Name)
		}
		slices.Sort(out)
		return out
	}
	views, err := accessor.List(client.MatchingFields{"pool": "x"})
	if err != nil || !slices.Equal(names(views), []string{"a", "c"}) {
		t.Fatalf("List = %v, %v, want a and c", names(views), err)
	}
	nodeClaims.Observe(nodeClaim("c", "y", 4, "2"))
	if views, _ := accessor.List(client.MatchingFields{"pool": "x"}); !slices.Equal(names(views), []string{"a"}) {
		t.Fatalf("List = %v once c moved, want a", names(views))
	}
	if views, err := accessor.List(client.MatchingFields{"pool": "none"}); err != nil || len(views) != 0 {
		t.Fatalf("List = %v, %v for a value nothing indexes to, want nothing", names(views), err)
	}
	if _, err := accessor.List(client.MatchingFields{"unindexed": "x"}); err == nil {
		t.Fatal("List by an unindexed field succeeded, want an error as the client returns")
	}
}
