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
	"testing"
	"time"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestAccessorEnqueuesObjectsWhoseDerivedFieldsChange(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	accessor, counts, _ := nodePoolAccessor(t, "a", "b", "c")
	counts.Update("a", func(int) int { return 1 })
	queue := newQueue(t)
	must(t, accessor.Start(ctx, queue))
	if got := drain(queue); len(got) != 1 || got[0] != "a" {
		t.Fatalf("Start queued %v, want a, the one NodePool with derived fields", got)
	}
	counts.Update("b", func(int) int { return 2 })
	counts.Update("b", func(int) int { return 3 })
	counts.Update("a", func(int) int { return 0 })
	counts.Update("c", func(int) int { return 0 })
	if got := drain(queue); len(got) != 2 || got[0] != "b" || got[1] != "a" {
		t.Fatalf("queued %v, want b once for its two changes, then a for its fields going away, and not c, which didn't change", got)
	}
}

func TestAccessorStopsEnqueueingOnceItsContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	accessor, counts, _ := nodePoolAccessor(t, "a")
	queue := newQueue(t)
	must(t, accessor.Start(ctx, queue))
	cancel()
	deadline := time.Now().Add(10 * time.Second)
	for {
		counts.Update("a", func(n int) int { return n + 1 })
		if drain(queue) == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the accessor still queues changes after its context is done")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
