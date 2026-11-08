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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func pod(name, rv string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: types.UID(name), ResourceVersion: rv}}
}

// TestObjectStoreHoldsTheInformersOwnObjects checks that a store following an informer holds the objects in the
// informer's cache, not copies: a copy from the first List or a write stands in for an object only until the informer
// delivers its own object for the same version.
func TestObjectStoreHoldsTheInformersOwnObjects(t *testing.T) {
	ctx := t.Context()
	listed := pod("listed", "")
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(listed).Build()
	informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
	pods := NewObjectStore[*corev1.Pod](kubeClient, informers)
	go func() { _ = pods.Start(ctx) }()
	for deadline := time.Now().Add(10 * time.Second); !pods.Synced(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the store didn't sync")
		}
	}
	informer, err := informers.FakeInformerFor(ctx, &corev1.Pod{})
	must(t, err)
	held := func(name string) *corev1.Pod {
		p, _ := pods.Get(types.NamespacedName{Namespace: "default", Name: name})
		return p
	}

	// The List's copy stands in for the listed pod until the informer's initial delivery of it.
	cachedListed := &corev1.Pod{}
	must(t, kubeClient.Get(ctx, client.ObjectKeyFromObject(listed), cachedListed))
	if held("listed") == cachedListed {
		t.Fatal("the store holds the cache's pod before the informer delivered it")
	}
	informer.Add(cachedListed)
	if held("listed") != cachedListed {
		t.Fatal("the store holds a copy of the listed pod after the informer delivered its own")
	}

	// The informer's pod is held as it is.
	cachedAdded := pod("added", "5")
	informer.Add(cachedAdded)
	if held("added") != cachedAdded {
		t.Fatal("the store holds a copy of the pod the informer added")
	}

	// A write's copy is the newest version until the informer delivers its own object for it.
	written := pod("added", "6")
	pods.change(written, false, copied)
	if held("added") != written {
		t.Fatal("the store doesn't hold the write, the newest version")
	}
	cachedUpdated := pod("added", "6")
	informer.Update(cachedAdded, cachedUpdated)
	if held("added") != cachedUpdated {
		t.Fatal("the store holds the write's copy after the informer delivered its own object for the version")
	}

	// A caller's version is replaced by the informer's own object for it in the same way.
	observedPod := pod("added", "7")
	pods.Observe(observedPod)
	cachedObserved := pod("added", "7")
	informer.Update(cachedUpdated, cachedObserved)
	if held("added") != cachedObserved {
		t.Fatal("the store holds the observed copy after the informer delivered its own object for the version")
	}
	// An older version from the informer still never replaces a newer one.
	informer.Update(cachedObserved, cachedUpdated)
	if held("added") != cachedObserved {
		t.Fatal("an older version from the informer replaced the newer one")
	}
}
