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

package cluster

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	cpfake "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

// startNodeState starts node state's object stores against kubeClient and informers, as State.Register does under the
// manager, and returns once they're synced.
func startNodeState(t testing.TB, ctx context.Context, s *NodeState, informers *informertest.FakeInformers) {
	t.Helper()
	// FakeInformers makes informers unsafely, so make them before the stores start.
	for _, obj := range []client.Object{&v1.NodeClaim{}, &corev1.Node{}, &corev1.Pod{}} {
		if _, err := informers.FakeInformerFor(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	for _, store := range []interface{ Start(context.Context) error }{s.NodeClaimStore, s.NodeStore, s.PodStore} {
		go func() { _ = store.Start(ctx) }()
	}
	for deadline := time.Now().Add(10 * time.Second); !s.Synced(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("node state didn't sync")
		}
	}
}

// listSynced is the check cluster state's Synced made against the API server before node state's stores followed it:
// no NodeClaim cluster state holds is unlaunched, and it holds every managed NodeClaim and every Node the API server
// does.
func listSynced(ctx context.Context, t *testing.T, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, s *NodeState) bool {
	t.Helper()
	held := sets.New[string]()
	for name, f := range s.NodeClaimStore.List() {
		if f.NodeClaim == nil {
			continue
		}
		if f.NodeClaim.Status.ProviderID == "" {
			return false
		}
		held.Insert(name)
	}
	nodeClaims, err := nodeclaimutils.ListManaged(ctx, kubeClient, cloudProvider)
	if err != nil {
		t.Fatal(err)
	}
	nodes := &corev1.NodeList{}
	if err := kubeClient.List(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	tracked := sets.New[string]()
	for name := range s.TrackedNodeNames() {
		tracked.Insert(name)
	}
	for _, nodeClaim := range nodeClaims {
		if !held.Has(nodeClaim.Name) {
			return false
		}
	}
	for _, node := range nodes.Items {
		if !tracked.Has(node.Name) {
			return false
		}
	}
	return true
}

// TestStoreSyncAndNoUnlaunchedNodeClaimsIsAtLeastListSync checks that node state's stores having synced, with no
// unlaunched NodeClaim, implies what cluster state's Synced checked against a List of the API server: that it holds
// every managed NodeClaim and every Node, and none of its NodeClaims is unlaunched. So gating on the stores' sync with
// state.Requires, and on HasUnlaunchedNodeClaims, is at least as strong as the List check it replaces.
func TestStoreSyncAndNoUnlaunchedNodeClaimsIsAtLeastListSync(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a seeded source makes failures reproducible
	cloudProvider := cpfake.NewCloudProvider()
	for round := range 50 {
		ctx, cancel := context.WithCancel(options.ToContext(context.Background(), test.Options()))
		var objs []client.Object
		for i := range r.IntN(20) {
			nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nodeclaim-%d", i)}})
			switch r.IntN(4) {
			case 0:
				nodeClaim.Status.ProviderID = ""
			case 1:
				// Another cloud provider's NodeClaim, which cluster state doesn't hold or wait on.
				nodeClaim.Spec.NodeClassRef = &v1.NodeClassReference{Group: "other.sh", Kind: "OtherNodeClass", Name: "default"}
			}
			objs = append(objs, nodeClaim)
			if nodeClaim.Status.ProviderID != "" && r.IntN(2) == 0 {
				objs = append(objs, test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i)}, ProviderID: nodeClaim.Status.ProviderID}))
			}
		}
		for i := range r.IntN(5) {
			objs = append(objs, test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("unmanaged-%d", i)}}))
		}
		kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build()
		informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
		s := NewNodeState(ctx, clock.RealClock{}, kubeClient, informers, cloudProvider, nil)
		if s.Synced() {
			t.Fatalf("round %d: node state is synced before its stores listed", round)
		}
		startNodeState(t, ctx, s, informers)
		if launched := !s.HasUnlaunchedNodeClaims(); launched != listSynced(ctx, t, kubeClient, cloudProvider, s) {
			t.Fatalf("round %d: synced stores with every NodeClaim launched = %t, but the List check = %t", round, launched, !launched)
		}
		cancel()
	}
}

func heapInUse() float64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return float64(stats.HeapAlloc)
}

// BenchmarkPodStoreRetained reports the heap node state keeps for its pods beyond the informer's cache, which holds
// the pods either way: with the pod store following the informer, which holds the cache's pods, and with the pod store
// fed a deep copy of each pod, as the state.pod controller fed it from a client Get before it followed the informer.
func BenchmarkPodStoreRetained(b *testing.B) {
	const nodes, podsPerNode = 100, 50
	for _, feed := range []struct {
		name string
		feed func(s *NodeState, informer interface{ Add(metav1.Object) }, pod *corev1.Pod)
	}{
		{"informer", func(_ *NodeState, informer interface{ Add(metav1.Object) }, pod *corev1.Pod) { informer.Add(pod) }},
		{"deepcopy", func(s *NodeState, _ interface{ Add(metav1.Object) }, pod *corev1.Pod) {
			s.PodStore.Observe(pod.DeepCopy())
		}},
	} {
		b.Run(fmt.Sprintf("feed=%s/pods=%d", feed.name, nodes*podsPerNode), func(b *testing.B) {
			ctx, cancel := context.WithCancel(options.ToContext(context.Background(), test.Options()))
			defer cancel()
			cache := make([]*corev1.Pod, 0, nodes*podsPerNode)
			for n := range nodes {
				for range podsPerNode {
					cache = append(cache, test.Pod(test.PodOptions{NodeName: fmt.Sprintf("node-%d", n), ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "wide", "tier": "web"}, Annotations: map[string]string{"note": "a pod of a wide deployment"},
					}}))
				}
			}
			var retained float64
			for b.Loop() {
				informers := &informertest.FakeInformers{Scheme: scheme.Scheme}
				informer, err := informers.FakeInformerFor(ctx, &corev1.Pod{})
				if err != nil {
					b.Fatal(err)
				}
				before := heapInUse()
				s := NewNodeState(ctx, clock.RealClock{}, fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(), informers, cpfake.NewCloudProvider(), nil)
				startNodeState(b, ctx, s, informers)
				for _, pod := range cache {
					feed.feed(s, informer, pod)
				}
				retained += heapInUse() - before
				runtime.KeepAlive(s)
			}
			b.ReportMetric(retained/float64(b.N), "retained-B/op")
			runtime.KeepAlive(cache)
		})
	}
}
