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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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
	"sigs.k8s.io/karpenter/pkg/state/overlay"
	"sigs.k8s.io/karpenter/pkg/test"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

// startNodeState starts node state's object stores against kubeClient and informers, as State.Register does under the
// manager, and returns once they're synced. t is a spec's GinkgoT() or a benchmark's *testing.B.
func startNodeState(t interface {
	Helper()
	Fatal(args ...any)
}, ctx context.Context, s *NodeState, informers *informertest.FakeInformers) {
	t.Helper()
	// FakeInformers makes informers unsafely, so make them before the stores start.
	for _, obj := range []client.Object{&v1.NodeClaim{}, &corev1.Node{}, &corev1.Pod{}, &storagev1.CSINode{}} {
		if _, err := informers.FakeInformerFor(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	for _, store := range []interface{ Start(context.Context) error }{s.NodeClaimStore.store, s.NodeStore.store, s.PodStore.store, s.CSINodeStore.store} {
		go func() { _ = store.Start(ctx) }()
	}
	for deadline := time.Now().Add(10 * time.Second); !s.Synced(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("node state didn't sync")
		}
	}
}

// overlaid returns kubeClient in the overlay that node state reads NodeClaims through, over informers whose NodeClaim
// informer stores the NodeClaims among objs, as the informer cache's would once synced.
func overlaid(ctx context.Context, kubeClient client.Client, informers *test.StoredInformers, objs ...client.Object) client.Client {
	GinkgoHelper()
	nodeClaims := informers.Stored(ctx, &v1.NodeClaim{})
	for _, obj := range objs {
		if nodeClaim, ok := obj.(*v1.NodeClaim); ok {
			Expect(nodeClaims.Store.Add(nodeClaim)).To(Succeed())
		}
	}
	return lo.Must(overlay.New(ctx, kubeClient, informers, &v1.NodeClaim{}))
}

// listSynced is the check cluster state's Synced made against the API server before node state's stores followed it:
// no NodeClaim cluster state holds is unlaunched, and it holds every managed NodeClaim and every Node the API server
// does.
func listSynced(ctx context.Context, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, s *NodeState) bool {
	GinkgoHelper()
	held := sets.New[string]()
	for name, e := range s.NodeClaimStore.List() {
		if e.Object.Status.ProviderID == "" {
			return false
		}
		held.Insert(name)
	}
	nodeClaims, err := nodeclaimutils.ListManaged(ctx, kubeClient, cloudProvider)
	Expect(err).ToNot(HaveOccurred())
	nodes := &corev1.NodeList{}
	Expect(kubeClient.List(ctx, nodes)).To(Succeed())
	tracked := sets.New[string]()
	for name := range s.NodeStore.List() {
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

var _ = Describe("NodeState", func() {
	// Node state's stores having synced, with no unlaunched NodeClaim, implies what cluster state's Synced checked
	// against a List of the API server: that it holds every managed NodeClaim and every Node, and none of its NodeClaims
	// is unlaunched. So gating on the stores' sync with state.Requires, and on HasUnlaunchedNodeClaims, is at least as
	// strong as the List check it replaces.
	It("should be at least as strongly synced as the List check once its stores sync with no unlaunched NodeClaims", func() {
		r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a seeded source makes failures reproducible
		cloudProvider := cpfake.NewCloudProvider()
		for round := range 50 {
			func() {
				ctx, cancel := context.WithCancel(options.ToContext(context.Background(), test.Options()))
				defer cancel()
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
				informers := test.NewStoredInformers(scheme.Scheme)
				s := NewNodeState(ctx, clock.RealClock{}, overlaid(ctx, kubeClient, informers, objs...), informers, cloudProvider, nil)
				Expect(s.Synced()).To(BeFalse(), "round %d: node state is synced before its stores listed", round)
				startNodeState(GinkgoT(), ctx, s, informers.FakeInformers)
				launched := !s.HasUnlaunchedNodeClaims()
				Expect(listSynced(ctx, kubeClient, cloudProvider, s)).To(Equal(launched),
					"round %d: synced stores with every NodeClaim launched = %t, but the List check = %t", round, launched, !launched)
			}()
		}
	})
})

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
				informers := test.NewStoredInformers(scheme.Scheme)
				informer, err := informers.FakeInformerFor(ctx, &corev1.Pod{})
				if err != nil {
					b.Fatal(err)
				}
				before := heapInUse()
				s := NewNodeState(ctx, clock.RealClock{}, overlaid(ctx, fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(), informers), informers, cpfake.NewCloudProvider(), nil)
				startNodeState(b, ctx, s, informers.FakeInformers)
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
