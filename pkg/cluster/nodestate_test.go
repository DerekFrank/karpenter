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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	cpfake "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

// startNodeState starts node state's object stores, as State.Register does under the manager, marks srcs synced, as
// the informers sync, and returns once the stores are synced. t is a spec's GinkgoT() or a benchmark's *testing.B.
func startNodeState(t interface {
	Helper()
	Fatal(args ...any)
}, ctx context.Context, s *NodeState, srcs *statetest.Sources) {
	t.Helper()
	for _, store := range []interface{ Start(context.Context) error }{s.NodeClaimStore.store, s.NodeStore.store, s.PodStore.store, s.CSINodeStore.store} {
		go func() { _ = store.Start(ctx) }()
	}
	srcs.SetSynced()
	for deadline := time.Now().Add(10 * time.Second); !s.Synced(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("node state didn't sync")
		}
	}
}

// unsyncedNodeState builds node state over sources that haven't synced, holding objs, as informers before their first
// List would once they deliver it.
func unsyncedNodeState(ctx context.Context, cloudProvider cloudprovider.CloudProvider, objs ...client.Object) (*NodeState, *statetest.Sources) {
	srcs := statetest.NewUnsyncedSources()
	for _, obj := range objs {
		srcs.For(obj).Hold(obj)
	}
	s := NewNodeState(ctx, clock.RealClock{}, func(obj client.Object) state.Newest { return srcs.For(obj) }, cloudProvider, nil)
	for _, accessor := range []any{s, s.NodeClaimStore, s.NodeStore, s.PodStore, s.CSINodeStore} {
		sourcesOf.Store(accessor, srcs)
	}
	return s, srcs
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
				s, srcs := unsyncedNodeState(ctx, cloudProvider, objs...)
				Expect(s.Synced()).To(BeFalse(), "round %d: node state is synced before its sources", round)
				startNodeState(GinkgoT(), ctx, s, srcs)
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
// the pods either way, with every pod in the pod store, and with a filter that leaves out the pods that have finished
// (half of them here), the one filter worth considering: a finished pod uses nothing on its node.
func BenchmarkPodStoreRetained(b *testing.B) {
	const nodes, podsPerNode = 100, 50
	for _, filter := range []struct {
		name string
		opts []state.ObjectOption
	}{
		{"none", nil},
		{"unfinished", []state.ObjectOption{state.WithFilter(func(pod *corev1.Pod) bool {
			return pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
		})}},
	} {
		b.Run(fmt.Sprintf("filter=%s/pods=%d", filter.name, nodes*podsPerNode), func(b *testing.B) {
			cache := make([]*corev1.Pod, 0, nodes*podsPerNode)
			for n := range nodes {
				for i := range podsPerNode {
					pod := test.Pod(test.PodOptions{NodeName: fmt.Sprintf("node-%d", n), ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "wide", "tier": "web"}, Annotations: map[string]string{"note": "a pod of a wide deployment"},
					}})
					if i%2 == 0 {
						pod.Status.Phase = corev1.PodSucceeded
					}
					cache = append(cache, pod)
				}
			}
			var retained float64
			for b.Loop() {
				from := statetest.NewObjects()
				for _, pod := range cache {
					from.Hold(pod)
				}
				before := heapInUse()
				pods := state.NewObjectStore[types.NamespacedName, *corev1.Pod, struct{}, struct{}](from, filter.opts...)
				retained += heapInUse() - before
				runtime.KeepAlive(pods)
				runtime.KeepAlive(from)
			}
			b.ReportMetric(retained/float64(b.N), "retained-B/op")
			runtime.KeepAlive(cache)
		})
	}
}
