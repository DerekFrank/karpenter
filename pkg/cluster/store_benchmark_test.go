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

package cluster_test

// The cluster state benchmarks of pkg/controllers/state's cluster_compare_test.go, ported to the stores: a snapshot is
// the scheduler's existing capacity, NodeClaims.List and Nodes.Unmanaged without a copy, as NodeViews.

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	fakecp "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

type storeFixture struct {
	cs         karpentercluster.State
	srcs       *statetest.Sources
	nodeClaims []*v1.NodeClaim
	nodes      []*corev1.Node
	pods       [][]*corev1.Pod
	rv         atomic.Uint64
}

func (f *storeFixture) version(obj client.Object) {
	obj.SetResourceVersion(strconv.FormatUint(f.rv.Add(1), 10))
}

// newStoreFixture builds numNodes NodeClaims and Nodes with podsPerNode pods each. Each Node carries its NodeClaim's
// owner reference, as registered Nodes do.
func newStoreFixture(numNodes, podsPerNode int) *storeFixture {
	f := &storeFixture{}
	for i := range numNodes {
		providerID := fmt.Sprintf("fake://%d", i)
		labels := map[string]string{
			v1.NodePoolLabelKey:            fmt.Sprintf("pool-%d", i%10),
			corev1.LabelInstanceTypeStable: "m5.large",
			v1.NodeInitializedLabelKey:     "true",
			v1.NodeRegisteredLabelKey:      "true",
		}
		capacity := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("64Gi"), corev1.ResourcePods: resource.MustParse("110")}
		nc := &v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nc-%d", i), UID: types.UID(fmt.Sprintf("nc-uid-%d", i)), Labels: labels},
			Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
			Status:     v1.NodeClaimStatus{ProviderID: providerID, NodeName: fmt.Sprintf("node-%d", i), Capacity: capacity, Allocatable: capacity},
		}
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		nc.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i), UID: types.UID(fmt.Sprintf("node-uid-%d", i)), Labels: labels},
			Spec:       corev1.NodeSpec{ProviderID: providerID},
			Status:     corev1.NodeStatus{Capacity: capacity, Allocatable: capacity, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		}
		node = nodeclaimutils.UpdateNodeOwnerReferences(nc, node)
		f.version(nc)
		f.version(node)
		f.nodeClaims = append(f.nodeClaims, nc)
		f.nodes = append(f.nodes, node)
		var pods []*corev1.Pod
		for j := range podsPerNode {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d-%d", i, j), Namespace: "default", UID: types.UID(fmt.Sprintf("pod-uid-%d-%d", i, j))},
				Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
				}}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			f.version(pod)
			pods = append(pods, pod)
		}
		f.pods = append(f.pods, pods)
	}
	ctx := options.ToContext(context.Background(), &options.Options{BatchMaxDuration: 10 * time.Second})
	f.srcs = statetest.NewSources()
	f.cs = karpentercluster.New(ctx, clock.NewFakeClock(time.Now()), nil, fakecp.NewCloudProvider(),
		karpentercluster.WithSources(func(obj client.Object) karpenterstate.Newest { return f.srcs.For(obj) }))
	return f
}

func (f *storeFixture) ingest() {
	for i := range f.nodes {
		f.srcs.Put(f.nodeClaims[i])
		f.srcs.Put(f.nodes[i])
		for _, pod := range f.pods[i] {
			f.srcs.Put(pod)
		}
	}
}

// snapshot is the scheduler's existing capacity: each launched NodeClaim's node that isn't deleting, and each unmanaged
// Node, read without a copy.
func (f *storeFixture) snapshot() []karpentercluster.NodeView {
	var nodes []karpentercluster.NodeView
	for _, e := range f.cs.NodeClaims.List(client.UnsafeDisableDeepCopy) {
		if karpentercluster.Launched(e) && !karpentercluster.Deleting(e) {
			nodes = append(nodes, karpentercluster.ViewOfNodeClaim(e))
		}
	}
	for _, e := range f.cs.Nodes.Unmanaged(client.UnsafeDisableDeepCopy) {
		nodes = append(nodes, karpentercluster.ViewOfNode(e))
	}
	return nodes
}

// BenchmarkClusterIngest feeds every NodeClaim, Node and pod to cluster state and takes the first snapshot, which is
// when cluster state is ready to schedule against.
func BenchmarkClusterIngest(b *testing.B) {
	for _, n := range []int{1000, 5000} {
		b.Run(fmt.Sprintf("nodes=%d/pods=30", n), func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				f := newStoreFixture(n, 30)
				b.StartTimer()
				f.ingest()
				_ = f.snapshot()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n*31)/1e3, "µs/event")
		})
	}
}

// BenchmarkClusterPodChurn replaces a pod, as one deleted and recreated on its node.
func BenchmarkClusterPodChurn(b *testing.B) {
	f := newStoreFixture(1000, 30)
	f.ingest()
	b.ResetTimer()
	for i := range b.N {
		pod := f.pods[i%len(f.pods)][i%30]
		f.srcs.Remove(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name}})
		next := pod.DeepCopy()
		f.version(next)
		f.srcs.Put(next)
	}
}

// benchConditionUpdate returns the pod with a changed condition, the most common pod update, which can't change usage.
func benchConditionUpdate(pod *corev1.Pod, i int) *corev1.Pod {
	pod = pod.DeepCopy()
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastProbeTime: metav1.NewTime(time.Unix(int64(i), 0))}}
	return pod
}

// benchResize returns the pod resized in place to a cpu request that alternates with i, an update that changes usage.
func benchResize(pod *corev1.Pod, i int) *corev1.Pod {
	pod = pod.DeepCopy()
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%dm", 100+100*(i%2))),
		corev1.ResourceMemory: resource.MustParse("128Mi"),
	}
	return pod
}

// BenchmarkClusterPodChurnThenSnapshot updates one pod on every node between snapshots: its conditions, which can't
// change the node's usage, or its requests, which change every node's usage, and so every NodeClaim's entry.
func BenchmarkClusterPodChurnThenSnapshot(b *testing.B) {
	for _, update := range []struct {
		name string
		fn   func(*corev1.Pod, int) *corev1.Pod
	}{{"conditions", benchConditionUpdate}, {"resize", benchResize}} {
		b.Run(update.name, func(b *testing.B) {
			f := newStoreFixture(1000, 30)
			f.ingest()
			b.ResetTimer()
			for i := range b.N {
				for n := range f.pods {
					pod := update.fn(f.pods[n][i%30], i)
					f.version(pod)
					f.srcs.Put(pod)
				}
				_ = f.snapshot()
			}
		})
	}
}

// BenchmarkClusterSnapshot takes the scheduler's existing capacity, where DeepCopyNodes did before.
func BenchmarkClusterSnapshot(b *testing.B) {
	for _, n := range []int{1000, 5000} {
		b.Run(fmt.Sprintf("nodes=%d/pods=30", n), func(b *testing.B) {
			f := newStoreFixture(n, 30)
			f.ingest()
			_ = f.snapshot()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if got := len(f.snapshot()); got != n {
					b.Fatalf("got %d nodes", got)
				}
			}
		})
	}
}

// BenchmarkClusterSnapshotUnderChurn runs a disruption-style reader taking snapshots while writers update pods'
// conditions, and reports the reader's p99 and the writers' throughput.
func BenchmarkClusterSnapshotUnderChurn(b *testing.B) {
	f := newStoreFixture(2000, 30)
	f.ingest()
	var stop atomic.Bool
	var writes atomic.Int64
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := w; !stop.Load(); i += 8 {
				pod := benchConditionUpdate(f.pods[i%len(f.pods)][i%30], i)
				f.version(pod)
				f.srcs.Put(pod)
				writes.Add(1)
			}
		})
	}
	var reads []time.Duration
	start := time.Now()
	b.ResetTimer()
	for range b.N {
		s := time.Now()
		_ = f.snapshot()
		reads = append(reads, time.Since(s))
	}
	b.StopTimer()
	elapsed := time.Since(start)
	stop.Store(true)
	wg.Wait()
	slices.Sort(reads)
	b.ReportMetric(float64(reads[len(reads)*99/100].Microseconds()), "p99-snapshot-µs")
	b.ReportMetric(float64(writes.Load())/elapsed.Seconds(), "pod-writes/s")
}

// BenchmarkNodeClaimsList lists 5000 NodeClaims, copying each object, as a reader that writes them does, and without a
// copy, as a hot read that only reads does.
func BenchmarkNodeClaimsList(b *testing.B) {
	f := newStoreFixture(5000, 0)
	f.ingest()
	for _, c := range []struct {
		name string
		opts []client.ListOption
	}{{"copy", nil}, {"UnsafeDisableDeepCopy", []client.ListOption{client.UnsafeDisableDeepCopy}}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				n := 0
				for range f.cs.NodeClaims.List(c.opts...) {
					n++
				}
				if n != 5000 {
					b.Fatalf("got %d NodeClaims", n)
				}
			}
		})
	}
}
