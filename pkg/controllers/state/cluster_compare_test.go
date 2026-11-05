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

package state_test

// Compares cluster state implementations through the public Cluster API only, so the same file runs against each.

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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	fakecp "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

type fixture struct {
	cluster    *state.Cluster
	cache      *cacheClient
	nodeClaims []*v1.NodeClaim
	nodes      []*corev1.Node
	pods       [][]*corev1.Pod
	rv         atomic.Uint64
}

func compareContext() context.Context {
	return options.ToContext(context.Background(), &options.Options{BatchMaxDuration: 10 * time.Second})
}

func (f *fixture) version(obj client.Object) {
	obj.SetResourceVersion(strconv.FormatUint(f.rv.Add(1), 10))
}

// newFixture builds numNodes NodeClaims and Nodes with podsPerNode pods each, with the pods in the cache.
func newFixture(numNodes, podsPerNode int) *fixture {
	f := &fixture{cache: &cacheClient{
		Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
		pods:   map[types.NamespacedName]*corev1.Pod{},
		byNode: map[string]map[types.NamespacedName]*corev1.Pod{},
	}}
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
			f.cache.put(pod)
			pods = append(pods, pod)
		}
		f.pods = append(f.pods, pods)
	}
	f.cluster = state.NewCluster(clock.NewFakeClock(time.Now()), f.cache, fakecp.NewCloudProvider())
	return f
}

// cacheClient serves pods from memory as the informer cache does: Gets and Lists by spec.nodeName deep copy them,
// unless a List passes UnsafeDisableDeepCopy. Everything else goes to the fake client.
type cacheClient struct {
	client.Client
	mu     sync.RWMutex
	pods   map[types.NamespacedName]*corev1.Pod
	byNode map[string]map[types.NamespacedName]*corev1.Pod
	// lists counts Lists of pods by node.
	lists atomic.Int64
}

// put stores pod in the cache, as the informer does before it delivers the pod's event.
func (c *cacheClient) put(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := client.ObjectKeyFromObject(pod)
	if old, ok := c.pods[key]; ok {
		delete(c.byNode[old.Spec.NodeName], key)
	}
	stored := pod.DeepCopy()
	c.pods[key] = stored
	if c.byNode[pod.Spec.NodeName] == nil {
		c.byNode[pod.Spec.NodeName] = map[types.NamespacedName]*corev1.Pod{}
	}
	c.byNode[pod.Spec.NodeName][key] = stored
}

func (c *cacheClient) remove(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := client.ObjectKeyFromObject(pod)
	if old, ok := c.pods[key]; ok {
		delete(c.byNode[old.Spec.NodeName], key)
		delete(c.pods, key)
	}
}

func (c *cacheClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return c.Client.Get(ctx, key, obj, opts...)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	stored, ok := c.pods[key]
	if !ok {
		return errors.NewNotFound(corev1.Resource("pods"), key.Name)
	}
	stored.DeepCopyInto(pod)
	return nil
}

func (c *cacheClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	podList, ok := list.(*corev1.PodList)
	listOpts := (&client.ListOptions{}).ApplyOptions(opts)
	if !ok || listOpts.FieldSelector == nil {
		return c.Client.List(ctx, list, opts...)
	}
	nodeName, found := listOpts.FieldSelector.RequiresExactMatch("spec.nodeName")
	if !found {
		return c.Client.List(ctx, list, opts...)
	}
	unsafe := listOpts.UnsafeDisableDeepCopy != nil && *listOpts.UnsafeDisableDeepCopy
	c.lists.Add(1)
	c.mu.RLock()
	defer c.mu.RUnlock()
	podList.Items = make([]corev1.Pod, 0, len(c.byNode[nodeName]))
	for _, pod := range c.byNode[nodeName] {
		if unsafe {
			podList.Items = append(podList.Items, *pod)
		} else {
			podList.Items = append(podList.Items, *pod.DeepCopy())
		}
	}
	return nil
}

func (f *fixture) ingest(ctx context.Context, tb testing.TB) {
	for i := range f.nodes {
		f.cluster.UpdateNodeClaim(f.nodeClaims[i])
		if err := f.cluster.UpdateNode(ctx, f.nodes[i]); err != nil {
			tb.Fatal(err)
		}
		for _, pod := range f.pods[i] {
			if err := f.cluster.UpdatePod(ctx, pod); err != nil {
				tb.Fatal(err)
			}
		}
	}
}

// BenchmarkClusterIngest feeds every NodeClaim, Node and pod to cluster state and takes the first snapshot, which is
// when cluster state is ready to schedule against.
func BenchmarkClusterIngest(b *testing.B) {
	ctx := compareContext()
	for _, n := range []int{1000, 5000} {
		b.Run(fmt.Sprintf("nodes=%d/pods=30", n), func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				f := newFixture(n, 30)
				b.StartTimer()
				f.ingest(ctx, b)
				_ = f.cluster.DeepCopyNodes()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n*31)/1e3, "µs/event")
		})
	}
}

func BenchmarkClusterNodeUpdate(b *testing.B) {
	ctx := compareContext()
	f := newFixture(1000, 30)
	f.ingest(ctx, b)
	b.ResetTimer()
	for i := range b.N {
		node := f.nodes[i%len(f.nodes)].DeepCopy()
		node.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(time.Unix(int64(i), 0))
		f.version(node)
		if err := f.cluster.UpdateNode(ctx, node); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterPodChurn(b *testing.B) {
	ctx := compareContext()
	f := newFixture(1000, 30)
	f.ingest(ctx, b)
	b.ResetTimer()
	for i := range b.N {
		pod := f.pods[i%len(f.pods)][i%30]
		f.cache.remove(pod)
		f.cluster.DeletePod(client.ObjectKeyFromObject(pod))
		next := pod.DeepCopy()
		f.version(next)
		f.cache.put(next)
		if err := f.cluster.UpdatePod(ctx, next); err != nil {
			b.Fatal(err)
		}
	}
}

// conditionUpdate returns the pod with a changed condition, the most common pod update, which can't change usage.
func conditionUpdate(pod *corev1.Pod, i int) *corev1.Pod {
	pod = pod.DeepCopy()
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastProbeTime: metav1.NewTime(time.Unix(int64(i), 0))}}
	return pod
}

// resize returns the pod resized in place to a cpu request that alternates with i, an update that changes usage.
func resize(pod *corev1.Pod, i int) *corev1.Pod {
	pod = pod.DeepCopy()
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%dm", 100+100*(i%2))),
		corev1.ResourceMemory: resource.MustParse("128Mi"),
	}
	return pod
}

// BenchmarkClusterPodChurnThenSnapshot updates one pod on every node between snapshots: its conditions, which can't
// change the node's usage, or its requests, which change every node's usage.
func BenchmarkClusterPodChurnThenSnapshot(b *testing.B) {
	ctx := compareContext()
	for _, update := range []struct {
		name string
		fn   func(*corev1.Pod, int) *corev1.Pod
	}{{"conditions", conditionUpdate}, {"resize", resize}} {
		b.Run(update.name, func(b *testing.B) {
			f := newFixture(1000, 30)
			f.ingest(ctx, b)
			b.ResetTimer()
			for i := range b.N {
				for n := range f.pods {
					pod := update.fn(f.pods[n][i%30], i)
					f.version(pod)
					f.cache.put(pod)
					if err := f.cluster.UpdatePod(ctx, pod); err != nil {
						b.Fatal(err)
					}
				}
				_ = f.cluster.DeepCopyNodes()
			}
		})
	}
}

func BenchmarkClusterDeepCopyNodes(b *testing.B) {
	ctx := compareContext()
	for _, n := range []int{1000, 5000} {
		b.Run(fmt.Sprintf("nodes=%d/pods=30", n), func(b *testing.B) {
			f := newFixture(n, 30)
			f.ingest(ctx, b)
			_ = f.cluster.DeepCopyNodes()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if got := len(f.cluster.DeepCopyNodes()); got != n {
					b.Fatalf("got %d nodes", got)
				}
			}
		})
	}
}

// BenchmarkClusterSnapshotUnderChurn runs a disruption-style reader taking snapshots while writers update pods'
// conditions, and reports the reader's p99 and the writers' throughput.
func BenchmarkClusterSnapshotUnderChurn(b *testing.B) {
	ctx := compareContext()
	f := newFixture(2000, 30)
	f.ingest(ctx, b)
	var stop atomic.Bool
	var writes atomic.Int64
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := w; !stop.Load(); i += 8 {
				pod := conditionUpdate(f.pods[i%len(f.pods)][i%30], i)
				f.version(pod)
				f.cache.put(pod)
				_ = f.cluster.UpdatePod(ctx, pod)
				writes.Add(1)
			}
		})
	}
	var reads []time.Duration
	start := time.Now()
	b.ResetTimer()
	for range b.N {
		s := time.Now()
		_ = f.cluster.DeepCopyNodes()
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

// TestClusterConcurrentAccess runs every kind of write against every kind of read, for the race detector, and checks
// the totals once the writers stop.
func TestClusterConcurrentAccess(t *testing.T) {
	ctx := compareContext()
	f := newFixture(200, 10)
	f.ingest(ctx, t)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := w; !stop.Load(); i += 4 {
				n := i % len(f.nodes)
				pod := f.pods[n][i%10].DeepCopy()
				f.version(pod)
				f.cache.put(pod)
				_ = f.cluster.UpdatePod(ctx, pod)
				node := f.nodes[n].DeepCopy()
				node.Labels["churn"] = strconv.Itoa(i)
				f.version(node)
				_ = f.cluster.UpdateNode(ctx, node)
				nc := f.nodeClaims[n].DeepCopy()
				f.version(nc)
				f.cluster.UpdateNodeClaim(nc)
				f.cluster.MarkForDeletion(nc.Status.ProviderID)
				f.cluster.UnmarkForDeletion(nc.Status.ProviderID)
				f.cluster.NominateNodeForPod(ctx, nc.Status.ProviderID)
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				for _, n := range f.cluster.DeepCopyNodes() {
					_ = n.PodRequests()
					_ = n.Available()
					_ = n.DisruptionCost()
				}
				for n := range f.cluster.Nodes() {
					_ = n.PodRequests()
				}
				f.cluster.ForPodsWithAntiAffinity(func(*corev1.Pod, *corev1.Node) bool { return true })
				_ = f.cluster.NodePoolResourcesFor("pool-0")
				_ = f.cluster.IsNodeNominated("fake://0")
			}
		})
	}
	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()
	cpu := resource.Quantity{}
	for _, n := range f.cluster.DeepCopyNodes() {
		requests := n.PodRequests()
		cpu.Add(*requests.Cpu())
	}
	if want := resource.MustParse("200"); cpu.Cmp(want) != 0 {
		t.Fatalf("pod cpu requests across nodes = %s once writers stop, want %s", cpu.String(), want.String())
	}
}

// TestClusterCountsPodsBoundBeforeTheirNode binds pods to a node before cluster state learns of the node, as happens
// when the pod informer is ahead of the node informer, and checks the node's usage once the node arrives without the
// pods being reconciled again.
func TestClusterCountsPodsBoundBeforeTheirNode(t *testing.T) {
	ctx := compareContext()
	f := newFixture(1, 10)
	for _, pod := range f.pods[0] {
		_ = f.cluster.UpdatePod(ctx, pod)
	}
	f.cluster.UpdateNodeClaim(f.nodeClaims[0])
	if err := f.cluster.UpdateNode(ctx, f.nodes[0]); err != nil {
		t.Fatal(err)
	}
	nodes := f.cluster.DeepCopyNodes()
	requests := nodes[0].PodRequests()
	if got := requests.Cpu(); got.Cmp(resource.MustParse("1")) != 0 {
		t.Fatalf("pod cpu requests = %s once the node arrived, want 1 (10 pods of 100m) without reconciling the pods again", got.String())
	}
}

// TestClusterForgetsPodsDeletedBeforeTheirEvent deletes a pod the cache held when cluster state last computed its
// node's usage, but that cluster state never reconciled, and checks the node's usage after the delete.
func TestClusterForgetsPodsDeletedBeforeTheirEvent(t *testing.T) {
	ctx := compareContext()
	f := newFixture(1, 10)
	f.ingest(ctx, t)
	extra := f.pods[0][0].DeepCopy()
	extra.Name, extra.UID = "extra", "extra-uid"
	f.version(extra)
	f.cache.put(extra)
	node := f.nodes[0].DeepCopy()
	f.version(node)
	if err := f.cluster.UpdateNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	_ = f.cluster.DeepCopyNodes()
	f.cache.remove(extra)
	f.cluster.DeletePod(client.ObjectKeyFromObject(extra))
	requests := f.cluster.DeepCopyNodes()[0].PodRequests()
	if got := requests.Cpu(); got.Cmp(resource.MustParse("1")) != 0 {
		t.Fatalf("pod cpu requests = %s after the extra pod's delete, want 1 (10 pods of 100m)", got.String())
	}
}

// TestClusterRecomputesUsageOnlyWhenItCanChange updates a pod's conditions, which can't change its node's usage, and
// then resizes it in place, which can, and checks which update lists the node's pods again.
func TestClusterRecomputesUsageOnlyWhenItCanChange(t *testing.T) {
	ctx := compareContext()
	f := newFixture(1, 10)
	f.ingest(ctx, t)
	_ = f.cluster.DeepCopyNodes()
	lists := f.cache.lists.Load()

	ready := f.pods[0][0].DeepCopy()
	ready.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	f.version(ready)
	f.cache.put(ready)
	if err := f.cluster.UpdatePod(ctx, ready); err != nil {
		t.Fatal(err)
	}
	_ = f.cluster.DeepCopyNodes()
	if got := f.cache.lists.Load() - lists; got != 0 {
		t.Fatalf("listed the node's pods %d times after a condition update, want 0", got)
	}

	resized := ready.DeepCopy()
	resized.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("300m")
	f.version(resized)
	f.cache.put(resized)
	if err := f.cluster.UpdatePod(ctx, resized); err != nil {
		t.Fatal(err)
	}
	requests := f.cluster.DeepCopyNodes()[0].PodRequests()
	if got := requests.Cpu(); got.Cmp(resource.MustParse("1200m")) != 0 {
		t.Fatalf("pod cpu requests = %s after a resize, want 1200m", got.String())
	}
}

// TestClusterKeepsDisruptionCostAcrossNodeClaimUpdates checks the node's disruption cost after its NodeClaim changes.
func TestClusterKeepsDisruptionCostAcrossNodeClaimUpdates(t *testing.T) {
	ctx := compareContext()
	f := newFixture(1, 10)
	f.ingest(ctx, t)
	before := f.cluster.DeepCopyNodes()[0].DisruptionCost()
	nc := f.nodeClaims[0].DeepCopy()
	nc.Labels["updated"] = "true"
	f.version(nc)
	f.cluster.UpdateNodeClaim(nc)
	if after := f.cluster.DeepCopyNodes()[0].DisruptionCost(); after != before {
		t.Fatalf("disruption cost = %v after a NodeClaim update, want %v", after, before)
	}
}

// TestClusterSnapshotsAreConsistentAcrossNodes moves pods between nodes while a reader sums the pods' requests across
// a snapshot of every node, and counts the snapshots whose total isn't the constant total.
func TestClusterSnapshotsAreConsistentAcrossNodes(t *testing.T) {
	ctx := compareContext()
	f := newFixture(50, 10)
	f.ingest(ctx, t)
	want := resource.MustParse("50")
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; !stop.Load(); i++ {
			pod := f.pods[i%50][i%10].DeepCopy()
			pod.Spec.NodeName = f.nodes[(i+1)%50].Name
			f.version(pod)
			f.cache.put(pod)
			_ = f.cluster.UpdatePod(ctx, pod)
		}
	})
	torn, total := 0, 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cpu := resource.Quantity{}
		for _, n := range f.cluster.DeepCopyNodes() {
			requests := n.PodRequests()
			cpu.Add(*requests.Cpu())
		}
		total++
		if cpu.Cmp(want) != 0 {
			torn++
		}
	}
	stop.Store(true)
	wg.Wait()
	t.Logf("%d of %d snapshots had a total other than %s", torn, total, want.String())
}
