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

package node

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
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

// The benchmarks measure one scrape of the node metrics built at scrape time. Run with
// go test ./pkg/controllers/metrics/node/ -run '^$' -bench . -benchmem

func benchNodeState(b *testing.B, numNodes, podsPerNode int) karpentercluster.State {
	b.Helper()
	ctx := options.ToContext(context.Background(), &options.Options{BatchMaxDuration: 10 * time.Second})
	srcs := statetest.NewSources()
	cs := karpentercluster.New(ctx, clock.NewFakeClock(time.Now()), nil, fakecp.NewCloudProvider(),
		karpentercluster.WithSources(func(obj client.Object) karpenterstate.Newest { return srcs.For(obj) }))
	rv := 0
	version := func(obj client.Object) {
		rv++
		obj.SetResourceVersion(strconv.Itoa(rv))
	}
	for i := range numNodes {
		providerID := fmt.Sprintf("fake://%d", i)
		labels := map[string]string{
			v1.NodePoolLabelKey:            fmt.Sprintf("pool-%d", i%10),
			corev1.LabelInstanceTypeStable: "m5.large",
			corev1.LabelTopologyZone:       fmt.Sprintf("zone-%d", i%3),
			corev1.LabelArchStable:         "amd64",
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			v1.NodeInitializedLabelKey:     "true",
			v1.NodeRegisteredLabelKey:      "true",
		}
		capacity := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("64Gi"), corev1.ResourcePods: resource.MustParse("110"), corev1.ResourceEphemeralStorage: resource.MustParse("100Gi")}
		allocatable := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("15800m"), corev1.ResourceMemory: resource.MustParse("62Gi"), corev1.ResourcePods: resource.MustParse("110"), corev1.ResourceEphemeralStorage: resource.MustParse("90Gi")}
		nc := &v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nc-%d", i), UID: types.UID(fmt.Sprintf("nc-uid-%d", i)), Labels: labels},
			Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
			Status:     v1.NodeClaimStatus{ProviderID: providerID, NodeName: fmt.Sprintf("node-%d", i), Capacity: capacity, Allocatable: allocatable},
		}
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		nc.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i), UID: types.UID(fmt.Sprintf("node-uid-%d", i)), Labels: labels, CreationTimestamp: metav1.Now()},
			Spec:       corev1.NodeSpec{ProviderID: providerID},
			Status:     corev1.NodeStatus{Phase: corev1.NodeRunning, Capacity: capacity, Allocatable: allocatable, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		}
		node = nodeclaimutils.UpdateNodeOwnerReferences(nc, node)
		version(nc)
		version(node)
		srcs.Put(nc, node)
		for j := range podsPerNode {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d-%d", i, j), Namespace: "default", UID: types.UID(fmt.Sprintf("pod-uid-%d-%d", i, j))},
				Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
				}}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			version(pod)
			srcs.Put(pod)
		}
	}
	return cs
}

var benchSizes = []struct{ nodes, pods int }{{1000, 30}, {5000, 30}}

// BenchmarkScrape is one scrape of the collector: the whole cost of the node metrics at scrape time.
func BenchmarkScrape(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("nodes=%d", size.nodes), func(b *testing.B) {
			cs := benchNodeState(b, size.nodes, size.pods)
			c := NewController(cs.NodeClaims, cs.Nodes)
			registry := prometheus.NewRegistry()
			// Register derives the node count. Only the first size registers on the global registry; the scrape reads its own.
			_ = c.Register(context.Background(), nil)
			registry.MustRegister(c.collector())
			families := lo.Must(registry.Gather())
			if len(families) == 0 {
				b.Fatal("the collector emitted nothing: the stores aren't synced")
			}
			series := lo.SumBy(families, func(f *dto.MetricFamily) int { return len(f.Metric) })
			b.ResetTimer()
			for range b.N {
				lo.Must(registry.Gather())
			}
			b.ReportMetric(float64(series), "series")
		})
	}
}
