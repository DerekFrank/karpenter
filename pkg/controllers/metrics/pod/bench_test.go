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

package pod

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
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

// The benchmarks measure one scrape of the pod metrics built at scrape time. Run with
// go test ./pkg/controllers/metrics/pod/ -run '^$' -bench . -benchmem

var benchSizes = []struct{ nodes, pods int }{{1000, 30}, {5000, 30}}

type benchFixture struct {
	cs karpentercluster.State
}

func newBenchFixture(numNodes, podsPerNode int) *benchFixture {
	ctx := options.ToContext(context.Background(), &options.Options{BatchMaxDuration: 10 * time.Second})
	srcs := statetest.NewSources()
	f := &benchFixture{cs: karpentercluster.New(ctx, clock.NewFakeClock(time.Now()), nil, fakecp.NewCloudProvider(),
		karpentercluster.WithSources(func(obj client.Object) state.Newest { return srcs.For(obj) }))}
	rv := 0
	version := func(obj client.Object) {
		rv++
		obj.SetResourceVersion(strconv.Itoa(rv))
	}
	for i := range numNodes {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i), UID: types.UID(fmt.Sprintf("node-uid-%d", i)), Labels: map[string]string{
				v1.NodePoolLabelKey:            fmt.Sprintf("pool-%d", i%10),
				corev1.LabelInstanceTypeStable: "m5.large",
				corev1.LabelTopologyZone:       fmt.Sprintf("zone-%d", i%3),
				corev1.LabelArchStable:         "amd64",
				v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			}},
			Spec: corev1.NodeSpec{ProviderID: fmt.Sprintf("fake://%d", i)},
		}
		version(node)
		srcs.Put(node)
		for j := range podsPerNode {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d-%d", i, j), Namespace: "default", UID: types.UID(fmt.Sprintf("pod-uid-%d-%d", i, j)),
					CreationTimestamp: metav1.Now(),
					OwnerReferences:   []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: fmt.Sprintf("rs-%d", j)}}},
				Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				}}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				}},
			}
			version(pod)
			srcs.Put(pod)
		}
	}
	return f
}

// BenchmarkScrape is one scrape of the collector: the whole cost of the pod gauges at scrape time.
func BenchmarkScrape(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("pods=%d", size.nodes*size.pods), func(b *testing.B) {
			f := newBenchFixture(size.nodes, size.pods)
			c := NewController(f.cs.Pods, f.cs.Nodes)
			c.progress = state.NewFedStore[types.NamespacedName, progress, struct{}, struct{}]()
			registry := prometheus.NewRegistry()
			registry.MustRegister(c.collector())
			series := lo.SumBy(lo.Must(registry.Gather()), func(f *dto.MetricFamily) int { return len(f.Metric) })
			if series != size.nodes*size.pods {
				b.Fatalf("want a series per pod, got %d", series)
			}
			b.ResetTimer()
			for range b.N {
				lo.Must(registry.Gather())
			}
			b.ReportMetric(float64(series), "series")
		})
	}
}
