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
	"strings"
	"testing"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
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
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state"
)

// The benchmarks compare one scrape of the pod metrics built at scrape time with the work the polling controller did
// for the same pods: a pass that reconciled each pod's series into a metrics.Store, which it did every 30s for every
// pod, plus the scrape of the gauge vector it wrote. The legacy pass reads each pod's Node from a map rather than the
// cache, so it understates the controller's cost. Run with
// go test ./pkg/controllers/metrics/pod/ -run '^$' -bench . -benchmem

var benchSizes = []struct{ nodes, pods int }{{1000, 30}, {5000, 30}}

type benchFixture struct {
	cs    karpentercluster.State
	nodes map[string]*corev1.Node
	pods  []*corev1.Pod
}

func newBenchFixture(numNodes, podsPerNode int) *benchFixture {
	ctx := options.ToContext(context.Background(), &options.Options{BatchMaxDuration: 10 * time.Second})
	f := &benchFixture{cs: karpentercluster.New(ctx, clock.NewFakeClock(time.Now()), nil, nil, fakecp.NewCloudProvider()), nodes: map[string]*corev1.Node{}}
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
		f.nodes[node.Name] = node
		f.cs.Nodes.NodeStore.Observe(node)
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
			f.pods = append(f.pods, pod)
			f.cs.Nodes.PodStore.Observe(pod)
		}
	}
	return f
}

// BenchmarkScrape is one scrape of the collector: the whole cost of the pod gauges at scrape time.
func BenchmarkScrape(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("pods=%d", size.nodes*size.pods), func(b *testing.B) {
			f := newBenchFixture(size.nodes, size.pods)
			c := NewController(f.cs.Nodes, f.cs.PodScheduling)
			c.progress = state.NewFedStore[types.NamespacedName, progress, struct{}, struct{}]()
			registry := prometheus.NewRegistry()
			registry.MustRegister(metrics.NewCollector(func() bool { return true }, c.collect,
				PodState, PodUnstartedTimeSeconds, PodUnboundTimeSeconds, PodProvisioningUnboundTimeSeconds, PodProvisioningUnstartedTimeSeconds, PodSchedulingUndecidedTimeSeconds))
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

// BenchmarkLegacyReconcilePass is the polling controller reconciling every pod once, as it did every 30s: each pod's
// labels built and its series updated in the metrics.Store.
func BenchmarkLegacyReconcilePass(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("pods=%d", size.nodes*size.pods), func(b *testing.B) {
			f := newBenchFixture(size.nodes, size.pods)
			l := newLegacy()
			l.pass(f)
			b.ResetTimer()
			for range b.N {
				l.pass(f)
			}
		})
	}
}

// BenchmarkLegacyScrape is one scrape of the gauge vector the polling controller wrote.
func BenchmarkLegacyScrape(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("pods=%d", size.nodes*size.pods), func(b *testing.B) {
			f := newBenchFixture(size.nodes, size.pods)
			l := newLegacy()
			l.pass(f)
			b.ResetTimer()
			for range b.N {
				lo.Must(l.registry.Gather())
			}
		})
	}
}

type legacy struct {
	registry *prometheus.Registry
	store    *metrics.Store
	podState opmetrics.GaugeMetric
}

func newLegacy() *legacy {
	l := &legacy{registry: prometheus.NewRegistry(), store: metrics.NewStore()}
	l.podState = opmetrics.NewPrometheusGauge(l.registry, prometheus.GaugeOpts{Namespace: metrics.Namespace, Subsystem: metrics.PodSubsystem, Name: "state"}, labelNames(), opmetrics.Beta)
	return l
}

// pass reconciles every pod as 413134ed's controller did for a running, bound, ready pod.
func (l *legacy) pass(f *benchFixture) {
	for _, pod := range f.pods {
		l.store.Update(client.ObjectKeyFromObject(pod).String(), []*metrics.StoreMetric{{GaugeMetric: l.podState, Value: 1, Labels: legacyLabels(pod, f.nodes[pod.Spec.NodeName])}})
	}
}

func legacyLabels(pod *corev1.Pod, node *corev1.Node) prometheus.Labels {
	metricLabels := map[string]string{}
	metricLabels[podName] = pod.Name
	metricLabels[podNamespace] = pod.Namespace
	selflink := ""
	if len(pod.OwnerReferences) > 0 {
		selflink = fmt.Sprintf("/apis/%s/namespaces/%s/%ss/%s", pod.OwnerReferences[0].APIVersion, pod.Namespace, strings.ToLower(pod.OwnerReferences[0].Kind), pod.OwnerReferences[0].Name)
	}
	metricLabels[ownerSelfLink] = selflink
	metricLabels[podHostName] = pod.Spec.NodeName
	metricLabels[podScheduled] = lo.Ternary(pod.Spec.NodeName != "", "true", "false")
	metricLabels[podPhase] = string(pod.Status.Phase)
	_, ready := lo.Find(pod.Status.Conditions, func(c corev1.PodCondition) bool {
		return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
	})
	metricLabels[podReady] = lo.Ternary(ready, "true", "false")
	metricLabels[metrics.ZoneLabel] = node.Labels[corev1.LabelTopologyZone]
	metricLabels[podHostArchitecture] = node.Labels[corev1.LabelArchStable]
	metricLabels[metrics.CapacityTypeLabel] = node.Labels[v1.CapacityTypeLabelKey]
	metricLabels[podHostInstanceType] = node.Labels[corev1.LabelInstanceTypeStable]
	metricLabels[metrics.NodePoolLabel] = node.Labels[v1.NodePoolLabelKey]
	metricLabels[managed] = strconv.FormatBool(node.Labels[v1.NodePoolLabelKey] != "")
	return metricLabels
}
