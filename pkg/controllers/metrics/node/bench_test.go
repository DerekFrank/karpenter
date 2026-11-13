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
	"sigs.k8s.io/karpenter/pkg/test"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// The benchmarks compare one scrape of the node metrics built at scrape time with the work the polling controller did
// for the same nodes: a Reconcile that rebuilt every series into a metrics.Store, which diffs against the previous set
// and deletes stale series, plus the scrape of the gauge vectors it wrote. Run with
// go test ./pkg/controllers/metrics/node/ -run '^$' -bench . -benchmem

func benchNodeState(b *testing.B, numNodes, podsPerNode int) *karpentercluster.NodeState {
	b.Helper()
	ctx := options.ToContext(context.Background(), &options.Options{BatchMaxDuration: 10 * time.Second})
	cs := karpentercluster.New(ctx, clock.NewFakeClock(time.Now()), nil, nil, fakecp.NewCloudProvider())
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
		cs.Nodes.NodeClaimStore.Observe(nc)
		cs.Nodes.NodeStore.Observe(node)
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
			cs.Nodes.PodStore.Observe(pod)
		}
	}
	return cs.Nodes
}

var benchSizes = []struct{ nodes, pods int }{{1000, 30}, {5000, 30}}

// BenchmarkScrape is one scrape of the collector: the whole cost of the node metrics at scrape time.
func BenchmarkScrape(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("nodes=%d", size.nodes), func(b *testing.B) {
			initializeMetrics()
			c := &Controller{nodes: benchNodeState(b, size.nodes, size.pods)}
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

// BenchmarkLegacyReconcile is one rebuild of the polling controller, as on 413134ed: every series rebuilt into a
// metrics.Store, which diffs it against the previous set. It ran up to every 5s while nodes changed.
func BenchmarkLegacyReconcile(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("nodes=%d", size.nodes), func(b *testing.B) {
			l := newLegacy(benchNodeState(b, size.nodes, size.pods))
			l.reconcile()
			b.ResetTimer()
			for range b.N {
				l.reconcile()
			}
		})
	}
}

// BenchmarkLegacyScrape is one scrape of the gauge vectors the polling controller wrote.
func BenchmarkLegacyScrape(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("nodes=%d", size.nodes), func(b *testing.B) {
			l := newLegacy(benchNodeState(b, size.nodes, size.pods))
			l.reconcile()
			b.ResetTimer()
			for range b.N {
				lo.Must(l.registry.Gather())
			}
		})
	}
}

// legacy is the polling controller's metric building, copied from 413134ed, writing gauge vectors on its own registry.
type legacy struct {
	nodes    *karpentercluster.NodeState
	registry *prometheus.Registry
	store    *metrics.Store

	allocatable, totalPodRequests, totalPodLimits, totalDaemonRequests, totalDaemonLimits, systemOverhead, lifetime, clusterUtilization opmetrics.GaugeMetric
}

func newLegacy(nodes *karpentercluster.NodeState) *legacy {
	initializeMetrics()
	l := &legacy{nodes: nodes, registry: prometheus.NewRegistry(), store: metrics.NewStore()}
	gauge := func(subsystem, name string, labels []opmetrics.Label) opmetrics.GaugeMetric {
		return opmetrics.NewPrometheusGauge(l.registry, prometheus.GaugeOpts{Namespace: metrics.Namespace, Subsystem: subsystem, Name: name}, labels, opmetrics.Beta)
	}
	l.allocatable = gauge(metrics.NodeSubsystem, "allocatable", nodeLabelNamesWithResourceType())
	l.totalPodRequests = gauge(metrics.NodeSubsystem, "total_pod_requests", nodeLabelNamesWithResourceType())
	l.totalPodLimits = gauge(metrics.NodeSubsystem, "total_pod_limits", nodeLabelNamesWithResourceType())
	l.totalDaemonRequests = gauge(metrics.NodeSubsystem, "total_daemon_requests", nodeLabelNamesWithResourceType())
	l.totalDaemonLimits = gauge(metrics.NodeSubsystem, "total_daemon_limits", nodeLabelNamesWithResourceType())
	l.systemOverhead = gauge(metrics.NodeSubsystem, "system_overhead", nodeLabelNamesWithResourceType())
	l.lifetime = gauge(metrics.NodeSubsystem, "current_lifetime_seconds", nodeLabelNames())
	l.clusterUtilization = gauge("cluster", "utilization_percent", []opmetrics.Label{metrics.ResourceType})
	return l
}

func (l *legacy) reconcile() {
	var nodes karpentercluster.StateNodes
	for n := range l.nodes.Nodes() {
		if n.Node != nil {
			nodes = append(nodes, n)
		}
	}
	metricsMap := lo.SliceToMap(nodes, func(n *karpentercluster.StateNode) (string, []*metrics.StoreMetric) {
		return client.ObjectKeyFromObject(n.Node).String(), l.buildMetrics(n)
	})
	metricsMap["clusterUtilization"] = l.buildClusterUtilizationMetric(nodes)
	l.store.ReplaceAll(metricsMap)
}

func (l *legacy) buildClusterUtilizationMetric(nodes karpentercluster.StateNodes) []*metrics.StoreMetric {
	allocatableAggregate, utilizedAggregate := corev1.ResourceList{}, corev1.ResourceList{}
	for _, node := range nodes {
		resources.MergeInto(allocatableAggregate, node.Allocatable())
		resources.MergeInto(utilizedAggregate, node.PodRequests())
	}
	res := make([]*metrics.StoreMetric, 0, len(allocatableAggregate))
	for resourceName, allocatableResource := range allocatableAggregate {
		if allocatableResource.Value() == 0 {
			continue
		}
		utilizedResource := utilizedAggregate[resourceName]
		utilizationPercentage := 100 * lo.Ternary(
			resourceName == corev1.ResourceCPU,
			float64(utilizedResource.MilliValue())/float64(allocatableResource.MilliValue()),
			float64(utilizedResource.Value())/float64(allocatableResource.Value()))
		res = append(res, &metrics.StoreMetric{
			GaugeMetric: l.clusterUtilization,
			Value:       utilizationPercentage,
			Labels:      map[string]string{metrics.ResourceTypeLabel: resourceNameToString(resourceName)},
		})
	}
	return res
}

func (l *legacy) buildMetrics(n *karpentercluster.StateNode) (res []*metrics.StoreMetric) {
	for gaugeMetric, resourceList := range map[opmetrics.GaugeMetric]corev1.ResourceList{
		l.systemOverhead:      resources.Subtract(n.Node.Status.Capacity, n.Node.Status.Allocatable),
		l.totalPodRequests:    n.PodRequests(),
		l.totalPodLimits:      n.PodLimits(),
		l.totalDaemonRequests: n.DaemonSetRequests(),
		l.totalDaemonLimits:   n.DaemonSetLimits(),
		l.allocatable:         n.Node.Status.Allocatable,
	} {
		for resourceName, quantity := range resourceList {
			res = append(res, &metrics.StoreMetric{
				GaugeMetric: gaugeMetric,
				Value:       lo.Ternary(resourceName == corev1.ResourceCPU, float64(quantity.MilliValue())/float64(1000), float64(quantity.Value())),
				Labels:      legacyNodeLabelsWithResourceType(n, resourceNameToString(resourceName)),
			})
		}
	}
	return append(res,
		&metrics.StoreMetric{
			GaugeMetric: l.lifetime,
			Value:       time.Since(n.Node.GetCreationTimestamp().Time).Seconds(),
			Labels:      legacyNodeLabels(n),
		})
}

func legacyNodeLabelsWithResourceType(n *karpentercluster.StateNode, resourceTypeName string) prometheus.Labels {
	metricLabels := legacyNodeLabels(n)
	metricLabels[metrics.ResourceTypeLabel] = resourceTypeName
	return metricLabels
}

// legacyNodeLabels rebuilds the well-known label table for every series, as 413134ed did.
func legacyNodeLabels(n *karpentercluster.StateNode) prometheus.Labels {
	node := n.Node
	metricLabels := map[string]string{}
	metricLabels[nodeName] = node.Name
	metricLabels[nodePhase] = string(node.Status.Phase)
	metricLabels[managed] = strconv.FormatBool(n.Managed())
	for _, l := range getWellKnownLabels() {
		metricLabels[l.name] = node.Labels[l.key]
	}
	return metricLabels
}
