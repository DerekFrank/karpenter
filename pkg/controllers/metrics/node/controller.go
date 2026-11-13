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
	"strconv"
	"strings"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const (
	nodeName  = "node_name"
	nodePhase = "phase"
	managed   = "managed"
)

// Fixed node metric dimensions. The remaining dimensions are the cloud
// provider's well-known labels, which are self-describing and wrapped without
// help text (see nodeLabelNames).
var (
	nodeNameLabel  = opmetrics.Label{Name: nodeName, Help: "The name of the node.", Key: true}
	nodePhaseLabel = opmetrics.Label{Name: nodePhase, Help: "The node's lifecycle phase, e.g. `Pending`, `Running`."}
	managedLabel   = opmetrics.Label{Name: managed, Help: "Whether the node is managed by Karpenter.", Values: metrics.BoolValues}
)

var (
	Allocatable         opmetrics.GaugeMetric
	TotalPodRequests    opmetrics.GaugeMetric
	TotalPodLimits      opmetrics.GaugeMetric
	TotalDaemonRequests opmetrics.GaugeMetric
	TotalDaemonLimits   opmetrics.GaugeMetric
	SystemOverhead      opmetrics.GaugeMetric
	Lifetime            opmetrics.GaugeMetric
	ClusterUtilization  opmetrics.GaugeMetric
)

// Initialize metrics at runtime to ensure cloud provider's well-known labels are properly
// injected, preventing race conditions in dependency ordering during label injection for global variable. .
func initializeMetrics() {
	Allocatable = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "allocatable",
			Help:      "Node allocatable are the resources allocatable by nodes.",
		},
		nodeLabelNamesWithResourceType(),
		opmetrics.Beta,
	)
	TotalPodRequests = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "total_pod_requests",
			Help:      "Node total pod requests are the resources requested by pods bound to nodes, including the DaemonSet pods.",
		},
		nodeLabelNamesWithResourceType(),
		opmetrics.Beta,
	)
	TotalPodLimits = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "total_pod_limits",
			Help:      "Node total pod limits are the resources specified by pod limits, including the DaemonSet pods.",
		},
		nodeLabelNamesWithResourceType(),
		opmetrics.Beta,
	)
	TotalDaemonRequests = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "total_daemon_requests",
			Help:      "Node total daemon requests are the resource requested by DaemonSet pods bound to nodes.",
		},
		nodeLabelNamesWithResourceType(),
		opmetrics.Beta,
	)
	TotalDaemonLimits = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "total_daemon_limits",
			Help:      "Node total daemon limits are the resources specified by DaemonSet pod limits.",
		},
		nodeLabelNamesWithResourceType(),
		opmetrics.Beta,
	)
	SystemOverhead = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "system_overhead",
			Help:      "Node system daemon overhead are the resources reserved for system overhead, the difference between the node's capacity and allocatable values are reported by the status.",
		},
		nodeLabelNamesWithResourceType(),
		opmetrics.Beta,
	)
	Lifetime = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeSubsystem,
			Name:      "current_lifetime_seconds",
			Help:      "Node age in seconds",
		},
		nodeLabelNames(),
		opmetrics.Alpha,
	)
	ClusterUtilization = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: "cluster",
			Name:      "utilization_percent",
			Help:      "Utilization of allocatable resources by pod requests",
		},
		[]opmetrics.Label{metrics.ResourceType},
		opmetrics.Alpha,
	)
}

func nodeLabelNamesWithResourceType() []opmetrics.Label {
	return append(
		nodeLabelNames(),
		metrics.ResourceType,
	)
}

func nodeLabelNames() []opmetrics.Label {
	return append(
		// WellKnownLabels includes the nodepool label, so we don't need to add it as its own item here.
		// If we do, prometheus will panic since there would be duplicate labels.
		// The well-known labels are self-describing, so they are wrapped without help text.
		lo.Map(sets.New(lo.Values(getWellKnownLabels())...).UnsortedList(), func(l string, _ int) opmetrics.Label {
			return opmetrics.Label{Name: l}
		}),
		nodeNameLabel,
		nodePhaseLabel,
		managedLabel,
	)
}

type Controller struct {
	cluster *state.Cluster
	// nodes and resourceTypes are the node names and cluster utilization resource types
	// the last reconcile emitted, so the next can delete the series of those now gone.
	nodes         sets.Set[string]
	resourceTypes sets.Set[string]
}

func NewController(cluster *state.Cluster) *Controller {
	initializeMetrics()
	return &Controller{
		cluster:       cluster,
		nodes:         sets.New[string](),
		resourceTypes: sets.New[string](),
	}
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name()) //nolint:ineffassign,staticcheck

	nodes := lo.Reject(c.cluster.DeepCopyNodes(), func(n *state.StateNode, _ int) bool {
		return n.Node == nil
	})

	names := sets.New[string]()
	for _, n := range nodes {
		names.Insert(n.Node.Name)
		for gauge, series := range buildMetrics(n) {
			gauge.ReplaceByKey(map[string]string{nodeName: n.Node.Name}, series...)
		}
	}
	for name := range c.nodes.Difference(names) {
		for _, gauge := range []opmetrics.GaugeMetric{Allocatable, TotalPodRequests, TotalPodLimits, TotalDaemonRequests, TotalDaemonLimits, SystemOverhead, Lifetime} {
			gauge.DeleteByKey(map[string]string{nodeName: name})
		}
	}
	c.nodes = names

	utilization := buildClusterUtilization(nodes)
	for resourceType, v := range utilization {
		ClusterUtilization.Set(v, map[string]string{metrics.ResourceTypeLabel: resourceType})
	}
	for resourceType := range c.resourceTypes.Difference(sets.KeySet(utilization)) {
		ClusterUtilization.Delete(map[string]string{metrics.ResourceTypeLabel: resourceType})
	}
	c.resourceTypes = sets.KeySet(utilization)

	return reconciler.Result{RequeueAfter: time.Second * 5}, nil
}

func (c *Controller) Name() string {
	return "metrics.node"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

// buildClusterUtilization returns the utilization percent of each resource type.
func buildClusterUtilization(nodes state.StateNodes) map[string]float64 {

	// Aggregate resources allocated/utilized for all the nodes and pods inside the nodes
	allocatableAggregate, utilizedAggregate := corev1.ResourceList{}, corev1.ResourceList{}

	for _, node := range nodes {
		resources.MergeInto(allocatableAggregate, node.Allocatable())
		resources.MergeInto(utilizedAggregate, node.PodRequests())
	}

	res := make(map[string]float64, len(allocatableAggregate))

	for resourceName, allocatableResource := range allocatableAggregate {

		if allocatableResource.Value() == 0 {
			// This zero check may be unnecessary. I'm erring towards caution.
			continue
		}

		utilizedResource := utilizedAggregate[resourceName]

		// Typecast to float before the calculation to maximize resolution
		utilizationPercentage := 100 * lo.Ternary(
			resourceName == corev1.ResourceCPU,
			float64(utilizedResource.MilliValue())/float64(allocatableResource.MilliValue()),
			float64(utilizedResource.Value())/float64(allocatableResource.Value()))

		res[resourceNameToString(resourceName)] = utilizationPercentage
	}

	return res
}

// buildMetrics returns the series of each node gauge for n. Every gauge is present, even
// without series, so that ReplaceByKey deletes series n no longer has.
func buildMetrics(n *state.StateNode) map[opmetrics.GaugeMetric][]opmetrics.Sample {
	res := map[opmetrics.GaugeMetric][]opmetrics.Sample{
		Lifetime: {{Value: time.Since(n.Node.GetCreationTimestamp().Time).Seconds(), Labels: getNodeLabels(n)}},
	}
	for gaugeMetric, resourceList := range map[opmetrics.GaugeMetric]corev1.ResourceList{
		SystemOverhead:      resources.Subtract(n.Node.Status.Capacity, n.Node.Status.Allocatable),
		TotalPodRequests:    n.PodRequests(),
		TotalPodLimits:      n.PodLimits(),
		TotalDaemonRequests: n.DaemonSetRequests(),
		TotalDaemonLimits:   n.DaemonSetLimits(),
		Allocatable:         n.Node.Status.Allocatable,
	} {
		res[gaugeMetric] = lo.MapToSlice(resourceList, func(resourceName corev1.ResourceName, quantity resource.Quantity) opmetrics.Sample {
			return opmetrics.Sample{
				Value:  lo.Ternary(resourceName == corev1.ResourceCPU, float64(quantity.MilliValue())/float64(1000), float64(quantity.Value())),
				Labels: getNodeLabelsWithResourceType(n, resourceNameToString(resourceName)),
			}
		})
	}
	return res
}

func getNodeLabelsWithResourceType(n *state.StateNode, resourceTypeName string) prometheus.Labels {
	metricLabels := getNodeLabels(n)
	metricLabels[metrics.ResourceTypeLabel] = resourceTypeName
	return metricLabels
}

func getNodeLabels(n *state.StateNode) prometheus.Labels {
	node := n.Node
	metricLabels := map[string]string{}
	metricLabels[nodeName] = node.Name
	metricLabels[nodePhase] = string(node.Status.Phase)
	metricLabels[managed] = strconv.FormatBool(n.Managed())

	// Populate well known labels
	for wellKnownLabel, label := range getWellKnownLabels() {
		metricLabels[label] = node.Labels[wellKnownLabel]
	}
	return metricLabels
}

func getWellKnownLabels() map[string]string {
	labels := make(map[string]string)
	for wellKnownLabel := range v1.WellKnownLabels {
		if parts := strings.Split(wellKnownLabel, "/"); len(parts) == 2 {
			label := parts[1]
			// Reformat label names to be consistent with Prometheus naming conventions (snake_case)
			label = strings.ReplaceAll(strings.ToLower(label), "-", "_")
			labels[wellKnownLabel] = label
		}
	}
	return labels
}

func resourceNameToString(resourceName corev1.ResourceName) string {
	return strings.ReplaceAll(strings.ToLower(string(resourceName)), "-", "_")
}
