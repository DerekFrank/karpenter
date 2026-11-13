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
	"k8s.io/apimachinery/pkg/util/sets"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/state"
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
	nodeNameLabel  = opmetrics.Label{Name: nodeName, Help: "The name of the node."}
	nodePhaseLabel = opmetrics.Label{Name: nodePhase, Help: "The node's lifecycle phase, e.g. `Pending`, `Running`."}
	managedLabel   = opmetrics.Label{Name: managed, Help: "Whether the node is managed by Karpenter.", Values: metrics.BoolValues}
)

// ClusterStateNodesCount is how many nodes cluster state reads (NodeState.Nodes): the launched NodeClaims, and the
// unmanaged Nodes.
var ClusterStateNodesCount = opmetrics.NewPrometheusGauge(
	crmetrics.Registry,
	prometheus.GaugeOpts{
		Namespace: metrics.Namespace,
		Subsystem: "cluster_state",
		Name:      "node_count",
		Help:      "Current count of nodes in cluster state",
	},
	[]opmetrics.Label{},
	opmetrics.GA,
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

const (
	// BatchWindow is how long the controller folds node, NodeClaim and pod changes into one rebuild, which bounds how
	// stale the metrics are after a change.
	BatchWindow = 5 * time.Second
	// LifetimeRefresh is how often the controller rebuilds while nothing changes, since the Lifetime metric is a
	// function of the time.
	LifetimeRefresh = time.Minute
)

type Controller struct {
	nodes       *karpentercluster.NodeState
	metricStore *metrics.Store
	// counts holds the node count ClusterStateNodesCount publishes, derived from the NodeClaim and Node stores in
	// Register.
	counts *state.Store[nodeCountKey, nodeCountKey, nodeCounts, struct{}]
}

// nodeCountKey is the key of the one entry of the controller's node count. Its zero value contributes to nothing.
type nodeCountKey struct{ counted bool }

// counted is the key every counted node contributes to.
var counted = nodeCountKey{counted: true}

// nodeCounts are the nodes cluster state reads: the launched NodeClaims, and the unmanaged Nodes.
type nodeCounts struct {
	// LaunchedNodeClaims counts the NodeClaims with a providerID, deleting or not.
	LaunchedNodeClaims int
	// UnmanagedNodes counts the Nodes with neither a NodeClaim owner reference nor the NodePool label.
	UnmanagedNodes int
}

// launchedNodeClaim is a launched NodeClaim's contribution of one to LaunchedNodeClaims.
type launchedNodeClaim int

func (n launchedNodeClaim) Update(c nodeCounts, prev launchedNodeClaim) nodeCounts {
	c.LaunchedNodeClaims += int(n - prev)
	return c
}

func (n launchedNodeClaim) Delete(c nodeCounts) nodeCounts {
	c.LaunchedNodeClaims -= int(n)
	return c
}

// unmanagedNode is an unmanaged Node's contribution of one to UnmanagedNodes.
type unmanagedNode int

func (n unmanagedNode) Update(c nodeCounts, prev unmanagedNode) nodeCounts {
	c.UnmanagedNodes += int(n - prev)
	return c
}

func (n unmanagedNode) Delete(c nodeCounts) nodeCounts {
	c.UnmanagedNodes -= int(n)
	return c
}

// nodeCount is how many nodes cluster state reads.
func (c *Controller) nodeCount() int {
	e, _ := c.counts.Get(counted)
	return e.Derived.LaunchedNodeClaims + e.Derived.UnmanagedNodes
}

// NewController returns the node metrics controller, which reads the nodes of cluster state.
func NewController(nodes *karpentercluster.NodeState) *Controller {
	initializeMetrics()
	return &Controller{
		nodes:       nodes,
		metricStore: metrics.NewStore(),
	}
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name()) //nolint:ineffassign,staticcheck

	// The metrics only read the nodes, so they share cluster state's rather than copying their usage.
	var nodes karpentercluster.StateNodes
	for n := range c.nodes.Nodes() {
		if n.Node != nil {
			nodes = append(nodes, n)
		}
	}

	// Build per-node metrics
	metricsMap := lo.SliceToMap(nodes, func(n *karpentercluster.StateNode) (string, []*metrics.StoreMetric) {
		return client.ObjectKeyFromObject(n.Node).String(), buildMetrics(n)
	})

	// Build cluster level metric
	metricsMap["clusterUtilization"] = buildClusterUtilizationMetric(nodes)

	c.metricStore.ReplaceAll(metricsMap)
	ClusterStateNodesCount.Set(float64(c.nodeCount()), nil)

	// The sources wake the controller on every change but the time passing, which Lifetime reads.
	return reconciler.Result{RequeueAfter: LifetimeRefresh}, nil //storelint:deadline the Lifetime metric is a function of the time
}

func (c *Controller) Name() string {
	return "metrics.node"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	// The node count is derived from cluster state's NodeClaim and Node stores.
	c.counts = state.NewSyntheticStore[nodeCountKey, nodeCountKey, nodeCounts, struct{}](func(k nodeCountKey) nodeCountKey { return k })
	state.Derive(c.nodes.NodeClaimStore.Reader, c.counts,
		func(_ string, e karpentercluster.NodeClaimEntry) nodeCountKey {
			return lo.Ternary(karpentercluster.Launched(e), counted, nodeCountKey{})
		},
		func(string, karpentercluster.NodeClaimEntry) launchedNodeClaim { return 1 })
	state.DeriveObject(c.nodes.NodeStore.Reader, c.counts,
		func(_ string, node *corev1.Node) nodeCountKey {
			return lo.Ternary(karpentercluster.Unmanaged(node), counted, nodeCountKey{})
		},
		func(string, *corev1.Node) unmanagedNode { return 1 })
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		// The controller rebuilds every metric at most once per BatchWindow while the nodes change.
		WatchesRawSource(state.After(BatchWindow, c.wakes())).
		WatchesRawSource(state.Requires(c.nodes)).
		Complete(singleton.AsReconciler(c))
}

// wakes are the changes the metrics read: a launched NodeClaim appearing, going or linking its Node; and a Node changing,
// or the requests and limits of its pods. A NodeClaim launching or going, and a Node changing, also cover every change of
// the node count. A NodeClaim's status and intent, and a pod's status, are left out.
func (c *Controller) wakes() source.SyncingSource {
	return state.Wake(
		c.nodes.NodeClaimStore.SourceOn(func(_ string, e karpentercluster.NodeClaimEntry) any {
			if e.Object.Status.ProviderID == "" {
				return nil
			}
			return e.Derived.Node
		}),
		c.nodes.NodeStore.SourceOn(func(_ string, e karpentercluster.NodeEntry) any {
			u := e.Derived.Usage
			return nodeInputs{Node: e.Object, Pods: u.Pods, Requests: u.Requests, Limits: u.Limits, DaemonSetRequests: u.DaemonSetRequests, DaemonSetLimits: u.DaemonSetLimits}
		}),
	)
}

// nodeInputs are what the metrics read of a Node's entry.
type nodeInputs struct {
	Node                                                 *corev1.Node
	Pods                                                 int
	Requests, Limits, DaemonSetRequests, DaemonSetLimits corev1.ResourceList
}

func buildClusterUtilizationMetric(nodes karpentercluster.StateNodes) []*metrics.StoreMetric {

	// Aggregate resources allocated/utilized for all the nodes and pods inside the nodes
	allocatableAggregate, utilizedAggregate := corev1.ResourceList{}, corev1.ResourceList{}

	for _, node := range nodes {
		resources.MergeInto(allocatableAggregate, node.Allocatable())
		resources.MergeInto(utilizedAggregate, node.PodRequests())
	}

	res := make([]*metrics.StoreMetric, 0, len(allocatableAggregate))

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

		res = append(res, &metrics.StoreMetric{
			GaugeMetric: ClusterUtilization,
			Value:       utilizationPercentage,
			Labels:      map[string]string{metrics.ResourceTypeLabel: resourceNameToString(resourceName)},
		})
	}

	return res
}

func buildMetrics(n *karpentercluster.StateNode) (res []*metrics.StoreMetric) {
	for gaugeMetric, resourceList := range map[opmetrics.GaugeMetric]corev1.ResourceList{
		SystemOverhead:      resources.Subtract(n.Node.Status.Capacity, n.Node.Status.Allocatable),
		TotalPodRequests:    n.PodRequests(),
		TotalPodLimits:      n.PodLimits(),
		TotalDaemonRequests: n.DaemonSetRequests(),
		TotalDaemonLimits:   n.DaemonSetLimits(),
		Allocatable:         n.Node.Status.Allocatable,
	} {
		for resourceName, quantity := range resourceList {
			res = append(res, &metrics.StoreMetric{
				GaugeMetric: gaugeMetric,
				Value:       lo.Ternary(resourceName == corev1.ResourceCPU, float64(quantity.MilliValue())/float64(1000), float64(quantity.Value())),
				Labels:      getNodeLabelsWithResourceType(n, resourceNameToString(resourceName)),
			})
		}
	}
	return append(res,
		&metrics.StoreMetric{
			GaugeMetric: Lifetime,
			Value:       time.Since(n.Node.GetCreationTimestamp().Time).Seconds(),
			Labels:      getNodeLabels(n),
		})
}

func getNodeLabelsWithResourceType(n *karpentercluster.StateNode, resourceTypeName string) prometheus.Labels {
	metricLabels := getNodeLabels(n)
	metricLabels[metrics.ResourceTypeLabel] = resourceTypeName
	return metricLabels
}

func getNodeLabels(n *karpentercluster.StateNode) prometheus.Labels {
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
