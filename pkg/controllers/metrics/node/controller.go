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
	"sort"
	"strconv"
	"strings"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/metrics"
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

// The node gauges are built at scrape time from cluster state's stores, by the Collector Register registers.
var (
	// ClusterStateNodesCount is how many nodes cluster state reads: the launched NodeClaims, and the unmanaged Nodes.
	ClusterStateNodesCount = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: "cluster_state",
			Name:      "node_count",
			Help:      "Current count of nodes in cluster state",
		},
		[]opmetrics.Label{},
		opmetrics.GA,
	)
	// ClusterStateSynced and ClusterStateUnsyncedTimeSeconds are built by their own Collector, which emits while the
	// stores are unsynced.
	ClusterStateSynced = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: "cluster_state",
			Name:      "synced",
			Help:      "Returns 1 if cluster state is synced and 0 otherwise. Synced checks that nodeclaims and nodes that are stored in the APIServer have the same representation as Karpenter's cluster state",
		},
		[]opmetrics.Label{},
		opmetrics.GA,
	)
	ClusterStateUnsyncedTimeSeconds = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: "cluster_state",
			Name:      "unsynced_time_seconds",
			Help:      "The time for which cluster state is not synced",
		},
		[]opmetrics.Label{},
		opmetrics.GA,
	)
	Allocatable         *metrics.CollectedGauge
	TotalPodRequests    *metrics.CollectedGauge
	TotalPodLimits      *metrics.CollectedGauge
	TotalDaemonRequests *metrics.CollectedGauge
	TotalDaemonLimits   *metrics.CollectedGauge
	SystemOverhead      *metrics.CollectedGauge
	Lifetime            *metrics.CollectedGauge
	ClusterUtilization  *metrics.CollectedGauge

	// wellKnownLabels are the node labels the gauges carry, each with its metric label name, in the order of the
	// gauges' Labels.
	wellKnownLabels []wellKnownLabel
)

type wellKnownLabel struct{ key, name string }

// Initialize metrics at runtime to ensure cloud provider's well-known labels are properly
// injected, preventing race conditions in dependency ordering during label injection for global variable.
func initializeMetrics() {
	wellKnownLabels = getWellKnownLabels()
	gauge := func(subsystem, name, help string, labels []opmetrics.Label, stage opmetrics.Stage) *metrics.CollectedGauge {
		return metrics.NewCollectedGauge(prometheus.GaugeOpts{Namespace: metrics.Namespace, Subsystem: subsystem, Name: name, Help: help}, labels, stage)
	}
	Allocatable = gauge(metrics.NodeSubsystem, "allocatable", "Node allocatable are the resources allocatable by nodes.", nodeLabelNamesWithResourceType(), opmetrics.Beta)
	TotalPodRequests = gauge(metrics.NodeSubsystem, "total_pod_requests", "Node total pod requests are the resources requested by pods bound to nodes, including the DaemonSet pods.", nodeLabelNamesWithResourceType(), opmetrics.Beta)
	TotalPodLimits = gauge(metrics.NodeSubsystem, "total_pod_limits", "Node total pod limits are the resources specified by pod limits, including the DaemonSet pods.", nodeLabelNamesWithResourceType(), opmetrics.Beta)
	TotalDaemonRequests = gauge(metrics.NodeSubsystem, "total_daemon_requests", "Node total daemon requests are the resource requested by DaemonSet pods bound to nodes.", nodeLabelNamesWithResourceType(), opmetrics.Beta)
	TotalDaemonLimits = gauge(metrics.NodeSubsystem, "total_daemon_limits", "Node total daemon limits are the resources specified by DaemonSet pod limits.", nodeLabelNamesWithResourceType(), opmetrics.Beta)
	SystemOverhead = gauge(metrics.NodeSubsystem, "system_overhead", "Node system daemon overhead are the resources reserved for system overhead, the difference between the node's capacity and allocatable values are reported by the status.", nodeLabelNamesWithResourceType(), opmetrics.Beta)
	Lifetime = gauge(metrics.NodeSubsystem, "current_lifetime_seconds", "Node age in seconds", nodeLabelNames(), opmetrics.Alpha)
	ClusterUtilization = gauge("cluster", "utilization_percent", "Utilization of allocatable resources by pod requests", []opmetrics.Label{metrics.ResourceType}, opmetrics.Alpha)
}

func nodeLabelNamesWithResourceType() []opmetrics.Label {
	return append(nodeLabelNames(), metrics.ResourceType)
}

// nodeLabelNames are the per-node dimensions: the well-known labels, which include the NodePool label and are
// self-describing so are wrapped without help text, then the node's name, phase and whether it's managed.
func nodeLabelNames() []opmetrics.Label {
	return append(
		lo.Map(wellKnownLabels, func(l wellKnownLabel, _ int) opmetrics.Label { return opmetrics.Label{Name: l.name} }),
		nodeNameLabel,
		nodePhaseLabel,
		managedLabel,
	)
}

type Controller struct {
	nodeClaims *karpentercluster.NodeClaims
	nodes      *karpentercluster.Nodes
	// created is when the controller was built, from which ClusterStateUnsyncedTimeSeconds counts until the stores sync.
	created time.Time
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

// NewController returns the node metrics, which read the managed nodes from the NodeClaim store and the unmanaged
// Nodes from the Node store at scrape time.
func NewController(nodeClaims *karpentercluster.NodeClaims, nodes *karpentercluster.Nodes) *Controller {
	initializeMetrics()
	return &Controller{nodeClaims: nodeClaims, nodes: nodes, created: time.Now()}
}

func (c *Controller) Name() string {
	return "metrics.node"
}

// Register derives the node count and registers the Collectors. Nothing reconciles: each scrape reads the stores.
func (c *Controller) Register(_ context.Context, _ manager.Manager) error {
	// The node count is derived from cluster state's NodeClaim and Node stores.
	c.counts = state.NewSyntheticStore[nodeCountKey, nodeCountKey, nodeCounts, struct{}](func(k nodeCountKey) nodeCountKey { return k })
	state.Derive(c.nodeClaims.Reader, c.counts,
		func(_ string, e karpentercluster.NodeClaimEntry) nodeCountKey {
			return lo.Ternary(karpentercluster.Launched(e), counted, nodeCountKey{})
		},
		func(string, karpentercluster.NodeClaimEntry) launchedNodeClaim { return 1 })
	state.DeriveObject(c.nodes.Reader, c.counts,
		func(_ string, node *corev1.Node) nodeCountKey {
			return lo.Ternary(karpentercluster.Unmanaged(node), counted, nodeCountKey{})
		},
		func(string, *corev1.Node) unmanagedNode { return 1 })
	if err := crmetrics.Registry.Register(c.syncedCollector()); err != nil {
		return err
	}
	return crmetrics.Registry.Register(c.collector())
}

// synced returns whether the stores the node metrics read hold what the API server held when they started.
func (c *Controller) synced() bool { return c.nodeClaims.Synced() && c.nodes.Synced() }

// syncedCollector builds whether cluster state is synced, and for how long it hasn't been. It isn't gated on the
// stores' sync, which is what it reports.
func (c *Controller) syncedCollector() *metrics.Collector {
	return metrics.NewCollector(func() bool { return true }, c.collectSynced, ClusterStateSynced, ClusterStateUnsyncedTimeSeconds)
}

// collectSynced emits whether the NodeClaim and Node stores are synced, and for how long since the controller was
// built they haven't been. A store's sync is one-way, so the unsynced time is zero from the moment they sync.
func (c *Controller) collectSynced(emit metrics.Emit) {
	if c.synced() {
		emit.Gauge(ClusterStateSynced, 1)
		emit.Gauge(ClusterStateUnsyncedTimeSeconds, 0)
		return
	}
	emit.Gauge(ClusterStateSynced, 0)
	emit.Gauge(ClusterStateUnsyncedTimeSeconds, time.Since(c.created).Seconds())
}

// collector builds the gauges at scrape time. It reads the stores' own objects and usage, copying none of them.
func (c *Controller) collector() *metrics.Collector {
	return metrics.NewCollector(
		func() bool { return c.synced() && c.counts.Synced() },
		c.collect,
		ClusterStateNodesCount, Allocatable, TotalPodRequests, TotalPodLimits, TotalDaemonRequests, TotalDaemonLimits, SystemOverhead, Lifetime, ClusterUtilization,
	)
}

// collect emits every node gauge for the nodes cluster state reads now. It reads each node's entries once, and sums
// allocatable and requests for the cluster's utilization as it goes.
func (c *Controller) collect(emit metrics.Emit) {
	emit.Gauge(ClusterStateNodesCount, float64(c.nodeCount()))
	allocatable, utilized := corev1.ResourceList{}, corev1.ResourceList{}
	collect := func(n karpentercluster.NodeView) {
		collectNode(emit, n)
		resources.MergeInto(allocatable, n.Allocatable())
		resources.MergeInto(utilized, n.PodRequests())
	}
	// Each launched NodeClaim's node once its Node links to it, and each unmanaged Node.
	for _, e := range c.nodeClaims.List(client.UnsafeDisableDeepCopy) {
		if karpentercluster.Launched(e) && !karpentercluster.InFlight(e) {
			collect(karpentercluster.ViewOfNodeClaim(e))
		}
	}
	for _, e := range c.nodes.Unmanaged(client.UnsafeDisableDeepCopy) {
		collect(karpentercluster.ViewOfNode(e))
	}
	collectClusterUtilization(emit, allocatable, utilized)
}

func collectClusterUtilization(emit metrics.Emit, allocatable, utilized corev1.ResourceList) {
	for resourceName, allocatableResource := range allocatable {
		if allocatableResource.Value() == 0 {
			continue
		}
		utilizedResource := utilized[resourceName]
		// Typecast to float before the calculation to maximize resolution
		utilizationPercentage := 100 * lo.Ternary(
			resourceName == corev1.ResourceCPU,
			float64(utilizedResource.MilliValue())/float64(allocatableResource.MilliValue()),
			float64(utilizedResource.Value())/float64(allocatableResource.Value()))
		emit.Gauge(ClusterUtilization, utilizationPercentage, resourceNameToString(resourceName))
	}
}

// collectNode emits a node's gauges. Its label values are built once, and each resource's series adds its resource
// type.
func collectNode(emit metrics.Emit, n karpentercluster.NodeView) {
	node := n.Node()
	values := nodeLabelValues(n)
	for _, g := range []struct {
		gauge     *metrics.CollectedGauge
		resources corev1.ResourceList
	}{
		{SystemOverhead, resources.Subtract(node.Status.Capacity, node.Status.Allocatable)},
		{TotalPodRequests, n.PodRequests()},
		{TotalPodLimits, n.PodLimits()},
		{TotalDaemonRequests, n.DaemonSetRequests()},
		{TotalDaemonLimits, n.DaemonSetLimits()},
		{Allocatable, node.Status.Allocatable},
	} {
		for resourceName, quantity := range g.resources {
			emit.Gauge(g.gauge, lo.Ternary(resourceName == corev1.ResourceCPU, float64(quantity.MilliValue())/float64(1000), float64(quantity.Value())),
				append(values, resourceNameToString(resourceName))...)
		}
	}
	emit.Gauge(Lifetime, time.Since(node.GetCreationTimestamp().Time).Seconds(), values...)
}

// nodeLabelValues are a node's values for nodeLabelNames, in its order, with room for the resource type.
func nodeLabelValues(n karpentercluster.NodeView) []string {
	node := n.Node()
	values := make([]string, 0, len(wellKnownLabels)+4)
	for _, l := range wellKnownLabels {
		values = append(values, node.Labels[l.key])
	}
	return append(values, node.Name, string(node.Status.Phase), strconv.FormatBool(n.Managed()))
}

// getWellKnownLabels returns the well-known node labels, each with its metric label name in snake_case, sorted and
// deduplicated by name, since a gauge can't carry a label twice.
func getWellKnownLabels() []wellKnownLabel {
	byName := map[string]string{}
	for key := range v1.WellKnownLabels {
		if parts := strings.Split(key, "/"); len(parts) == 2 {
			// Reformat label names to be consistent with Prometheus naming conventions (snake_case)
			byName[strings.ReplaceAll(strings.ToLower(parts[1]), "-", "_")] = key
		}
	}
	labels := lo.MapToSlice(byName, func(name, key string) wellKnownLabel { return wellKnownLabel{key: key, name: name} })
	sort.Slice(labels, func(i, j int) bool { return labels[i].name < labels[j].name })
	return labels
}

func resourceNameToString(resourceName corev1.ResourceName) string {
	return strings.ReplaceAll(strings.ToLower(string(resourceName)), "-", "_")
}
