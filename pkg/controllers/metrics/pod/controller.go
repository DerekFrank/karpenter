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
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/state"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

const (
	podName             = "name"
	podNamespace        = "namespace"
	ownerSelfLink       = "owner"
	podHostName         = "node"
	podHostArchitecture = "arch"
	podHostInstanceType = "instance_type"
	podPhase            = "phase"
	podScheduled        = "scheduled"
	podReady            = "ready"
	managed             = "managed"
	podDynamicResources = "dynamic_resources"
)

// Pod metric dimensions. zone, nodepool, and capacity_type reuse the shared
// dimensions from pkg/metrics.
var (
	podNameLabel             = opmetrics.Label{Name: podName, Help: "The name of the pod."}
	podNamespaceLabel        = opmetrics.Label{Name: podNamespace, Help: "The namespace of the pod."}
	ownerLabel               = opmetrics.Label{Name: ownerSelfLink, Help: "The owning workload of the pod, formatted as `<kind>/<name>`."}
	podHostNameLabel         = opmetrics.Label{Name: podHostName, Help: "The name of the node the pod is bound to."}
	podHostArchitectureLabel = opmetrics.Label{Name: podHostArchitecture, Help: "The CPU architecture of the node the pod is bound to."}
	podHostInstanceTypeLabel = opmetrics.Label{Name: podHostInstanceType, Help: "The instance type of the node the pod is bound to."}
	podScheduledLabel        = opmetrics.Label{Name: podScheduled, Help: "Whether the pod has been scheduled to a node.", Values: metrics.BoolValues}
	podReadyLabel            = opmetrics.Label{Name: podReady, Help: "Whether the pod is ready.", Values: metrics.BoolValues}
	managedLabel             = opmetrics.Label{Name: managed, Help: "Whether the pod is bound to a Karpenter-managed node.", Values: metrics.BoolValues}
	podDynamicResourcesLabel = opmetrics.Label{Name: podDynamicResources, Help: "Whether the pod has DRA (dynamic resource allocation) requirements.", Values: metrics.BoolValues}
	podPhaseLabel            = opmetrics.Label{
		Name: podPhase,
		Help: "The pod's lifecycle phase.",
		Values: []opmetrics.Value{
			{Name: string(corev1.PodPending), Help: "The pod has been accepted but not all containers are running."},
			{Name: string(corev1.PodRunning), Help: "The pod is bound to a node and all containers are running."},
			{Name: string(corev1.PodSucceeded), Help: "All containers terminated successfully."},
			{Name: string(corev1.PodFailed), Help: "All containers terminated and at least one failed."},
			{Name: string(corev1.PodUnknown), Help: "The pod's state could not be obtained."},
		},
	}
)

// The gauges are built at scrape time, by the Collector Register registers: each is a level, what each pod is now.
var (
	PodState = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "state",
			Help:      "Pod state is the current state of pods. This metric can be used several ways as it is labeled by the pod name, namespace, owner, node, whether the pod is scheduled, nodepool name, zone, architecture, capacity type, instance type, pod phase, pod readiness, and whether the node is Karpenter-managed.",
		},
		labelNames(),
		opmetrics.Beta,
	)
	PodUnstartedTimeSeconds = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "unstarted_time_seconds",
			Help:      "The time from pod creation until the pod is running.",
		},
		[]opmetrics.Label{podNameLabel, podNamespaceLabel},
		opmetrics.Alpha,
	)
	PodUnboundTimeSeconds = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "unbound_time_seconds",
			Help:      "The time from pod creation until the pod is bound.",
		},
		[]opmetrics.Label{podNameLabel, podNamespaceLabel, podDynamicResourcesLabel},
		opmetrics.Alpha,
	)
	// Stage: alpha
	PodProvisioningUnboundTimeSeconds = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "provisioning_unbound_time_seconds",
			Help:      "The time from when Karpenter first thinks the pod can schedule until it binds. Note: this calculated from a point in memory, not by the pod creation timestamp.",
		},
		[]opmetrics.Label{podNameLabel, podNamespaceLabel, podDynamicResourcesLabel},
		opmetrics.Alpha,
	)
	// Stage: alpha
	PodProvisioningUnstartedTimeSeconds = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "provisioning_unstarted_time_seconds",
			Help:      "The time from when Karpenter first thinks the pod can schedule until the pod is running. Note: this calculated from a point in memory, not by the pod creation timestamp.",
		},
		[]opmetrics.Label{podNameLabel, podNamespaceLabel},
		opmetrics.Alpha,
	)
	// Stage: alpha
	PodSchedulingUndecidedTimeSeconds = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "provisioning_scheduling_undecided_time_seconds",
			Help:      "The time from when Karpenter has seen a pod without making a scheduling decision for the pod. Note: this calculated from a point in memory, not by the pod creation timestamp.",
		},
		[]opmetrics.Label{podNameLabel, podNamespaceLabel},
		opmetrics.Alpha,
	)
)

// The durations are transitions, observed once each by the subscriber Register subscribes to the pod store.
var (
	PodStartupDurationSeconds = opmetrics.NewPrometheusSummary(
		crmetrics.Registry,
		prometheus.SummaryOpts{
			Namespace:  metrics.Namespace,
			Subsystem:  metrics.PodSubsystem,
			Name:       "startup_duration_seconds",
			Help:       "The time from pod creation until the pod is running.",
			Objectives: metrics.SummaryObjectives(),
		},
		[]opmetrics.Label{},
		opmetrics.GA,
	)
	PodBoundDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "bound_duration_seconds",
			Help:      "The time from pod creation until the pod is bound.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{podDynamicResourcesLabel},
		opmetrics.Alpha,
	)
	// Stage: alpha
	PodProvisioningBoundDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "provisioning_bound_duration_seconds",
			Help:      "The time from when Karpenter first thinks the pod can schedule until it binds. Note: this calculated from a point in memory, not by the pod creation timestamp.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{podDynamicResourcesLabel},
		opmetrics.Alpha,
	)
	// Stage: alpha
	PodProvisioningStartupDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.PodSubsystem,
			Name:      "provisioning_startup_duration_seconds",
			Help:      "The time from when Karpenter first thinks the pod can schedule until the pod is running. Note: this calculated from a point in memory, not by the pod creation timestamp.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{},
		opmetrics.Alpha,
	)
)

func labelNames() []opmetrics.Label {
	return []opmetrics.Label{
		podNameLabel,
		podNamespaceLabel,
		ownerLabel,
		podHostNameLabel,
		podScheduledLabel,
		metrics.NodePool,
		metrics.Zone,
		podHostArchitectureLabel,
		metrics.CapacityType,
		podHostInstanceTypeLabel,
		podPhaseLabel,
		podReadyLabel,
		managedLabel,
	}
}

// progress is what the pod metrics have seen of a pod that was pending: whether it's still to start and to bind. A
// duration is observed only for a pod seen pending, so that a restart doesn't observe the pods already running.
type progress struct {
	Starting bool
	Binding  bool
}

type Controller struct {
	nodes         *karpentercluster.NodeState
	podScheduling *karpentercluster.PodScheduling
	// progress is fed by the subscriber, by pod: a synthetic store this controller alone reads, whose entry drops once
	// the pod has started and bound, or is gone.
	progress *state.Store[types.NamespacedName, progress, struct{}, struct{}]
}

// NewController returns the pod metrics: gauges built at scrape time from the pod, Node and pod scheduling stores, and
// durations observed by a subscriber to the pod store.
func NewController(nodes *karpentercluster.NodeState, podScheduling *karpentercluster.PodScheduling) *Controller {
	return &Controller{nodes: nodes, podScheduling: podScheduling}
}

func (c *Controller) Name() string {
	return "metrics.pod"
}

// Register subscribes to the pod store and registers the Collector. Nothing reconciles.
func (c *Controller) Register(_ context.Context, _ manager.Manager) error {
	c.progress = state.NewFedStore[types.NamespacedName, progress, struct{}, struct{}]()
	c.nodes.PodStore.Subscribe(c.observe)
	return crmetrics.Registry.Register(metrics.NewCollector(
		func() bool { return c.nodes.PodStore.Synced() && c.nodes.NodeStore.Synced() },
		c.collect,
		PodState, PodUnstartedTimeSeconds, PodUnboundTimeSeconds, PodProvisioningUnboundTimeSeconds, PodProvisioningUnstartedTimeSeconds, PodSchedulingUndecidedTimeSeconds,
	))
}

// observe follows each pod that was pending until it starts and binds, and observes each duration once, on the change
// that ends it. It's a subscriber: it reads stores, updates histograms and feeds its own store, and never blocks.
func (c *Controller) observe(change state.Change[types.NamespacedName, *corev1.Pod, struct{}, struct{}]) {
	if !change.Exists {
		c.feed(change.Key, progress{})
		return
	}
	pod := change.New.Object
	if pod.Status.Phase == corev1.PodPending {
		c.feed(change.Key, progress{Starting: true, Binding: true})
		return
	}
	e, ok := c.progress.Get(change.Key)
	if !ok {
		return
	}
	p := e.Object
	schedulableTime := c.podScheduling.Get(change.Key).SchedulableAt
	if p.Starting && observeStarted(pod, schedulableTime) {
		p.Starting = false
		// The pod scheduling telemetry goes once the pod has started.
		c.podScheduling.Update(change.Key, func(karpentercluster.PodSchedulingFields) karpentercluster.PodSchedulingFields {
			return karpentercluster.PodSchedulingFields{}
		})
	}
	if p.Binding && observeBound(pod, schedulableTime) {
		p.Binding = false
	}
	c.feed(change.Key, p)
}

// observeStarted observes a starting pod's startup durations once it's ready, and returns whether it's no longer
// starting: ready, or terminal, whose Ready condition is false, so it has no startup duration.
func observeStarted(pod *corev1.Pod, schedulableTime time.Time) bool {
	cond, ready := condition(pod, corev1.PodReady)
	if !ready {
		return podutils.IsTerminal(pod)
	}
	PodStartupDurationSeconds.Observe(cond.LastTransitionTime.Sub(pod.CreationTimestamp.Time).Seconds(), nil)
	if !schedulableTime.IsZero() {
		PodProvisioningStartupDurationSeconds.Observe(cond.LastTransitionTime.Sub(schedulableTime).Seconds(), nil)
	}
	return true
}

// observeBound observes a binding pod's bound durations once it's scheduled, and returns whether it is.
func observeBound(pod *corev1.Pod, schedulableTime time.Time) bool {
	cond, scheduled := condition(pod, corev1.PodScheduled)
	if !scheduled {
		return false
	}
	dynamicResources := map[string]string{podDynamicResources: strconv.FormatBool(podutils.HasDRARequirements(pod))}
	PodBoundDurationSeconds.Observe(cond.LastTransitionTime.Sub(pod.CreationTimestamp.Time).Seconds(), dynamicResources)
	if !schedulableTime.IsZero() {
		PodProvisioningBoundDurationSeconds.Observe(cond.LastTransitionTime.Sub(schedulableTime).Seconds(), dynamicResources)
	}
	return true
}

// feed records a pod's progress. The zero progress drops it.
func (c *Controller) feed(key types.NamespacedName, p progress) {
	c.progress.Feed(key, func(progress, bool) (progress, bool) { return p, p != progress{} })
}

// collect emits each pod's gauges from the stores' own objects, copying none.
func (c *Controller) collect(emit metrics.Emit) {
	now := time.Now()
	values := make([]string, 0, len(labelNames()))
	for key, e := range c.nodes.PodStore.List() {
		c.collectPod(emit, now, key, e.Object, values[:0])
	}
}

// collectPod emits a pod's gauges. values is room for its pod_state label values.
func (c *Controller) collectPod(emit metrics.Emit, now time.Time, key types.NamespacedName, pod *corev1.Pod, values []string) {
	emit.Gauge(PodState, 1, c.podLabelValues(values, pod)...)

	cond, scheduled := condition(pod, corev1.PodScheduled)
	sched := c.podScheduling.Get(key)
	if sched.DecidedAt.IsZero() && !scheduled && pod.Spec.NodeName == "" && !podutils.IsTerminal(pod) && !sched.AckedAt.IsZero() {
		emit.Gauge(PodSchedulingUndecidedTimeSeconds, now.Sub(sched.AckedAt).Seconds(), pod.Name, pod.Namespace)
	}
	// A pending pod's unbound time runs until it binds, and holds at the time it took while the pod is still pending.
	if pod.Status.Phase == corev1.PodPending {
		until := lo.Ternary(scheduled, cond.LastTransitionTime.Time, now)
		dynamicResources := strconv.FormatBool(podutils.HasDRARequirements(pod))
		emit.Gauge(PodUnboundTimeSeconds, until.Sub(pod.CreationTimestamp.Time).Seconds(), pod.Name, pod.Namespace, dynamicResources)
		if !sched.SchedulableAt.IsZero() {
			emit.Gauge(PodProvisioningUnboundTimeSeconds, until.Sub(sched.SchedulableAt).Seconds(), pod.Name, pod.Namespace, dynamicResources)
		}
	}
	if p, ok := c.progress.Get(key); ok && p.Object.Starting {
		emit.Gauge(PodUnstartedTimeSeconds, now.Sub(pod.CreationTimestamp.Time).Seconds(), pod.Name, pod.Namespace)
		if !sched.SchedulableAt.IsZero() {
			emit.Gauge(PodProvisioningUnstartedTimeSeconds, now.Sub(sched.SchedulableAt).Seconds(), pod.Name, pod.Namespace)
		}
	}
}

// podLabelValues appends the pod's values for labelNames, in its order, reading its Node from the Node store.
func (c *Controller) podLabelValues(values []string, pod *corev1.Pod) []string {
	// Selflink has been deprecated after v.1.20
	// Manually generate the selflink for the first owner reference
	// Currently we do not support multiple owner references
	selflink := ""
	if len(pod.OwnerReferences) > 0 {
		selflink = fmt.Sprintf("/apis/%s/namespaces/%s/%ss/%s", pod.OwnerReferences[0].APIVersion, pod.Namespace, strings.ToLower(pod.OwnerReferences[0].Kind), pod.OwnerReferences[0].Name)
	}
	var nodeLabels map[string]string
	if pod.Spec.NodeName != "" {
		if node, ok := c.nodes.NodeStore.Get(pod.Spec.NodeName); ok {
			nodeLabels = node.Object.Labels
		}
	}
	_, ready := condition(pod, corev1.PodReady)
	return append(values,
		pod.Name,
		pod.Namespace,
		selflink,
		pod.Spec.NodeName,
		strconv.FormatBool(pod.Spec.NodeName != ""),
		nodeLabels[v1.NodePoolLabelKey],
		nodeLabels[corev1.LabelTopologyZone],
		nodeLabels[corev1.LabelArchStable],
		nodeLabels[v1.CapacityTypeLabelKey],
		nodeLabels[corev1.LabelInstanceTypeStable],
		string(pod.Status.Phase),
		strconv.FormatBool(ready),
		strconv.FormatBool(nodeLabels[v1.NodePoolLabelKey] != ""),
	)
}

// condition returns the pod's condition of type t, and whether it's true.
func condition(pod *corev1.Pod, t corev1.PodConditionType) (corev1.PodCondition, bool) {
	cond, ok := lo.Find(pod.Status.Conditions, func(c corev1.PodCondition) bool { return c.Type == t })
	return cond, ok && cond.Status == corev1.ConditionTrue
}
