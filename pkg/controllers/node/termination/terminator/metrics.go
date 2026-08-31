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

package terminator

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	// CodeLabel for eviction request
	CodeLabel = "code"
	// ReasonLabel for pod draining
	ReasonLabel = "reason"
	// ForcefulTerminationReason is the drain `reason` value emitted when a pod is
	// force-deleted (the node's terminationGracePeriod elapsed) rather than
	// drained under a disruption reason.
	ForcefulTerminationReason = "Forceful Termination"
)

// Code describes the code dimension emitted by the pod eviction metric.
var Code = opmetrics.Label{
	Name: CodeLabel,
	Help: "The HTTP response code returned by the Kubernetes eviction API (https://kubernetes.io/docs/concepts/scheduling-eviction/api-eviction/) for the eviction request.",
}

// reasonForcefulTermination is the drain reason for force-deleted pods.
var reasonForcefulTermination = opmetrics.Value{
	Name: ForcefulTerminationReason,
	Help: "The pod was force-deleted because the node's terminationGracePeriod elapsed.",
}

// DrainReason describes the reason dimension emitted by the pod drain metric.
// A drained pod's reason is the owning NodeClaim's disruption reason, so the
// value set is the disruption reasons plus forceful termination.
var DrainReason = opmetrics.Label{
	Name:   ReasonLabel,
	Help:   "Why the pod was drained: the owning NodeClaim's disruption reason, or forceful termination.",
	Values: append(append([]opmetrics.Value{}, metrics.NodeClaimDisruptedReason.Values...), reasonForcefulTermination),
}

var PodsEvictionRequestsTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.PodSubsystem,
		Name:      "eviction_requests_total",
		Help:      "The total number of pod eviction requests made by Karpenter, labeled by response code",
	},
	[]opmetrics.Label{Code},
)

var PodsDrainedTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.PodSubsystem,
		Name:      "drained_total",
		Help:      "The total number of pods drained during node termination by Karpenter, labeled by reason",
	},
	[]opmetrics.Label{DrainReason},
)
