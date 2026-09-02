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
)

var PodsEvictionRequestsTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.PodSubsystem,
		Name:      "eviction_requests_total",
		Help:      "The total number of pod eviction requests made by Karpenter, labeled by response code",
	},
	[]string{CodeLabel},
)

var PodsDrainedTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.PodSubsystem,
		Name:      "drained_total",
		Help:      "The total number of pods drained during node termination by Karpenter, labeled by reason",
	},
	[]string{ReasonLabel},
)

// PodsForceDeletedTotal counts pods that were force-deleted past the node's grace-period expiration (the terminator's
// DeleteExpiringPods path). Unlike PodsDrainedTotal — which counts pods that completed a graceful eviction (honoring the
// PDB and grace period) — this counts disruptions that BYPASSED the PDB and grace, i.e. "disrupted without a completed
// drain". Together they split the disruption cost into graceful vs forceful.
var PodsForceDeletedTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.PodSubsystem,
		Name:      "force_deleted_total",
		Help:      "The total number of pods force-deleted past their grace period during node termination by Karpenter (bypassing PDB and grace), labeled by reason",
	},
	[]string{ReasonLabel},
)
