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

package metrics

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

const (
	NodeSubsystem      = "nodes"
	NodeClaimSubsystem = "nodeclaims"
	NodePoolSubsystem  = "nodepools"
	PodSubsystem       = "pods"
)

var (
	NodeClaimsCreatedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: NodeClaimSubsystem,
			Name:      "created_total",
			Help:      "Number of nodeclaims created in total by Karpenter. Labeled by reason the nodeclaim was created, the owning nodepool, and if min values was relaxed for this nodeclaim.",
		},
		[]opmetrics.Label{
			NodeClaimCreatedReason,
			NodePool,
			MinValuesRelaxed,
		},
		opmetrics.GA,
	)
	NodeClaimsTerminatedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: NodeClaimSubsystem,
			Name:      "terminated_total",
			Help:      "Number of nodeclaims terminated in total by Karpenter. Labeled by the owning nodepool, capacity type, and zone.",
		},
		[]opmetrics.Label{
			NodePool,
			CapacityType,
			Zone,
		},
		opmetrics.GA,
	)
	NodeClaimsDisruptedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: NodeClaimSubsystem,
			Name:      "disrupted_total",
			Help:      "Number of nodeclaims disrupted in total by Karpenter. Labeled by reason the nodeclaim was disrupted, the owning nodepool, the capacity type, the consolidation policy, and the termination mode.",
		},
		[]opmetrics.Label{
			NodeClaimDisruptedReason,
			NodePool,
			CapacityType,
			ConsolidationPolicy,
			TerminationMode,
		},
		opmetrics.Alpha,
	)
	PodsDisruptionInitiatedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: PodSubsystem,
			Name:      "disruption_initiated_total",
			Help:      "Number of pod disruptions initiated in total by Karpenter, incremented by the reschedulable pod count whenever the underlying nodeclaim is disrupted. Labeled by reason the nodeclaim was disrupted, the owning nodepool, the capacity type, the consolidation policy, and the termination mode. Pods owned by DaemonSets and mirror pods are excluded.",
		},
		[]opmetrics.Label{
			NodeClaimDisruptedReason,
			NodePool,
			CapacityType,
			ConsolidationPolicy,
			TerminationMode,
		},
		opmetrics.Alpha,
	)
	NodesCreatedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: NodeSubsystem,
			Name:      "created_total",
			Help:      "Number of nodes created in total by Karpenter. Labeled by owning nodepool and zone.",
		},
		[]opmetrics.Label{
			NodePool,
			Zone,
		},
		opmetrics.GA,
	)
	NodesTerminatedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: NodeSubsystem,
			Name:      "terminated_total",
			Help:      "Number of nodes terminated in total by Karpenter. Labeled by owning nodepool and zone.",
		},
		[]opmetrics.Label{
			NodePool,
			Zone,
		},
		opmetrics.GA,
	)
)

// NodeClaimsTerminatedTotal, NodesCreatedTotal, and NodesTerminatedTotal are sparse along zone, whose values aren't
// known ahead of time, so they aren't seeded.
func init() {
	RegisterNodePoolMetric(NodeClaimsCreatedTotal, NodeClaimCreatedReason, MinValuesRelaxed)
	// The disruption controller registers the voluntary-disruption series of these counters. The involuntary paths
	// don't report a consolidation policy.
	involuntaryReason := opmetrics.Label{Name: ReasonLabel, Values: involuntaryDisruptionReasonValues}
	for _, m := range []opmetrics.CounterMetric{NodeClaimsDisruptedTotal, PodsDisruptionInitiatedTotal} {
		RegisterNodePoolSeries(m, func(nodePool *v1.NodePool) []map[string]string {
			return Combinations(map[string]string{
				NodePoolLabel:            nodePool.Name,
				ConsolidationPolicyLabel: "",
				TerminationModeLabel:     TerminationModeValue(nodePool.Spec.Template.Spec.TerminationGracePeriod),
			}, involuntaryReason, CapacityType)
		})
	}
}
