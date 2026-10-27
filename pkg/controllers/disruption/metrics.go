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

package disruption

import (
	"strings"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	voluntaryDisruptionSubsystem = "voluntary_disruption"
	decisionLabel                = "decision"
	ConsolidationTypeLabel       = "consolidation_type"
	CandidatesIneligible         = "candidates_ineligible"
	policyLabel                  = "policy"
	terminateFirstReasonLabel    = "terminate_first_reason"
)

var (
	MultiNodeConsolidationType = opmetrics.Value{
		Name: "multi",
		Help: "Consolidation that considers removing multiple nodes at once.",
	}
	SingleNodeConsolidationType = opmetrics.Value{
		Name: "single",
		Help: "Consolidation that considers removing a single node.",
	}
	EmptyConsolidationType = opmetrics.Value{
		Name: "empty",
		Help: "Consolidation that removes empty nodes.",
	}
)

var (
	ConsolidationType = opmetrics.Label{
		Name:   ConsolidationTypeLabel,
		Help:   "The consolidation algorithm that produced the decision.",
		Values: []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType, EmptyConsolidationType},
	}
	// DecisionDim is the `decision` dimension for the voluntary-disruption decision
	// counters, whose value is the command's action.
	DecisionDim = opmetrics.Label{
		Name: decisionLabel,
		Help: "The disruption decision taken for the candidate(s).",
		Values: []opmetrics.Value{
			{
				Name: string(NoOpDecision),
				Help: "No disruption action was taken.",
			},
			{
				Name: string(ReplaceDecision),
				Help: "The candidate(s) were replaced with more efficient capacity.",
			},
			{
				Name: string(DeleteDecision),
				Help: "The candidate(s) were deleted without replacement.",
			},
			{
				Name: string(TerminateFirstDecision),
				Help: "The candidate(s) were deleted without staging a replacement first; reactive provisioning refills afterward.",
			},
			{
				Name: string(RebootDecision),
				Help: "The candidate(s) were rebooted in place.",
			},
		},
	}
	// ApprovalDim is the `decision` dimension for the balanced-consolidation move
	// metrics, which score each candidate move and record whether it was approved or
	// rejected — a disjoint value set from DecisionDim, so it is a separate Label.
	ApprovalDim = opmetrics.Label{
		Name: decisionLabel,
		Help: "Whether a scored balanced-consolidation move was approved or rejected.",
		Values: []opmetrics.Value{
			{
				Name: string(ApprovedDecision),
				Help: "The move's cost savings justified the pod disruption; it was approved.",
			},
			{
				Name: string(RejectedDecision),
				Help: "The move's cost savings did not justify the pod disruption; it was rejected.",
			},
		},
	}
	Policy = opmetrics.Label{
		Name: policyLabel,
		Help: "The NodePool consolidation policy in effect for the move.",
	}
	TerminateFirstReasonDim = opmetrics.Label{
		Name: terminateFirstReasonLabel,
		Help: "Why the terminate-first command couldn't stage its replacement before terminating the node.",
		Values: []opmetrics.Value{
			{
				Name: string(TerminateFirstNoReservedCapacity),
				Help: "The replacement has no reserved capacity to launch into except the slot the node holds in a full capacity reservation.",
			},
			{
				Name: string(TerminateFirstStaticAtLimit),
				Help: "The node's static NodePool is at its node limit, so the replacement can't launch alongside it.",
			},
		},
	}
)

// seededMethods are the disruption methods whose series are seeded. Each method fixes the reason and
// consolidation_type it reports, so its series are listed per method rather than as a product of the two dimensions.
var seededMethods = []Method{&Repair{}, &Emptiness{}, &StaticDrift{}, &Drift{}, &MultiNodeConsolidation{}, &SingleNodeConsolidation{}}

// seededDecisions are the decisions a recorded command can carry; no-op commands are never recorded.
var seededDecisions = lo.Reject(DecisionDim.Values, func(v opmetrics.Value, _ int) bool { return v.Name == string(NoOpDecision) })

// methodSeries returns, for each seeded method, base merged with the method's reason and consolidation type.
func methodSeries(base map[string]string) []map[string]string {
	return lo.Map(seededMethods, func(m Method, _ int) map[string]string {
		return lo.Assign(base, map[string]string{
			metrics.ReasonLabel:    strings.ToLower(string(m.Reason())),
			ConsolidationTypeLabel: m.ConsolidationType(),
		})
	})
}

// methodDecisionSeries returns methodSeries(base) crossed with every seeded decision.
func methodDecisionSeries(base map[string]string) []map[string]string {
	return lo.FlatMap(methodSeries(base), func(labels map[string]string, _ int) []map[string]string {
		return metrics.Combinations(labels, opmetrics.Label{Name: decisionLabel, Values: seededDecisions})
	})
}

// NodeClaimsUnhealthyDisruptedTotal is sparse along image_id and condition, whose values aren't known ahead of time,
// so it isn't seeded.
func init() {
	metrics.Seed(EvaluationDurationSeconds, methodSeries(nil)...)
	metrics.Seed(DecisionsPerformedTotal, methodDecisionSeries(nil)...)
	metrics.Seed(DisruptionQueueFailuresTotal, methodDecisionSeries(nil)...)
	metrics.Seed(FailedValidationsTotal, metrics.Combinations(nil, ConsolidationType)...)
	// Only the multi- and single-node algorithms run a bounded search that can hit a timeout.
	for _, ct := range []opmetrics.Value{MultiNodeConsolidationType, SingleNodeConsolidationType} {
		metrics.Seed(ConsolidationTimeoutsTotal, map[string]string{ConsolidationTypeLabel: ct.Name})
	}

	metrics.RegisterNodePoolSeries(NodepoolDecisionsPerformed, func(nodePool *v1.NodePool) []map[string]string {
		return methodDecisionSeries(map[string]string{metrics.NodePoolLabel: nodePool.Name})
	})
	metrics.RegisterNodePoolSeries(TerminateFirstDecisionsTotal, func(nodePool *v1.NodePool) []map[string]string {
		reasons := lo.Uniq(lo.Map(seededMethods, func(m Method, _ int) string { return strings.ToLower(string(m.Reason())) }))
		return lo.FlatMap(reasons, func(reason string, _ int) []map[string]string {
			return metrics.Combinations(map[string]string{metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: reason}, TerminateFirstReasonDim)
		})
	})
	// Balanced consolidation counts moves under every policy but only scores them under the Balanced policy.
	metrics.RegisterNodePoolSeries(ConsolidationMovesTotal, func(nodePool *v1.NodePool) []map[string]string {
		return metrics.Combinations(map[string]string{
			metrics.NodePoolLabel: nodePool.Name,
			policyLabel:           string(nodePool.Spec.Disruption.ConsolidationPolicy),
		}, ApprovalDim)
	})
	metrics.RegisterNodePoolSeries(ConsolidationScoreHistogram, func(nodePool *v1.NodePool) []map[string]string {
		if nodePool.Spec.Disruption.ConsolidationPolicy != v1.ConsolidationPolicyBalanced {
			return nil
		}
		return metrics.Combinations(map[string]string{
			metrics.NodePoolLabel: nodePool.Name,
			policyLabel:           string(nodePool.Spec.Disruption.ConsolidationPolicy),
		}, ApprovalDim)
	})
	// The voluntary-disruption series of the NodeClaim/Pod disruption counters; pkg/metrics registers the
	// involuntary ones. Only consolidation reports a consolidation policy, see recordCandidateDisrupted.
	for _, m := range []opmetrics.CounterMetric{metrics.NodeClaimsDisruptedTotal, metrics.PodsDisruptionInitiatedTotal} {
		metrics.RegisterNodePoolSeries(m, func(nodePool *v1.NodePool) []map[string]string {
			return lo.FlatMap(seededMethods, func(method Method, _ int) []map[string]string {
				return metrics.Combinations(map[string]string{
					metrics.NodePoolLabel:            nodePool.Name,
					metrics.ReasonLabel:              strings.ToLower(string(method.Reason())),
					metrics.ConsolidationPolicyLabel: lo.Ternary(method.ConsolidationType() != "", metrics.ConsolidationPolicyValue(nodePool), ""),
					metrics.TerminationModeLabel:     metrics.TerminationModeValue(nodePool.Spec.Template.Spec.TerminationGracePeriod),
				}, metrics.CapacityType)
			})
		})
	}
}

var (
	EvaluationDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decision_evaluation_duration_seconds",
			Help:      "Duration of the disruption decision evaluation process in seconds. Labeled by method and consolidation type.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{metrics.DisruptionReason, ConsolidationType},
		opmetrics.Beta,
	)
	DecisionsPerformedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_total",
			Help:      "Number of disruption decisions performed. Labeled by disruption decision, reason, and consolidation type.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.GA,
	)
	NodepoolDecisionsPerformed = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "decisions_by_nodepool_total",
			Help:      "Number of disruption decisions performed by nodepool. Labeled by nodepool name, disruption decision, reason, and consolidation type.",
		},
		[]opmetrics.Label{metrics.NodePool, DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Alpha,
	)
	TerminateFirstDecisionsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "terminate_first_decisions_total",
			Help:      "Number of terminate-first disruption decisions performed. Labeled by nodepool name, reason, and why the replacement couldn't be staged first.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason, TerminateFirstReasonDim},
		opmetrics.Alpha,
	)
	EligibleNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "eligible_nodes",
			Help:      "Number of nodes eligible for disruption by Karpenter. Labeled by disruption reason.",
		},
		[]opmetrics.Label{metrics.DisruptionReason},
		opmetrics.Beta,
	)
	ConsolidationTimeoutsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "consolidation_timeouts_total",
			Help:      "Number of times the Consolidation algorithm has reached a timeout. Labeled by consolidation type.",
		},
		[]opmetrics.Label{ConsolidationType},
		opmetrics.Beta,
	)
	FailedValidationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "failed_validations_total",
			Help:      "Number of candidates that were selected for disruption but failed validation. Labeled by consolidation type.",
		},
		[]opmetrics.Label{ConsolidationType},
		opmetrics.Alpha,
	)
	NodePoolAllowedDisruptions = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "allowed_disruptions",
			Help:      "The number of nodes for a given NodePool that can be concurrently disrupting at a point in time. Labeled by NodePool. Note that allowed disruptions can change very rapidly, as new nodes may be created and others may be deleted at any point.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason},
		opmetrics.GA,
	)
	NodePoolNodesConsumingBudgets = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "nodes_consuming_budgets",
			Help:      "The number of nodes consuming the budget of a nodepool at a point in time. Labeled by NodePool.",
		},
		[]opmetrics.Label{metrics.NodePool, metrics.DisruptionReason},
		opmetrics.Alpha,
	)
	DisruptionQueueFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "queue_failures_total",
			Help:      "The number of times that an enqueued disruption decision failed. Labeled by disruption method.",
		},
		[]opmetrics.Label{DecisionDim, metrics.DisruptionReason, ConsolidationType},
		opmetrics.Beta,
	)
	ConsolidationScoreHistogram = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_score",
			Help:      "Score of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
			Buckets:   []float64{0.1, 0.25, 0.33, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
		[]opmetrics.Label{ApprovalDim, metrics.NodePool, Policy},
		opmetrics.Alpha,
	)
	ConsolidationMovesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Name:      "consolidation_moves_total",
			Help:      "Number of balanced consolidation moves. Labeled by decision, NodePool, and policy.",
		},
		[]opmetrics.Label{ApprovalDim, metrics.NodePool, Policy},
		opmetrics.Alpha,
	)
	// NodeClaimsUnhealthyDisruptedTotal preserves the per-condition/per-image breakdown the retired node.health
	// controller emitted, which the reason-labeled karpenter_nodeclaims_disrupted_total loses. Labeled by the repair
	// condition, the owning NodePool, the capacity type, and the image ID.
	NodeClaimsUnhealthyDisruptedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodeClaimSubsystem,
			Name:      "unhealthy_disrupted_total",
			Help:      "Number of unhealthy nodeclaims disrupted in total by node repair. Labeled by the condition the node was disrupted on, the owning nodepool, the capacity type, the image ID, and the termination mode.",
		},
		[]opmetrics.Label{RepairCondition, metrics.NodePool, metrics.CapacityType, ImageID, metrics.TerminationMode},
		opmetrics.Alpha,
	)
)

const (
	conditionLabel = "condition"
	imageIDLabel   = "image_id"
)

var (
	RepairCondition = opmetrics.Label{
		Name: conditionLabel,
		Help: "The node status condition type that triggered node repair disruption.",
	}
	ImageID = opmetrics.Label{
		Name: imageIDLabel,
		Help: "The image ID of the node that was disrupted.",
	}
)
