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
	"fmt"
	"sync"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// Metrics are non-sparse: every series a metric can emit exists, at zero, from the moment its label values are known,
// so that queries like increase() and rate() return 0 rather than nothing before the first event. See
// https://prometheus.io/docs/practices/instrumentation/#avoid-missing-metrics and AGENTS.md for the conventions.

// Seed creates a zero-valued series of metric for each label set without recording a sample. Seeding an existing
// series leaves it unchanged.
func Seed(metric any, labelSets ...map[string]string) {
	for _, labels := range labelSets {
		switch m := metric.(type) {
		case *opmetrics.PrometheusCounter:
			m.With(labels)
		case *opmetrics.PrometheusGauge:
			m.With(labels)
		case *opmetrics.PrometheusHistogram:
			m.With(labels)
		case *opmetrics.PrometheusSummary:
			m.With(labels)
		default:
			panic(fmt.Sprintf("cannot seed metric of type %T", metric))
		}
	}
}

// Combinations returns base merged with every combination of the enumerated Values of labels. Use it only when the
// dimensions vary independently; when one dimension's value determines another's, list the label sets explicitly.
func Combinations(base map[string]string, labels ...opmetrics.Label) []map[string]string {
	sets := []map[string]string{lo.Assign(base)}
	for _, label := range labels {
		if len(label.Values) == 0 {
			panic(fmt.Sprintf("cannot enumerate label %q, it has no Values", label.Name))
		}
		sets = lo.FlatMap(sets, func(set map[string]string, _ int) []map[string]string {
			return lo.Map(label.Values, func(value opmetrics.Value, _ int) map[string]string {
				return lo.Assign(set, map[string]string{label.Name: value.Name})
			})
		})
	}
	return sets
}

type deletableMetric interface {
	DeletePartialMatch(labels map[string]string)
}

type nodePoolSeries struct {
	metric deletableMetric
	series func(*v1.NodePool) []map[string]string
}

var (
	nodePoolSeriesMu         sync.RWMutex
	registeredNodePoolSeries []nodePoolSeries
)

// RegisterNodePoolSeries registers the series metric emits for a NodePool. They are seeded while the NodePool is
// managed, and every series of metric labeled with the NodePool, seeded or not, is deleted once it is deleted. Each
// label set must carry the NodePool's name under NodePoolLabel. Call it from init().
func RegisterNodePoolSeries(metric deletableMetric, series func(*v1.NodePool) []map[string]string) {
	nodePoolSeriesMu.Lock()
	defer nodePoolSeriesMu.Unlock()
	registeredNodePoolSeries = append(registeredNodePoolSeries, nodePoolSeries{metric: metric, series: series})
}

// RegisterNodePoolMetric registers a metric whose series for a NodePool are every combination of the enumerated
// Values of labels. See RegisterNodePoolSeries.
func RegisterNodePoolMetric(metric deletableMetric, labels ...opmetrics.Label) {
	RegisterNodePoolSeries(metric, func(nodePool *v1.NodePool) []map[string]string {
		return Combinations(map[string]string{NodePoolLabel: nodePool.Name}, labels...)
	})
}

// SeedNodePool seeds every series registered for the NodePool.
func SeedNodePool(nodePool *v1.NodePool) {
	nodePoolSeriesMu.RLock()
	defer nodePoolSeriesMu.RUnlock()
	for _, s := range registeredNodePoolSeries {
		labelSets := s.series(nodePool)
		for _, labels := range labelSets {
			if labels[NodePoolLabel] != nodePool.Name {
				panic(fmt.Sprintf("series %v registered for NodePool %q is not labeled with it", labels, nodePool.Name))
			}
		}
		Seed(s.metric, labelSets...)
	}
}

// DeleteNodePool deletes every series labeled with the NodePool from each registered metric.
func DeleteNodePool(name string) {
	nodePoolSeriesMu.RLock()
	defer nodePoolSeriesMu.RUnlock()
	for _, s := range registeredNodePoolSeries {
		s.metric.DeletePartialMatch(map[string]string{NodePoolLabel: name})
	}
}

// ConsolidationPolicyValue is the consolidation_policy value emitted for a NodePool's consolidation policy.
func ConsolidationPolicyValue(nodePool *v1.NodePool) string {
	return pretty.ToSnakeCase(string(nodePool.Spec.Disruption.ConsolidationPolicy))
}

// TerminationModeValue is the termination_mode value emitted for a terminationGracePeriod.
func TerminationModeValue(terminationGracePeriod *metav1.Duration) string {
	if terminationGracePeriod == nil {
		return TerminationModeGraceful
	}
	if terminationGracePeriod.Duration <= 0 {
		return TerminationModeForceful
	}
	return TerminationModeEventual
}
