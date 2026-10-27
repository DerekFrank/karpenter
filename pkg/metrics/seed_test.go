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

package metrics_test

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var (
	colorLabel = opmetrics.Label{Name: "color", Values: []opmetrics.Value{{Name: "red"}, {Name: "blue"}}}
	shapeLabel = opmetrics.Label{Name: "shape", Values: []opmetrics.Value{{Name: "circle"}, {Name: "square"}, {Name: "triangle"}}}

	seedCounter = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{Name: "test_seed_counter"},
		[]opmetrics.Label{colorLabel}, opmetrics.Alpha)
	seedHistogram = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{Name: "test_seed_histogram"},
		[]opmetrics.Label{colorLabel}, opmetrics.Alpha)
	seedNodePoolCounter = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{Name: "test_seed_nodepool_counter"},
		[]opmetrics.Label{metrics.NodePool, colorLabel}, opmetrics.Alpha)
	seedNodePoolPinnedCounter = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{Name: "test_seed_nodepool_pinned_counter"},
		[]opmetrics.Label{metrics.NodePool, metrics.TerminationMode}, opmetrics.Alpha)
)

func init() {
	metrics.RegisterNodePoolMetric(seedNodePoolCounter, colorLabel)
	metrics.RegisterNodePoolSeries(seedNodePoolPinnedCounter, func(nodePool *v1.NodePool) []map[string]string {
		return []map[string]string{{
			metrics.NodePoolLabel:        nodePool.Name,
			metrics.TerminationModeLabel: metrics.TerminationModeValue(nodePool.Spec.Template.Spec.TerminationGracePeriod),
		}}
	})
}

var _ = Describe("Seed", func() {
	BeforeEach(func() {
		seedCounter.Reset()
		seedHistogram.Reset()
		seedNodePoolCounter.Reset()
		seedNodePoolPinnedCounter.Reset()
	})
	Context("Combinations", func() {
		It("should return every combination of the labels' values merged over the base", func() {
			Expect(metrics.Combinations(map[string]string{"base": "b"}, colorLabel, shapeLabel)).To(ConsistOf(
				lo.FlatMap([]string{"red", "blue"}, func(color string, _ int) []map[string]string {
					return lo.Map([]string{"circle", "square", "triangle"}, func(shape string, _ int) map[string]string {
						return map[string]string{"base": "b", "color": color, "shape": shape}
					})
				}),
			))
		})
		It("should return the base when there are no labels", func() {
			Expect(metrics.Combinations(map[string]string{"base": "b"})).To(ConsistOf(map[string]string{"base": "b"}))
		})
		It("should not modify the base", func() {
			base := map[string]string{"base": "b"}
			metrics.Combinations(base, colorLabel)
			Expect(base).To(Equal(map[string]string{"base": "b"}))
		})
		It("should panic for a label without values", func() {
			Expect(func() { metrics.Combinations(nil, opmetrics.Label{Name: "unenumerated"}) }).To(Panic())
		})
	})
	It("should create zero-valued counter series", func() {
		metrics.Seed(seedCounter, metrics.Combinations(nil, colorLabel)...)
		for _, color := range []string{"red", "blue"} {
			m, ok := FindMetricWithLabelValues("test_seed_counter", map[string]string{"color": color})
			Expect(ok).To(BeTrue())
			Expect(m.GetCounter().GetValue()).To(BeZero())
		}
	})
	It("should create histogram series without recording an observation", func() {
		metrics.Seed(seedHistogram, map[string]string{"color": "red"})
		m, ok := FindMetricWithLabelValues("test_seed_histogram", map[string]string{"color": "red"})
		Expect(ok).To(BeTrue())
		Expect(m.GetHistogram().GetSampleCount()).To(BeZero())
	})
	It("should not reset an existing series", func() {
		seedCounter.Inc(map[string]string{"color": "red"})
		metrics.Seed(seedCounter, map[string]string{"color": "red"})
		m, ok := FindMetricWithLabelValues("test_seed_counter", map[string]string{"color": "red"})
		Expect(ok).To(BeTrue())
		Expect(m.GetCounter().GetValue()).To(BeNumerically("==", 1))
	})
	It("should panic for a metric it can't seed", func() {
		Expect(func() { metrics.Seed(opmetrics.NewMultiCounter(seedCounter), map[string]string{"color": "red"}) }).To(Panic())
	})
	Context("NodePool", func() {
		var nodePool *v1.NodePool
		BeforeEach(func() {
			nodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
		})
		It("should seed the series registered for the NodePool", func() {
			metrics.SeedNodePool(nodePool)
			for _, color := range []string{"red", "blue"} {
				m, ok := FindMetricWithLabelValues("test_seed_nodepool_counter", map[string]string{metrics.NodePoolLabel: "default", "color": color})
				Expect(ok).To(BeTrue())
				Expect(m.GetCounter().GetValue()).To(BeZero())
			}
		})
		It("should pin labels determined by the NodePool to its value", func() {
			nodePool.Spec.Template.Spec.TerminationGracePeriod = &metav1.Duration{}
			metrics.SeedNodePool(nodePool)
			_, ok := FindMetricWithLabelValues("test_seed_nodepool_pinned_counter", map[string]string{metrics.NodePoolLabel: "default", metrics.TerminationModeLabel: metrics.TerminationModeForceful})
			Expect(ok).To(BeTrue())
			_, ok = FindMetricWithLabelValues("test_seed_nodepool_pinned_counter", map[string]string{metrics.NodePoolLabel: "default", metrics.TerminationModeLabel: metrics.TerminationModeGraceful})
			Expect(ok).To(BeFalse())
		})
		It("should delete every series labeled with the NodePool, including ones that recorded events", func() {
			other := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "other"}}
			metrics.SeedNodePool(nodePool)
			metrics.SeedNodePool(other)
			seedNodePoolCounter.Inc(map[string]string{metrics.NodePoolLabel: "default", "color": "red"})

			metrics.DeleteNodePool("default")
			_, ok := FindMetricWithLabelValues("test_seed_nodepool_counter", map[string]string{metrics.NodePoolLabel: "default"})
			Expect(ok).To(BeFalse())
			_, ok = FindMetricWithLabelValues("test_seed_nodepool_pinned_counter", map[string]string{metrics.NodePoolLabel: "default"})
			Expect(ok).To(BeFalse())
			_, ok = FindMetricWithLabelValues("test_seed_nodepool_counter", map[string]string{metrics.NodePoolLabel: "other"})
			Expect(ok).To(BeTrue())
		})
	})
})
