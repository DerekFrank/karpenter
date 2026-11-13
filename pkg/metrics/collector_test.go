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
	dto "github.com/prometheus/client_model/go"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

var _ = Describe("Collector", func() {
	var registry *prometheus.Registry
	var gauge *metrics.CollectedGauge
	var synced bool
	var things map[string]float64

	BeforeEach(func() {
		registry = prometheus.NewPedanticRegistry()
		gauge = metrics.NewCollectedGauge(prometheus.GaugeOpts{Namespace: "test", Name: "collected"}, []opmetrics.Label{{Name: "thing"}}, opmetrics.Alpha)
		synced, things = true, map[string]float64{}
		Expect(registry.Register(metrics.NewCollector(func() bool { return synced }, func(emit metrics.Emit) {
			for name, v := range things {
				emit.Gauge(gauge, v, name)
			}
		}, gauge))).To(Succeed())
	})
	scrape := func() map[string]float64 {
		GinkgoHelper()
		families, err := registry.Gather()
		Expect(err).ToNot(HaveOccurred())
		series := map[string]float64{}
		for _, f := range families {
			for _, m := range f.Metric {
				series[labelValue(m, "thing")] = m.GetGauge().GetValue()
			}
		}
		return series
	}
	It("should emit what exists at each scrape, and nothing for what's gone", func() {
		things["a"], things["b"] = 1, 2
		Expect(scrape()).To(Equal(map[string]float64{"a": 1, "b": 2}))
		delete(things, "a")
		things["b"] = 3
		Expect(scrape()).To(Equal(map[string]float64{"b": 3}))
	})
	It("should emit nothing until the stores it reads are synced", func() {
		things["a"] = 1
		synced = false
		Expect(scrape()).To(BeEmpty())
		synced = true
		Expect(scrape()).To(Equal(map[string]float64{"a": 1}))
	})
	It("should write each series' labels in sorted order whatever the order of the gauge's Labels, and copy its values", func() {
		registry := prometheus.NewPedanticRegistry()
		unsorted := metrics.NewCollectedGauge(prometheus.GaugeOpts{Namespace: "test", Name: "unsorted"}, []opmetrics.Label{{Name: "zeta"}, {Name: "alpha"}, {Name: "mid"}}, opmetrics.Alpha)
		Expect(registry.Register(metrics.NewCollector(func() bool { return true }, func(emit metrics.Emit) {
			values := []string{"z1", "a1", "m1"}
			emit.Gauge(unsorted, 1, values...)
			values[0], values[1], values[2] = "z2", "a2", "m2"
			emit.Gauge(unsorted, 2, values...)
		}, unsorted))).To(Succeed())
		families, err := registry.Gather()
		Expect(err).ToNot(HaveOccurred())
		Expect(families).To(HaveLen(1))
		got := map[float64][]string{}
		for _, m := range families[0].Metric {
			for _, l := range m.Label {
				got[m.GetGauge().GetValue()] = append(got[m.GetGauge().GetValue()], l.GetName()+"="+l.GetValue())
			}
		}
		Expect(got).To(Equal(map[float64][]string{
			1: {"alpha=a1", "mid=m1", "zeta=z1"},
			2: {"alpha=a2", "mid=m2", "zeta=z2"},
		}))
	})
	It("should keep the gauge's labels and stage for the docs", func() {
		Expect(gauge.Labels()).To(Equal([]opmetrics.Label{{Name: "thing"}}))
		Expect(gauge.Stage()).To(Equal(opmetrics.Alpha))
	})
})

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.Label {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}
