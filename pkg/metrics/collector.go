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
)

// CollectedGauge is a gauge whose series are built at scrape time by a Collector, from what exists at that moment,
// rather than set and deleted as things change. It declares its dimensions as Labels, like operatorpkg's gauges, so
// the docs generator sees them.
type CollectedGauge struct {
	desc   *prometheus.Desc
	labels []opmetrics.Label
	stage  opmetrics.Stage
}

// NewCollectedGauge returns a gauge a Collector emits. It isn't registered itself: the Collector that emits it is.
func NewCollectedGauge(opts prometheus.GaugeOpts, labels []opmetrics.Label, stage opmetrics.Stage) *CollectedGauge {
	names := make([]string, len(labels))
	for i, l := range labels {
		names[i] = l.Name
	}
	return &CollectedGauge{
		desc:   prometheus.NewDesc(prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name), opts.Help, names, opts.ConstLabels),
		labels: labels,
		stage:  stage,
	}
}

func (g *CollectedGauge) Labels() []opmetrics.Label { return g.labels }

func (g *CollectedGauge) Stage() opmetrics.Stage { return g.stage }

// Emit writes the series of one scrape.
type Emit struct{ ch chan<- prometheus.Metric }

// Gauge emits one series of g. values are g's label values, in the order of its Labels. A series emitted twice in one
// scrape fails the scrape, so each series comes from one entry.
func (e Emit) Gauge(g *CollectedGauge, value float64, values ...string) {
	e.ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, value, values...)
}

// Collector builds its gauges at scrape time. collect reads stores and emits a series for each thing that exists now,
// so a series for something gone is never emitted again and nothing is deleted. collect is pure: it reads stores, never
// blocks and does no I/O, and it emits nothing until synced reports that the stores it reads are synced.
type Collector struct {
	gauges  []*CollectedGauge
	synced  func() bool
	collect func(Emit)
}

// NewCollector returns a Collector of gauges, which the caller registers.
func NewCollector(synced func() bool, collect func(Emit), gauges ...*CollectedGauge) *Collector {
	return &Collector{gauges: gauges, synced: synced, collect: collect}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, g := range c.gauges {
		ch <- g.desc
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	if !c.synced() {
		return
	}
	c.collect(Emit{ch: ch})
}
