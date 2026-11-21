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
	"sort"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// CollectedGauge is a gauge whose series are built at scrape time by a Collector, from what exists at that moment,
// rather than set and deleted as things change. It declares its dimensions as Labels, like operatorpkg's gauges, so
// the docs generator sees them.
type CollectedGauge struct {
	desc   *prometheus.Desc
	labels []opmetrics.Label
	stage  opmetrics.Stage
	// names are the label names, and order the indexes of the label values in the names' sorted order, which a series'
	// label pairs are written in, so a series sorts nothing.
	names []*string
	order []int
}

// NewCollectedGauge returns a gauge a Collector emits. It isn't registered itself: the Collector that emits it is.
// Const labels aren't supported: a Label with one value is a dimension like any other.
func NewCollectedGauge(opts prometheus.GaugeOpts, labels []opmetrics.Label, stage opmetrics.Stage) *CollectedGauge {
	if len(opts.ConstLabels) > 0 {
		panic(fmt.Sprintf("collected gauge %s: const labels aren't supported", opts.Name))
	}
	names := make([]string, len(labels))
	g := &CollectedGauge{labels: labels, stage: stage, names: make([]*string, len(labels)), order: make([]int, len(labels))}
	for i, l := range labels {
		names[i] = l.Name
		g.names[i] = &names[i]
		g.order[i] = i
	}
	sort.Slice(g.order, func(i, j int) bool { return names[g.order[i]] < names[g.order[j]] })
	g.desc = prometheus.NewDesc(prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name), opts.Help, names, nil)
	return g
}

func (g *CollectedGauge) Labels() []opmetrics.Label { return g.labels }

func (g *CollectedGauge) Stage() opmetrics.Stage { return g.stage }

// Emit writes the series of one scrape.
type Emit struct {
	ch chan<- prometheus.Metric
	// last holds the label pair each label name last had in this scrape. Consecutive series of one entry share most
	// of their labels, such as a node's labels across its resources' series, so they share the pairs too.
	last map[string]*dto.LabelPair
}

// Gauge emits one series of g. values are g's label values, in the order of its Labels, and may be reused for the
// next series. A series emitted twice in one scrape fails the scrape, so each series comes from one entry.
func (e Emit) Gauge(g *CollectedGauge, value float64, values ...string) {
	if len(values) != len(g.names) {
		panic(fmt.Sprintf("%s: %d label values for %d labels", g.desc, len(values), len(g.names)))
	}
	s := &series{desc: g.desc, value: value, labels: make([]*dto.LabelPair, len(values))}
	misses := 0
	for i, v := range g.order {
		if pair, ok := e.last[*g.names[v]]; ok && *pair.Value == values[v] {
			s.labels[i] = pair
		} else {
			misses++
		}
	}
	// The labels that differ from the last series are allocated together: their pairs, and their values, which the pairs
	// point into, so a series is at most four allocations.
	if misses > 0 {
		pairs, vals := make([]dto.LabelPair, 0, misses), make([]string, 0, misses)
		for i, v := range g.order {
			if s.labels[i] != nil {
				continue
			}
			vals = append(vals, values[v])
			pairs = append(pairs, dto.LabelPair{Name: g.names[v], Value: &vals[len(vals)-1]})
			s.labels[i] = &pairs[len(pairs)-1]
			e.last[*g.names[v]] = s.labels[i]
		}
	}
	e.ch <- s
}

// series is one series of a CollectedGauge, with its label pairs in sorted order. Its pairs may be shared with the
// scrape's other series, and nothing it holds is modified once it's emitted.
type series struct {
	desc   *prometheus.Desc
	value  float64
	labels []*dto.LabelPair
}

func (s *series) Desc() *prometheus.Desc { return s.desc }

func (s *series) Write(m *dto.Metric) error {
	m.Label = s.labels
	m.Gauge = &dto.Gauge{Value: &s.value}
	return nil
}

// Collector builds its gauges at scrape time. collect reads stores and emits a series for each thing that exists now,
// so a series for something gone is never emitted again and nothing is deleted. collect is pure: it reads stores, never
// blocks and does no I/O, and it emits nothing until synced reports that the stores it reads are synced. It reads the
// stores' own objects rather than copies, since a scrape walks every entry and modifies none.
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
	c.collect(Emit{ch: ch, last: map[string]*dto.LabelPair{}})
}
