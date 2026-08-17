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

package disruption_test

// Node-repair behavior simulation / regression matrix.
//
// This is the "trunk": a black-box cross-product of end-user scenarios, scored on three observable metrics. The
// implementation under test is a swappable OPTION (see the seam below) — the same cases run against the 20% breaker,
// budget+restraint (replacement pinned to failure domain), and budget+restraint (unpinned). Running the matrix across
// options and reading the numbers is the "simulation"; the winning option keeps these as its regression suite.
//
// The scenario space is a cross product of five axes (repair_simulation_axes.go). The fault MODEL — how a pre-spun
// replacement behaves — is what makes a cell a false-positive vs a true fault, expressed only in observable terms
// (the replacement comes up healthy / gets re-flagged / never launches), never by reaching into the implementation.
//
// Metrics per (cell, option):
//   - Time to mitigation: sim-clock from fault onset until the genuine fault is resolved (Inf if never).
//   - Disruption incurred: summed Karpenter DisruptionCost of terminated nodes (pod-eviction-cost weighted).
//   - Cloud-provider calls: launches (+ deletes) the option drove — the load proxy.

import (
	"fmt"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
)

// simResult is one (cell, option) measurement.
type simResult struct {
	cell   simCell
	option string
	m      simMetrics
}

// simMetrics are the three observable outcomes. timeToMitigation is nil when the genuine fault was never mitigated
// (or when the cell has no genuine fault to mitigate — Axis1 != helpYes).
type simMetrics struct {
	timeToMitigation *time.Duration
	disruptionCost   float64
	cloudProviderCalls int
}

// simReport accumulates every (cell, option) result across the matrix so it can be written out once at the end.
var simReport []simResult

// recordResult appends a measurement to the global report.
func recordResult(cell simCell, option string, m simMetrics) {
	simReport = append(simReport, simResult{cell: cell, option: option, m: m})
}

// formatSimReport renders the accumulated results as a markdown table, grouped by cell, one column per option per
// metric. It is emitted at the end of the suite so the whole matrix can be reviewed as actual numbers.
func formatSimReport(results []simResult) string {
	if len(results) == 0 {
		return "(no simulation results recorded)\n"
	}
	// Stable ordering: by cell id, then option.
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].cell.id() != results[j].cell.id() {
			return results[i].cell.id() < results[j].cell.id()
		}
		return results[i].option < results[j].option
	})
	var b strings.Builder
	b.WriteString("| cell | option | time-to-mitigation | disruption-cost | cp-calls |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, r := range results {
		ttm := "n/a"
		if r.m.timeToMitigation != nil {
			ttm = r.m.timeToMitigation.String()
		} else if r.cell.help == helpYes {
			ttm = "NEVER" // a genuine fault that should have been mitigated but wasn't
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s | %.1f | %d |\n",
			r.cell.id(), r.option, ttm, r.m.disruptionCost, r.m.cloudProviderCalls))
	}
	return b.String()
}

// ReportAfterSuite writes the full matrix of numbers once every cell x option has run.
var _ = ReportAfterSuite("node-repair simulation matrix", func(_ Report) {
	if len(simReport) == 0 {
		return
	}
	GinkgoWriter.Printf("\n===== NODE REPAIR SIMULATION MATRIX (%d results) =====\n%s\n", len(simReport), formatSimReport(simReport))
})
