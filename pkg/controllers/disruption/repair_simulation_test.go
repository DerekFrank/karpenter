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

// Node-repair behavior simulation / regression matrix — metrics + reporting.
//
// This is the "trunk": a black-box cross product of end-user scenarios, scored on OBSERVABLE metrics, run across
// swappable implementation OPTIONS (the same cells run against the 20% breaker at this base, and against
// budget+restraint pinned/unpinned from the POC layer stacked on top). Running the matrix across options and reading
// the numbers is the "simulation"; the winning option keeps these cells as its regression suite.
//
// Metrics are deliberately observable (not Karpenter-internal): a repair implementation must not be graded on its own
// objective function.
//   - timeToMitigation: sim-clock from fault onset until the genuine fault is truly resolved (a healthy replacement is
//     carrying the workload). nil when there is no genuine fault to resolve (help != helpYes) or it was never resolved
//     — disambiguated by `outcome`, never by an overloaded nil.
//   - disruptedPods: pods actually evicted/deleted during the run (the workload-facing disruption cost, observable —
//     NOT Karpenter's internal DisruptionCost heuristic).
//   - cpWrites: cloud-provider write ATTEMPTS (Create+Delete), counting failed launches too, so an ICE/capacity
//     retry-storm shows up as call inflation rather than being hidden by a success-only count.

import (
	"fmt"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
)

// simOutcome names what actually happened, so a never-terminated (wedged) run is never confused with a correct
// decision not to repair (declined) or with a genuine miss (unmitigated).
type simOutcome string

const (
	outcomeMitigated   simOutcome = "mitigated"    // genuine fault resolved: healthy replacement carrying the workload
	outcomeDeclined    simOutcome = "declined"     // repair took no disrupting action (correct when repair can't help)
	outcomeChurned     simOutcome = "churned"      // repair disrupted node(s) but the fault was NOT resolved (waste)
	outcomeWedged      simOutcome = "wedged"       // repair tried but could not complete (e.g. PDB-blocked, never drained)
	outcomeUnmitigated simOutcome = "unmitigated"  // genuine fault, repair acted, but replacement never came up healthy
)

// simMetrics are the observable outcomes of running one cell under one option.
type simMetrics struct {
	timeToMitigation *time.Duration
	disruptedPods    int
	cpWrites         int
	outcome          simOutcome
}

// simResult is one (cell, option) measurement.
type simResult struct {
	cell   simCell
	option string
	m      simMetrics
}

// simReport accumulates every (cell, option) result. NOTE: for the real matrix run this is fed from DescribeTable
// entries; because the suite can run in parallel processes, aggregate reporting is done per-process via GinkgoWriter
// and this global is only authoritative within a single process. (Cross-process aggregation, if needed, reads the
// per-process dumps.)
var simReport []simResult

func recordResult(cell simCell, option string, m simMetrics) {
	simReport = append(simReport, simResult{cell: cell, option: option, m: m})
}

func ttmString(r simResult) string {
	if r.m.timeToMitigation != nil {
		return r.m.timeToMitigation.String()
	}
	return "-"
}

// formatSimReport renders every measurement as a markdown table (one row per cell per option).
func formatSimReport(results []simResult) string {
	if len(results) == 0 {
		return "(no simulation results recorded)\n"
	}
	sorted := append([]simResult(nil), results...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].option != sorted[j].option {
			return sorted[i].option < sorted[j].option
		}
		return sorted[i].cell.id() < sorted[j].cell.id()
	})
	var b strings.Builder
	b.WriteString("| option | cell | outcome | time-to-mitigation | disrupted-pods | cp-writes |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, r := range sorted {
		b.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %d | %d |\n",
			r.option, r.cell.id(), r.m.outcome, ttmString(r), r.m.disruptedPods, r.m.cpWrites))
	}
	return b.String()
}

// formatCollapseReport reveals degeneracy EMPIRICALLY: within each option, cells whose full metric tuple is identical
// are grouped. Each group of size > 1 is a set of cells that this implementation could not distinguish — the honest,
// measured version of what a static skip() predicate would only have claimed. (A group that is a singleton for one
// option but collapsed for another is itself a finding.)
func formatCollapseReport(results []simResult) string {
	byOption := map[string]map[string][]string{} // option -> metric-signature -> []cellID
	for _, r := range results {
		sig := fmt.Sprintf("%s|ttm=%s|pods=%d|cpw=%d", r.m.outcome, ttmString(r), r.m.disruptedPods, r.m.cpWrites)
		if byOption[r.option] == nil {
			byOption[r.option] = map[string][]string{}
		}
		byOption[r.option][sig] = append(byOption[r.option][sig], r.cell.id())
	}
	options := make([]string, 0, len(byOption))
	for o := range byOption {
		options = append(options, o)
	}
	sort.Strings(options)

	var b strings.Builder
	b.WriteString("Empirically collapsed cell groups (identical metrics ⇒ this option can't tell them apart):\n")
	for _, o := range options {
		var groups [][]string
		for _, cells := range byOption[o] {
			if len(cells) > 1 {
				sort.Strings(cells)
				groups = append(groups, cells)
			}
		}
		sort.SliceStable(groups, func(i, j int) bool { return len(groups[i]) > len(groups[j]) })
		b.WriteString(fmt.Sprintf("  [%s] %d collapsed group(s):\n", o, len(groups)))
		for _, g := range groups {
			b.WriteString(fmt.Sprintf("    - %d cells: %s\n", len(g), strings.Join(g, ", ")))
		}
	}
	return b.String()
}

// ReportAfterEach dumps this process's accumulated matrix + collapse analysis once the suite finishes. Per-process
// (see simReport note) so it is parallel-safe.
var _ = ReportAfterSuite("node-repair simulation matrix", func(_ Report) {
	if len(simReport) == 0 {
		return
	}
	GinkgoWriter.Printf("\n===== NODE REPAIR SIMULATION MATRIX (%d results) =====\n%s\n%s\n",
		len(simReport), formatSimReport(simReport), formatCollapseReport(simReport))
})
