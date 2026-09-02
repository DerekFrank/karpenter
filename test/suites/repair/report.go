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

package repair

// Node-repair performance matrix — observable metrics + reporting, ported from the envtest sim
// (repair_simulation_test.go on branch node-repair-simulation-rig).
//
// This is the "trunk": a black-box cross product of end-user scenarios, scored on OBSERVABLE metrics, run against a
// swappable implementation OPTION (the deployed Karpenter build — breaker / restraint-unpinned / restraint-pinned).
// Running the matrix across options and reading the numbers is the "simulation"; the winning option keeps these cells
// as its regression suite.
//
// Metrics are deliberately observable (not Karpenter-internal): a repair implementation must not be graded on its own
// objective function. They are RAW observables only — there is no interpreted verdict field. The old
// mitigated/declined/churned/wedged/unmitigated taxonomy was derivable from these four numbers crossed with the
// impairment/oracle, so interpretation is left to analysis and every cell is measured the same way.
//   - timeToRecovery: wall-clock from fault onset until the workload is healthy again on good capacity. nil when the
//     fault was never fully resolved by the deadline (a partial recovery reads as mitigatedFraction < 1 with a nil TTR).
//   - disruptedPods: workload pods actually deleted/recreated during the run (the workload-facing disruption cost,
//     observable — NOT Karpenter's internal DisruptionCost heuristic).
//   - cpCalls: cloud-provider call count (Create+Delete) scraped from karpenter_cloudprovider_* metrics, counting
//     failed launches too, so an ICE/capacity retry-storm shows up as call inflation rather than being hidden.

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck
)

// repairMetrics are the observable results of running one cell under one option. RAW observables only — no interpreted
// verdict field (that taxonomy is derivable from these numbers + the impairment and is reconstructed in analysis).
type repairMetrics struct {
	timeToRecovery    *time.Duration
	disruptedPods     int
	gracefulPods      int // pods gracefully disrupted (drained via eviction, PDB+grace honored)
	forcedPods        int // pods disrupted WITHOUT drain (force-deleted past the drain bound, PDB+grace bypassed)
	cpCalls           int
	mitigatedFraction float64       // fraction of the faulted nodes repair resolved by the deadline (0..1); 1.0 == fully mitigated
	wallTime          time.Duration // total wall-clock the cell took (setup+measure+teardown) — for "how fast does each case run"
}

// repairResult is one (cell, option) measurement.
type repairResult struct {
	cell   cell
	option string
	m      repairMetrics
}

// repairReport accumulates every (cell, option) result. For a parallel Ginkgo run this global is only authoritative
// within a single process; the ReportAfterSuite dump is therefore per-process (cross-process aggregation, if needed,
// reads the per-process dumps).
var repairReport []repairResult

func recordResult(c cell, option string, m repairMetrics) {
	repairReport = append(repairReport, repairResult{cell: c, option: option, m: m})
}

func ttrString(r repairResult) string {
	if r.m.timeToRecovery != nil {
		return r.m.timeToRecovery.Round(time.Second).String()
	}
	return "-"
}

// formatReport renders every measurement as a markdown table (one row per cell per option).
func formatReport(results []repairResult) string {
	if len(results) == 0 {
		return "(no repair results recorded)\n"
	}
	sorted := append([]repairResult(nil), results...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].option != sorted[j].option {
			return sorted[i].option < sorted[j].option
		}
		return sorted[i].cell.id() < sorted[j].cell.id()
	})
	var b strings.Builder
	b.WriteString("| option | cell | mitigated% | time-to-recovery | disrupted-pods | graceful-pods | forced-pods | cp-calls | runtime |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range sorted {
		b.WriteString(fmt.Sprintf("| %s | %s | %d%% | %s | %d | %d | %d | %d | %s |\n",
			r.option, r.cell.id(), int(r.m.mitigatedFraction*100+0.5), ttrString(r), r.m.disruptedPods,
			r.m.gracefulPods, r.m.forcedPods, r.m.cpCalls, r.m.wallTime.Round(time.Second)))
	}
	return b.String()
}

// formatCollapseReport reveals degeneracy EMPIRICALLY: within each option, cells whose full metric tuple is identical
// are grouped. Each group of size > 1 is a set of cells that this implementation could not distinguish — the honest,
// measured version of what a static skip() predicate would only have claimed. (A group that is a singleton for one
// option but collapsed for another is itself a finding.)
func formatCollapseReport(results []repairResult) string {
	byOption := map[string]map[string][]string{} // option -> metric-signature -> []cellID
	for _, r := range results {
		sig := fmt.Sprintf("mit=%d%%|ttr=%s|pods=%d|graceful=%d|forced=%d|cp=%d", int(r.m.mitigatedFraction*100+0.5), ttrString(r), r.m.disruptedPods, r.m.gracefulPods, r.m.forcedPods, r.m.cpCalls)
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

// ReportAfterSuite dumps this process's accumulated matrix + collapse analysis once the suite finishes. Per-process
// (see repairReport note) so it is parallel-safe.
var _ = ReportAfterSuite("node-repair performance matrix", func(_ Report) {
	if len(repairReport) == 0 {
		return
	}
	dump := fmt.Sprintf("\n===== NODE REPAIR PERFORMANCE MATRIX (%d results) =====\n%s\n%s\n",
		len(repairReport), formatReport(repairReport), formatCollapseReport(repairReport))
	GinkgoWriter.Print(dump)
	// Also persist to a durable file when REPAIR_REPORT_FILE is set (append, so per-option runs accumulate) — the
	// GinkgoWriter dump is only visible under `go test -v`, but a full-matrix run wants the numbers on disk regardless.
	if path := os.Getenv("REPAIR_REPORT_FILE"); path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			GinkgoWriter.Printf("failed opening REPAIR_REPORT_FILE %s: %v\n", path, err)
			return
		}
		defer f.Close()
		if _, err := f.WriteString(dump); err != nil {
			GinkgoWriter.Printf("failed writing REPAIR_REPORT_FILE %s: %v\n", path, err)
		}
	}
})
