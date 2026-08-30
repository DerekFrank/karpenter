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

import "fmt"

// The node-repair scenario space is a cross product of five axes. A simCell is one point in it. Every axis value is
// expressed in end-user-observable terms so the cases stay implementation-agnostic (black box) across options.

// Axis 1 — ground truth: will repairing actually help? This is the scoring oracle AND the replacement fault model.
type helpTruth int

const (
	helpYes                  helpTruth = iota // replacement comes up healthy and stays healthy → repair helps
	helpNoLaunchFail                          // replacement never launches (ICE/capacity) → repair can't help
	helpNoUnhealthyInDwell                    // replacement boots Ready, re-flagged WITHIN the success dwell
	helpNoUnhealthyAfterDwell                 // replacement healthy PAST the dwell (scored "proven"), then fails
	helpNoWorkloadBroken                      // node Ready+healthy to Karpenter, but the workload still fails (blind spot)
)

func (h helpTruth) String() string {
	switch h {
	case helpYes:
		return "help"
	case helpNoLaunchFail:
		return "launchfail"
	case helpNoUnhealthyInDwell:
		return "reflag-in-dwell"
	case helpNoUnhealthyAfterDwell:
		return "reflag-after-dwell"
	case helpNoWorkloadBroken:
		return "workload-broken"
	}
	return "?"
}

// helps reports whether repairing genuinely resolves the fault (the oracle direction: minimize TTM when true,
// minimize disruption/cp-calls when false). Note workload-broken is observationally healthy but does NOT help.
func (h helpTruth) helps() bool { return h == helpYes }

// Axis 2 — is the failure correlated on a specific failure domain (and thus a burst), or an isolated single fault?
type correlation int

const (
	corrNone   correlation = iota // isolated single fault
	corrZone                      // burst shares a zone
	corrAMI                       // burst shares an AMI (only distinct under per-AMI topology)
	corrPolicy                    // burst shares a policy/condition (a bad detector)
)

func (c correlation) String() string {
	return [...]string{"isolated", "zone", "ami", "policy"}[c]
}

// isolated reports whether this is a single-node fault (no burst to pace).
func (c correlation) isolated() bool { return c == corrNone }

// Axis 3 — can the node be drained, and do PDBs participate?
type drainability int

const (
	drainYesNoPDB      drainability = iota // kubelet alive, no PDB → drains freely (baseline)
	drainYesPDB                            // kubelet alive, permissive PDB → drains, PDB paces eviction
	drainNoPDBBlock                        // kubelet alive, restrictive PDB → drain stalls/bounded
	drainNoKubeletDead                     // Ready=Unknown → forceful, drain skipped, PDBs irrelevant
)

func (d drainability) String() string {
	return [...]string{"drain", "drain+pdb", "pdb-block", "kubelet-dead"}[d]
}

// hasPDB reports whether the scenario places a PodDisruptionBudget on the workload.
func (d drainability) hasPDB() bool { return d == drainYesPDB || d == drainNoPDBBlock }

// Axis 4 — replacement strategy, derived from capacity posture (how repair acts, not whether it helps).
type strategy int

const (
	stratReplaceFirst   strategy = iota // headroom → pre-spin a replacement, then terminate
	stratTerminateFirst                 // no headroom (reserved/static) → delete-only, reactive refill
)

func (s strategy) String() string {
	return [...]string{"replace-first", "terminate-first"}[s]
}

// Axis 5 — the customer's NodePool topology, which decides how restraint's domains and the per-NodePool budget line up.
type topology int

const (
	topoSingle topology = iota // one NodePool spanning all AZs + AMIs
	topoPerAZ                  // one NodePool per AZ (pool ≡ zone)
	topoPerAMI                 // one NodePool per AMI/arch variant (pool ≡ ami)
)

func (t topology) String() string {
	return [...]string{"single", "per-az", "per-ami"}[t]
}

// simCell is one scenario in the cross product.
type simCell struct {
	help  helpTruth
	corr  correlation
	drain drainability
	strat strategy
	topo  topology
}

// id is a stable, compact identifier for the cell (used for ordering and report rows).
func (c simCell) id() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s", c.help, c.corr, c.drain, c.strat, c.topo)
}

// enumerateCells returns the FULL cross product — every cell is an explicit, authoritative entry. There is deliberately
// no skip()/pruning predicate: which cells are degenerate is not a static property of a cell (a cell that collapses for
// the breaker may not for restraint), so we do not *assert* degeneracy up front — we run every cell and let identical
// metric rows reveal degeneracy *empirically* (see the collapse report in repair_simulation_test.go). This also avoids
// leaking implementation mechanism into enumeration and can't silently drop a cell that's live for some implementation.
func enumerateCells() []simCell {
	var cells []simCell
	for _, h := range []helpTruth{helpYes, helpNoLaunchFail, helpNoUnhealthyInDwell, helpNoUnhealthyAfterDwell, helpNoWorkloadBroken} {
		for _, corr := range []correlation{corrNone, corrZone, corrAMI, corrPolicy} {
			for _, d := range []drainability{drainYesNoPDB, drainYesPDB, drainNoPDBBlock, drainNoKubeletDead} {
				for _, s := range []strategy{stratReplaceFirst, stratTerminateFirst} {
					for _, t := range []topology{topoSingle, topoPerAZ, topoPerAMI} {
						cells = append(cells, simCell{help: h, corr: corr, drain: d, strat: s, topo: t})
					}
				}
			}
		}
	}
	return cells
}
