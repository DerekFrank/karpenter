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

// skip encodes the degenerate cells (rules 1–3 from the design review): cells that are impossible or observationally
// identical to another cell, so running them adds no signal.
func (c simCell) skip() bool {
	// Rule 1: topology is inert for an isolated fault (one node, one pool, one budget) — only `single` is meaningful.
	if c.corr.isolated() && c.topo != topoSingle {
		return true
	}
	// Rule 2: an ami-correlated burst is only observationally distinct under per-AMI topology; otherwise it collapses
	// into a nodepool-wide (single) or zonal burst.
	if c.corr == corrAMI && c.topo != topoPerAMI {
		return true
	}
	// Rule 3: if nothing is ever drained, the PDB values collapse to the no-PDB baseline. The original is never
	// terminated when we replace-first AND the replacement never becomes healthy (the pre-spin gate never clears).
	replacementNeverHealthy := c.help == helpNoLaunchFail || c.help == helpNoUnhealthyInDwell
	if c.strat == stratReplaceFirst && replacementNeverHealthy && c.drain != drainYesNoPDB {
		return true
	}
	return false
}

// enumerateCells returns every non-degenerate cell in the cross product.
func enumerateCells() []simCell {
	var cells []simCell
	for _, h := range []helpTruth{helpYes, helpNoLaunchFail, helpNoUnhealthyInDwell, helpNoUnhealthyAfterDwell, helpNoWorkloadBroken} {
		for _, corr := range []correlation{corrNone, corrZone, corrAMI, corrPolicy} {
			for _, d := range []drainability{drainYesNoPDB, drainYesPDB, drainNoPDBBlock, drainNoKubeletDead} {
				for _, s := range []strategy{stratReplaceFirst, stratTerminateFirst} {
					for _, t := range []topology{topoSingle, topoPerAZ, topoPerAMI} {
						cell := simCell{help: h, corr: corr, drain: d, strat: s, topo: t}
						if !cell.skip() {
							cells = append(cells, cell)
						}
					}
				}
			}
		}
	}
	return cells
}
