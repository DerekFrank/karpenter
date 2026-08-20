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

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

// The node-repair scenario space is a cross product of end-user-observable axes. A cell is one point in it. Every axis
// value is expressed in end-user terms so the cases stay implementation-agnostic (black box) across options.
//
// Axes:
//   help      — ground truth: will repairing actually help? (the scoring oracle + replacement fault model)
//   scale     — fleet size, swept across orders of magnitude, because a fault's MEANING depends on the fraction of the
//               fleet it hits (1 of 3 is 33% and trips a 20% breaker; 1 of 5000 is negligible).
//   blast     — how much of the fleet fails: a single node, a minority (~10%), or a majority (~35%).
//   structure — how the failing nodes relate: unrelated (share no failure domain — independent faults) vs a correlated
//               outage sharing a zone / AMI / failure reason. Only meaningful when more than one node fails.
//   drain     — can the node be drained, and do PDBs participate?
//   strat     — replacement strategy (replace-first vs terminate-first; the latter is currently un-runnable, see
//               enabledStrategies).
//   topo      — the customer's NodePool topology (single / per-AZ / per-AMI).

// helpTruth — ground truth: will repairing actually help? This is the scoring oracle AND the replacement fault model.
type helpTruth int

const (
	helpYes                   helpTruth = iota // replacement comes up healthy and stays healthy → repair helps
	helpNoLaunchFail                           // replacement never launches (ICE/capacity) → repair can't help
	helpNoUnhealthyInDwell                     // replacement boots Ready, re-flagged WITHIN the success dwell
	helpNoUnhealthyAfterDwell                  // replacement healthy PAST the dwell (scored "proven"), then fails
	helpNoWorkloadBroken                       // node Ready+healthy to Karpenter, but the workload still fails (blind spot)
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

// helps reports whether repairing genuinely resolves the fault (the oracle direction: minimize time-to-recovery when
// true, minimize disruption/cp-calls when false). Note workload-broken is observationally healthy but does NOT help.
func (h helpTruth) helps() bool { return h == helpYes }

// scale — the fleet size the workload occupies. First-class because the fraction of the fleet a fault hits (not its
// absolute count) is what a fraction-keyed breaker reacts to.
type scale int

const (
	scale3 scale = iota
	scale10
	scale100
	scale1000
	scale5000
)

func (s scale) nodes() int { return [...]int{3, 10, 100, 1000, 5000}[s] }

func (s scale) String() string { return fmt.Sprintf("n%d", s.nodes()) }

// blast — how much of the fleet fails. Combined with scale, this is the fraction a breaker keys on.
type blast int

const (
	blastSingle   blast = iota // exactly one node
	blastMinority              // ~10% of the fleet
	blastMajority              // ~35% of the fleet
)

func (b blast) String() string { return [...]string{"single", "minority", "majority"}[b] }

// count is how many nodes fail for this blast at the given scale (at least one).
func (b blast) count(s scale) int {
	n := s.nodes()
	switch b {
	case blastMinority:
		return max(1, int(math.Ceil(0.10*float64(n))))
	case blastMajority:
		return max(1, int(math.Ceil(0.35*float64(n))))
	default: // blastSingle
		return 1
	}
}

// structure — how the failing nodes relate. unrelated failures share no failure domain (different zones, AMIs, and
// reasons): independent faults a structure-aware repair may address in parallel. zone/ami/reason failures share a
// domain: a correlated outage that should be paced. Meaningless for a single failure (blastSingle ignores it).
type structure int

const (
	structUnrelated structure = iota
	structZone
	structAMI
	structReason
)

func (st structure) String() string { return [...]string{"unrelated", "zone", "ami", "reason"}[st] }

// drainability — can the node be drained, and do PDBs participate?
type drainability int

const (
	drainYesNoPDB      drainability = iota // kubelet alive, no PDB → drains freely (baseline)
	drainYesPDB                            // kubelet alive, permissive PDB → drains, PDB paces eviction
	drainNoPDBBlock                        // kubelet alive, restrictive PDB → drain stalls/bounded
	drainNoKubeletDead                     // eviction won't succeed → forceful past the NodePool TGP, or wedged
)

func (d drainability) String() string {
	return [...]string{"drain", "drain+pdb", "pdb-block", "kubelet-dead"}[d]
}

// hasPDB reports whether the scenario places a PodDisruptionBudget on the workload.
func (d drainability) hasPDB() bool { return d == drainYesPDB || d == drainNoPDBBlock }

// strategy — replacement strategy, derived from capacity posture (how repair acts, not whether it helps).
type strategy int

const (
	stratReplaceFirst   strategy = iota // headroom → pre-spin a replacement, then terminate
	stratTerminateFirst                 // no headroom (reserved/static) → delete-only, reactive refill
)

func (s strategy) String() string {
	return [...]string{"replace-first", "terminate-first"}[s]
}

// topology — the customer's NodePool topology, which decides how a repair's domains and the per-NodePool budget line up.
type topology int

const (
	topoSingle topology = iota // one NodePool spanning all AZs + AMIs
	topoPerAZ                  // one NodePool per AZ (pool ≡ zone)
	topoPerAMI                 // one NodePool per AMI/arch variant (pool ≡ ami)
)

func (t topology) String() string {
	return [...]string{"single", "per-az", "per-ami"}[t]
}

// cell is one scenario in the cross product.
type cell struct {
	help      helpTruth
	scale     scale
	blast     blast
	structure structure
	drain     drainability
	strat     strategy
	topo      topology
}

// id is a stable, compact identifier for the cell (used for ordering and report rows). structure is rendered "-" for a
// single-node blast, where it does not apply.
func (c cell) id() string {
	st := "-"
	if c.blast != blastSingle {
		st = c.structure.String()
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s", c.help, c.scale, c.blast, st, c.drain, c.strat, c.topo)
}

// enumerateCells returns the FULL cross product — every cell is an explicit, authoritative entry. There is deliberately
// no skip()/pruning predicate for DEGENERACY: which cells are degenerate is not a static property of a cell (a cell that
// collapses for the breaker may not for restraint), so we run every cell and let identical metric rows reveal degeneracy
// empirically (see the collapse report in report.go). The single-node blast is the one structural collapse baked into
// enumeration — a single failure has no correlation structure, so it is emitted once rather than crossed with all four
// structures.
//
// Note: the top scales (n1000/n5000) are enumerated for completeness but are expensive to actually run — provisioning
// thousands of KWOK nodes per cell. Running the full product is a resourced activity; the enumeration stays authoritative
// so no scale is silently dropped.
func enumerateCells() []cell {
	var cells []cell
	for _, h := range []helpTruth{helpYes, helpNoLaunchFail, helpNoUnhealthyInDwell, helpNoUnhealthyAfterDwell, helpNoWorkloadBroken} {
		for _, sc := range []scale{scale3, scale10, scale100, scale1000, scale5000} {
			for _, b := range []blast{blastSingle, blastMinority, blastMajority} {
				for _, st := range structuresFor(b) {
					for _, d := range []drainability{drainYesNoPDB, drainYesPDB, drainNoPDBBlock, drainNoKubeletDead} {
						for _, s := range enabledStrategies() {
							for _, t := range []topology{topoSingle, topoPerAZ, topoPerAMI} {
								cells = append(cells, cell{help: h, scale: sc, blast: b, structure: st, drain: d, strat: s, topo: t})
							}
						}
					}
				}
			}
		}
	}
	return cells
}

// shardCells returns this process's shard of the full enumeration, for running the matrix across K parallel clusters:
// SHARD_COUNT=K and SHARD_INDEX=i (0-based) keep every K-th cell. Unset/invalid ⇒ the whole product (one shard). The
// stride assignment (i, i+K, i+2K, …) interleaves scales, so each shard gets a similar mix of cheap and expensive cells
// rather than one shard drawing all the n5000 work.
func shardCells(all []cell) []cell {
	count := envInt("SHARD_COUNT", 1)
	index := envInt("SHARD_INDEX", 0)
	if count <= 1 || index < 0 || index >= count {
		return all
	}
	var out []cell
	for i := index; i < len(all); i += count {
		out = append(out, all[i])
	}
	return out
}

func envInt(name string, def int) int {
	if v, ok := os.LookupEnv(name); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// structuresFor returns the structure values to enumerate for a blast: a single failure has no correlation structure
// (emitted once), a multi-node blast is crossed with all four.
func structuresFor(b blast) []structure {
	if b == blastSingle {
		return []structure{structUnrelated} // sentinel; id() renders "-" for single
	}
	return []structure{structUnrelated, structZone, structAMI, structReason}
}

// enabledStrategies is the strategy axis restricted to values that have a testable implementation on the deployed build.
// stratTerminateFirst is NOT degenerate — it is un-runnable here: it models reserved/static capacity
// (NodePool.Spec.Replicas), which needs the StaticCapacity feature, and a terminate-first repair path that does not yet
// exist. Its scenario wiring (newNodePools) is kept intact; re-add stratTerminateFirst below once the implementation and
// feature gate are in place. This exclusion is a capability gate, distinct from the deliberate no-skip policy for
// degeneracy (which is still revealed empirically in the collapse report).
func enabledStrategies() []strategy {
	return []strategy{stratReplaceFirst}
}
