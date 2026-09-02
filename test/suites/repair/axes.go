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
	"time"

	corev1 "k8s.io/api/core/v1"
)

// The node-repair scenario space is a cross product of end-user-observable axes. A cell is one point in it. Every axis
// value is expressed in end-user terms so the cases stay implementation-agnostic (black box) across options.
//
// Axes:
//   impairment — the correlation domain AND whether/where it is impaired. Replaces the old help + structure axes:
//                "will repair help" is EMERGENT (derived from the physical situation crossed with the implementation),
//                not an input.
//   scale      — fleet size, swept across orders of magnitude, because a fault's MEANING depends on the fraction of the
//                fleet it hits (1 of 3 is 33% and trips a 20% breaker; 1 of 5000 is negligible).
//   blast      — how much of the fleet fails: a single node, a minority (~10%), or a majority (~35%).
//   drain      — can the node be drained, and do PDBs participate?
//   strat      — replacement strategy (replace-first vs terminate-first; the latter is currently un-runnable, see
//                enabledStrategies).
//   topo       — the customer's NodePool topology (single / per-AZ / per-AMI).

// impairment encodes the correlation domain AND whether/where that domain is impaired. This single axis replaces the
// old help + structure axes: whether repair helps is not a knob but a consequence of the physical situation. There are
// 7 values across two domains (zone / ami) plus an uncorrelated baseline. The three impaired states per domain differ
// by WHEN they manifest:
//   - benign         — a replacement in the faulted domain comes up healthy; repair helps regardless of where the
//     replacement lands (the false-correlation probe: does an implementation needlessly throttle a
//     co-located-but-fine failure?).
//   - node-broken    — manifests at LAUNCH: a replacement NODE placed in the impaired domain comes up unhealthy (it
//     re-inherits the fault). Karpenter-observable (the replacement is itself repair-eligible), so a
//     good restraint can detect the dead domain and back off. Repair helps ONLY by ESCAPING the domain.
//   - workload-broken — manifests at SCHEDULE: the replacement node is genuinely healthy, but the WORKLOAD pod placed
//     on it never becomes ready. Karpenter-BLIND (it sees a healthy node), so repair churns uselessly.
//     Repair can NEVER help; the correct behavior is to decline.
//
// uncorrelated is a single value: independent node faults with no shared domain, each individually repairable by a
// healthy replacement (benign by nature).
type impairment int

const (
	impUncorrelated       impairment = iota // independent node faults, no shared domain (benign by nature)
	impZoneBenign                           // zone-correlated; an in-zone replacement comes up healthy
	impZoneNodeBroken                       // zone-correlated; an in-zone replacement NODE is re-born unhealthy (launch)
	impZoneWorkloadBroken                   // zone-correlated; an in-zone replacement's POD never becomes ready (schedule)
	impAMIBenign                            // ami/arch-correlated; an in-arch replacement comes up healthy
	impAMINodeBroken                        // ami/arch-correlated; an in-arch replacement NODE is re-born unhealthy (launch)
	impAMIWorkloadBroken                    // ami/arch-correlated; an in-arch replacement's POD never becomes ready (schedule)
)

func (i impairment) String() string {
	return [...]string{
		"uncorrelated",
		"zone-benign", "zone-node-broken", "zone-workload-broken",
		"ami-benign", "ami-node-broken", "ami-workload-broken",
	}[i]
}

// domainKey is the node label whose value defines the impaired domain: zone for zone-*, arch for ami-*, "" for
// uncorrelated (no shared domain).
func (i impairment) domainKey() string {
	switch i {
	case impZoneBenign, impZoneNodeBroken, impZoneWorkloadBroken:
		return corev1.LabelTopologyZone
	case impAMIBenign, impAMINodeBroken, impAMIWorkloadBroken:
		return corev1.LabelArchStable
	default:
		return ""
	}
}

// nodeBroken reports the LAUNCH-time mechanism: a replacement NODE landing in the impaired domain is re-born unhealthy.
func (i impairment) nodeBroken() bool { return i == impZoneNodeBroken || i == impAMINodeBroken }

// workloadBroken reports the SCHEDULE-time mechanism: a replacement POD landing in the impaired domain never becomes
// ready even though its node is healthy (the Karpenter-blind blind spot).
func (i impairment) workloadBroken() bool {
	return i == impZoneWorkloadBroken || i == impAMIWorkloadBroken
}

// scale — the fleet size the workload occupies. First-class because the fraction of the fleet a fault hits (not its
// absolute count) is what a fraction-keyed breaker reacts to.
type scale int

const (
	scale3 scale = iota
	scale10
	scale100
	scale1000
	scale5000
	scale10000
)

func (s scale) nodes() int { return [...]int{3, 10, 100, 1000, 5000, 10000}[s] }

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
	impairment impairment
	scale      scale
	blast      blast
	drain      drainability
	strat      strategy
	topo       topology
	// flakeTTL: 0 = durable fault (SimUnhealthy sticks until repair terminates the node); >0 = flaky fault that KWOK's
	// node-fault-clear stage auto-clears after this long (models a flapping detector). See data/kwok-node-fault-design.md.
	flakeTTL time.Duration
	// reflag (node-broken only) — WHEN an in-domain replacement goes unhealthy after joining healthy (see reflagDelay).
	reflag reflagDelay
}

// reflagDelay controls, for a node-broken in-domain replacement, how long it stays HEALTHY after joining before it
// re-flags unhealthy — the axis that stresses the success dwell / optimistic crediting:
//   - immediate : reborn unhealthy at launch (never proves healthy) — the original launch-failure model.
//   - tbelow    : healthy ~0.5× the reference dwell, then re-fails — WITHIN the confirmation window (AIMD should catch it).
//   - tabove    : healthy ~2× the reference dwell, then re-fails — AFTER it's credited proven (retroactive claw-back case).
//   - trand     : healthy a RANDOM window in [0, 2× dwell) per replacement — straddles the dwell (jitter/optimistic stress).
type reflagDelay int

const (
	reflagImmediate reflagDelay = iota
	reflagBelowDwell
	reflagAboveDwell
	reflagRandom
)

func (r reflagDelay) String() string { return [...]string{"immediate", "tbelow", "tabove", "trand"}[r] }

// id is a stable, compact identifier for the cell (used for ordering and report rows). flakeTTL is appended only when
// set, so durable cells keep their historical IDs (and aggregation across older runs) unchanged.
func (c cell) id() string {
	base := fmt.Sprintf("%s|%s|%s|%s|%s|%s", c.impairment, c.scale, c.blast, c.drain, c.strat, c.topo)
	if c.reflag != reflagImmediate {
		base += "|" + c.reflag.String()
	}
	if c.flakeTTL > 0 {
		base += "|flake" + c.flakeTTL.String()
	}
	return base
}

// enumerateCells returns the FULL cross product — every cell is an explicit, authoritative entry. There is deliberately
// no skip()/pruning predicate for DEGENERACY: which cells are degenerate is not a static property of a cell (a cell that
// collapses for the breaker may not for restraint), so we run every cell and let identical metric rows reveal degeneracy
// empirically (see the collapse report in report.go). The impairment×blast coupling (see impairmentsFor) is the one
// structural collapse baked into enumeration — a single-node fault has no correlation domain, so it is emitted once as
// uncorrelated rather than crossed with every domained impairment.
//
// Note: the top scales (n1000/n5000/n10000) are enumerated for completeness but are expensive to actually run —
// provisioning thousands of KWOK nodes per cell. Running the full product is a resourced activity; the enumeration stays
// authoritative so no scale is silently dropped.
func enumerateCells() []cell {
	var cells []cell
	for _, sc := range []scale{scale3, scale10, scale100, scale1000, scale5000, scale10000} {
		for _, b := range []blast{blastSingle, blastMinority, blastMajority} {
			for _, imp := range impairmentsFor(b) {
				for _, d := range []drainability{drainYesNoPDB, drainYesPDB, drainNoPDBBlock, drainNoKubeletDead} {
					for _, s := range enabledStrategies() {
						for _, t := range []topology{topoSingle, topoPerAZ, topoPerAMI} {
							// reflag-delay only means anything for node-broken (the only impairment that re-flags an
							// in-domain replacement); every other impairment gets a single "immediate" cell.
							reflags := []reflagDelay{reflagImmediate}
							if imp.nodeBroken() {
								reflags = append(reflags, reflagBelowDwell, reflagAboveDwell, reflagRandom)
							}
							for _, rf := range reflags {
								cells = append(cells, cell{impairment: imp, scale: sc, blast: b, drain: d, strat: s, topo: t, reflag: rf})
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

// impairmentsFor returns the impairment values valid for a blast, enforcing the enumeration couplings (not a free cross):
//   - blast=single ⟹ impairment=uncorrelated: a one-node fault has no correlation domain (the single-collapses-
//     correlation rule).
//   - the domained values (zone-*/ami-*) need more than one node to correlate, so they appear only at minority/majority.
//   - uncorrelated is valid at every blast.
//
// So impairment×blast yields 15 valid combos: single→1, minority→7, majority→7.
func impairmentsFor(b blast) []impairment {
	if b == blastSingle {
		return []impairment{impUncorrelated}
	}
	return []impairment{
		impUncorrelated,
		impZoneBenign, impZoneNodeBroken, impZoneWorkloadBroken,
		impAMIBenign, impAMINodeBroken, impAMIWorkloadBroken,
	}
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
