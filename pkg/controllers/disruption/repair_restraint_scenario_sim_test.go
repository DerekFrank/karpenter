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

package disruption

import (
	"fmt"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	clocktesting "k8s.io/utils/clock/testing"
)

// Scenario simulations for correlated-failure restraint (F3), answering the review ask for "a really simple simulation
// of the motivating customer cases." These drive the REAL restraint policy (CanDisrupt/Record) over a fake clock across
// the motivating scenarios at fleet scale, and compare it against the budget-only baseline (noRestraint — what repair
// does today with only the disruption budget pacing it). They record two operator-facing outcomes per scenario:
//
//   - launched   — replacements pre-spun (wasted-capacity blast radius; the "slow pointless stampede" cost).
//   - terminated — originals actually removed (proven repairs = real workload disruption). With pre-spin a FAILED probe
//                  never terminates its original, so a scenario whose probes fail disrupts zero workloads regardless of
//                  how many launches it wastes — the harm there is only wasted capacity.
//
// The sim is deterministic (fake clock, fixed iteration order) so the emitted series are reproducible. It exercises the
// dials, not the queue/scheduling plumbing (the behavioral suite in repair_restraint_test.go covers the wiring).

const (
	simTick  = 1 * time.Minute // one reconcile "round"; many rounds happen per dwell/cooldown, so width shows directly
	simDwell = 5 * time.Minute // a proven probe resolves this long after admission (matches repairDwell)
	simFail  = 5 * time.Minute // a failed probe resolves (replacement boots and is re-flagged / never heartbeats) this soon
)

// simNode is one faulted node in a scenario. outcome is what a probe on it yields when it resolves; healAt (if non-zero)
// is when its fault clears on its own (a rolled-back AMI, a recovered zone) without a completed repair.
type simNode struct {
	id, nodePool, zone, cond string
	replZone                 string // zone the replacement comes up in; defaults to the candidate's zone (differs for C2)
	priority                 int    // higher is repaired first (proxy for the score ordering)
	outcome                  RepairOutcome
	healAt                   time.Time

	// runtime
	faulted   bool
	inflight  bool
	resolveAt time.Time
	repaired  bool // proven (original terminated) or self-healed
}

func (n *simNode) candidate() *Candidate { return wbCandidate(n.id, n.nodePool, n.zone, n.cond) }

// replacement is the Node the repair pre-spun, used for learn-from-replacement attribution — its zone is where the
// replacement actually came up (the same zone unless the scenario routes it elsewhere, e.g. the C2 partition case).
func (n *simNode) replacement() *corev1.Node {
	z := n.replZone
	if z == "" {
		z = n.zone
	}
	return wbReplacement(z)
}

type simResult struct {
	launched, terminated, healed int
	maxConcurrent                int
	inflightSeries               []int
	firstRepairTick              map[string]int // cond -> first tick a node of that cond was proven+terminated
}

// runSim plays a fleet through `horizon` ticks under restraint policy rr, capped by `budget` concurrent probes (the
// disruption budget, applied in every arm so the only difference between arms is the restraint policy). widthDomain, if
// set and rr is the AIMD *restraint, is sampled each tick into widthOut.
func runSim(rr RepairRestraint, clk *clocktesting.FakeClock, nodes []*simNode, budget, horizon int, widthDomain *failureDomain, widthOut *[]int) simResult {
	res := simResult{firstRepairTick: map[string]int{}}
	// Deterministic order: highest priority first (genuine fault ahead of a flood), then id.
	order := append([]*simNode{}, nodes...)
	sort.Slice(order, func(i, j int) bool {
		if order[i].priority != order[j].priority {
			return order[i].priority > order[j].priority
		}
		return order[i].id < order[j].id
	})
	for _, n := range nodes {
		n.faulted = true
	}

	for tick := 0; tick < horizon; tick++ {
		now := clk.Now()

		// 1) Resolve in-flight probes whose outcome time has arrived.
		for _, n := range order {
			if !n.inflight || now.Before(n.resolveAt) {
				continue
			}
			rr.Record(n.candidate(), n.replacement(), n.outcome)
			n.inflight = false
			if n.outcome == RepairProven {
				n.faulted = false
				n.repaired = true
				res.terminated++
				if _, ok := res.firstRepairTick[n.cond]; !ok {
					res.firstRepairTick[n.cond] = tick
				}
			}
			// RepairFailed: pre-spin means the original is never terminated — the node stays faulted and is retried later.
		}

		// 2) Self-healing faults clear on their own (RepairCleared) — the out-of-band fix landed.
		for _, n := range order {
			if n.faulted && !n.inflight && !n.healAt.IsZero() && !now.Before(n.healAt) {
				rr.Record(n.candidate(), nil, RepairCleared)
				n.faulted = false
				n.repaired = true
				res.healed++
			}
		}

		// 3) Admit: one CanDisrupt call per still-faulted, not-in-flight node (calling twice would double-count in-flight).
		//    A single ordered pass admits as many as the dials + budget allow — each admit updates the policy's in-flight
		//    counts, so the min-width gate naturally stops the pass.
		inflightNow := 0
		for _, n := range order {
			if n.inflight {
				inflightNow++
			}
		}
		for _, n := range order {
			if inflightNow >= budget {
				break
			}
			if !n.faulted || n.inflight {
				continue
			}
			if !rr.CanDisrupt(n.candidate()) {
				continue
			}
			n.inflight = true
			if n.outcome == RepairProven {
				n.resolveAt = now.Add(simDwell)
			} else {
				n.resolveAt = now.Add(simFail)
			}
			res.launched++
			inflightNow++
		}

		if inflightNow > res.maxConcurrent {
			res.maxConcurrent = inflightNow
		}
		res.inflightSeries = append(res.inflightSeries, inflightNow)
		if widthDomain != nil && widthOut != nil {
			if r, ok := rr.(*restraint); ok {
				*widthOut = append(*widthOut, r.widthFor(*widthDomain))
			}
		}
		clk.Step(simTick)
	}
	return res
}

func newSimClock() *clocktesting.FakeClock {
	return clocktesting.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
}

// emit prints a compact, greppable summary + the concurrency series so the run is legible in test output.
func emit(scenario, arm string, r simResult) {
	fmt.Fprintf(GinkgoWriter, "SIMRESULT scenario=%s arm=%s launched=%d terminated=%d healed=%d maxConcurrent=%d firstRepairTick=%v\n",
		scenario, arm, r.launched, r.terminated, r.healed, r.maxConcurrent, r.firstRepairTick)
	fmt.Fprintf(GinkgoWriter, "SIMSERIES scenario=%s arm=%s inflight=%v\n", scenario, arm, r.inflightSeries)
}

// --- Scenario A "today" arm: the hardcoded allowedUnhealthyPercent=20% latching breaker (shipped node.health) ---

type breakerResult struct {
	terminated        int // force-deletions (workload disruptions; the breaker has no pre-spin)
	maxStampede       int // peak force-deletions in a single pass — the recovery-side stampede
	genuineRepairTick int // tick the genuine fault was finally force-deleted (repaired); -1 if never within horizon
	frozenTicks       int // passes the breaker did nothing because the pool was over the 20% line
}

// runBreakerModel models the shipped node.health breaker: per pass, if the pool's unhealthy fraction exceeds 20% it
// FREEZES (force-deletes nothing — the latch, which starves a genuine fault behind a false-positive flood), otherwise it
// force-deletes EVERY still-flagged node at once (no budget, no pre-spin, no dwell). The detector bug is fixed at
// fixTick, after which the false positives clear at clearPerTick/pass; a genuine fault never clears on its own. When the
// clearing brings the fraction back under 20% the breaker re-opens and force-deletes whatever is still flagged in one
// pass — including healthy-but-still-flagged nodes that were about to recover (the stampede).
func runBreakerModel(domainSize, floodN, fixTick, clearPerTick, horizon int) breakerResult {
	res := breakerResult{genuineRepairTick: -1}
	floodFlagged := floodN
	genuineFlagged := true
	for tick := 0; tick < horizon; tick++ {
		if tick >= fixTick && floodFlagged > 0 { // detector fixed: false positives clear on their own
			floodFlagged -= clearPerTick
			if floodFlagged < 0 {
				floodFlagged = 0
			}
		}
		unhealthy := floodFlagged
		if genuineFlagged {
			unhealthy++
		}
		if unhealthy == 0 {
			break
		}
		if float64(unhealthy)/float64(domainSize) > 0.20 {
			res.frozenTicks++ // LATCH: freeze — force-delete nothing, including the genuine fault
			continue
		}
		// Breaker open: force-delete every still-flagged node at once.
		stampede := floodFlagged
		if genuineFlagged {
			stampede++
			res.genuineRepairTick = tick
			genuineFlagged = false
		}
		res.terminated += stampede
		if stampede > res.maxStampede {
			res.maxStampede = stampede
		}
		floodFlagged = 0
	}
	return res
}

var _ = Describe("Repair/Restraint/Scenarios (simulation)", func() {
	var clk *clocktesting.FakeClock
	BeforeEach(func() { clk = newSimClock() })

	// SCENARIO A — the motivating incident. 130 healthy GPU nodes are false-positive-flagged (their replacements re-boot
	// the same bad NMA and are re-flagged → the probe FAILS), sharing NodePool ∧ Zone ∧ the fabric Policy. One genuinely
	// broken node in the SAME pool/zone carries a DIFFERENT policy (a real ECC fault; its replacement is healthy → PROVEN).
	// Budget = 13 (10% of 130). Restraint should (a) hold the flood to a trickle of wasted launches, and (b) still repair
	// the genuine fault promptly — it is eligible through its own fresh policy domain even while the flood cools.
	Describe("false-positive flood + one genuine fault", func() {
		build := func() []*simNode {
			nodes := []*simNode{{id: "ecc-genuine", nodePool: "gpu", zone: "zone-a", cond: "ECCError", priority: 10, outcome: RepairProven}}
			for i := 0; i < 130; i++ {
				nodes = append(nodes, &simNode{id: fmt.Sprintf("flood-%03d", i), nodePool: "gpu", zone: "zone-a", cond: "FabricError", priority: 1, outcome: RepairFailed})
			}
			return nodes
		}
		const horizon = 120 // 2h

		It("restraint bounds the flood's wasted launches while still repairing the genuine fault", func() {
			r := runSim(newRestraint(clk), clk, build(), 13, horizon, nil, nil)
			emit("flood", "restraint", r)

			// The genuine ECC fault is repaired, and early (within the first dwell + a couple rounds).
			Expect(r.firstRepairTick).To(HaveKey("ECCError"))
			Expect(r.firstRepairTick["ECCError"]).To(BeNumerically("<=", 8))
			// The flood is held to at most one concurrent probe (its policy domain floors width at 1 and never proves out).
			Expect(r.maxConcurrent).To(BeNumerically("<=", 2)) // ≤1 flood + the single genuine probe overlapping at most
			// Over 2h the flood wastes only a cooldown-limited handful of launches — nowhere near 130.
			Expect(r.launched).To(BeNumerically("<", 20))
			// No workloads are disrupted by the flood (all its probes fail → pre-spin keeps every original); only the one
			// genuine node is terminated.
			Expect(r.terminated).To(Equal(1))
		})

		It("baseline (budget-only) stampedes the whole cohort at the budget rate", func() {
			r := runSim(noRestraint{}, clk, build(), 13, horizon, nil, nil)
			emit("flood", "baseline", r)

			// Budget-only churns up to the full budget concurrently and keeps re-launching every failed flood node, wasting
			// an order of magnitude more launches than restraint.
			Expect(r.maxConcurrent).To(BeNumerically(">=", 13))
			Expect(r.launched).To(BeNumerically(">", 100))
		})

		It("today's shipped 20% breaker freezes the genuine fault, then stampedes on recovery", func() {
			// 130 false positives + 1 genuine fault in a 200-node pool → 65% unhealthy, far over the 20% line. The breaker
			// latches: it force-deletes nothing while the flood is up, so the one genuine fault is starved. The detector is
			// fixed at tick 60 and the false positives clear; when the fraction finally drops under 20% the breaker re-opens
			// and force-deletes everything still flagged at once.
			r := runBreakerModel(200 /*domain*/, 130 /*flood*/, 60 /*fixTick*/, 13 /*clearPerTick*/, 120 /*horizon*/)
			fmt.Fprintf(GinkgoWriter, "SIMRESULT scenario=flood arm=breaker terminated=%d maxStampede=%d genuineRepairTick=%d frozenTicks=%d\n",
				r.terminated, r.maxStampede, r.genuineRepairTick, r.frozenTicks)

			// The genuine fault is frozen for the whole flood — repaired only long after the external fix, not promptly like
			// restraint (which repairs it at tick ~5). This is the 19-day-incident failure mode.
			Expect(r.genuineRepairTick).To(BeNumerically(">=", 60))
			Expect(r.frozenTicks).To(BeNumerically(">=", 60))
			// And when it finally re-opens it force-deletes a burst of still-flagged (healthy, about-to-recover) nodes at
			// once — the stampede — versus restraint's max of one concurrent probe and zero healthy-node terminations.
			Expect(r.maxStampede).To(BeNumerically(">", 1))
		})
	})

	// SCENARIO C1 — network partition, replacement lands in the SAME (partitioned) zone. A whole zone's kubelets stop
	// heart-beating (Ready→False) but the pods are fine; the partition self-resolves at t=60m. A replacement launched into
	// the same zone also can't heartbeat → the probe FAILS → pre-spin keeps every original, so NO workload is disrupted.
	// Restraint should pace the wasted launches to a trickle and then let the fault clear itself.
	Describe("network partition (replacement in the impaired zone, self-heals)", func() {
		build := func() []*simNode {
			nodes := []*simNode{}
			heal := clk.Now().Add(60 * time.Minute)
			for i := 0; i < 60; i++ {
				nodes = append(nodes, &simNode{id: fmt.Sprintf("part-%03d", i), nodePool: "pool", zone: "zone-p", cond: "KubeletReady", priority: 1, outcome: RepairFailed, healAt: heal})
			}
			return nodes
		}
		const horizon = 90

		It("restraint disrupts zero healthy workloads and wastes few launches; the partition self-heals", func() {
			r := runSim(newRestraint(clk), clk, build(), 6, horizon, nil, nil)
			emit("partition-c1", "restraint", r)

			Expect(r.terminated).To(Equal(0))                  // pre-spin: failed probes never remove an original → no workload harm
			Expect(r.maxConcurrent).To(BeNumerically("<=", 1)) // paced into the impaired zone one probe at a time
			Expect(r.launched).To(BeNumerically("<", 15))      // only a cooldown-limited handful of wasted launches over the outage
			Expect(r.healed).To(Equal(60))                     // every node's fault clears on its own once the partition resolves
		})

		It("baseline (budget-only) mass-probes the impaired zone", func() {
			r := runSim(noRestraint{}, clk, build(), 6, horizon, nil, nil)
			emit("partition-c1", "baseline", r)
			Expect(r.maxConcurrent).To(BeNumerically(">=", 6)) // fills the budget with probes into a zone that cannot come up
			Expect(r.launched).To(BeNumerically(">", 30))
		})
	})

	// SCENARIO C2 — network partition, replacement lands in a HEALTHY zone (the jmdeal review concern). The partitioned
	// nodes are actually fine, but a pre-spun replacement scheduled into a *different*, healthy zone comes up Ready → the
	// probe is (falsely) PROVEN. WITHOUT learn-from-replacement, restraint would read this as success and ACCELERATE
	// (width doubles), mass-disrupting the partitioned-but-healthy workloads. WITH the fix (Record attributes a zone
	// outcome only when the replacement came up in that zone), the healthy-zone success never widens the impaired zone, so
	// concurrency stays pinned at one — no acceleration. The residual (still terminating impaired-but-healthy nodes one at
	// a time) is what the offering-unavailable defer (invariant 8, an integ test) eliminates by refusing to launch a
	// replacement out of the impaired zone at all.
	Describe("network partition (false success from a replacement in a healthy zone)", func() {
		build := func() []*simNode {
			nodes := []*simNode{}
			for i := 0; i < 60; i++ {
				// The candidate is in the impaired zone-p, but its replacement comes up in the healthy zone-h.
				nodes = append(nodes, &simNode{id: fmt.Sprintf("part-%03d", i), nodePool: "pool", zone: "zone-p", replZone: "zone-h", cond: "KubeletReady", priority: 1, outcome: RepairProven})
			}
			return nodes
		}
		const horizon = 90

		It("learn-from-replacement stops the acceleration: the impaired zone never widens (concurrency stays at one)", func() {
			r := runSim(newRestraint(clk), clk, build(), 8, horizon, nil, nil)
			emit("partition-c2", "restraint", r)
			// A healthy replacement in zone-h does not credit the impaired zone-p, so its width stays at the floor and
			// min-width across the candidate's domains keeps concurrency at one — restraint no longer accelerates into the
			// partition. (Contrast the pre-fix behavior, where width doubled to 8.)
			Expect(r.maxConcurrent).To(Equal(1))
			// It still terminates impaired-but-healthy nodes one at a time (the residual the offering-unavailable defer
			// closes) — but far fewer than the 60 it would churn while accelerating.
			Expect(r.terminated).To(BeNumerically("<", 30))
		})
	})

	// SCENARIO D — a genuinely large fault where repair IS working (replacements come up healthy → PROVEN). This answers
	// the "does starting slow throttle real repair?" question and the ×2-vs-additive open question: multiplicative width
	// reaches the budget in ~log2(budget) proven generations, so a big real outage drains fast, not in hundreds of rounds.
	Describe("large genuine fault, repairs succeeding (fast climb)", func() {
		It("concurrency climbs geometrically to the budget fast (not throttling a real large repair)", func() {
			nodes := []*simNode{}
			for i := 0; i < 400; i++ {
				nodes = append(nodes, &simNode{id: fmt.Sprintf("real-%03d", i), nodePool: "pool", zone: "zone-a", cond: "BadNode", priority: 1, outcome: RepairProven})
			}
			var width []int
			d := failureDomain{kind: "nodepool", value: "pool"}
			r := runSim(newRestraint(clk), clk, nodes, 64, 120, &d, &width)
			emit("fastclimb", "restraint", r)
			fmt.Fprintf(GinkgoWriter, "SIMWIDTH scenario=fastclimb arm=restraint nodepool_width=%v\n", width)

			// Operator-visible behavior: admitted concurrency reaches the budget ceiling (64), so a genuine large repair is
			// not throttled by starting slow.
			Expect(r.maxConcurrent).To(Equal(64))
			// And it gets there fast — within a handful of dwell generations, far from the ~64 rounds an additive +1 climb
			// would need. Find the first tick concurrency hits the budget.
			reached := -1
			for i, c := range r.inflightSeries {
				if c >= 64 {
					reached = i
					break
				}
			}
			Expect(reached).To(BeNumerically(">=", 0))
			Expect(reached).To(BeNumerically("<=", 40)) // minutes

			// FINDING (documented, not asserted as desired): the stored width overshoots the budget by orders of magnitude
			// because Record(RepairProven) doubles width per proven PROBE, and many probes prove in the same generation
			// (2^G per generation, not the RFC's per-round ×2). It is harmless for concurrency (min(L, budget) clamps it),
			// but the stored dial is not a meaningful "earned width," and because width is only forgotten on self-healed
			// clearance (RepairCleared), a domain repaired entirely by TERMINATION keeps this inflated width — so a later
			// correlated burst in the same domain is admitted at full budget for one wave before the first failure resets
			// it to the floor (an invariant-5 "start slow" gap for the repaired-by-termination path). See POC findings.
			Expect(width[len(width)-1]).To(BeNumerically(">", 64)) // pins the overshoot so the finding regresses loudly if it changes
		})
	})
})
