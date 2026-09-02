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
	"time"

	corev1 "k8s.io/api/core/v1"
)

// RepairOutcome is what happened to a candidate repair drove through the pipeline, reported back to the restraint
// policy. Repair owns detecting these (from the queue + NodeClaim state + a success dwell); the policy only reacts.
type RepairOutcome int

const (
	// RepairProven — the replacement came up and held Ready+Healthy for the success dwell: a durable success.
	RepairProven RepairOutcome = iota
	// RepairFailed — the repair did not produce a healthy replacement (bad-component loop, partitioned zone, timeout).
	RepairFailed
	// RepairCleared — the candidate's fault resolved on its own before/without a completed repair; its fault episode is
	// over. Lets a policy forget any state it earned so a new episode starts fresh.
	RepairCleared
	// RepairClawedBack — a repair that was already reported RepairProven re-failed within the claw-back watch window
	// (the optimistic-credit variant credits on first-healthy, then watches). The domain must undo the width it earned
	// and back off, but WITHOUT a second in-flight decrement — RepairProven already released the probe.
	RepairClawedBack
)

// RepairRestraint is the swappable policy that decides, beneath the disruption budget, whether repair may probe a
// candidate right now. It is a pure decision policy over a stream of outcomes: repair owns all the I/O (tracking
// in-flight probes, polling the queue and NodeClaims, timing the success dwell) and simply asks CanDisrupt and reports
// Record. Nothing about the AIMD mechanism (failure domains, width, cooldown) appears in the interface, so an entirely
// different policy — rate-based, signal-fed, or none — drops in without changing repair.
type RepairRestraint interface {
	// CanDisrupt reports whether this candidate may be probed now.
	CanDisrupt(c *Candidate) bool
	// Record informs the policy of the outcome of a candidate it previously admitted (or one whose fault just cleared).
	// replacement is the Node the repair pre-spun (nil for a delete-only/terminate-first repair, or a launch that never
	// produced a node), so the policy can attribute a location-scoped outcome to where the replacement actually came up.
	Record(c *Candidate, replacement *corev1.Node, outcome RepairOutcome)
}

// noRestraint is the identity policy: always admit, remember nothing — the behavior when only the budget paces repair.
type noRestraint struct{}

func (noRestraint) CanDisrupt(*Candidate) bool                     { return true }
func (noRestraint) Record(*Candidate, *corev1.Node, RepairOutcome) {}

// Correlated-failure restraint (Node Repair Under Correlated Failure, F3) — the default RepairRestraint. Per failure
// domain (NodePool, zone, policy-condition), repair opens up geometrically while replacements prove out and backs off
// sharply when they don't — floored at one probe and capped at a max cooldown, so it always keeps trying but never
// stampedes. It layers strictly *beneath* the disruption budget.
//
// Statelessness (invariant): the dials are an optimization, never a correctness requirement. A freshly restarted
// controller rebuilds an empty store, and an empty store cold-starts every domain at width 1 — the most conservative
// value — so a zero-state restart is always safe.
// restraintWidthFloor is never 0: repair always eventually retries (invariant: pause, don't stop). The cooldown floor/
// ceiling are NOT constants — they are the provider's RepairTiming (so the whole restraint timescale scales together).
const restraintWidthFloor = 1

// failureDomain is one axis restraint scopes to (NodePool, zone, or policy-condition). A correlated burst collapses into
// one domain that cold-starts at width 1, so correlation is handled by scoping + cold-start, not a correlation statistic.
type failureDomain struct {
	kind  string // "nodepool" | "zone" | "policy"
	value string
}

// restraint is the AIMD RepairRestraint. width/cooldown/failCount are per-domain dials; inFlight counts the probes
// currently admitted-but-unresolved per domain (repair reports resolution via Record). It is only ever accessed from
// the disruption singleton reconciler, so it needs no locking.
type restraint struct {
	width           map[failureDomain]int
	cooldownUntil   map[failureDomain]time.Time
	failCount       map[failureDomain]int
	inFlight        map[failureDomain]int
	clock           clockNow
	cooldownFloor   time.Duration // first cooldown after a failure (provider RepairTiming.CooldownFloor)
	cooldownCeiling time.Duration // exponential-backoff cap (provider RepairTiming.CooldownCeiling)
}

// clockNow is the sliver of the clock restraint needs; repair passes its own clock so tests control time.
type clockNow interface{ Now() time.Time }

func newRestraint(clk clockNow, cooldownFloor, cooldownCeiling time.Duration) *restraint {
	return &restraint{
		width:           map[failureDomain]int{},
		cooldownUntil:   map[failureDomain]time.Time{},
		failCount:       map[failureDomain]int{},
		inFlight:        map[failureDomain]int{},
		clock:           clk,
		cooldownFloor:   cooldownFloor,
		cooldownCeiling: cooldownCeiling,
	}
}

// domainsOf returns the failure domains a candidate belongs to: its NodePool, its zone, and one per unhealthy
// (Status=False) node condition — the condition type is a proxy for the detector, so a bad detector flooding one
// condition cools that policy domain without touching an unrelated real fault. A candidate is in several domains at
// once, and pacing takes the most conservative across them. Reading conditions straight off the node keeps the
// interface a pure function of the candidate — restraint needs nothing threaded in from repair.
func domainsOf(c *Candidate) []failureDomain {
	domains := []failureDomain{
		{kind: "nodepool", value: c.NodePool.Name},
		{kind: "zone", value: c.zone},
	}
	if c.Node != nil {
		for _, cond := range c.Node.Status.Conditions {
			if cond.Status == corev1.ConditionFalse {
				domains = append(domains, failureDomain{kind: "policy", value: string(cond.Type)})
			}
		}
	}
	return domains
}

func (r *restraint) widthFor(d failureDomain) int {
	if w, ok := r.width[d]; ok && w > restraintWidthFloor {
		return w
	}
	return restraintWidthFloor
}

// CanDisrupt admits a candidate under two combine rules across its domains:
//   - Width — take the min: the candidate paces down if ANY domain is at its width ceiling of in-flight probes.
//   - Cooldown — eligible if ANY domain is out of cooldown: a fault in a fresh domain runs right away even when it
//     shares a backed-off domain with a flood.
//
// A CanDisrupt that returns true is treated as an admitted probe (its domains' in-flight counts increment), so repair
// must follow an admitted candidate with a Record once its outcome is known.
func (r *restraint) CanDisrupt(c *Candidate) bool {
	domains := domainsOf(c)

	eligibleByCooldown := false
	for _, d := range domains {
		if !r.clock.Now().Before(r.cooldownUntil[d]) {
			eligibleByCooldown = true
			break
		}
	}
	if !eligibleByCooldown {
		return false
	}
	for _, d := range domains {
		if r.inFlight[d] >= r.widthFor(d) {
			return false
		}
	}
	// Admitted: count it in-flight in every domain until repair reports the outcome via Record.
	for _, d := range domains {
		r.inFlight[d]++
	}
	return true
}

// Record folds a candidate's outcome back into the dials:
//   - Proven → double the width in every domain and clear their cooldown (speed up when it works).
//   - Failed → reset width to the floor and arm an exponentially-backed-off, capped cooldown (slow down when it doesn't).
//   - Cleared → the fault episode is over with no in-flight probe left; forget the domains' earned width so a new
//     correlated burst starts slow again (past success ≠ predictor of a new correlated failure).
func (r *restraint) Record(c *Candidate, replacement *corev1.Node, outcome RepairOutcome) {
	for _, d := range domainsOf(c) {
		switch outcome {
		case RepairProven:
			if r.inFlight[d] > 0 {
				r.inFlight[d]--
			}
			// Widen cautiously: only credit (speed up) a zone we actually tested — a replacement that came up healthy in a
			// different zone says nothing about the candidate's zone, so it must not widen it (the C2 false-signal).
			if r.credits(d, replacement) {
				r.width[d] = r.widthFor(d) * 2
				delete(r.cooldownUntil, d)
				delete(r.failCount, d)
			}
		case RepairFailed:
			if r.inFlight[d] > 0 {
				r.inFlight[d]--
			}
			// Back off quickly: a failure is always a reason to slow this candidate's domains, so cool every one of them
			// unconditionally. (Gating this on the replacement's zone too would let a re-flagged flood whose replacement
			// landed elsewhere slip back through the still-fresh zone via the any-domain-out-of-cooldown admission rule.)
			r.width[d] = restraintWidthFloor
			r.failCount[d]++
			backoff := r.cooldownFloor << (r.failCount[d] - 1)
			if backoff > r.cooldownCeiling || backoff <= 0 {
				backoff = r.cooldownCeiling
			}
			r.cooldownUntil[d] = r.clock.Now().Add(backoff)
		case RepairCleared:
			// Only forget a domain once nothing is in flight for it — an in-flight probe still owns its earned width.
			if r.inFlight[d] == 0 {
				delete(r.width, d)
				delete(r.failCount, d)
				delete(r.cooldownUntil, d)
			}
		case RepairClawedBack:
			// A previously-proven repair re-failed inside the watch window: undo the widen and arm backoff exactly like
			// RepairFailed, but do NOT touch inFlight (RepairProven already decremented it — a second decrement would
			// under-count this domain's live probes and let a flood slip through the width gate).
			r.width[d] = restraintWidthFloor
			r.failCount[d]++
			backoff := r.cooldownFloor << (r.failCount[d] - 1)
			if backoff > r.cooldownCeiling || backoff <= 0 {
				backoff = r.cooldownCeiling
			}
			r.cooldownUntil[d] = r.clock.Now().Add(backoff)
		}
	}
}

// credits reports whether a SUCCESS may speed up (widen) domain d. Only a zone (location) domain can be mis-credited,
// and only when the replacement came up in a DIFFERENT zone: a healthy replacement elsewhere says nothing about the
// candidate's (possibly impaired) zone, so it must not widen it — this is the network-partition false-signal the review
// flagged ("only learn about a failure domain if the replacement is in it"). A delete-only / launch-failed repair
// (replacement == nil) has no other zone to attribute to, so it credits the candidate's zone as before. NodePool (the
// replacement is always in the candidate's pool) and policy (the candidate's own condition — success means repairing
// that condition worked) are always the candidate's, so they are always credited. Backing off on failure is NOT gated
// by this — a failure always cools the candidate's own domains (see Record).
func (r *restraint) credits(d failureDomain, replacement *corev1.Node) bool {
	if d.kind != "zone" || replacement == nil {
		return true
	}
	return replacement.Labels[corev1.LabelTopologyZone] == d.value
}
