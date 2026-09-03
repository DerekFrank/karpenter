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
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Correlated-failure restraint (Node Repair Under Correlated Failure, RFC kubernetes-sigs/karpenter#3031). Per failure
// domain, repair opens up geometrically while replacements prove out and slams shut when they don't — floored at one
// probe and capped at a max cooldown, so it always keeps trying but never stampedes. It layers strictly *beneath* the
// disruption budget, and replaces the placeholder per-NodePool backoff the voluntary-repair POC carried in its score.
//
// The dial state lives in a Window shared between two callers: repair's disruption loop READS it (Width/Backoff/Admits)
// to pace admission, and the clawback controller WRITES it (Widen when a repair replacement proves Ready, SlamShut when
// one re-breaks inside the clawback window). Because both hold the same instance from different goroutines it is
// mutex-guarded.
//
// Statelessness (invariant): the dials are an optimization, never a correctness requirement. A freshly restarted
// controller rebuilds an empty Window, and an empty Window cold-starts every domain at width 1 — the most conservative
// value — so a zero-state restart is always safe.
const (
	// restraintWidthFloor is never 0: repair always eventually retries (invariant: pause, don't stop).
	restraintWidthFloor = 1
	// restraintCooldownFloor / restraintCooldownCeiling bound the per-domain failure backoff (first cooldown after a
	// slam, and its exponential cap so repair always eventually retries in a domain).
	restraintCooldownFloor   = 1 * time.Minute
	restraintCooldownCeiling = 10 * time.Minute
	// restraintClawbackWindow is how long an optimistically-credited repair replacement is watched for a re-break before
	// the credit becomes final. It must exceed the time for a re-inherited fault to resurface (kubelet/agent re-flag
	// latency), or a systematic false positive slips a widened burst through before clawback fires.
	restraintClawbackWindow = 5 * time.Minute
)

// failureDomain is one axis restraint scopes to (NodePool, zone, or policy-condition). A correlated burst collapses into
// one domain that cold-starts at width 1, so correlation is handled by scoping + cold-start, not a correlation statistic.
type failureDomain struct {
	kind  string // "nodepool" | "zone" | "policy"
	value string
}

// domainsForNode returns the failure domains a node belongs to: its NodePool, its zone, and one per unhealthy
// (Status=False) node condition — the condition type is a proxy for the detector, so a bad detector flooding one
// condition cools that policy domain without touching an unrelated real fault. A node is in several domains at once,
// and pacing takes the most conservative across them. Reading conditions straight off the node keeps this a pure
// function of observable state — usable from a Candidate (admission) and from a bare StateNode (in-flight derivation /
// the clawback controller), with nothing threaded in from repair.
func domainsForNode(nodePool, zone string, node *corev1.Node) []failureDomain {
	domains := []failureDomain{
		{kind: "nodepool", value: nodePool},
		{kind: "zone", value: zone},
	}
	if node != nil {
		for _, cond := range node.Status.Conditions {
			if cond.Status == corev1.ConditionFalse {
				domains = append(domains, failureDomain{kind: "policy", value: string(cond.Type)})
			}
		}
	}
	return domains
}

// domainsOf returns the failure domains a candidate belongs to.
func domainsOf(c *Candidate) []failureDomain {
	return domainsForNode(c.NodePool.Name, c.zone, c.Node)
}

// Window is the shared per-domain slow-start + backoff dial ("the window/backoff tracker"). It owns width (how many
// concurrent unproven repairs a domain allows) and cooldown per failure domain — nothing else. In-flight counts are
// state-derived by the reader; domain membership is caller-supplied. Empty == every domain at the floor.
type Window struct {
	mu        sync.Mutex
	width     map[failureDomain]int
	coolUntil map[failureDomain]time.Time
	failCount map[failureDomain]int
	now       func() time.Time
}

func NewWindow(now func() time.Time) *Window {
	return &Window{
		width:     map[failureDomain]int{},
		coolUntil: map[failureDomain]time.Time{},
		failCount: map[failureDomain]int{},
		now:       now,
	}
}

func (w *Window) widthLocked(d failureDomain) int {
	if v, ok := w.width[d]; ok && v > restraintWidthFloor {
		return v
	}
	return restraintWidthFloor
}

// Width is the current window for a domain (>= floor). Reader side.
func (w *Window) Width(d failureDomain) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.widthLocked(d)
}

// Backoff is the remaining cooldown for a domain; 0 == ready. Reader side (also the backoff_seconds gauge).
func (w *Window) Backoff(d failureDomain) time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if until, ok := w.coolUntil[d]; ok {
		if rem := until.Sub(w.now()); rem > 0 {
			return rem
		}
	}
	return 0
}

// Admits applies the two combine rules across a candidate's domains, given the current per-domain in-flight count:
//   - Cooldown — eligible if ANY domain is out of cooldown: a fault in a fresh domain runs right away even when it
//     shares a backed-off domain with a flood.
//   - Width — take the min: admit only if EVERY domain has in-flight headroom under its width.
//
// Admits is a pure read: it does not mutate the dials (in-flight is derived from cluster state, not counted here), so it
// is safe to call once per candidate per pass.
func (w *Window) Admits(domains []failureDomain, inFlight map[failureDomain]int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	ready := false
	for _, d := range domains {
		if until, ok := w.coolUntil[d]; !ok || !now.Before(until) {
			ready = true
			break
		}
	}
	if !ready {
		return false
	}
	for _, d := range domains {
		if inFlight[d] >= w.widthLocked(d) {
			return false
		}
	}
	return true
}

// Widen credits a proven success: double the width in every domain and clear its cooldown (speed up when it works).
// Writer side — called by the clawback controller when a repair replacement holds Ready.
func (w *Window) Widen(domains ...failureDomain) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range domains {
		w.width[d] = w.widthLocked(d) * 2
		delete(w.coolUntil, d)
		delete(w.failCount, d)
	}
}

// SlamShut records a failure/clawback: reset width to the floor and arm an exponentially-backed-off, capped cooldown so
// repair pauses in the domain but never stops. Writer side — called by the clawback controller when a repair
// replacement re-breaks inside the clawback window.
func (w *Window) SlamShut(domains ...failureDomain) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	for _, d := range domains {
		w.width[d] = restraintWidthFloor
		w.failCount[d]++
		backoff := restraintCooldownFloor << (w.failCount[d] - 1)
		if backoff > restraintCooldownCeiling || backoff <= 0 {
			backoff = restraintCooldownCeiling
		}
		w.coolUntil[d] = now.Add(backoff)
	}
}

// Reset forgets a domain's earned width/cooldown (episode over: the domain has no unhealthy nodes), so a new correlated
// burst starts slow again — past success is not a predictor of a new correlated failure.
func (w *Window) Reset(domains ...failureDomain) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range domains {
		delete(w.width, d)
		delete(w.coolUntil, d)
		delete(w.failCount, d)
	}
}
