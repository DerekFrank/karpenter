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
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Correlated-failure restraint (Node Repair Under Correlated Failure, F3). Per failure domain, repair opens up
// geometrically while replacements prove out and backs off sharply when they don't — floored at one probe and capped
// at a max cooldown, so it always keeps trying but never stampedes. It layers strictly *beneath* the disruption budget.
//
// Statelessness (invariant): the store is an optimization, never a correctness requirement. A freshly restarted
// controller rebuilds an empty store, and an empty store cold-starts every domain at width 1 — the most conservative
// value — so a zero-state restart is always safe.
const (
	restraintWidthFloor = 1                // never 0: repair always eventually retries (invariant: pause, don't stop)
	restraintDwell      = 5 * time.Minute  // a probe is proven only after its replacement holds healthy this long
	restraintCooldownT0 = 1 * time.Minute  // initial per-domain cooldown after a failure
	restraintCooldownTMax = 10 * time.Minute // cooldown ceiling — so repair always eventually retries in a domain
)

// domainKind enumerates the failure domains repair scopes restraint to. A correlated burst collapses into one domain
// that cold-starts at width 1, so correlation is handled by scoping + cold-start, not by a correlation statistic.
type failureDomain struct {
	kind  string // "nodepool" | "zone" | "policy"
	value string
}

// probe is a single in-flight, not-yet-proven repair. Success is delayed: a probe counts as proven only once its
// replacement has held healthy for restraintDwell after the original was terminated.
type probe struct {
	nodeClaimName string
	domains       []failureDomain
	succeededAt   time.Time // when the candidate was observed terminated (replacement came up); zero until then
}

// restraint holds the per-domain dials plus the set of in-flight probes. All access is guarded so the Repair method
// (disruption loop) and any future queue callback don't race.
type restraint struct {
	mu            sync.Mutex
	width         map[failureDomain]int
	cooldownUntil map[failureDomain]time.Time
	failCount     map[failureDomain]int
	probes        map[string]*probe // keyed by candidate providerID
}

func newRestraint() *restraint {
	return &restraint{
		width:         map[failureDomain]int{},
		cooldownUntil: map[failureDomain]time.Time{},
		failCount:     map[failureDomain]int{},
		probes:        map[string]*probe{},
	}
}

// domainsOf returns the failure domains a candidate belongs to. A candidate is in several at once, and pacing takes the
// most conservative across them.
func domainsOf(c *Candidate, policyConditionType string) []failureDomain {
	return []failureDomain{
		{kind: "nodepool", value: c.NodePool.Name},
		{kind: "zone", value: c.zone},
		{kind: "policy", value: policyConditionType},
	}
}

func (r *restraint) widthFor(d failureDomain) int {
	if w, ok := r.width[d]; ok && w > restraintWidthFloor {
		return w
	}
	return restraintWidthFloor
}

// inFlight counts not-yet-proven probes whose domain set includes d.
func (r *restraint) inFlight(d failureDomain) int {
	n := 0
	for _, p := range r.probes {
		for _, pd := range p.domains {
			if pd == d {
				n++
				break
			}
		}
	}
	return n
}

// admit reports whether a candidate may probe now. Two combine rules across its domains:
//   - Width — take the min: the candidate paces down if ANY of its domains is at its width ceiling of in-flight probes.
//   - Cooldown — eligible if ANY domain is out of cooldown: a fault in a fresh domain runs right away even when it
//     shares a backed-off domain with a flood.
func (r *restraint) admit(c *Candidate, policyConditionType string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	domains := domainsOf(c, policyConditionType)

	eligibleByCooldown := false
	for _, d := range domains {
		if !now.Before(r.cooldownUntil[d]) {
			eligibleByCooldown = true
			break
		}
	}
	if !eligibleByCooldown {
		return false
	}
	// Width: the most-constrained domain gates. Admit only if every domain has in-flight headroom under its width.
	for _, d := range domains {
		if r.inFlight(d) >= r.widthFor(d) {
			return false
		}
	}
	return true
}

// recordProbe registers a newly-issued repair as an in-flight probe.
func (r *restraint) recordProbe(c *Candidate, policyConditionType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes[c.ProviderID()] = &probe{nodeClaimName: c.NodeClaim.Name, domains: domainsOf(c, policyConditionType)}
}

// observe feeds probe outcomes back into the dials. It reads the current state of each in-flight probe:
//   - still enqueued (queue holds it until the replacement initializes) → pending, no change.
//   - dequeued and the original NodeClaim is gone → the replacement came up and the original was terminated; once it
//     has held for the dwell, the probe is proven → double the width in every domain and clear their cooldown.
//   - dequeued and the original NodeClaim still exists → the command failed (e.g. the replacement never came up healthy
//     — a bad-AMI loop, a partitioned zone) → reset width to the floor and back the cooldown off exponentially.
func (r *restraint) observe(ctx context.Context, kubeClient client.Client, queue *Queue, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for providerID, p := range r.probes {
		if queue.HasAny(providerID) {
			continue // replacement still launching/waiting; outcome not yet known
		}
		nc := &v1.NodeClaim{}
		err := kubeClient.Get(ctx, types.NamespacedName{Name: p.nodeClaimName}, nc)
		switch {
		case errors.IsNotFound(err):
			// The original was terminated — the replacement proved healthy enough for the queue to proceed. Require the
			// dwell to elapse before counting it as a durable success (invariant: success = healthy for a while).
			if p.succeededAt.IsZero() {
				p.succeededAt = now
			}
			if !now.Before(p.succeededAt.Add(restraintDwell)) {
				for _, d := range p.domains {
					r.width[d] = r.widthFor(d) * 2
					delete(r.cooldownUntil, d)
					delete(r.failCount, d)
				}
				delete(r.probes, providerID)
			}
		case err == nil:
			// The command completed without terminating the original — a failed repair. Cut width to the floor and arm
			// an exponentially-backed-off, capped cooldown so repair pauses in this domain but never stops.
			for _, d := range p.domains {
				r.width[d] = restraintWidthFloor
				r.failCount[d]++
				backoff := restraintCooldownT0 << (r.failCount[d] - 1)
				if backoff > restraintCooldownTMax || backoff <= 0 {
					backoff = restraintCooldownTMax
				}
				r.cooldownUntil[d] = now.Add(backoff)
			}
			delete(r.probes, providerID)
		default:
			// transient get error; leave the probe to be re-observed next pass
		}
	}
}
