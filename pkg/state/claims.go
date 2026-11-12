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

package state

import (
	"time"

	"k8s.io/utils/clock"
)

// Claims hold values in the Derived fields of the entries a Derivation feeds, for objects a caller is about to create,
// so that a decision against a limit counts what is already on its way. A Claim is held from Reserve until the
// Derivation counts the source it was bound to, until that source stops contributing or goes away, until it is
// released, or until it expires, whichever is first. Reserve, the handoff to the Derivation and Release all run under
// the store's one lock, so the entry's fields never count a claimed object zero times: at most twice, while a claim is
// bound to an object already counted.
//
// The TTL bounds what a caller that never says what became of its object holds: one that dropped its work without
// releasing, whose create failed ambiguously, or whose object went before the Derivation saw it. A claim bound to a
// source the Derivation sees contributing doesn't expire, however long the source takes to count it, because the
// source ends it: the Derivation counts it, or sees it stop contributing or go. Expiry is an event: the Claims expire
// a claim when its TTL passes and notify the key.
//
// A held value H is a Contribution to the Derived fields: Update from the zero H holds it, and Delete releases it.
// counts returns whether a source's contribution counts what a claim held for it, such as a NodeClaim's launched
// capacity for a claim of its largest instance type.
type Claims[K1, K comparable, D any, C Contribution[D, C], H Contribution[D, H]] struct {
	derivation *Derivation[K1, K, D, C]
	counts     func(C) bool
	ttl        time.Duration
	clock      clock.Clock
	// onExpire, if set, is called under the store's lock for each claim that expires.
	onExpire func(key K, held H)

	live  map[*Claim[K1, K, H]]struct{}
	bound map[K1]*Claim[K1, K, H]

	// timer fires at armed, the earliest expiry among the live claims when it was armed, and stop ends the goroutine
	// waiting on it. Both are nil while no live claim expires, and are guarded by the store's lock.
	timer clock.Timer
	stop  chan struct{}
	armed time.Time
	// stopped is whether the Derivation stopped, after which the claims never expire.
	stopped bool
}

// Claim holds a value against a key until the source it is for is counted. Every Claim ends exactly once: Bind it to
// its source once created, Release it if the source won't be, or let it expire.
type Claim[K1, K comparable, H any] struct {
	bind    func(*Claim[K1, K, H], K1)
	release func(*Claim[K1, K, H])
	key     K
	held    H
	// expires is when the claim expires, or zero once it's bound to a source the Derivation sees contributing.
	expires time.Time
	source  K1
	done    bool
}

// NewClaims returns Claims against the keys derivation feeds. A claim whose source the Derivation hasn't seen
// contributing within ttl of its Reserve expires at its TTL: the Claims arm one timer, on clk, at the earliest expiry
// among their live claims, and end the claims due when it fires, notifying the store's subscribers. The timer stops
// once no live claim expires, the store is cleared, or the Derivation stops or is removed.
func NewClaims[K1, K comparable, D any, C Contribution[D, C], H Contribution[D, H]](derivation *Derivation[K1, K, D, C], counts func(C) bool,
	ttl time.Duration, clk clock.Clock) *Claims[K1, K, D, C, H] {
	c := &Claims[K1, K, D, C, H]{
		derivation: derivation,
		counts:     counts,
		ttl:        ttl,
		clock:      clk,
		live:       map[*Claim[K1, K, H]]struct{}{},
		bound:      map[K1]*Claim[K1, K, H]{},
	}
	to := derivation.to
	to.lock()
	derivation.settles = append(derivation.settles, c.settleLocked)
	derivation.removes = append(derivation.removes, func() {
		c.releaseAllLocked()
		to.removeSourceLocked(c)
	})
	derivation.stops = append(derivation.stops, c.stopLocked)
	to.addSourceLocked(c, nil, c.clearLocked)
	to.unlock()
	return c
}

// OnExpire makes Claims call fn, under the store's lock, for each claim that expires, such as to count claims whose
// caller never bound or released them. fn must not block. Call it before the Claims are shared.
func (c *Claims[K1, K, D, C, H]) OnExpire(fn func(key K, held H)) *Claims[K1, K, D, C, H] {
	c.onExpire = fn
	return c
}

// Reserve holds up to n claims of value against key, one at a time while admit accepts key's Derived fields with the
// claim held, and returns them. admit sees the fields as every write before it left them, including other claims, and
// checks every condition the decision depends on. n is the most the caller wants, never a shortfall computed from a
// read taken outside Reserve.
func (c *Claims[K1, K, D, C, H]) Reserve(key K, n int, value H, admit func(D) bool) []*Claim[K1, K, H] {
	return c.ReserveReplacing(key, n, value, nil, admit)
}

// ReserveReplacing is Reserve for objects that replace the sources in replacing, which stop contributing before the new
// objects count, such as the nodes a disruption replaces. admit sees key's fields without what replacing contribute to
// key, as the Derivation counts them under the same lock, so a replaced source that already stopped contributing isn't
// credited twice.
func (c *Claims[K1, K, D, C, H]) ReserveReplacing(key K, n int, value H, replacing []K1, admit func(D) bool) []*Claim[K1, K, H] {
	store := c.derivation.to
	store.lock()
	defer store.unlock()
	if c.derivation.removed {
		return nil
	}
	var claims []*Claim[K1, K, H]
	var zero H
	expires := c.clock.Now().Add(c.ttl)
	f := store.derivedOfLocked(key)
	view := f
	seen := make(map[K1]struct{}, len(replacing))
	for _, source := range replacing {
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		if r, ok := c.derivation.records[source]; ok && r.key == key {
			view = r.value.Delete(view)
		}
	}
	for range n {
		next := value.Update(view, zero)
		if !admit(next) {
			break
		}
		view = next
		f = value.Update(f, zero)
		claim := &Claim[K1, K, H]{bind: c.bind, release: c.release, key: key, held: value, expires: expires}
		c.live[claim] = struct{}{}
		claims = append(claims, claim)
	}
	if len(claims) == 0 {
		return nil
	}
	store.derivedLocked(key, func(D) D { return f }, len(claims), false)
	c.armLocked(expires)
	return claims
}

// Held returns the value the claim holds, whether or not it still holds it.
func (cl *Claim[K1, K, H]) Held() H { return cl.held }

// Bind ties the claim to the source it was reserved for, once created. The claim is released when the Derivation
// counts that source, at once if it already has, or when the source stops contributing or goes away first. Once the
// Derivation sees the source contributing, the claim doesn't expire.
func (cl *Claim[K1, K, H]) Bind(source K1) { cl.bind(cl, source) }

// Release releases the claim, for a source the caller won't create. It does nothing to a claim that has ended.
func (cl *Claim[K1, K, H]) Release() { cl.release(cl) }

func (c *Claims[K1, K, D, C, H]) bind(claim *Claim[K1, K, H], source K1) {
	store := c.derivation.to
	store.lock()
	defer store.unlock()
	if claim.done {
		return
	}
	r, seen := c.derivation.records[source]
	if seen && c.counts(r.value) {
		c.releaseLocked(claim)
		return
	}
	if other, ok := c.bound[source]; ok && other != claim {
		c.releaseLocked(other)
	}
	claim.source = source
	c.bound[source] = claim
	if seen {
		claim.expires = time.Time{}
	}
}

func (c *Claims[K1, K, D, C, H]) release(claim *Claim[K1, K, H]) {
	store := c.derivation.to
	store.lock()
	defer store.unlock()
	c.releaseLocked(claim)
}

// settleLocked releases the claim bound to source once the Derivation counts it, or once it stops contributing or is
// gone. A claim whose source contributes without counting it no longer expires.
func (c *Claims[K1, K, D, C, H]) settleLocked(source K1, value C, contributes bool) {
	claim, ok := c.bound[source]
	if !ok {
		return
	}
	if contributes && !c.counts(value) {
		claim.expires = time.Time{}
		return
	}
	c.releaseLocked(claim)
}

func (c *Claims[K1, K, D, C, H]) releaseLocked(claim *Claim[K1, K, H]) {
	if claim.done {
		return
	}
	claim.done = true
	delete(c.live, claim)
	if c.bound[claim.source] == claim {
		delete(c.bound, claim.source)
	}
	if len(c.live) == 0 {
		c.disarmLocked()
	}
	c.derivation.to.derivedLocked(claim.key, claim.held.Delete, -1, false)
}

// releaseAllLocked releases every claim, for a Derivation that's being removed.
func (c *Claims[K1, K, D, C, H]) releaseAllLocked() {
	for claim := range c.live {
		c.releaseLocked(claim)
	}
}

// clearLocked ends every claim without releasing what it held, for a store that's being cleared.
func (c *Claims[K1, K, D, C, H]) clearLocked() {
	for claim := range c.live {
		claim.done = true
	}
	c.live = map[*Claim[K1, K, H]]struct{}{}
	c.bound = map[K1]*Claim[K1, K, H]{}
	c.disarmLocked()
}

// stopLocked stops the timer for good, for a Derivation that stopped following its source: its claims no longer
// expire.
func (c *Claims[K1, K, D, C, H]) stopLocked() {
	c.stopped = true
	c.disarmLocked()
}

// armLocked arms the timer to fire at expires, unless it's armed to fire by then already.
func (c *Claims[K1, K, D, C, H]) armLocked(expires time.Time) {
	if c.stopped || expires.IsZero() || (c.timer != nil && !c.armed.After(expires)) {
		return
	}
	c.disarmLocked()
	timer, stop := c.clock.NewTimer(expires.Sub(c.clock.Now())), make(chan struct{})
	c.timer, c.stop, c.armed = timer, stop, expires
	go func() {
		select {
		case <-timer.C():
			c.fire(timer)
		case <-stop:
		}
	}()
}

func (c *Claims[K1, K, D, C, H]) disarmLocked() {
	if c.timer == nil {
		return
	}
	c.timer.Stop()
	close(c.stop)
	c.timer, c.stop, c.armed = nil, nil, time.Time{}
}

// fire ends the claims whose TTL has passed, notifies the store's subscribers, and arms the timer at the next expiry. A
// timer that was disarmed or replaced after it fired does nothing.
func (c *Claims[K1, K, D, C, H]) fire(timer clock.Timer) {
	store := c.derivation.to
	store.lock()
	defer store.unlock()
	if c.timer != timer {
		return
	}
	c.timer, c.stop, c.armed = nil, nil, time.Time{}
	now := c.clock.Now()
	var next time.Time
	for claim := range c.live {
		if claim.expires.IsZero() {
			continue
		}
		if now.Before(claim.expires) {
			if next.IsZero() || claim.expires.Before(next) {
				next = claim.expires
			}
			continue
		}
		if c.onExpire != nil {
			c.onExpire(claim.key, claim.held)
		}
		c.releaseLocked(claim)
	}
	c.armLocked(next)
}
