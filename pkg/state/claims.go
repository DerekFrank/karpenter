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

// Claims hold values against the keys of a derived store for objects a caller is about to create, so that a decision
// against a limit counts what is already on its way. A Claim is held from Reserve until the Derivation counts the
// source it was bound to, until that source goes away, until it is released, or until it expires, whichever is first.
// Reserve, the handoff to the Derivation and Release all run under the derived store's lock, so the key's fields never
// count a claimed object zero times: at most twice, while a claim is bound to an object already counted.
//
// A held value H is a Contribution to the key's fields: Update from the zero H holds it, and Delete releases it. counts
// returns whether a source's contribution counts what a claim held for it, such as a NodeClaim's launched capacity for
// a claim of its largest instance type.
type Claims[K1, K comparable, F1, F any, C Contribution[F, C], H Contribution[F, H]] struct {
	derivation *Derivation[K1, K, F1, F, C]
	counts     func(C) bool
	ttl        time.Duration
	clock      clock.Clock

	live  map[*Claim[K1, K, H]]struct{}
	bound map[K1]*Claim[K1, K, H]
}

// Claim holds a value against a key until the source it is for is counted. Every Claim must end: Bind it to its source
// once created, Release it if the source won't be, or let it expire.
type Claim[K1, K comparable, H any] struct {
	bind    func(*Claim[K1, K, H], K1)
	release func(*Claim[K1, K, H])
	key     K
	held    H
	expires time.Time
	source  K1
	done    bool
}

// NewClaims returns Claims against the keys derivation feeds. A claim not counted within ttl of its Reserve expires.
func NewClaims[K1, K comparable, F1, F any, C Contribution[F, C], H Contribution[F, H]](derivation *Derivation[K1, K, F1, F, C], counts func(C) bool,
	ttl time.Duration, clk clock.Clock) *Claims[K1, K, F1, F, C, H] {
	c := &Claims[K1, K, F1, F, C, H]{
		derivation: derivation,
		counts:     counts,
		ttl:        ttl,
		clock:      clk,
		live:       map[*Claim[K1, K, H]]struct{}{},
		bound:      map[K1]*Claim[K1, K, H]{},
	}
	derivation.to.mu.Lock()
	derivation.settle = c.settleLocked
	derivation.to.cleared = append(derivation.to.cleared, c.clearLocked)
	derivation.to.mu.Unlock()
	return c
}

// Reserve holds up to n claims of value against key, one at a time while admit accepts key's fields with the claim
// held, and returns them. admit sees the fields as every write before it left them, including other claims.
func (c *Claims[K1, K, F1, F, C, H]) Reserve(key K, n int, value H, admit func(F) bool) []*Claim[K1, K, H] {
	store := c.derivation.to
	store.mu.Lock()
	changed := c.expireLocked()
	var claims []*Claim[K1, K, H]
	var zero H
	changed = append(changed, store.updateLocked(key, func(f F) F {
		for range n {
			next := value.Update(f, zero)
			if !admit(next) {
				break
			}
			f = next
			claim := &Claim[K1, K, H]{bind: c.bind, release: c.release, key: key, held: value, expires: c.clock.Now().Add(c.ttl)}
			c.live[claim] = struct{}{}
			claims = append(claims, claim)
		}
		return f
	}, 0)...)
	store.refs[key] += len(claims)
	subscribers := store.subscribers
	store.mu.Unlock()
	notify(subscribers, changed)
	return claims
}

// Held returns the value the claim holds, whether or not it still holds it.
func (cl *Claim[K1, K, H]) Held() H { return cl.held }

// Bind ties the claim to the source it was reserved for, once created. The claim is released when the Derivation
// counts that source, at once if it already has, or when the source goes away first.
func (cl *Claim[K1, K, H]) Bind(source K1) { cl.bind(cl, source) }

// Release releases the claim, for a source the caller won't create. It does nothing to a claim that has ended.
func (cl *Claim[K1, K, H]) Release() { cl.release(cl) }

func (c *Claims[K1, K, F1, F, C, H]) bind(claim *Claim[K1, K, H], source K1) {
	store := c.derivation.to
	store.mu.Lock()
	var changed []Change[K, F]
	if !claim.done {
		if r, ok := c.derivation.records[source]; ok && c.counts(r.value) {
			changed = c.releaseLocked(claim)
		}
		if !claim.done {
			if other, ok := c.bound[source]; ok && other != claim {
				changed = append(changed, c.releaseLocked(other)...)
			}
			claim.source = source
			c.bound[source] = claim
		}
	}
	subscribers := store.subscribers
	store.mu.Unlock()
	notify(subscribers, changed)
}

func (c *Claims[K1, K, F1, F, C, H]) release(claim *Claim[K1, K, H]) {
	store := c.derivation.to
	store.mu.Lock()
	changed := c.releaseLocked(claim)
	subscribers := store.subscribers
	store.mu.Unlock()
	notify(subscribers, changed)
}

// settleLocked releases the claim bound to source once the Derivation counts it, or once it's gone.
func (c *Claims[K1, K, F1, F, C, H]) settleLocked(source K1, value C, contributes bool) []Change[K, F] {
	claim, ok := c.bound[source]
	if !ok || (contributes && !c.counts(value)) {
		return nil
	}
	return c.releaseLocked(claim)
}

func (c *Claims[K1, K, F1, F, C, H]) releaseLocked(claim *Claim[K1, K, H]) []Change[K, F] {
	if claim.done {
		return nil
	}
	claim.done = true
	delete(c.live, claim)
	if c.bound[claim.source] == claim {
		delete(c.bound, claim.source)
	}
	return c.derivation.to.updateLocked(claim.key, claim.held.Delete, -1)
}

// clearLocked ends every claim without releasing what it held, for a derived store that's being cleared.
func (c *Claims[K1, K, F1, F, C, H]) clearLocked() {
	for claim := range c.live {
		claim.done = true
	}
	c.live = map[*Claim[K1, K, H]]struct{}{}
	c.bound = map[K1]*Claim[K1, K, H]{}
}

func (c *Claims[K1, K, F1, F, C, H]) expireLocked() []Change[K, F] {
	now := c.clock.Now()
	var changed []Change[K, F]
	for claim := range c.live {
		if !now.Before(claim.expires) {
			changed = append(changed, c.releaseLocked(claim)...)
		}
	}
	return changed
}
