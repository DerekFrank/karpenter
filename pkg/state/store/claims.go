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

package store

import (
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Claims hold values against a Store's keys for objects a caller is about to create, so that a decision against a
// limit counts what is already on its way. A Claim is held from Reserve until the Contributor counts the object it was
// bound to, until its object goes away, until it is released, or until it expires, whichever is first. Reserve, the
// handoff to the Contributor and Release all run under the store's lock, so the key's fields never count a claimed
// object zero times: at most twice, while a claim is bound to an object already counted.
//
// hold folds a change of a held value into a key's fields, as a Contributor's apply does, and must return new fields
// without modifying the ones it gets. counts returns whether an object's contribution counts what a claim held for it,
// such as a NodeClaim's launched capacity for a claim of its largest instance type.
type Claims[K comparable, S client.Object, F, C, H any] struct {
	contributor *Contributor[K, S, F, C]
	hold        func(fields F, prev, next H) F
	counts      func(C) bool
	ttl         time.Duration
	clock       clock.Clock

	live  map[*Claim[K, H]]struct{}
	bound map[types.NamespacedName]*Claim[K, H]
}

// Claim holds a value against a key until the object it is for is counted. Every Claim must end: Bind it to the object
// once created, Release it if the object won't be, or let it expire.
type Claim[K comparable, H any] struct {
	bind    func(*Claim[K, H], types.NamespacedName)
	release func(*Claim[K, H])
	key     K
	held    H
	expires time.Time
	name    types.NamespacedName
	done    bool
}

// NewClaims returns Claims against the keys contributor feeds. A claim not counted within ttl of its Reserve expires.
func NewClaims[K comparable, S client.Object, F, C, H any](contributor *Contributor[K, S, F, C], hold func(fields F, prev, next H) F, counts func(C) bool,
	ttl time.Duration, clk clock.Clock) *Claims[K, S, F, C, H] {
	c := &Claims[K, S, F, C, H]{
		contributor: contributor,
		hold:        hold,
		counts:      counts,
		ttl:         ttl,
		clock:       clk,
		live:        map[*Claim[K, H]]struct{}{},
		bound:       map[types.NamespacedName]*Claim[K, H]{},
	}
	contributor.store.mu.Lock()
	contributor.settle = c.settleLocked
	contributor.store.mu.Unlock()
	return c
}

// Reserve holds up to n claims of value against key, one at a time while admit accepts key's fields with the claim
// held, and returns them. admit sees the fields as every write before it left them, including other claims.
func (c *Claims[K, S, F, C, H]) Reserve(key K, n int, value H, admit func(F) bool) []*Claim[K, H] {
	store := c.contributor.store
	store.mu.Lock()
	changed := c.expireLocked()
	var claims []*Claim[K, H]
	var zero H
	changed = append(changed, store.updateLocked(key, func(f F) F {
		for range n {
			next := c.hold(f, zero, value)
			if !admit(next) {
				break
			}
			f = next
			claim := &Claim[K, H]{bind: c.bind, release: c.release, key: key, held: value, expires: c.clock.Now().Add(c.ttl)}
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
func (cl *Claim[K, H]) Held() H { return cl.held }

// Bind ties the claim to the object it was reserved for, once created. The claim is released when the Contributor
// counts that object, at once if it already has, or when the object goes away first.
func (cl *Claim[K, H]) Bind(name types.NamespacedName) { cl.bind(cl, name) }

// Release releases the claim, for an object the caller won't create. It does nothing to a claim that has ended.
func (cl *Claim[K, H]) Release() { cl.release(cl) }

func (c *Claims[K, S, F, C, H]) bind(claim *Claim[K, H], name types.NamespacedName) {
	store := c.contributor.store
	store.mu.Lock()
	var changed []change[K, F]
	if !claim.done {
		if uid, ok := c.contributor.sources.names[name]; ok {
			if r, ok := c.contributor.contributions[uid]; ok && c.counts(r.value) {
				changed = c.releaseLocked(claim)
			}
		}
		if !claim.done {
			if other, ok := c.bound[name]; ok && other != claim {
				changed = append(changed, c.releaseLocked(other)...)
			}
			claim.name = name
			c.bound[name] = claim
		}
	}
	subscribers := store.subscribers
	store.mu.Unlock()
	notify(subscribers, changed)
}

func (c *Claims[K, S, F, C, H]) release(claim *Claim[K, H]) {
	store := c.contributor.store
	store.mu.Lock()
	changed := c.releaseLocked(claim)
	subscribers := store.subscribers
	store.mu.Unlock()
	notify(subscribers, changed)
}

// settleLocked releases the claim bound to name once the Contributor counts the object, or once the object is gone.
func (c *Claims[K, S, F, C, H]) settleLocked(name types.NamespacedName, value C, contributes bool) []change[K, F] {
	claim, ok := c.bound[name]
	if !ok || (contributes && !c.counts(value)) {
		return nil
	}
	return c.releaseLocked(claim)
}

func (c *Claims[K, S, F, C, H]) releaseLocked(claim *Claim[K, H]) []change[K, F] {
	if claim.done {
		return nil
	}
	claim.done = true
	delete(c.live, claim)
	if c.bound[claim.name] == claim {
		delete(c.bound, claim.name)
	}
	var zero H
	return c.contributor.store.updateLocked(claim.key, func(f F) F { return c.hold(f, claim.held, zero) }, -1)
}

func (c *Claims[K, S, F, C, H]) expireLocked() []change[K, F] {
	now := c.clock.Now()
	var changed []change[K, F]
	for claim := range c.live {
		if !now.Before(claim.expires) {
			changed = append(changed, c.releaseLocked(claim)...)
		}
	}
	return changed
}
