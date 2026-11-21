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

// Intended is one intent's value in an entry's Intent fields, and when it lapses, for an intent made WithExpiry.
type Intended[V any] struct {
	Value V
	// Until is when the value lapses, or zero for a value that holds until it's cleared.
	Until time.Time
}

// At returns the value at now, and whether it holds then.
func (i Intended[V]) At(now time.Time) (V, bool) {
	if !i.Until.IsZero() && !now.Before(i.Until) {
		var zero V
		return zero, false
	}
	return i.Value, true
}

// Intent is the typed handle of one intent: a field of the Intent of a store's entries. Its holder is the intent's
// only writer, so pkg/cluster hands each handle to its one writer, as the deletion mark to disruption. An intent drops
// with its entry: Set on an entry that doesn't exist does nothing.
type Intent[K comparable, O, D, I, V any] struct {
	store *Store[K, O, D, I]
	field func(*I) *Intended[V]

	clock clock.Clock
	ttl   time.Duration
	// lapses are when each key's value lapses, and timer fires at the earliest, under the store's lock.
	lapses map[K]time.Time
	timer  clock.Timer
	stop   chan struct{}
	armed  time.Time
}

// NewIntent returns the handle of the intent in the field of the entries' Intent that field points to.
func NewIntent[K comparable, O, D, I, V any](store *Store[K, O, D, I], field func(*I) *Intended[V]) *Intent[K, O, D, I, V] {
	return &Intent[K, O, D, I, V]{store: store, field: field}
}

// WithExpiry makes each value Set lapse ttl after it's set, on clk. A lapse is an event: the handle clears the value
// from a timer on clk at the earliest lapse, and the entry notifies. Get and Intended.At read a value as lapsed as soon
// as clk passes its time. The timer stops once no value can lapse: each is cleared, or the store is. Call it before
// the handle is shared.
func (h *Intent[K, O, D, I, V]) WithExpiry(clk clock.Clock, ttl time.Duration) *Intent[K, O, D, I, V] {
	h.clock, h.ttl, h.lapses = clk, ttl, map[K]time.Time{}
	c := h.store.s
	c.lock()
	c.addSourceLocked(h, nil, h.clearLocked)
	c.unlock()
	return h
}

// clearLocked forgets every lapse and stops the timer, for a store that's being cleared.
func (h *Intent[K, O, D, I, V]) clearLocked() {
	h.lapses = map[K]time.Time{}
	h.disarmLocked()
}

// Set sets key's value, and returns whether the entry exists to hold it. A value made WithExpiry lapses ttl from now,
// which extends one that holds.
func (h *Intent[K, O, D, I, V]) Set(key K, value V) bool {
	var until time.Time
	if h.clock != nil {
		until = h.clock.Now().Add(h.ttl)
	}
	return h.SetUntil(key, value, until)
}

// SetUntil sets key's value to lapse at until, for an intent made WithExpiry whose holder decides each value's TTL,
// such as a nomination's from the request's options. It returns whether the entry exists to hold it.
func (h *Intent[K, O, D, I, V]) SetUntil(key K, value V, until time.Time) bool {
	c := h.store.s
	c.mu.Lock()
	defer c.unlock()
	next := Intended[V]{Value: value}
	if h.clock != nil {
		next.Until = until
	}
	ok := c.intentLocked(key, func(i I) I { *h.field(&i) = next; return i })
	if ok && h.clock != nil {
		h.lapses[key] = next.Until
		h.armLocked(next.Until)
	}
	return ok
}

// Update sets key's value to what update returns of its current value, under the store's lock, for a holder whose
// writers race, such as two controllers recording a NodePool's launch outcomes. It returns whether the entry exists.
func (h *Intent[K, O, D, I, V]) Update(key K, update func(V) V) bool {
	c := h.store.s
	c.mu.Lock()
	defer c.unlock()
	var until time.Time
	if h.clock != nil {
		until = h.clock.Now().Add(h.ttl)
	}
	ok := c.intentLocked(key, func(i I) I {
		f := h.field(&i)
		cur, _ := f.At(h.now())
		*f = Intended[V]{Value: update(cur), Until: until}
		return i
	})
	if ok && h.clock != nil {
		h.lapses[key] = until
		h.armLocked(until)
	}
	return ok
}

func (h *Intent[K, O, D, I, V]) now() time.Time {
	if h.clock == nil {
		return time.Time{}
	}
	return h.clock.Now()
}

// Clear clears key's value.
func (h *Intent[K, O, D, I, V]) Clear(key K) {
	c := h.store.s
	c.mu.Lock()
	defer c.unlock()
	c.intentLocked(key, func(i I) I { *h.field(&i) = Intended[V]{}; return i })
	if h.lapses != nil {
		delete(h.lapses, key)
		if len(h.lapses) == 0 {
			h.disarmLocked()
		}
	}
}

// Get returns key's value, the zero value if it's unset, and false if the entry doesn't exist or the value lapsed.
func (h *Intent[K, O, D, I, V]) Get(key K) (V, bool) {
	e, ok := h.store.Get(key)
	if !ok {
		var zero V
		return zero, false
	}
	i := *h.field(&e.Intent)
	if h.clock == nil {
		return i.Value, true
	}
	return i.At(h.clock.Now())
}

func (h *Intent[K, O, D, I, V]) armLocked(until time.Time) {
	if h.timer != nil && !h.armed.After(until) {
		return
	}
	h.disarmLocked()
	timer, stop := h.clock.NewTimer(until.Sub(h.clock.Now())), make(chan struct{})
	h.timer, h.stop, h.armed = timer, stop, until
	go func() {
		select {
		case <-timer.C():
			h.fire(timer)
		case <-stop:
		}
	}()
}

func (h *Intent[K, O, D, I, V]) disarmLocked() {
	if h.timer == nil {
		return
	}
	h.timer.Stop()
	close(h.stop)
	h.timer, h.stop, h.armed = nil, nil, time.Time{}
}

// fire clears the values that lapsed, which notifies their entries, and arms the timer at the next lapse.
func (h *Intent[K, O, D, I, V]) fire(timer clock.Timer) {
	c := h.store.s
	c.mu.Lock()
	defer c.unlock()
	if h.timer != timer {
		return
	}
	h.timer, h.stop, h.armed = nil, nil, time.Time{}
	now := h.clock.Now()
	var next time.Time
	for key, until := range h.lapses {
		if now.Before(until) {
			if next.IsZero() || until.Before(next) {
				next = until
			}
			continue
		}
		delete(h.lapses, key)
		c.intentLocked(key, func(i I) I {
			if f := h.field(&i); f.Until.Equal(until) {
				*f = Intended[V]{}
			}
			return i
		})
	}
	if !next.IsZero() {
		h.armLocked(next)
	}
}
