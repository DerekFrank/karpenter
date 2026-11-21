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

package test

import (
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
)

// ClockView is a shared fake clock as one component under test sees it. It reads, steps and waits on the shared clock,
// but HasWaiters and Waiters count only what the component waits on through the view, so that a test waiting for the
// component to block on the clock waits for that, not for a timer another component, such as cluster state, armed.
type ClockView struct {
	*clocktesting.FakeClock
	waiters atomic.Int64
}

// NewClockView returns a view of shared for the component it's given to.
func NewClockView(shared *clocktesting.FakeClock) *ClockView {
	return &ClockView{FakeClock: shared}
}

// HasWaiters returns whether anything waits on the clock through the view.
func (v *ClockView) HasWaiters() bool {
	return v.Waiters() > 0
}

// Waiters returns the number of waiters on the clock made through the view that haven't fired or stopped.
func (v *ClockView) Waiters() int {
	return int(v.waiters.Load())
}

// NewTimer is the shared clock's NewTimer, counted by the view until it fires or stops.
func (v *ClockView) NewTimer(d time.Duration) clock.Timer {
	return v.AfterFunc(d, func() {})
}

// After is the shared clock's After, counted by the view until it fires.
func (v *ClockView) After(d time.Duration) <-chan time.Time {
	return v.NewTimer(d).C()
}

// Sleep is the shared clock's Sleep, counted by the view until it returns.
func (v *ClockView) Sleep(d time.Duration) {
	<-v.After(d)
}

// AfterFunc is the shared clock's AfterFunc, counted by the view until it fires or stops. The shared clock runs cb, and
// the view's count, synchronously in the Step that fires it, so the count is never stale once Step returns.
func (v *ClockView) AfterFunc(d time.Duration, cb func()) clock.Timer {
	t := &viewTimer{view: v}
	t.arm()
	t.Timer = v.FakeClock.AfterFunc(d, func() {
		t.disarm()
		cb()
	})
	return t
}

// Tick is the shared clock's Tick. A tick channel waits on the shared clock for good, so the view counts it for good.
func (v *ClockView) Tick(d time.Duration) <-chan time.Time {
	if d > 0 {
		v.waiters.Add(1)
	}
	return v.FakeClock.Tick(d)
}

// NewTicker is the shared clock's NewTicker. A ticker waits on the shared clock for good, even once stopped, so the
// view counts it for good.
func (v *ClockView) NewTicker(d time.Duration) clock.Ticker {
	v.waiters.Add(1)
	return v.FakeClock.NewTicker(d)
}

// viewTimer is a timer on the shared clock that the view counts while it's armed.
type viewTimer struct {
	clock.Timer
	view  *ClockView
	armed atomic.Bool
}

func (t *viewTimer) arm() {
	if t.armed.CompareAndSwap(false, true) {
		t.view.waiters.Add(1)
	}
}

func (t *viewTimer) disarm() {
	if t.armed.CompareAndSwap(true, false) {
		t.view.waiters.Add(-1)
	}
}

func (t *viewTimer) Stop() bool {
	defer t.disarm()
	return t.Timer.Stop()
}

func (t *viewTimer) Reset(d time.Duration) bool {
	t.arm()
	return t.Timer.Reset(d)
}
