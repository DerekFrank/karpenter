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
	"testing"
	"time"
)

// Unit tests for the shared correlated-failure restraint Window. Pure logic — no envtest — so they run even without
// the API server the Ginkgo suite needs.

func newTestWindow(now *time.Time) *Window {
	return NewWindow(func() time.Time { return *now })
}

func TestWindowColdStartsAtFloor(t *testing.T) {
	now := time.Now()
	w := newTestWindow(&now)
	d := failureDomain{kind: "zone", value: "us-west-2a"}
	if got := w.Width(d); got != restraintWidthFloor {
		t.Fatalf("cold-start Width = %d, want floor %d", got, restraintWidthFloor)
	}
	if got := w.Backoff(d); got != 0 {
		t.Fatalf("cold-start Backoff = %v, want 0", got)
	}
	if !w.Admits([]failureDomain{d}, map[failureDomain]int{}) {
		t.Fatal("expected admit at inFlight=0, width=1")
	}
	if w.Admits([]failureDomain{d}, map[failureDomain]int{d: 1}) {
		t.Fatal("expected NO admit at inFlight=1, width=1 (min-width)")
	}
}

func TestWindowWidenDoubles(t *testing.T) {
	now := time.Now()
	w := newTestWindow(&now)
	d := failureDomain{kind: "nodepool", value: "gpu"}
	w.Widen(d)
	if got := w.Width(d); got != 2 {
		t.Fatalf("Width after one Widen = %d, want 2", got)
	}
	w.Widen(d)
	if got := w.Width(d); got != 4 {
		t.Fatalf("Width after two Widen = %d, want 4", got)
	}
	if !w.Admits([]failureDomain{d}, map[failureDomain]int{d: 3}) {
		t.Fatal("expected admit at inFlight=3, width=4")
	}
	if w.Admits([]failureDomain{d}, map[failureDomain]int{d: 4}) {
		t.Fatal("expected NO admit at inFlight=4, width=4")
	}
}

func TestWindowSlamShutFloorsAndCools(t *testing.T) {
	now := time.Now()
	w := newTestWindow(&now)
	d := failureDomain{kind: "policy", value: "Ready"}
	w.Widen(d)
	w.Widen(d) // width 4
	w.SlamShut(d)
	if got := w.Width(d); got != restraintWidthFloor {
		t.Fatalf("Width after SlamShut = %d, want floor %d", got, restraintWidthFloor)
	}
	if got := w.Backoff(d); got != restraintCooldownFloor {
		t.Fatalf("Backoff after first SlamShut = %v, want %v", got, restraintCooldownFloor)
	}
	if w.Admits([]failureDomain{d}, map[failureDomain]int{}) {
		t.Fatal("expected NO admit while in cooldown")
	}
	now = now.Add(restraintCooldownFloor)
	if !w.Admits([]failureDomain{d}, map[failureDomain]int{}) {
		t.Fatal("expected admit after cooldown elapsed")
	}
}

func TestWindowBackoffExponentialCapped(t *testing.T) {
	now := time.Now()
	w := newTestWindow(&now)
	d := failureDomain{kind: "zone", value: "z"}
	// 1m, 2m, 4m, 8m, then capped at 10m.
	for i, want := range []time.Duration{1, 2, 4, 8, 10, 10} {
		w.SlamShut(d)
		if got := w.Backoff(d); got != want*time.Minute {
			t.Fatalf("SlamShut #%d Backoff = %v, want %v", i+1, got, want*time.Minute)
		}
	}
}

func TestWindowAnyDomainReady(t *testing.T) {
	now := time.Now()
	w := newTestWindow(&now)
	cooled := failureDomain{kind: "policy", value: "flood"}
	fresh := failureDomain{kind: "zone", value: "healthy-zone"}
	w.SlamShut(cooled)
	// A candidate in BOTH domains is eligible because ANY domain (fresh) is out of cooldown.
	if !w.Admits([]failureDomain{cooled, fresh}, map[failureDomain]int{}) {
		t.Fatal("expected admit: at least one domain (fresh) is ready")
	}
	// But if the fresh domain has no width headroom, min-width still blocks.
	if w.Admits([]failureDomain{cooled, fresh}, map[failureDomain]int{fresh: 1}) {
		t.Fatal("expected NO admit: fresh domain at its width ceiling (min-width)")
	}
}

func TestWindowResetForgetsEarnedState(t *testing.T) {
	now := time.Now()
	w := newTestWindow(&now)
	d := failureDomain{kind: "nodepool", value: "gpu"}
	w.Widen(d)
	w.Widen(d) // width 4
	w.Reset(d)
	if got := w.Width(d); got != restraintWidthFloor {
		t.Fatalf("Width after Reset = %d, want floor %d (new episode starts slow)", got, restraintWidthFloor)
	}
	if got := w.Backoff(d); got != 0 {
		t.Fatalf("Backoff after Reset = %v, want 0", got)
	}
}
