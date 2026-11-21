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
	"math/rand"
	"sync"
	"time"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

const (
	// driftBackoffBaseDelay is the first back-off window applied to a NodePool after its first unrecoverable drift
	// replacement failure. Each failure after a window ends doubles it, up to driftBackoffMaxDelay.
	driftBackoffBaseDelay = 1 * time.Minute
	// driftBackoffMaxDelay is the ceiling on the back-off window, before jitter.
	driftBackoffMaxDelay = 10 * time.Minute
)

// DriftBackoff backs off drift disruption for a NodePool whose drift replacements fail. A NodePool is skipped by drift
// candidate selection while its window holds, so it stops monopolizing the single drift command each pass and stops
// burning launch attempts. Once the window ends the NodePool is eligible again; a successful replacement resets it, and
// a failure doubles the window, with jitter, up to the maximum. It keeps the back-off as intent on each NodePool's
// entry, through the DriftBackoffs handle, so it goes with the NodePool. A window's end notifies the NodePool's entry,
// which wakes the disruption controller.
//
// It's the handle's one writer: the queue records drift outcomes through it, and the controller resets the NodePools
// that have no drifted NodeClaims left. See designs/drift-per-nodepool-backoff.md.
type DriftBackoff struct {
	// mu serializes writes, so each computes from the back-off it replaces, and guards rand.
	mu        sync.Mutex
	handle    *karpentercluster.DriftBackoffs
	nodePools *karpentercluster.NodePoolState
	clock     clock.Clock
	rand      *rand.Rand
	base      time.Duration
	max       time.Duration
	maxLevel  int
}

// DriftBackoffOption configures a DriftBackoff.
type DriftBackoffOption func(*DriftBackoff)

// WithDriftBackoffRand makes jitter draw from r, so that it's reproducible in tests.
func WithDriftBackoffRand(r *rand.Rand) DriftBackoffOption {
	return func(b *DriftBackoff) { b.rand = r }
}

// WithDriftBackoffDelays overrides the base and maximum back-off windows, for tests.
func WithDriftBackoffDelays(base, max time.Duration) DriftBackoffOption {
	return func(b *DriftBackoff) { b.base, b.max = base, max }
}

// NewDriftBackoff returns the drift back-off of the NodePools, kept through handle.
func NewDriftBackoff(clk clock.Clock, nodePools *karpentercluster.NodePoolState, handle *karpentercluster.DriftBackoffs, opts ...DriftBackoffOption) *DriftBackoff {
	b := &DriftBackoff{
		handle:    handle,
		nodePools: nodePools,
		clock:     clk,
		rand:      rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // jitter does not need a cryptographic source
		base:      driftBackoffBaseDelay,
		max:       driftBackoffMaxDelay,
	}
	for _, opt := range opts {
		opt(b)
	}
	b.maxLevel = saturationLevel(b.base, b.max)
	return b
}

// Fail records an unrecoverable drift replacement failure for the NodePool, and arms or escalates its back-off window.
// It returns whether it did. A failure of an attempt started before the last failure that armed the window is ignored,
// even if it completes after that window ends, as is one while the window holds, or for a NodePool that's gone or was
// replaced under its name.
func (b *DriftBackoff) Fail(nodePool *v1.NodePool, attemptStartedAt time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.nodePools.Get(nodePool.Name, client.UnsafeDisableDeepCopy)
	if !ok || e.Object.UID != nodePool.UID {
		return false
	}
	now := b.clock.Now()
	current := e.Intent.DriftBackoff.Value
	if !current.LastFailure.IsZero() && !attemptStartedAt.After(current.LastFailure) {
		return false
	}
	if _, _, backedOff := karpentercluster.DriftBackoffAt(e, now); backedOff {
		return false
	}
	level := min(current.Level+1, b.maxLevel)
	return b.handle.BackOff(nodePool.Name, karpentercluster.DriftBackoff{Level: level, LastFailure: now}, now.Add(b.jitteredWindow(level)))
}

// Reset returns the NodePool to healthy, after a successful drift replacement.
func (b *DriftBackoff) Reset(nodePoolName string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handle.Reset(nodePoolName)
}

// ResetUndrifted resets the back-off, with its level, of every NodePool that has no drifted NodeClaim left that isn't
// deleting, so that its next drift starts over.
func (b *DriftBackoff) ResetUndrifted() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for name, e := range b.nodePools.List(client.UnsafeDisableDeepCopy) {
		if e.Intent.DriftBackoff.Value.Level > 0 && e.Derived.Drifted == 0 {
			b.handle.Reset(name)
		}
	}
}

// Get returns the NodePool's back-off level, the end of its window, and whether its window holds now. A level of 0 is
// a healthy NodePool.
func (b *DriftBackoff) Get(nodePool *v1.NodePool) (level int, until time.Time, backedOff bool) {
	e, ok := b.nodePools.Get(nodePool.Name, client.UnsafeDisableDeepCopy)
	if !ok || e.Object.UID != nodePool.UID {
		return 0, time.Time{}, false
	}
	_, until, backedOff = karpentercluster.DriftBackoffAt(e, b.clock.Now())
	return e.Intent.DriftBackoff.Value.Level, until, backedOff
}

// window returns the back-off window for a level, before jitter: base doubled for each level after the first, up to max.
func (b *DriftBackoff) window(level int) time.Duration {
	shift := max(level-1, 0)
	if shift >= 63 {
		return b.max
	}
	scaled := b.base << shift
	if scaled <= 0 || scaled >= b.max {
		return b.max
	}
	return scaled
}

// jitteredWindow is the level's window with equal jitter: uniform in [w/2, w), which keeps half the window while
// spreading out NodePools that fail at the same instant.
func (b *DriftBackoff) jitteredWindow(level int) time.Duration {
	w := b.window(level)
	half := w / 2
	if half <= 0 {
		return w
	}
	return half + time.Duration(b.rand.Int63n(int64(half)))
}

// saturationLevel returns the smallest level, from 1, whose window before jitter reaches max, so that the level stops
// growing once the window saturates.
func saturationLevel(base, max time.Duration) int {
	level := 1
	for base > 0 && level < 63 {
		if base<<(level-1) >= max {
			break
		}
		level++
	}
	return level
}

func driftBackoffEnabled(ctx context.Context, backoff *DriftBackoff) bool {
	return options.FromContext(ctx).FeatureGates.NodePoolDriftBackoff && backoff != nil
}

// resetUndriftedNodePoolBackoffs resets the back-off of every NodePool with no drifted NodeClaim left.
func resetUndriftedNodePoolBackoffs(ctx context.Context, backoff *DriftBackoff) {
	if !driftBackoffEnabled(ctx, backoff) {
		return
	}
	backoff.ResetUndrifted()
}

// initDriftBackoffMetrics registers a zero-valued back-off counter for each NodePool so the metric is visible (at 0)
// for healthy pools rather than being absent until the first back-off. Add(0) is idempotent: it only ensures the
// series exists and never clobbers an incremented value.
func initDriftBackoffMetrics(ctx context.Context, backoff *DriftBackoff, nodePoolNames ...string) {
	if !driftBackoffEnabled(ctx, backoff) {
		return
	}
	for _, nodePoolName := range nodePoolNames {
		DriftBackoffsTotal.Add(0, map[string]string{metrics.NodePoolLabel: nodePoolName})
	}
}

// isDriftBackedOff reports whether the NodePool is backed off after repeated unrecoverable drift replacement failures,
// publishing an event when it is.
func isDriftBackedOff(ctx context.Context, backoff *DriftBackoff, recorder events.Recorder, nodePool *v1.NodePool) bool {
	if !driftBackoffEnabled(ctx, backoff) {
		return false
	}
	level, until, backedOff := backoff.Get(nodePool)
	if backedOff {
		recorder.Publish(disruptionevents.NodePoolDriftBackoff(nodePool, until, level))
	}
	return backedOff
}
