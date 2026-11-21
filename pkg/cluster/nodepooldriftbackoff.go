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

package cluster

import (
	"time"

	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state"
)

// DriftBackoff is a NodePool's drift replacement back-off, intent on its entry: how far it has escalated, and the last
// failure that armed or escalated it. It holds until disruption resets it, and goes once the NodePool does.
type DriftBackoff struct {
	// Level is how many failures armed or escalated the back-off since it was last reset, up to the level whose window
	// saturates.
	Level int
	// LastFailure is when the last failure that armed or escalated the back-off was recorded.
	LastFailure time.Time
}

// DriftBackoffAt returns the NodePool's drift back-off window at now: its level, its end, and whether it holds then.
func DriftBackoffAt(e NodePoolEntry, now time.Time) (level int, until time.Time, backedOff bool) {
	level, backedOff = e.Intent.DriftBackoffWindow.At(now)
	if !backedOff || level == 0 {
		return 0, time.Time{}, false
	}
	return level, e.Intent.DriftBackoffWindow.Until, true
}

type nodePoolIntent[V any] = state.Intent[string, *v1.NodePool, NodePoolFields, NodePoolIntent, V]

// DriftBackoffs is the handle of the drift back-off on NodePools, which disruption, its one writer, holds. A window
// lapses at its end, and the entry notifies the lapse.
type DriftBackoffs struct {
	backoff *nodePoolIntent[DriftBackoff]
	window  *nodePoolIntent[int]
}

func newDriftBackoffs(clk clock.Clock, n *NodePoolState) *DriftBackoffs {
	return &DriftBackoffs{
		backoff: state.NewIntent(n.store, func(i *NodePoolIntent) *state.Intended[DriftBackoff] { return &i.DriftBackoff }),
		// Every window's end is given with SetUntil, so the TTL is never used.
		window: state.NewIntent(n.store, func(i *NodePoolIntent) *state.Intended[int] { return &i.DriftBackoffWindow }).WithExpiry(clk, 0),
	}
}

// BackOff records the NodePool's back-off and arms its window until until, at the back-off's level. It returns whether
// the NodePool's entry exists to hold them.
func (h *DriftBackoffs) BackOff(nodePoolName string, backoff DriftBackoff, until time.Time) bool {
	return h.backoff.Set(nodePoolName, backoff) && h.window.SetUntil(nodePoolName, backoff.Level, until)
}

// Reset clears the NodePool's back-off and its window.
func (h *DriftBackoffs) Reset(nodePoolName string) {
	h.window.Clear(nodePoolName)
	h.backoff.Clear(nodePoolName)
}
