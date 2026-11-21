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
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/source"

	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
)

// revalidationPeriod is the longest the controller goes without running, and the longest a consolidation method skips
// its work while nothing it watches changes. Some inputs show only with time, or aren't in a store the controller
// watches: disruption budget schedules, repair policy tolerations, nominations lapsing, do-not-disrupt durations,
// PodDisruptionBudgets, instance type availability and prices, NodeOverlays and VPA predictions.
const revalidationPeriod = 5 * time.Minute

// changes counts the changes to the stores the disruption controller wakes on, so that a consolidation method that
// found nothing to do skips its work until one comes: it's the "woken since the method last ran" bit, kept per method
// as the count it last saw.
type changes struct{ n atomic.Uint64 }

func (c *changes) add()          { c.n.Add(1) }
func (c *changes) count() uint64 { return c.n.Load() }

// changeTracker is a method that skips its work while nothing changes, as consolidation does.
type changeTracker interface{ trackChanges(*changes) }

// wakes are the sources the controller wakes on. Each change of the NodeClaim and Node stores' inputs counts, as the
// sources enqueue it, so the run it wakes sees it; a NodeClaim launching, which can end the wait on NodeClaimsLaunched,
// doesn't count, since a launch also changes the NodeClaim's inputs, and neither does a NodePool's drift back-off
// window arming or lapsing, which only drift reads.
func (c *Controller) wakes() source.SyncingSource {
	return karpenterstate.Wake(
		c.cluster.NodeClaims.UnlaunchedSource(),
		c.nodePools.SourceOn(func(_ string, e karpentercluster.NodePoolEntry) any {
			// The window's level and end, as values the semantic comparison can compare.
			w := e.Intent.DriftBackoffWindow
			return [2]int64{int64(w.Value), lo.Ternary(w.Until.IsZero(), 0, w.Until.UnixNano())}
		}),
		karpenterstate.WakeCounting(c.changes.add, c.cluster.NodeClaims.DisruptionSource()),
	)
}
