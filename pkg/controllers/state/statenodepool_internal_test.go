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
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/awslabs/operatorpkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// TestNodePoolStateMatchesARecount applies random NodeClaim updates, deletes, NodePool moves and stale versions, and
// checks the incremental counts against a recount of the latest version of each NodeClaim after every change.
//
//nolint:gocyclo
func TestNodePoolStateMatchesARecount(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic seed for reproducible test inputs
	n := NewNodePoolState()
	latest := map[string]*v1.NodeClaim{}
	var rv uint64
	for i := range 20000 {
		name := fmt.Sprintf("nc-%d", r.IntN(50))
		switch op := r.IntN(10); {
		case op == 0:
			n.DeleteNodeClaim(name)
			delete(latest, name)
		case op == 1 && latest[name] != nil:
			// A stale version arriving after a newer one doesn't count.
			current, _ := strconv.ParseUint(latest[name].ResourceVersion, 10, 64)
			if current > 1 {
				stale := latest[name].DeepCopy()
				stale.ResourceVersion = strconv.FormatUint(1+r.Uint64N(current-1), 10)
				stale.Labels[v1.NodePoolLabelKey] = "stale"
				n.UpdateNodeClaim(stale)
			}
		default:
			rv++
			nc := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: strconv.FormatUint(rv, 10), Labels: map[string]string{
				v1.NodePoolLabelKey: []string{"a", "b", ""}[r.IntN(3)],
			}}}
			if r.IntN(3) == 0 {
				nc.DeletionTimestamp = &metav1.Time{}
			}
			if r.IntN(3) == 0 {
				nc.Status.Conditions = []status.Condition{{Type: v1.ConditionTypeDisruptionReason, Status: metav1.ConditionTrue}}
			}
			n.UpdateNodeClaim(nc)
			latest[name] = nc
		}
		want := map[string]NodeClaimCounts{}
		for _, nc := range latest {
			if np := nc.Labels[v1.NodePoolLabelKey]; np != "" {
				counts := want[np]
				if deleting(nc) {
					counts.Deleting++
				} else {
					counts.Active++
				}
				want[np] = counts
			}
		}
		for _, np := range []string{"a", "b", "stale"} {
			if active, deleting, _ := n.GetNodeCount(np); active != want[np].Active || deleting != want[np].Deleting {
				t.Fatalf("after change %d, %s counts %d active and %d deleting, want %+v", i, np, active, deleting, want[np])
			}
		}
	}
}
