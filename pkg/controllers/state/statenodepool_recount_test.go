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

package state_test

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	fakecp "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("NodePoolState Recount", func() {
	// Applies random NodeClaim updates, deletes, NodePool moves and stale versions through cluster state, and checks
	// the NodePools' counts against a recount of the latest version of each NodeClaim after every change.
	//
	//nolint:gocyclo
	It("should match a recount of the latest version of each NodeClaim after every change", func() {
		r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic seed for reproducible test inputs
		clk := clock.NewFakeClock(time.Now())
		cs := karpentercluster.New(ctx, clk, nil, nil, fakecp.NewCloudProvider())
		c := state.NewCluster(clk, fake.NewClientBuilder().Build(), cs.Nodes, cs.NodePoolState, cs.CapacityQuotaState, cs.PodScheduling)
		latest := map[string]*v1.NodeClaim{}
		var rv, uid uint64
		for i := range 20000 {
			name := fmt.Sprintf("nc-%d", r.IntN(50))
			switch op := r.IntN(10); {
			case op == 0:
				c.DeleteNodeClaim(name)
				delete(latest, name)
			case op == 1 && latest[name] != nil:
				// A stale version arriving after a newer one doesn't count.
				current, _ := strconv.ParseUint(latest[name].ResourceVersion, 10, 64)
				if current > 1 {
					stale := latest[name].DeepCopy()
					stale.ResourceVersion = strconv.FormatUint(1+r.Uint64N(current-1), 10)
					stale.Labels[v1.NodePoolLabelKey] = "stale"
					c.UpdateNodeClaim(stale)
				}
			default:
				rv++
				nc := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: strconv.FormatUint(rv, 10), Labels: map[string]string{
					v1.NodePoolLabelKey: []string{"a", "b", ""}[r.IntN(3)],
				}}}
				nc.Spec.NodeClassRef = test.NodeClaim().Spec.NodeClassRef
				if old, ok := latest[name]; ok {
					nc.UID = old.UID
				} else {
					uid++
					nc.UID = types.UID(fmt.Sprintf("uid-%d", uid))
				}
				if r.IntN(3) == 0 {
					nc.DeletionTimestamp = &metav1.Time{Time: time.Unix(1, 0)}
					nc.Finalizers = []string{v1.TerminationFinalizer}
				}
				if r.IntN(3) == 0 {
					nc.Status.Conditions = []status.Condition{{Type: v1.ConditionTypeDisruptionReason, Status: metav1.ConditionTrue}}
				}
				c.UpdateNodeClaim(nc)
				latest[name] = nc
			}
			want := map[string]karpentercluster.NodeClaimCounts{}
			for _, nc := range latest {
				np := nc.Labels[v1.NodePoolLabelKey]
				if np == "" {
					continue
				}
				counts := want[np]
				if !nc.DeletionTimestamp.IsZero() || len(nc.Status.Conditions) > 0 {
					counts.Deleting++
				} else {
					counts.Active++
				}
				want[np] = counts
			}
			for _, np := range []string{"a", "b", "stale"} {
				active, deleting, _ := c.NodePoolState.GetNodeCount(np)
				Expect(karpentercluster.NodeClaimCounts{Active: active, Deleting: deleting}).To(Equal(want[np]),
					"after change %d, %s counts %d active and %d deleting, want %+v", i, np, active, deleting, want[np])
			}
		}
	})
})
