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
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	fakecp "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

// BenchmarkStaticScaleUp models a static NodePool scaling from 0 to replicas: workers reserve a slot, create a
// NodeClaim (cluster state learns it from the create's response), and release the slot; the NodeClaim then launches
// and registers (two more updates). Readers poll the counts the way static provisioning and drift do.
func BenchmarkStaticScaleUp(b *testing.B) {
	for _, replicas := range []int{2000, 20000} {
		for _, workers := range []int{64, 1000} {
			b.Run(fmt.Sprintf("replicas=%d/workers=%d", replicas, workers), func(b *testing.B) {
				var maxRead atomic.Int64
				var readsMu sync.Mutex
				var reads []time.Duration
				for range b.N {
					cluster := state.NewCluster(clock.NewFakeClock(time.Now()), fake.NewClientBuilder().Build(), fakecp.NewCloudProvider())
					var rv atomic.Uint64
					var next atomic.Int64
					var wg sync.WaitGroup
					stop := make(chan struct{})
					for range 4 {
						wg.Go(func() {
							var local []time.Duration
							defer func() { readsMu.Lock(); reads = append(reads, local...); readsMu.Unlock() }()
							for {
								select {
								case <-stop:
									return
								default:
								}
								start := time.Now()
								cluster.NodePoolState.GetNodeCount("static")
								d := time.Since(start)
								local = append(local, d)
								if d.Nanoseconds() > maxRead.Load() {
									maxRead.Store(d.Nanoseconds())
								}
							}
						})
					}
					var workersWG sync.WaitGroup
					for range workers {
						workersWG.Go(func() {
							for {
								i := int(next.Add(1)) - 1
								if i >= replicas {
									return
								}
								if cluster.NodePoolState.ReserveNodeCount("static", int64(replicas+workers), 1) == 0 {
									b.Errorf("no slot for NodeClaim %d", i)
									return
								}
								nc := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
									Name:   fmt.Sprintf("static-%d", i),
									UID:    types.UID(fmt.Sprintf("uid-%d", i)),
									Labels: map[string]string{v1.NodePoolLabelKey: "static"},
								}})
								nc.Status.ProviderID = ""
								nc.ResourceVersion = strconv.FormatUint(rv.Add(1), 10)
								cluster.UpdateNodeClaim(nc)
								cluster.NodePoolState.ReleaseNodeCount("static", 1)
								launched := nc.DeepCopy()
								launched.Status.ProviderID = fmt.Sprintf("fake://%d", i)
								launched.ResourceVersion = strconv.FormatUint(rv.Add(1), 10)
								cluster.UpdateNodeClaim(launched)
								registered := launched.DeepCopy()
								registered.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
								registered.ResourceVersion = strconv.FormatUint(rv.Add(1), 10)
								cluster.UpdateNodeClaim(registered)
							}
						})
					}
					workersWG.Wait()
					close(stop)
					wg.Wait()
					if active, _, _ := cluster.NodePoolState.GetNodeCount("static"); active != replicas {
						b.Fatalf("active = %d, want %d", active, replicas)
					}
				}
				slices.Sort(reads)
				b.ReportMetric(float64(reads[len(reads)*99/100].Nanoseconds())/1e3, "p99-read-µs")
				b.ReportMetric(float64(maxRead.Load())/1e3, "max-read-µs")
				b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N)/float64(replicas), "µs/nodeclaim")
			})
		}
	}
}
