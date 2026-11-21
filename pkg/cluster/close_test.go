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

package cluster_test

import (
	"sync"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// subscriptions counts the subscriptions held on env.Overlay's Sources through it.
type subscriptions struct{ n atomic.Int64 }

// sources are env.Overlay's Sources, each counting the subscriptions made through it.
func (s *subscriptions) sources(obj client.Object) state.Newest {
	src, err := env.Overlay.Source(obj)
	if err != nil {
		return nil
	}
	return countingSource{Newest: src, n: &s.n}
}

type countingSource struct {
	state.Newest
	n *atomic.Int64
}

func (c countingSource) Subscribe(fn func(types.NamespacedName)) func() {
	unsubscribe := c.Newest.Subscribe(fn)
	c.n.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() {
			unsubscribe()
			c.n.Add(-1)
		})
	}
}

var _ = Describe("Close", func() {
	It("should leave no subscribers on the overlay and no timers on the clock", func() {
		// The State reads env.Overlay's own Sources, through a count of the subscriptions it holds on them.
		subs := &subscriptions{}
		clk := test.NewClockView(env.Clock)
		Expect(subs.n.Load()).To(BeZero())
		s := karpentercluster.New(ctx, clk, env.Overlay, cloudProvider, karpentercluster.WithSources(subs.sources))
		Expect(subs.n.Load()).To(BeNumerically(">", 0), "the object stores don't follow the overlay")

		nodeClaim := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name}}})
		ExpectApplied(ctx, env.Client, nodeClaim)
		s.Nomination.Nominate(ctx, nodeClaim.Name)
		Expect(s.NodePoolState.ReserveNodeCount(nodePool.Name, 2, 1)).To(HaveLen(1))
		Expect(clk.Waiters()).To(Equal(2), "want the nomination's lapse timer and the reservation's expiry timer")

		s.Close()
		Expect(subs.n.Load()).To(BeZero(), "a closed State still follows the overlay")
		Expect(clk.HasWaiters()).To(BeFalse(), "a closed State still waits on the clock")
		s.Close()
		Expect(subs.n.Load()).To(BeZero())
	})
})
