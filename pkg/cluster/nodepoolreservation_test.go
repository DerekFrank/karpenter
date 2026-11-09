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
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

var _ = Describe("NodePool Reservations", func() {
	It("should keep the contribution laws for reserved nodes", func() {
		for _, b := range []NodePoolFields{{}, contributionBase()} {
			statetest.ExpectContribution(GinkgoT(), b, reservedNodes(1), reservedNodes(2))
		}
	})
	It("should keep the contribution laws for reserved resources", func() {
		for _, b := range []NodePoolFields{{}, contributionBase()} {
			statetest.ExpectContribution(GinkgoT(), b, reservedResources(nodeResources("4")), reservedResources(nodeResources("2")))
		}
	})

	Context("Replicas", func() {
		It("should reserve up to replicas less what's active and reserved, within the node limit", func() {
			s := New(options.ToContext(context.Background(), test.Options()), clocktesting.NewFakeClock(time.Now()), nil, nil, fake.NewCloudProvider())
			s.Nodes.NodeClaimStore.Observe(limitNodeClaim("a", "1", "2", true))
			replacement := s.NodePoolState.ReserveNodeCount("np", math.MaxInt64, 1)
			Expect(s.NodePoolState.ReserveReplicas("np", 4, math.MaxInt64)).To(HaveLen(2), "reserved past 1 active and 1 reserved of 4 replicas")
			Expect(s.NodePoolState.ReserveReplicas("np", 4, math.MaxInt64)).To(BeEmpty())
			replacement.Release()
			Expect(s.NodePoolState.ReserveReplicas("np", 4, 3)).To(BeEmpty(), "reserved past the node limit")
			Expect(s.NodePoolState.ReserveReplicas("np", 4, 4)).To(HaveLen(1))
		})
	})

	Context("Resources", func() {
		var s State
		var clk *clocktesting.FakeClock
		limits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10")}
		BeforeEach(func() {
			clk = clocktesting.NewFakeClock(time.Now())
			s = New(options.ToContext(context.Background(), test.Options()), clk, nil, nil, fake.NewCloudProvider())
		})
		reserved := func() corev1.ResourceList { return s.NodePoolState.Get("np").ReservedResources }
		usage := func() corev1.ResourceList { return s.NodePoolState.Get("np").Usage }

		It("should admit up to the limit, counting usage and what is held", func() {
			s.Nodes.NodeClaimStore.Observe(limitNodeClaim("a", "1", "2", true))
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("4"), nil)).ToNot(BeNil())
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("4"), nil)).ToNot(BeNil())
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("4"), nil)).To(BeNil(), "admitted 2 counted and 12 held against 10")
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("1"), nil)).To(BeNil(), "admitted 2 counted and 9 held against 10")
			Expect(cpuOf(reserved()).String()).To(Equal("8"))
			Expect(reserved()[resources.Node]).To(BeComparableTo(resource.MustParse("2")))
		})
		It("should hold a bound reservation until its NodeClaim's usage counts its launched capacity", func() {
			reservation := s.NodePoolState.ReserveResources("np", limits, nodeResources("8"), nil)
			Expect(reservation).ToNot(BeNil())
			reservation.Bind("a")

			// Created, not launched: Usage counts the NodeClaim as a node, not its capacity, so the reservation holds.
			s.Nodes.NodeClaimStore.Observe(limitNodeClaim("a", "1", "", false))
			Expect(cpuOf(usage()).IsZero()).To(BeTrue())
			Expect(cpuOf(reserved()).String()).To(Equal("8"))

			s.Nodes.NodeClaimStore.Observe(limitNodeClaim("a", "2", "2", true))
			Expect(cpuOf(usage()).String()).To(Equal("2"))
			Expect(reserved()).To(BeEmpty())
		})
		It("should release a bound reservation once its NodeClaim is deleting before it launches", func() {
			reservation := s.NodePoolState.ReserveResources("np", limits, nodeResources("8"), nil)
			reservation.Bind("a")
			nc := limitNodeClaim("a", "1", "", false)
			nc.DeletionTimestamp, nc.Finalizers = &metav1.Time{Time: time.Unix(1, 0)}, []string{"test"}
			s.Nodes.NodeClaimStore.Observe(nc)
			Expect(reserved()).To(BeEmpty())
		})
		It("should release a reservation for a NodeClaim that's never created", func() {
			s.NodePoolState.ReserveResources("np", limits, nodeResources("8"), nil).Release()
			Expect(reserved()).To(BeEmpty())
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("10"), nil)).ToNot(BeNil())
		})
		It("should credit what the NodeClaims it replaces count, once", func() {
			s.Nodes.NodeClaimStore.Observe(limitNodeClaim("a", "1", "8", true))
			s.Nodes.NodeClaimStore.Observe(limitNodeClaim("other-pool", "1", "8", true))
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("8"), nil)).To(BeNil())
			// A NodeClaim of another NodePool, or named twice, isn't credited again.
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("11"), []string{"a", "a", "other-pool"})).To(BeNil())
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("10"), []string{"a", "a", "other-pool"})).ToNot(BeNil())
			// What's held isn't credited, and the credit leaves Usage as it is.
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("1"), []string{"a"})).To(BeNil())
			Expect(cpuOf(usage()).String()).To(Equal("8"))
		})
		It("should credit nothing for a replaced NodeClaim that already stopped counting", func() {
			s.Nodes.NodeClaimStore.Observe(limitNodeClaim("a", "1", "8", true))
			disrupted := limitNodeClaim("a", "2", "8", true)
			disrupted.Status.Conditions = []status.Condition{{Type: v1.ConditionTypeDisruptionReason, Status: metav1.ConditionTrue, Reason: "Drifted"}}
			s.Nodes.NodeClaimStore.Observe(disrupted)
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("10"), []string{"a"})).ToNot(BeNil())
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("1"), []string{"a"})).To(BeNil(), "credited a NodeClaim that no longer counts")
		})
		It("should expire a reservation that's never counted", func() {
			s.NodePoolState.ReserveResources("np", limits, nodeResources("10"), nil)
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("1"), nil)).To(BeNil())
			clk.Step(ReservationTTL)
			Eventually(reserved).Should(BeEmpty())
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("1"), nil)).ToNot(BeNil())
		})
		It("should end every reservation once cleared", func() {
			s.NodePoolState.ReserveResources("np", limits, nodeResources("10"), nil)
			s.NodePoolState.Clear()
			Expect(reserved()).To(BeEmpty())
			Expect(s.NodePoolState.ReserveResources("np", limits, nodeResources("10"), nil)).ToNot(BeNil())
		})
		// Reservers that create, launch and delete NodeClaims of random sizes race against the limit, and after each step
		// the fields match a recount of the NodeClaim store and the live reservations: each NodeClaim counts at least once,
		// in Usage or held, and what's counted and held never exceeds the limit.
		It("should never exceed the limit or count a NodeClaim zero times", func() {
			var mu sync.Mutex // serializes a step with its check
			held, launched := map[string]int64{}, map[string]int64{}
			var violations []string
			check := func(step string) {
				var counted, holding int64
				for _, cpu := range launched {
					counted += cpu
				}
				for _, cpu := range held {
					holding += cpu
				}
				f := s.NodePoolState.Get("np")
				if f.Usage.Cpu().Value() != counted || f.ReservedResources.Cpu().Value() != holding || counted+holding > limits.Cpu().Value() {
					violations = append(violations, fmt.Sprintf("after %s, fields = %s counted and %s held, want %d and %d within %s",
						step, f.Usage.Cpu(), f.ReservedResources.Cpu(), counted, holding, limits.Cpu()))
				}
			}
			var wg sync.WaitGroup
			for w := range 16 {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(w), 0)) //nolint:gosec // a seeded source makes failures reproducible
					for i := range 100 {
						name := fmt.Sprintf("%d-%d", w, i)
						mu.Lock()
						reservation := s.NodePoolState.ReserveResources("np", limits, nodeResources("3"), nil)
						if reservation != nil {
							held[name] = 3
						}
						check("reserve " + name)
						mu.Unlock()
						if reservation == nil {
							continue
						}
						mu.Lock()
						if r.IntN(5) == 0 {
							reservation.Release()
							delete(held, name)
							check("release " + name)
							mu.Unlock()
							continue
						}
						reservation.Bind(name)
						s.Nodes.NodeClaimStore.Observe(limitNodeClaim(name, "1", "", false))
						check("create " + name)
						mu.Unlock()

						mu.Lock()
						cpu := int64(1 + r.IntN(3))
						s.Nodes.NodeClaimStore.Observe(limitNodeClaim(name, "2", fmt.Sprint(cpu), true))
						delete(held, name)
						launched[name] = cpu
						check("launch " + name)
						mu.Unlock()

						mu.Lock()
						s.Nodes.NodeClaimStore.Forget(name)
						delete(launched, name)
						check("delete " + name)
						mu.Unlock()
					}
				})
			}
			wg.Wait()
			Expect(violations).To(BeEmpty())
		})
		// Reservers race without serializing, each counting its NodeClaim's launched capacity before binding its
		// reservation, as a create through the overlay can, against a recount of Usage: what NodeClaims count never
		// exceeds the limit.
		It("should admit atomically under concurrency", func() {
			var mu sync.Mutex
			var violations []string
			s.NodePoolState.Observe(&v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np", UID: "np", ResourceVersion: "1"}})
			stop := s.NodePoolState.store.Subscribe(func(c state.Change[string, *v1.NodePool, NodePoolFields, NodePoolIntent]) {
				if c.New.Derived.Usage.Cpu().Cmp(*limits.Cpu()) > 0 {
					mu.Lock()
					defer mu.Unlock()
					violations = append(violations, fmt.Sprintf("%s counted, over %s", c.New.Derived.Usage.Cpu(), limits.Cpu()))
				}
			})
			defer stop()
			var wg sync.WaitGroup
			for w := range 32 {
				wg.Go(func() {
					for i := range 50 {
						name := fmt.Sprintf("%d-%d", w, i)
						reservation := s.NodePoolState.ReserveResources("np", limits, nodeResources("3"), nil)
						if reservation == nil {
							continue
						}
						s.Nodes.NodeClaimStore.Observe(limitNodeClaim(name, "1", "2", true))
						reservation.Bind(name)
						s.Nodes.NodeClaimStore.Forget(name)
					}
				})
			}
			wg.Wait()
			mu.Lock()
			defer mu.Unlock()
			Expect(violations).To(BeEmpty())
			Expect(usage()).To(BeEmpty(), "usage after every NodeClaim went away")
			Expect(reserved()).To(BeEmpty(), "held after every NodeClaim went away")
		})
	})
})

// limitNodeClaim is a NodeClaim of NodePool np, or of another NodePool for a name starting "other-pool", at
// resourceVersion rv, launched with cpu CPU if launched.
func limitNodeClaim(name, rv, cpu string, launched bool) *v1.NodeClaim {
	pool := "np"
	if name == "other-pool" {
		pool = "other"
	}
	nc := &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: rv, Labels: map[string]string{v1.NodePoolLabelKey: pool}},
		Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
	}
	if launched {
		nc.Status.ProviderID = "fake://" + name
		nc.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}
	}
	return nc
}

func cpuOf(l corev1.ResourceList) *resource.Quantity { return l.Cpu() }
