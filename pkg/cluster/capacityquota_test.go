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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	autoscalingv1beta1 "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

var _ = Describe("CapacityQuota", func() {
	It("should keep the contribution laws for quota usage and what's held", func() {
		base := CapacityQuotaFields{Used: nodeResources("8"), Reserved: nodeResources("2")}
		for _, b := range []CapacityQuotaFields{{}, base} {
			statetest.ExpectContribution(GinkgoT(), b, quotaUsage{usage: nodeResources("2"), capacityCounted: true}, quotaUsage{usage: corev1.ResourceList{resources.Node: resource.MustParse("1")}})
			statetest.ExpectContribution(GinkgoT(), b, quotaHeld(nodeResources("4")), quotaHeld(nodeResources("2")))
		}
		statetest.ExpectContribution(GinkgoT(), pendingQuotas(2), pendingQuotas(1), pendingQuotas(1))
	})

	var s State
	var clk *clocktesting.FakeClock
	BeforeEach(func() {
		clk = clocktesting.NewFakeClock(time.Now())
		s = New(options.ToContext(context.Background(), test.Options()), clk, nil, nil, fake.NewCloudProvider())
	})
	quotas := func() *CapacityQuotaState { return s.CapacityQuotaState }
	used := func(name string) corev1.ResourceList { return quotas().Get(name).Used }
	reserved := func(name string) corev1.ResourceList { return quotas().Get(name).Reserved }
	// teamA is a NodeClaim of team a that hasn't launched.
	teamA := []scheduling.Requirements{scheduling.NewLabelRequirements(map[string]string{"team": "a"})}

	It("should count launched NodeClaims against the quotas that select them", func() {
		quotas().Observe(capacityQuota("a", team("a"), "8"))
		quotas().Observe(capacityQuota("b", team("b"), "8"))
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc", "a", "1", "4"))
		Expect(cpuOf(used("a")).String()).To(Equal("4"))
		Expect(used("b")).To(BeEmpty())
	})
	It("should count a NodeClaim that hasn't launched as a node against every quota it may match", func() {
		quotas().Observe(capacityQuota("zone-1", &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelTopologyZone: "test-zone-1"}}, "8"))
		quotas().Observe(capacityQuota("b", team("b"), "8"))
		// The zone is open, so it may launch in test-zone-1; it has no team label, so it won't be team b's.
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc", "", "1", ""))
		Expect(used("zone-1")).To(HaveLen(1))
		Expect(used("zone-1")[resources.Node]).To(BeComparableTo(resource.MustParse("1")))
		Expect(used("b")).To(BeEmpty())
	})
	It("should stop counting a NodeClaim once it's disrupted", func() {
		quotas().Observe(capacityQuota("a", team("a"), "8"))
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc", "a", "1", "4"))
		disrupted := quotaNodeClaim("nc", "a", "2", "4")
		disrupted.Status.Conditions = []status.Condition{{Type: v1.ConditionTypeDisruptionReason, Status: metav1.ConditionTrue, Reason: "Drifted"}}
		s.Nodes.NodeClaimStore.Observe(disrupted)
		Expect(used("a")).To(BeEmpty())
	})
	It("should count Nodes Karpenter doesn't manage", func() {
		quotas().Observe(capacityQuota("a", team("a"), "8"))
		node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"team": "a"}}, Capacity: cpu("6")})
		s.Nodes.NodeStore.Observe(node)
		Expect(cpuOf(used("a")).String()).To(Equal("6"))
		managed := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"team": "a", v1.NodePoolLabelKey: "np"}}, Capacity: cpu("6")})
		s.Nodes.NodeStore.Observe(managed)
		Expect(cpuOf(used("a")).String()).To(Equal("6"), "counted a Node Karpenter manages")

		s.Nodes.NodeStore.Forget(node.Name)
		Expect(used("a")).To(BeEmpty())
	})
	It("should count what exists against a quota added after it, and recount once its selector changes", func() {
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc-a", "a", "1", "4"))
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc-b", "b", "1", "2"))
		quotas().Observe(capacityQuota("q", team("a"), "8"))
		Expect(cpuOf(used("q")).String()).To(Equal("4"))
		Expect(quotas().Synced()).To(BeTrue())

		changed := capacityQuota("q", team("b"), "8")
		changed.ResourceVersion = "2"
		quotas().Observe(changed)
		Expect(cpuOf(used("q")).String()).To(Equal("2"))

		quotas().Forget("q")
		Expect(quotas().Quotas()).To(BeEmpty())
		Expect(used("q")).To(BeEmpty())
	})
	It("should enforce a quota from its controller, not on the CapacityQuota store's notification, and be synced once it has", func() {
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc", "a", "1", "4"))
		// The informer feeds the CapacityQuota store; nothing rebuilds on the goroutine that changes it.
		quotas().quotas.Observe(capacityQuota("q", team("a"), "8"))
		Expect(quotas().Quotas()).To(BeEmpty(), "enforced on the store's notification")
		Expect(quotas().Synced()).To(BeFalse(), "synced before the quota is enforced")

		_, err := quotas().Controller().Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "q"}})
		Expect(err).ToNot(HaveOccurred())
		Expect(quotas().Quotas()).To(HaveLen(1))
		e, _ := quotas().quotas.Get("q")
		Expect(e.Intent.Enforced.Value.quota).To(Equal(quotas().Quotas()[0]), "the enforced quota is the CapacityQuota's intent")
		Expect(cpuOf(used("q")).String()).To(Equal("4"))
		Expect(quotas().Synced()).To(BeTrue())

		state.Forget(quotas().quotas, types.NamespacedName{Name: "q"})
		Expect(quotas().Synced()).To(BeFalse(), "synced while a deleted quota is enforced")
		_, err = quotas().Controller().Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "q"}})
		Expect(err).ToNot(HaveOccurred())
		Expect(quotas().Quotas()).To(BeEmpty())
		Expect(used("q")).To(BeEmpty())
		Expect(quotas().Synced()).To(BeTrue())
	})
	It("should not enforce a quota with an invalid selector", func() {
		invalid := capacityQuota("q", &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "team", Operator: "Bogus"}}}, "0")
		quotas().Observe(invalid)
		Expect(quotas().Quotas()).To(BeEmpty())
		Expect(quotas().Synced()).To(BeTrue())
		reservations, refused := quotas().Reserve(teamA, nodeResources("1"), nil)
		Expect(refused).To(BeEmpty())
		Expect(reservations).To(BeEmpty())
	})
	It("should reserve against every quota a NodeClaim may match, in name order, and hold none when one refuses", func() {
		quotas().Observe(capacityQuota("a", team("a"), "8"))
		quotas().Observe(capacityQuota("all", nil, "10"))
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc", "b", "1", "6"))

		// Fits team a's quota, but not the cluster-wide one, so it holds against neither.
		reservations, refused := quotas().Reserve(teamA, nodeResources("6"), nil)
		Expect(refused).To(Equal("all"))
		Expect(reservations).To(BeEmpty())
		Expect(reserved("a")).To(BeEmpty())

		reservations, refused = quotas().Reserve(teamA, nodeResources("4"), nil)
		Expect(refused).To(BeEmpty())
		Expect(reservations).To(HaveLen(2))
		Expect(cpuOf(reserved("a")).String()).To(Equal("4"))
		Expect(cpuOf(reserved("all")).String()).To(Equal("4"))
		reservations.Release()
		Expect(reserved("a")).To(BeEmpty())
		Expect(reserved("all")).To(BeEmpty())
	})
	It("should hold a bound reservation until its NodeClaim's launched capacity counts", func() {
		quotas().Observe(capacityQuota("a", team("a"), "8"))
		reservations, _ := quotas().Reserve(teamA, nodeResources("8"), nil)
		Expect(reservations).To(HaveLen(1))
		reservations[0].Bind("nc")
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc", "a", "1", ""))
		Expect(cpuOf(reserved("a")).String()).To(Equal("8"))

		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc", "a", "2", "2"))
		Expect(cpuOf(used("a")).String()).To(Equal("2"))
		Expect(reserved("a")).To(BeEmpty())
	})
	It("should release a bound reservation once its NodeClaim launches where the quota doesn't select it", func() {
		quotas().Observe(capacityQuota("zone-1", &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelTopologyZone: "test-zone-1"}}, "8"))
		reservations, _ := quotas().Reserve([]scheduling.Requirements{scheduling.NewRequirements()}, nodeResources("8"), nil)
		Expect(reservations).To(HaveLen(1))
		reservations[0].Bind("nc")
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("nc", "", "1", ""))
		launched := quotaNodeClaim("nc", "", "2", "2")
		launched.Labels[corev1.LabelTopologyZone] = "test-zone-2"
		s.Nodes.NodeClaimStore.Observe(launched)
		Expect(used("zone-1")).To(BeEmpty())
		Expect(reserved("zone-1")).To(BeEmpty())
	})
	It("should credit the NodeClaims a replacement replaces", func() {
		quotas().Observe(capacityQuota("a", team("a"), "8"))
		s.Nodes.NodeClaimStore.Observe(quotaNodeClaim("candidate", "a", "1", "8"))
		_, refused := quotas().Reserve(teamA, nodeResources("8"), nil)
		Expect(refused).To(Equal("a"))
		reservations, refused := quotas().Reserve(teamA, nodeResources("8"), []string{"candidate"})
		Expect(refused).To(BeEmpty())
		Expect(reservations).To(HaveLen(1))
	})
	It("should release the reservations against a quota whose spec changes, and admit against the new spec", func() {
		quotas().Observe(capacityQuota("a", team("a"), "8"))
		reservations, _ := quotas().Reserve(teamA, nodeResources("8"), nil)
		Expect(reservations).To(HaveLen(1))
		raised := capacityQuota("a", team("a"), "16")
		raised.ResourceVersion = "2"
		quotas().Observe(raised)
		Expect(reserved("a")).To(BeEmpty())
		reservations, refused := quotas().Reserve(teamA, nodeResources("16"), nil)
		Expect(refused).To(BeEmpty())
		Expect(reservations).To(HaveLen(1))
	})
	It("should end every reservation once cleared", func() {
		quotas().Observe(capacityQuota("a", team("a"), "8"))
		quotas().Reserve(teamA, nodeResources("8"), nil)
		quotas().Clear()
		Expect(quotas().Quotas()).To(BeEmpty())
		Expect(reserved("a")).To(BeEmpty())
	})
	// Reservers that create, launch and delete NodeClaims of random teams and sizes race against two overlapping quotas,
	// and after each step every quota's fields match a recount of the NodeClaim store and the live reservations: each
	// NodeClaim counts at least once, in Used or held, and what's counted and held never exceeds a quota's limit.
	It("should never exceed a quota or count a NodeClaim zero times", func() {
		quotas().Observe(capacityQuota("a", team("a"), "12"))
		quotas().Observe(capacityQuota("all", nil, "20"))
		limits := map[string]int64{"a": 12, "all": 20}
		var mu sync.Mutex // serializes a step with its check
		type live struct {
			team string
			cpu  int64
		}
		held, launched := map[string]live{}, map[string]live{}
		var violations []string
		check := func(step string) {
			for name, limit := range limits {
				var counted, holding int64
				for _, l := range launched {
					if name == "all" || l.team == name {
						counted += l.cpu
					}
				}
				for _, l := range held {
					if name == "all" || l.team == name {
						holding += l.cpu
					}
				}
				f := quotas().Get(name)
				if f.Used.Cpu().Value() != counted || f.Reserved.Cpu().Value() != holding || counted+holding > limit {
					violations = append(violations, fmt.Sprintf("after %s, quota %s = %s counted and %s held, want %d and %d within %d",
						step, name, f.Used.Cpu(), f.Reserved.Cpu(), counted, holding, limit))
				}
			}
		}
		var wg sync.WaitGroup
		for w := range 16 {
			wg.Go(func() {
				r := rand.New(rand.NewPCG(uint64(w), 1)) //nolint:gosec // a seeded source makes failures reproducible
				for i := range 60 {
					name, t := fmt.Sprintf("%d-%d", w, i), []string{"a", "b"}[r.IntN(2)]
					requirements := []scheduling.Requirements{scheduling.NewLabelRequirements(map[string]string{"team": t})}
					mu.Lock()
					reservations, _ := quotas().Reserve(requirements, nodeResources("3"), nil)
					if reservations != nil {
						held[name] = live{team: t, cpu: 3}
					}
					check("reserve " + name)
					mu.Unlock()
					if reservations == nil {
						continue
					}
					mu.Lock()
					if r.IntN(5) == 0 {
						reservations.Release()
						delete(held, name)
						check("release " + name)
						mu.Unlock()
						continue
					}
					for _, reservation := range reservations {
						reservation.Bind(name)
					}
					s.Nodes.NodeClaimStore.Observe(quotaNodeClaim(name, t, "1", ""))
					check("create " + name)
					mu.Unlock()

					mu.Lock()
					c := int64(1 + r.IntN(3))
					s.Nodes.NodeClaimStore.Observe(quotaNodeClaim(name, t, "2", fmt.Sprint(c)))
					delete(held, name)
					launched[name] = live{team: t, cpu: c}
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
	// reservations, as a create through the overlay can: what NodeClaims count never exceeds a quota.
	It("should admit atomically under concurrency", func() {
		quotas().Observe(capacityQuota("a", team("a"), "12"))
		quotas().Observe(capacityQuota("all", nil, "20"))
		limits := map[string]resource.Quantity{"a": resource.MustParse("12"), "all": resource.MustParse("20")}
		var mu sync.Mutex
		var violations []string
		stop := quotas().quotas.Subscribe(func(c state.Change[string, *autoscalingv1beta1.CapacityQuota, CapacityQuotaFields, CapacityQuotaIntent]) {
			if limit := limits[c.Key]; c.New.Derived.Used.Cpu().Cmp(limit) > 0 {
				mu.Lock()
				defer mu.Unlock()
				violations = append(violations, fmt.Sprintf("quota %s: %s counted, over %s", c.Key, c.New.Derived.Used.Cpu(), limit.String()))
			}
		})
		defer stop()
		var wg sync.WaitGroup
		for w := range 32 {
			wg.Go(func() {
				for i := range 40 {
					name, t := fmt.Sprintf("%d-%d", w, i), []string{"a", "b"}[(w+i)%2]
					requirements := []scheduling.Requirements{scheduling.NewLabelRequirements(map[string]string{"team": t})}
					reservations, _ := quotas().Reserve(requirements, nodeResources("3"), nil)
					if reservations == nil {
						continue
					}
					s.Nodes.NodeClaimStore.Observe(quotaNodeClaim(name, t, "1", "2"))
					for _, reservation := range reservations {
						reservation.Bind(name)
					}
					s.Nodes.NodeClaimStore.Forget(name)
				}
			})
		}
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		Expect(violations).To(BeEmpty())
		for name := range limits {
			Expect(used(name)).To(BeEmpty(), "quota %s's usage after every NodeClaim went away", name)
			Expect(reserved(name)).To(BeEmpty(), "quota %s's reservations after every NodeClaim went away", name)
		}
	})
})

func cpu(q string) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(q)}
}

func team(t string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{"team": t}}
}

// capacityQuota is a CapacityQuota limiting the CPU of the nodes selector selects.
func capacityQuota(name string, selector *metav1.LabelSelector, cpuLimit string) *autoscalingv1beta1.CapacityQuota {
	return &autoscalingv1beta1.CapacityQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: "1"},
		Spec: autoscalingv1beta1.CapacityQuotaSpec{Selector: selector, Limits: autoscalingv1beta1.CapacityQuotaLimits{
			Resources: autoscalingv1beta1.ResourceList{autoscalingv1beta1.ResourceCPU: resource.MustParse(cpuLimit)},
		}},
	}
}

// quotaNodeClaim is a NodeClaim of team t, or of no team for "", at resourceVersion rv, launched with c CPU unless c is
// "".
func quotaNodeClaim(name, t, rv, c string) *v1.NodeClaim {
	nc := &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: rv, Labels: map[string]string{v1.NodePoolLabelKey: "np"}},
		Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
	}
	if t != "" {
		nc.Labels["team"] = t
	}
	if c != "" {
		nc.Status.ProviderID = "fake://" + name
		nc.Status.Capacity = cpu(c)
	}
	return nc
}
