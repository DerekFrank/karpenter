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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// White-box Ginkgo specs for the AIMD restraint dials (F3). They drive the restraint state machine directly through
// its interface — CanDisrupt (admit + count in-flight) and Record (fold in an outcome) — covering the admission-side
// invariants whose end-to-end forms are awkward in the behavioral harness. The dials are a pure function of the
// candidate + reported outcomes, so this is the right altitude to pin them.

// wbCandidate builds a minimal Candidate carrying the fields restraint reads: NodePool name, zone, providerID, and a
// node with the given unhealthy condition type (the "policy" failure domain).
func wbCandidate(providerID, nodePool, zone, condType string) *Candidate {
	return &Candidate{
		StateNode: &state.StateNode{
			NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: providerID}, Status: v1.NodeClaimStatus{ProviderID: providerID}},
			Node: &corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeConditionType(condType), Status: corev1.ConditionFalse},
			}}},
		},
		NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: nodePool}},
		zone:     zone,
	}
}

// wbReplacement builds a minimal healthy replacement Node in the given zone, so Record can attribute a location-scoped
// outcome to where the replacement actually came up (its zone domain is learned only if it matches the candidate's).
func wbReplacement(zone string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{corev1.LabelTopologyZone: zone}}}
}

var _ = Describe("Repair/Restraint (dials)", func() {
	var r *restraint
	var fakeClock *clocktesting.FakeClock
	t0 := metav1.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Time

	BeforeEach(func() {
		fakeClock = clocktesting.NewFakeClock(t0)
		r = newRestraint(fakeClock, time.Minute, 10*time.Minute)
	})

	// INV-F3-1 / INV-F3-6: cold start at width 1 — the first probe in a domain is admitted, a second concurrent probe in
	// the same domain is blocked. A fresh (stateless) restraint starts every domain here.
	It("cold-starts each domain at width 1", func() {
		c1, c2 := wbCandidate("n1", "np", "z1", "BadNode"), wbCandidate("n2", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(c1)).To(BeTrue()) // CanDisrupt admits AND counts it in-flight
		Expect(r.CanDisrupt(c2)).To(BeFalse())
	})

	// INV-F3-3: a proven success doubles the width, so two concurrent probes are then admitted and a third is blocked.
	It("doubles concurrency on a proven success", func() {
		c1 := wbCandidate("n1", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(c1)).To(BeTrue())
		r.Record(c1, wbReplacement("z1"), RepairProven) // proven → width doubles to 2, in-flight back to 0

		c2, c3, c4 := wbCandidate("n2", "np", "z1", "BadNode"), wbCandidate("n3", "np", "z1", "BadNode"), wbCandidate("n4", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(c2)).To(BeTrue())
		Expect(r.CanDisrupt(c3)).To(BeTrue())
		Expect(r.CanDisrupt(c4)).To(BeFalse()) // width 2 is the ceiling
	})

	// INV-F3-2: the floor is 1, never 0 — even with an explicit 0 recorded, widthFor returns 1, so repair can never be
	// starved to zero concurrency (it always eventually retries).
	It("floors width at 1, never 0", func() {
		d := failureDomain{kind: "zone", value: "z1"}
		r.width[d] = 0
		Expect(r.widthFor(d)).To(Equal(1))
	})

	// INV-F3-2 / INV-F3-3: a failed probe pins width at the floor and arms a cooldown; admission pauses, then resumes
	// once the cooldown elapses (pause, not halt).
	It("pauses after a failed probe, then re-admits once the cooldown elapses", func() {
		c := wbCandidate("n1", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(c)).To(BeTrue())
		r.Record(c, wbReplacement("z1"), RepairFailed) // floor + cooldown armed (t0 + 1m)

		Expect(r.CanDisrupt(wbCandidate("n2", "np", "z1", "BadNode"))).To(BeFalse()) // still in cooldown at t0
		fakeClock.Step(2 * time.Minute)
		Expect(r.CanDisrupt(wbCandidate("n3", "np", "z1", "BadNode"))).To(BeTrue()) // cooldown elapsed → re-admitted
	})

	// Domain-combine (any-domain-out-of-cooldown): a candidate sharing only a cooled NodePool domain, but with a fresh
	// zone/policy, is still admitted — a separate fault isn't frozen behind an unrelated flood.
	It("admits via any domain that is out of cooldown", func() {
		r.cooldownUntil[failureDomain{kind: "nodepool", value: "np"}] = t0.Add(10 * time.Minute)
		Expect(r.CanDisrupt(wbCandidate("iso", "np", "z-fresh", "IsolatedCondition"))).To(BeTrue())
	})

	// Domain-combine (min width): admitted concurrency is the MIN across a candidate's domains. With NodePool widened to
	// 4 but the zone domain at the floor (1), a second concurrent probe is blocked by the zone domain.
	It("gates concurrency by the min width across domains", func() {
		r.width[failureDomain{kind: "nodepool", value: "np"}] = 4
		Expect(r.CanDisrupt(wbCandidate("n1", "np", "z1", "BadNode"))).To(BeTrue())
		Expect(r.CanDisrupt(wbCandidate("n2", "np", "z1", "BadNode"))).To(BeFalse()) // zone width 1 binds
	})

	// INV-F3-5: RepairCleared forgets a domain's earned width, so a NEW correlated burst starts slow again — past
	// success doesn't mask a new correlated failure.
	It("forgets a domain's width when its fault clears", func() {
		c := wbCandidate("n1", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(c)).To(BeTrue())
		r.Record(c, wbReplacement("z1"), RepairProven) // width 2 across c's domains, in-flight back to 0
		c2 := wbCandidate("n2", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(c2)).To(BeTrue())           // width 2 lets a 2nd concurrent probe in
		r.Record(c2, wbReplacement("z1"), RepairProven) // resolve it so nothing dangles in-flight

		r.Record(c, nil, RepairCleared) // episode over, no in-flight probes → forget earned width

		// A fresh burst in the same domains starts at the floor again: one admitted, second blocked.
		Expect(r.CanDisrupt(wbCandidate("m1", "np", "z1", "BadNode"))).To(BeTrue())
		Expect(r.CanDisrupt(wbCandidate("m2", "np", "z1", "BadNode"))).To(BeFalse())
	})

	// RepairCleared must NOT forget a domain that still has an in-flight probe.
	It("does not forget a domain with an in-flight probe", func() {
		c1 := wbCandidate("n1", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(c1)).To(BeTrue())
		r.Record(c1, wbReplacement("z1"), RepairProven) // width 2
		c2 := wbCandidate("n2", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(c2)).To(BeTrue()) // in-flight in the domain
		r.Record(c1, nil, RepairCleared)      // c1's fault cleared, but c2 is still in flight → keep the width
		Expect(r.width[failureDomain{kind: "zone", value: "z1"}]).To(Equal(2))
	})

	// FINDING (adversarial): a bad AMI rolled across a NodePool that spans multiple zones is NOT paced pool-wide the way a
	// single-zone flood is. A failed probe cools the candidate's {NodePool, zone, policy} domains, but admission is
	// "eligible if ANY domain is out of cooldown", so a sibling in the SAME NodePool + policy but a still-fresh OTHER zone
	// is admitted anyway — the NodePool/policy cooldown only pauses same-zone siblings. So a bad AMI churns ~one probe per
	// zone before the per-zone cooldowns catch up, rather than trickling as one. (Deterministic clock, so this is the
	// bypass itself, not a cooldown that quietly expired.)
	It("lets a still-fresh sibling zone bypass a cooling NodePool/policy backoff (multi-zone bad-AMI gap)", func() {
		a := wbCandidate("a", "np", "z1", "BadNode")
		Expect(r.CanDisrupt(a)).To(BeTrue())
		r.Record(a, nil, RepairFailed) // launch-failed (never registered): cools NodePool np, zone z1, policy BadNode

		// Same NodePool + same policy, DIFFERENT (fresh) zone → admitted despite the NodePool/policy cooldown.
		Expect(r.CanDisrupt(wbCandidate("b", "np", "z2", "BadNode"))).To(BeTrue())
		// Same NodePool + same policy, SAME (cooled) zone → correctly paused.
		Expect(r.CanDisrupt(wbCandidate("c", "np", "z1", "BadNode"))).To(BeFalse())
	})

	// INV-S3 (structural): repair registers first in NewMethods when repair policies exist, and not at all otherwise.
	It("registers repair first, gated on repair policies", func() {
		cp := fake.NewCloudProvider()
		cp.RepairPolicy = []cloudprovider.RepairPolicy{{ConditionType: "BadNode", ConditionStatus: "False", TolerationDuration: time.Minute}}
		methods := NewMethods(nil, nil, nil, nil, cp, nil, nil)
		Expect(methods).ToNot(BeEmpty())
		_, isRepair := methods[0].(*Repair)
		Expect(isRepair).To(BeTrue())

		cp.RepairPolicy = nil
		for _, m := range NewMethods(nil, nil, nil, nil, cp, nil, nil) {
			_, isRepair := m.(*Repair)
			Expect(isRepair).To(BeFalse())
		}
	})
})
