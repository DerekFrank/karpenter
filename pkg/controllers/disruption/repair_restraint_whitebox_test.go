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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// White-box Ginkgo specs for the correlated-failure restraint dials (F3). These drive the restraint state machine
// directly, covering the admission-side invariants whose end-to-end forms are awkward in the behavioral harness (the
// per-condition toleration exceeds the cooldown cap, so a clock step taken to make a fresh fault eligible also expires
// the cooldown under test). The dial mechanics are pure functions, so this is the right altitude to pin them.

// wbCandidate builds a minimal Candidate carrying the fields restraint reads: NodePool name, zone, providerID.
func wbCandidate(providerID, nodePool, zone string) *Candidate {
	return &Candidate{
		StateNode: &state.StateNode{NodeClaim: &v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: providerID},
			Status:     v1.NodeClaimStatus{ProviderID: providerID},
		}},
		NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: nodePool}},
		zone:     zone,
	}
}

var _ = Describe("Repair/Restraint (dials)", func() {
	var r *restraint
	// A fixed base time; the controller forbids time.Now, but white-box tests may construct times.
	t0 := metav1.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Time

	BeforeEach(func() {
		r = newRestraint()
	})

	// INV-F3-1 / INV-F3-6: cold start at width 1 — the first probe in a domain is admitted, a second concurrent probe
	// in the same domain is blocked. A fresh (stateless) restraint starts every domain here.
	It("cold-starts each domain at width 1", func() {
		c1, c2 := wbCandidate("n1", "np", "z1"), wbCandidate("n2", "np", "z1")
		Expect(r.admit(c1, "BadNode", t0)).To(BeTrue())
		r.recordProbe(c1, "BadNode")
		Expect(r.admit(c2, "BadNode", t0)).To(BeFalse())
	})

	// INV-F3-3: a proven success doubles the width, so two concurrent probes are then admitted and a third is blocked.
	It("doubles concurrency on a proven success", func() {
		c1 := wbCandidate("n1", "np", "z1")
		r.recordProbe(c1, "BadNode")
		for _, d := range domainsOf(c1, "BadNode") { // simulate what observe does on a proven, dwelled success
			r.width[d] = r.widthFor(d) * 2
		}
		delete(r.probes, c1.ProviderID())

		c2, c3, c4 := wbCandidate("n2", "np", "z1"), wbCandidate("n3", "np", "z1"), wbCandidate("n4", "np", "z1")
		Expect(r.admit(c2, "BadNode", t0)).To(BeTrue())
		r.recordProbe(c2, "BadNode")
		Expect(r.admit(c3, "BadNode", t0)).To(BeTrue())
		r.recordProbe(c3, "BadNode")
		Expect(r.admit(c4, "BadNode", t0)).To(BeFalse()) // width 2 is the ceiling
	})

	// INV-F3-2: the floor is 1, never 0 — even with an explicit 0 recorded, widthFor returns 1, so repair can never be
	// starved to zero concurrency (it always eventually retries).
	It("floors width at 1, never 0", func() {
		d := failureDomain{kind: "zone", value: "z1"}
		r.width[d] = 0
		Expect(r.widthFor(d)).To(Equal(1))
	})

	// INV-F3-2: a cooldown pauses admission, and once it elapses the candidate is admitted again (pause, not halt).
	It("pauses under cooldown, then re-admits once it elapses", func() {
		c := wbCandidate("n1", "np", "z1")
		for _, d := range domainsOf(c, "BadNode") {
			r.cooldownUntil[d] = t0.Add(5 * time.Minute)
		}
		Expect(r.admit(c, "BadNode", t0.Add(time.Minute))).To(BeFalse())
		Expect(r.admit(c, "BadNode", t0.Add(6*time.Minute))).To(BeTrue())
	})

	// Domain-combine (any-domain-out-of-cooldown): a candidate sharing only a cooled NodePool domain, but with fresh
	// zone/policy domains, is still admitted — a separate fault isn't frozen behind an unrelated flood.
	It("admits via any domain that is out of cooldown", func() {
		r.cooldownUntil[failureDomain{kind: "nodepool", value: "np"}] = t0.Add(10 * time.Minute)
		iso := wbCandidate("iso", "np", "z-fresh")
		Expect(r.admit(iso, "IsolatedReason", t0)).To(BeTrue())
	})

	// Domain-combine (min width): admitted concurrency is the MIN across a candidate's domains. With NodePool widened to
	// 4 but the zone domain at the floor (1), a second concurrent probe is blocked by the zone domain.
	It("gates concurrency by the min width across domains", func() {
		r.width[failureDomain{kind: "nodepool", value: "np"}] = 4
		c1, c2 := wbCandidate("n1", "np", "z1"), wbCandidate("n2", "np", "z1")
		Expect(r.admit(c1, "BadNode", t0)).To(BeTrue())
		r.recordProbe(c1, "BadNode")
		Expect(r.admit(c2, "BadNode", t0)).To(BeFalse()) // zone width 1 binds even though nodepool width is 4
	})

	// INV-F3-5: a domain with no current fault (idle) has its widened dials reset to the floor, so a NEW correlated
	// burst there starts slow again — past success doesn't mask a new correlated failure. An active domain is preserved.
	It("resets idle domains but preserves active ones", func() {
		active := failureDomain{kind: "zone", value: "z-active"}
		idle := failureDomain{kind: "zone", value: "z-idle"}
		r.width[active], r.width[idle] = 4, 4
		r.cooldownUntil[idle] = t0.Add(time.Hour)

		r.resetIdleDomains(map[failureDomain]struct{}{active: {}})

		Expect(r.widthFor(idle)).To(Equal(1))
		Expect(r.cooldownUntil).ToNot(HaveKey(idle))
		Expect(r.widthFor(active)).To(Equal(4))
	})

	// resetIdleDomains must not reset a domain that still has an in-flight probe (the probe hasn't resolved yet).
	It("does not reset a domain with an in-flight probe", func() {
		c := wbCandidate("n1", "np", "z1")
		r.recordProbe(c, "BadNode")
		for _, d := range domainsOf(c, "BadNode") {
			r.width[d] = 4
		}
		r.resetIdleDomains(map[failureDomain]struct{}{})
		Expect(r.widthFor(failureDomain{kind: "zone", value: "z1"})).To(Equal(4))
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
