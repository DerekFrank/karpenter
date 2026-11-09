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
	"slices"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// registrationNodeClassGeneration is the NodeClass generation outcomes are recorded against, unless a spec says
// otherwise.
const registrationNodeClassGeneration = 1

func newRegistrationState() (*NodePoolState, *Registrations) {
	st := New(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, nil, nil, fake.NewCloudProvider())
	return st.NodePoolState, st.Registrations
}

func registrationNodePool(name string) *v1.NodePool {
	return &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uuid.NewUUID(), ResourceVersion: "1", Generation: 1}}
}

// registration returns what's recorded on the NodePool's entry.
func registration(s *NodePoolState, name string) Registration {
	e, _ := s.store.Get(name)
	return e.Intent.Registration.Value
}

func recordOutcome(rec *Registrations, nodePool *v1.NodePool, registered bool) {
	outcome := RegistrationOutcome{Registered: registered}
	if !registered {
		outcome.Reason, outcome.Message = "RegistrationFailed", "Failed to register node"
	}
	rec.Record(nodePool, registrationNodeClassGeneration, outcome)
}

func expectRegistration(s *NodePoolState, nodePool *v1.NodePool, want RegistrationStatus) {
	GinkgoHelper()
	Expect(s.RegistrationHealth(nodePool, registrationNodeClassGeneration).Status).To(Equal(want), "RegistrationHealth(%s)", nodePool.Name)
}

// failures counts the failed outcomes.
func failures(outcomes []bool) int {
	n := 0
	for _, registered := range outcomes {
		if !registered {
			n++
		}
	}
	return n
}

var _ = Describe("NodePool Registration", func() {
	var s *NodePoolState
	var rec *Registrations
	var np *v1.NodePool
	BeforeEach(func() {
		s, rec = newRegistrationState()
		np = registrationNodePool("default")
		// The registration record is intent on the NodePool's entry, which exists while cluster state holds the NodePool.
		s.Observe(np)
	})

	DescribeTable("should take the registration status from the newest outcomes",
		func(outcomes []bool, want RegistrationStatus, kept []bool) {
			for _, registered := range outcomes {
				recordOutcome(rec, np, registered)
			}
			expectRegistration(s, np, want)
			Expect(slices.Equal(registration(s, np.Name).Outcomes, kept)).To(BeTrue(), "outcomes = %v, want %v", registration(s, np.Name).Outcomes, kept)
		},
		Entry("no outcomes", nil, RegistrationUnknown, []bool(nil)),
		Entry("one success", []bool{true}, RegistrationHealthy, []bool{true}),
		Entry("one failure", []bool{false}, RegistrationUnknown, []bool{false}),
		Entry("a failure, then a success", []bool{false, true}, RegistrationHealthy, []bool{false, true}),
		Entry("two failures", []bool{false, false}, RegistrationUnhealthy, []bool{false, false}),
		Entry("two failures, then a success", []bool{false, false, true}, RegistrationUnhealthy, []bool{false, false, true}),
		Entry("only the newest RegistrationBufferSize count", []bool{false, false, true, true, true}, RegistrationHealthy, []bool{false, true, true, true}),
	)
	It("should keep the newest failure's reason and message", func() {
		rec.Record(np, registrationNodeClassGeneration, RegistrationOutcome{Reason: "First", Message: "first"})
		rec.Record(np, registrationNodeClassGeneration, RegistrationOutcome{Reason: "Second", Message: "second"})
		rec.Record(np, registrationNodeClassGeneration, RegistrationOutcome{Registered: true})
		Expect(s.RegistrationHealth(np, registrationNodeClassGeneration)).To(Equal(RegistrationHealth{Status: RegistrationUnhealthy, Reason: "Second", Message: "second"}))
	})
	It("should keep each NodePool's own outcomes", func() {
		a, b := registrationNodePool("a"), registrationNodePool("b")
		s.Observe(a)
		s.Observe(b)
		recordOutcome(rec, a, true)
		recordOutcome(rec, b, false)
		recordOutcome(rec, b, false)
		expectRegistration(s, a, RegistrationHealthy)
		expectRegistration(s, b, RegistrationUnhealthy)
	})
	It("should never modify the outcomes a reader holds", func() {
		recordOutcome(rec, np, true)
		held := registration(s, np.Name).Outcomes
		copied := slices.Clone(held)
		recordOutcome(rec, np, false)
		Expect(held).To(Equal(copied), "outcomes a reader held changed")
	})
	It("should record every concurrent outcome", func() {
		// Each round records against a new NodePool spec, which starts with no outcomes, so every round races
		// RegistrationBufferSize records from none.
		for generation := int64(1); generation <= 200; generation++ {
			spec := np.DeepCopy()
			spec.Generation = generation
			var wg sync.WaitGroup
			for i := range RegistrationBufferSize {
				wg.Go(func() { recordOutcome(rec, spec, i%2 == 0) })
			}
			wg.Wait()
			got := registration(s, np.Name)
			Expect(got.RegistrationKey).To(Equal(RegistrationKeyOf(spec, registrationNodeClassGeneration)))
			Expect(got.Outcomes).To(HaveLen(RegistrationBufferSize), "generation %d recorded %v, want every outcome", generation, got.Outcomes)
			Expect(failures(got.Outcomes)).To(Equal(RegistrationBufferSize/2), "generation %d recorded %v, want every failure", generation, got.Outcomes)
			// Exactly half failed, which is unhealthy only if no outcome was lost.
			expectRegistration(s, spec, RegistrationUnhealthy)
		}
	})
	DescribeTable("should start a new spec with no outcomes",
		func(change func(np *v1.NodePool, nodeClassGeneration int64) (*v1.NodePool, int64)) {
			recordOutcome(rec, np, false)
			recordOutcome(rec, np, false)
			expectRegistration(s, np, RegistrationUnhealthy)

			changed, nodeClassGeneration := change(np.DeepCopy(), registrationNodeClassGeneration)
			Expect(s.RegistrationHealth(changed, nodeClassGeneration).Status).To(Equal(RegistrationUnknown), "the new spec counts the old spec's outcomes")
			rec.Record(changed, nodeClassGeneration, RegistrationOutcome{Registered: true})
			Expect(s.RegistrationHealth(changed, nodeClassGeneration).Status).To(Equal(RegistrationHealthy))
			Expect(registration(s, np.Name).Outcomes).To(Equal([]bool{true}), "the new spec's record keeps the old spec's outcomes")
			Expect(registration(s, np.Name).Reason).To(BeEmpty(), "the new spec's record keeps the old spec's failure")
			// The old spec's outcomes are gone, not hidden.
			expectRegistration(s, np, RegistrationUnknown)
		},
		Entry("on a NodePool generation change", func(np *v1.NodePool, nodeClassGeneration int64) (*v1.NodePool, int64) {
			np.Generation++
			return np, nodeClassGeneration
		}),
		Entry("on a NodeClass generation change", func(np *v1.NodePool, nodeClassGeneration int64) (*v1.NodePool, int64) {
			return np, nodeClassGeneration + 1
		}),
	)
	It("should ignore the outcomes of an earlier NodePool of the same name", func() {
		recordOutcome(rec, np, false)
		recordOutcome(rec, np, false)
		recreated := registrationNodePool(np.Name)
		recreated.ResourceVersion = "2"
		// Read before cluster state sees it, the recreated NodePool ignores the earlier one's outcomes.
		expectRegistration(s, recreated, RegistrationUnknown)
		s.Observe(recreated)
		recordOutcome(rec, recreated, true)
		expectRegistration(s, recreated, RegistrationHealthy)
		// The earlier NodePool's outcomes are gone, not hidden.
		expectRegistration(s, np, RegistrationUnknown)
		got := registration(s, np.Name)
		Expect(got.UID).To(Equal(recreated.UID), "Registration = %+v, want only the recreated NodePool's outcome", got)
		Expect(got.Outcomes).To(Equal([]bool{true}), "Registration = %+v, want only the recreated NodePool's outcome", got)
	})
	// A NodePool's outcomes drop with its entry, once the NodePool is gone or another NodePool takes its name, while
	// what's derived about the name stays.
	It("should go with its NodePool", func() {
		recordOutcome(rec, np, false)
		recordOutcome(rec, np, false)
		// An update of the same NodePool keeps them.
		updated := np.DeepCopy()
		updated.ResourceVersion = "2"
		s.Observe(updated)
		expectRegistration(s, np, RegistrationUnhealthy)

		s.Forget(np.Name)
		got := registration(s, np.Name)
		Expect(got.UID).To(BeEmpty(), "Registration = %+v once the NodePool is gone, want none", got)
		Expect(got.Outcomes).To(BeNil(), "Registration = %+v once the NodePool is gone, want none", got)
		_, ok := s.store.Get(np.Name)
		Expect(ok).To(BeFalse(), "a gone NodePool keeps its entry")
		recordOutcome(rec, np, true)
		Expect(registration(s, np.Name)).To(BeZero(), "recorded an outcome for a NodePool cluster state doesn't hold")

		// A NodePool that replaces another under its name starts with none, though it keeps what the NodeClaims count.
		s.Observe(np)
		recordOutcome(rec, np, true)
		s.nodes.NodeClaimStore.Observe(&v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "nc", UID: "nc", ResourceVersion: "1", Labels: map[string]string{v1.NodePoolLabelKey: np.Name}},
			Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
		})
		replacement := registrationNodePool(np.Name)
		replacement.ResourceVersion = "3"
		s.Observe(replacement)
		Expect(registration(s, np.Name).Outcomes).To(BeNil(), "outcomes once another NodePool took the name, want none")
		f := s.Get(np.Name)
		Expect(f.NodeClaims.Active).To(Equal(1), "fields = %+v once another NodePool took the name, want the counts kept", f)
	})
})
