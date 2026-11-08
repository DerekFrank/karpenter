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

package nodepoolhealth_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolhealth"
)

var (
	npState  *nodepoolhealth.State
	nodePool *v1.NodePool
)

func newNodePool(name string) *v1.NodePool {
	return &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uuid.NewUUID()}}
}

var _ = BeforeSuite(func() {
	nodePool = newNodePool("default")
	npState = nodepoolhealth.NewState()
})

var _ = AfterEach(func() {
	npState.SetStatus(nodePool, nodepoolhealth.StatusUnknown)
})

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "NodePoolHealthState")
}

var _ = Describe("NodePoolHealthState", func() {
	It("should record all concurrent updates correctly", func() {
		concurrentState := nodepoolhealth.NewState()
		done := make(chan bool, 4)
		same := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "test", UID: types.UID("test")}}
		// Add exactly 2 false values (50% of buffer size 4)
		for range 2 {
			go func() {
				defer func() { done <- true }()
				concurrentState.Update(same, false)
			}()
		}
		// Add exactly 2 true values
		for range 2 {
			go func() {
				defer func() { done <- true }()
				concurrentState.Update(same, true)
			}()
		}
		for range 4 {
			<-done
		}
		// With proper locks: exactly 50% false = StatusUnhealthy
		// Without locks: could lose updates = wrong status
		Expect(concurrentState.Status(same)).To(Equal(nodepoolhealth.StatusUnhealthy))
	})
	It("should expect status unknown for a new nodePool with empty buffer", func() {
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusUnknown))
	})
	It("should expect status healthy for a nodePool with one true entry", func() {
		npState.Update(nodePool, true)
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusHealthy))
	})
	It("should expect status unhealthy for a nodePool with two false entries", func() {
		npState.Update(nodePool, false)
		npState.Update(nodePool, false)
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusUnhealthy))
	})
	It("should expect status unhealthy for a nodePool with two false entries and one true entry", func() {
		npState.SetStatus(nodePool, nodepoolhealth.StatusUnhealthy)
		npState.Update(nodePool, true)
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusUnhealthy))
	})
	It("should return the correct status in case of multiple nodepools stored in the state", func() {
		npState.Update(nodePool, true)
		nodePool2 := newNodePool("other")
		npState.Update(nodePool2, false)
		npState.Update(nodePool2, false)
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusHealthy))
		Expect(npState.Status(nodePool2)).To(Equal(nodepoolhealth.StatusUnhealthy))
		npState.SetStatus(nodePool2, nodepoolhealth.StatusUnknown)
		Expect(npState.Status(nodePool2)).To(Equal(nodepoolhealth.StatusUnknown))
	})
	It("should create a copy of the tracker and update it and not the original tracker in case of Dry Run", func() {
		npState.SetStatus(nodePool, nodepoolhealth.StatusHealthy)
		npState.Update(nodePool, false)
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusHealthy))

		Expect(npState.DryRun(nodePool, false)).To(Equal(nodepoolhealth.StatusUnhealthy))
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusHealthy))
	})
	It("should reset the buffer first when setting status", func() {
		npState.Update(nodePool, false)
		npState.Update(nodePool, false)
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusUnhealthy))

		// This SetStatus call should first reset the buffer and then add entries to the buffer such that the status becomes healthy
		npState.SetStatus(nodePool, nodepoolhealth.StatusHealthy)
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusHealthy))
	})
	It("should keep only the newest BufferSize outcomes", func() {
		npState.SetStatus(nodePool, nodepoolhealth.StatusUnhealthy)
		for range nodepoolhealth.BufferSize - 1 {
			npState.Update(nodePool, true)
		}
		// One failure of the four newest outcomes is below the threshold.
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusHealthy))
		f, ok := npState.Get(nodePool.Name)
		Expect(ok).To(BeTrue())
		Expect(f.Outcomes).To(Equal([]bool{false, true, true, true}))
	})
	It("should not modify the outcomes a reader holds", func() {
		npState.Update(nodePool, true)
		before, _ := npState.Get(nodePool.Name)
		held := append([]bool(nil), before.Outcomes...)
		npState.Update(nodePool, false)
		Expect(before.Outcomes).To(Equal(held))
	})
	It("should drop a NodePool's outcomes when it's deleted", func() {
		npState.Update(nodePool, false)
		npState.Update(nodePool, false)
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusUnhealthy))

		npState.Delete(nodePool.Name)
		_, ok := npState.Get(nodePool.Name)
		Expect(ok).To(BeFalse())
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusUnknown))
	})
	It("should drop a NodePool's entry when its status is set to unknown", func() {
		npState.Update(nodePool, true)
		npState.SetStatus(nodePool, nodepoolhealth.StatusUnknown)
		_, ok := npState.Get(nodePool.Name)
		Expect(ok).To(BeFalse())
	})
	It("should not count the outcomes of an earlier NodePool with the same name", func() {
		npState.Update(nodePool, false)
		npState.Update(nodePool, false)
		recreated := newNodePool(nodePool.Name)
		Expect(npState.Status(recreated)).To(Equal(nodepoolhealth.StatusUnknown))
		Expect(npState.DryRun(recreated, true)).To(Equal(nodepoolhealth.StatusHealthy))

		npState.Update(recreated, true)
		Expect(npState.Status(recreated)).To(Equal(nodepoolhealth.StatusHealthy))
		f, _ := npState.Get(nodePool.Name)
		Expect(f).To(Equal(nodepoolhealth.Fields{UID: recreated.UID, Outcomes: []bool{true}}))
		// The earlier NodePool's outcomes are gone, not hidden.
		Expect(npState.Status(nodePool)).To(Equal(nodepoolhealth.StatusUnknown))
	})
})
