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

package disruption_test

import (
	"math/rand"
	"time"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// backoffNodePool builds a dynamic NodePool that disrupts freely (100% budget) so that back-off,
// not budgets, is what gates selection in these tests.
func backoffNodePool() *v1.NodePool {
	return test.NodePool(v1.NodePool{
		Spec: v1.NodePoolSpec{
			Disruption: v1.Disruption{
				ConsolidateAfter: v1.MustParseNillableDuration("1h"),
				Budgets:          []v1.Budget{{Nodes: "100%"}},
			},
		},
	})
}

// driftedNodeClaimAndNode builds a drifted NodeClaim/Node in the given NodePool. driftAge is added
// to the current time to control the Drifted condition's LastTransitionTime (more negative == older
// == selected first). When rs is non-nil, a bound pod is returned so the node is non-empty and drift
// launches a replacement for it.
func driftedNodeClaimAndNode(nodePoolName string, driftAge time.Duration, rs *appsv1.ReplicaSet) (*v1.NodeClaim, *corev1.Node, *corev1.Pod) {
	nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePoolName,
				corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
				v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			},
		},
		Status: v1.NodeClaimStatus{
			ProviderID: test.RandomProviderID(),
			Allocatable: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU:  resource.MustParse("32"),
				corev1.ResourcePods: resource.MustParse("100"),
			},
		},
	})
	nodeClaim.Status.Conditions = append(nodeClaim.Status.Conditions, status.Condition{
		Type:               v1.ConditionTypeDrifted,
		Status:             metav1.ConditionTrue,
		Reason:             v1.ConditionTypeDrifted,
		Message:            v1.ConditionTypeDrifted,
		LastTransitionTime: metav1.Time{Time: env.Clock.Now().Add(driftAge)},
	})
	var pod *corev1.Pod
	if rs != nil {
		pod = test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app": "test"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         "apps/v1",
					Kind:               "ReplicaSet",
					Name:               rs.Name,
					UID:                rs.UID,
					Controller:         new(true),
					BlockOwnerDeletion: new(true),
				}},
			},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("30")},
			},
		})
	}
	return nodeClaim, node, pod
}

var _ = Describe("Drift back-off", func() {
	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodePoolDriftBackoff: new(true)}}))
	})
	Context("Selection", func() {
		It("skips a backed-off NodePool's candidates and services a healthy NodePool instead", func() {
			backedOff := backoffNodePool()
			healthy := backoffNodePool()
			// The backed-off pool's candidate is the oldest, so absent back-off it would be selected first.
			bNC, bNode, _ := driftedNodeClaimAndNode(backedOff.Name, -time.Hour, nil)
			hNC, hNode, _ := driftedNodeClaimAndNode(healthy.Name, -time.Minute, nil)

			ExpectApplied(ctx, env.Client, backedOff, healthy, bNC, bNode, hNC, hNode)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{bNode, hNode}, []*v1.NodeClaim{bNC, hNC})

			// Back off the older pool.
			driftBackoff.Fail(backedOff, env.Clock.Now())
			Expect(driftBackedOff(backedOff)).To(BeTrue())

			ExpectSingletonReconciled(ctx, disruptionController)

			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Candidates).To(HaveLen(1))
			Expect(cmds[0].Candidates[0].NodePool.Name).To(Equal(healthy.Name))
			// A back-off skip event should have been surfaced for the backed-off pool.
			Expect(recorder.Calls(events.DisruptionBackoff)).To(BeNumerically(">=", 1))
		})
		It("does not skip a backed-off NodePool when NodePoolDriftBackoff is disabled", func() {
			backedOff := backoffNodePool()
			healthy := backoffNodePool()
			bNC, bNode, _ := driftedNodeClaimAndNode(backedOff.Name, -time.Hour, nil)
			hNC, hNode, _ := driftedNodeClaimAndNode(healthy.Name, -time.Minute, nil)

			ExpectApplied(ctx, env.Client, backedOff, healthy, bNC, bNode, hNC, hNode)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{bNode, hNode}, []*v1.NodeClaim{bNC, hNC})

			driftBackoff.Fail(backedOff, env.Clock.Now())
			Expect(driftBackedOff(backedOff)).To(BeTrue())

			ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodePoolDriftBackoff: new(false)}}))
			ExpectSingletonReconciled(ctx, disruptionController)

			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Candidates[0].NodePool.Name).To(Equal(backedOff.Name))
		})
		It("registers a zero-valued back-off counter for a healthy NodePool with drift candidates", func() {
			nodePool := backoffNodePool()
			nc, node, _ := driftedNodeClaimAndNode(nodePool.Name, -time.Hour, nil)
			ExpectApplied(ctx, env.Client, nodePool, nc, node)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node}, []*v1.NodeClaim{nc})

			// Evaluating drift seeds a 0 series so operators see healthy pools, not a missing metric.
			ExpectSingletonReconciled(ctx, disruptionController)
			ExpectMetricCounterValue(disruption.DriftBackoffsTotal, 0, map[string]string{metrics.NodePoolLabel: nodePool.Name})
		})
		It("becomes eligible again once the back-off window elapses", func() {
			nodePool := backoffNodePool()
			nc, node, _ := driftedNodeClaimAndNode(nodePool.Name, -time.Hour, nil)
			ExpectApplied(ctx, env.Client, nodePool, nc, node)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node}, []*v1.NodeClaim{nc})

			driftBackoff.Fail(nodePool, env.Clock.Now())
			Expect(driftBackedOff(nodePool)).To(BeTrue())

			// While backed off, drift produces no command for the pool.
			ExpectSingletonReconciled(ctx, disruptionController)
			Expect(queue.GetCommands()).To(HaveLen(0))

			// The default back-off window is <= 1m; step past it and the pool is serviceable again.
			env.Clock.Step(2 * time.Minute)
			Expect(driftBackedOff(nodePool)).To(BeFalse())

			ExpectSingletonReconciled(ctx, disruptionController)
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Candidates[0].NodePool.Name).To(Equal(nodePool.Name))
		})
		It("resets back-off and level when a NodePool becomes un-drifted", func() {
			nodePool := backoffNodePool()
			nc, node, _ := driftedNodeClaimAndNode(nodePool.Name, -time.Hour, nil)
			ExpectApplied(ctx, env.Client, nodePool, nc, node)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node}, []*v1.NodeClaim{nc})

			// Escalate to level 2 across two windows.
			Expect(driftBackoff.Fail(nodePool, env.Clock.Now())).To(BeTrue())
			env.Clock.Step(2 * time.Minute)
			Expect(driftBackoff.Fail(nodePool, env.Clock.Now())).To(BeTrue())
			level, _, backedOff := driftBackoff.Get(nodePool)
			Expect(level).To(Equal(2))
			Expect(backedOff).To(BeTrue())

			// Clear the Drifted condition on the NodeClaim and update cluster state.
			_ = nc.StatusConditions().Clear(v1.ConditionTypeDrifted)
			ExpectApplied(ctx, env.Client, nc)

			ExpectSingletonReconciled(ctx, disruptionController)
			level, _, backedOff = driftBackoff.Get(nodePool)
			Expect(level).To(Equal(0))
			Expect(backedOff).To(BeFalse())
		})
	})
	Context("Starvation regression", func() {
		It("lets a younger NodePool progress after the oldest pool's replacement fails unrecoverably", func() {
			rs := test.ReplicaSet()
			ExpectApplied(ctx, env.Client, rs)

			stuck := backoffNodePool()
			healthy := backoffNodePool()
			// Both non-empty so selection is ordered purely by drift age; stuck is oldest.
			stuckNC, stuckNode, stuckPod := driftedNodeClaimAndNode(stuck.Name, -time.Hour, rs)
			healthyNC, healthyNode, healthyPod := driftedNodeClaimAndNode(healthy.Name, -time.Minute, rs)

			ExpectApplied(ctx, env.Client, stuck, healthy, stuckNC, stuckNode, healthyNC, healthyNode, stuckPod, healthyPod)
			ExpectManualBinding(ctx, env.Client, stuckPod, stuckNode)
			ExpectManualBinding(ctx, env.Client, healthyPod, healthyNode)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{stuckNode, healthyNode}, []*v1.NodeClaim{stuckNC, healthyNC})

			// Pass 1: the oldest (stuck) pool is selected and a replacement is launched.
			ExpectSingletonReconciled(ctx, disruptionController)
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Candidates[0].NodePool.Name).To(Equal(stuck.Name))
			Expect(cmds[0].Replacements).To(HaveLen(1))

			// Simulate an ICE: the replacement NodeClaim is deleted before it initializes. The queue
			// observes this as an unrecoverable failure, which arms back-off for the stuck pool and
			// returns its candidate to the pool unchanged.
			replacementName := cmds[0].Replacements[0].Name
			replacementNC := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, types.NamespacedName{Name: replacementName}, replacementNC)).To(Succeed())
			ExpectDeleted(ctx, env.Client, replacementNC)

			ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
			Expect(driftBackedOff(stuck)).To(BeTrue())
			ExpectExists(ctx, env.Client, stuckNC)

			// Pass 2: the stuck pool is skipped, so the younger healthy pool finally makes progress.
			ExpectSingletonReconciled(ctx, disruptionController)
			cmds = queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Candidates[0].NodePool.Name).To(Equal(healthy.Name))
		})
	})
})

var _ = Describe("DriftBackoff", func() {
	const (
		base = time.Minute
		max  = 10 * time.Minute
	)
	var backoff *disruption.DriftBackoff
	var spark, ingress *v1.NodePool

	BeforeEach(func() {
		spark, ingress = test.NodePool(), test.NodePool()
		ExpectApplied(ctx, env.Client, spark, ingress)
		backoff = disruption.NewDriftBackoff(env.Clock, cs.NodePoolState, cs.DriftBackoffs,
			disruption.WithDriftBackoffDelays(base, max),
			disruption.WithDriftBackoffRand(rand.New(rand.NewSource(1))), //nolint:gosec // deterministic test source
		)
	})
	backedOff := func(nodePool *v1.NodePool) bool {
		_, _, b := backoff.Get(nodePool)
		return b
	}
	// expireWindow steps the clock past the NodePool's window, which lapses it.
	expireWindow := func(nodePool *v1.NodePool) {
		_, until, _ := backoff.Get(nodePool)
		if until.After(env.Clock.Now()) {
			env.Clock.Step(until.Sub(env.Clock.Now()) + time.Second)
		}
	}

	It("should treat a never-failed NodePool as healthy", func() {
		level, until, b := backoff.Get(spark)
		Expect(level).To(Equal(0))
		Expect(until).To(BeZero())
		Expect(b).To(BeFalse())
	})
	It("should arm an exponentially-growing, jittered window on consecutive failures", func() {
		for _, expected := range []time.Duration{base, 2 * base, 4 * base, 8 * base} {
			expireWindow(spark)
			Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
			_, until, b := backoff.Get(spark)
			window := until.Sub(env.Clock.Now())
			Expect(window).To(BeNumerically(">=", expected/2))
			Expect(window).To(BeNumerically("<", expected))
			Expect(b).To(BeTrue())
		}
	})
	It("should cap the window at the maximum and saturate the level", func() {
		var level int
		for range 12 {
			expireWindow(spark)
			Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
			level, _, _ = backoff.Get(spark)
		}
		Expect(level).To(Equal(5))
		_, until, _ := backoff.Get(spark)
		window := until.Sub(env.Clock.Now())
		Expect(window).To(BeNumerically(">=", max/2))
		Expect(window).To(BeNumerically("<", max))
	})
	It("should do nothing while the NodePool is already backed off", func() {
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		level1, until1, _ := backoff.Get(spark)
		Expect(level1).To(Equal(1))

		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeFalse())
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeFalse())
		level2, until2, _ := backoff.Get(spark)
		Expect(level2).To(Equal(1))
		Expect(until2).To(Equal(until1))
	})
	It("should not escalate for a delayed failure from the same attempt burst", func() {
		firstAttemptStartedAt := env.Clock.Now()
		delayedAttemptStartedAt := env.Clock.Now()

		env.Clock.Step(time.Second)
		Expect(backoff.Fail(spark, firstAttemptStartedAt)).To(BeTrue())
		_, until1, _ := backoff.Get(spark)

		expireWindow(spark)
		Expect(backoff.Fail(spark, delayedAttemptStartedAt)).To(BeFalse())
		level, _, b := backoff.Get(spark)
		Expect(level).To(Equal(1))
		Expect(b).To(BeFalse())
		Expect(until1.Before(env.Clock.Now())).To(BeTrue())

		// An attempt started after the prior effective failure belongs to the next retry cycle.
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		level, _, _ = backoff.Get(spark)
		Expect(level).To(Equal(2))
	})
	It("should keep the level once the window lapses, and escalate from it", func() {
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		expireWindow(spark)
		level, _, b := backoff.Get(spark)
		Expect(level).To(Equal(1))
		Expect(b).To(BeFalse())

		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		level, _, _ = backoff.Get(spark)
		Expect(level).To(Equal(2))
	})
	It("should return to healthy on Reset", func() {
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		Expect(backedOff(spark)).To(BeTrue())

		backoff.Reset(spark.Name)
		level, _, b := backoff.Get(spark)
		Expect(level).To(Equal(0))
		Expect(b).To(BeFalse())
	})
	It("should reset the back-off and level of the NodePools with no drifted NodeClaims", func() {
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: ingress.Name}}})
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		Expect(cs.NodePoolState.Fields(ingress.Name).Drifted).To(Equal(1))

		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		Expect(backoff.Fail(ingress, env.Clock.Now())).To(BeTrue())
		expireWindow(spark)
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())

		backoff.ResetUndrifted()
		level, _, b := backoff.Get(spark)
		Expect(level).To(Equal(0))
		Expect(b).To(BeFalse())
		Expect(backedOff(ingress)).To(BeTrue())
	})
	It("should spread out NodePools that fail at the same instant", func() {
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		Expect(backoff.Fail(ingress, env.Clock.Now())).To(BeTrue())
		_, sparkUntil, _ := backoff.Get(spark)
		_, ingressUntil, _ := backoff.Get(ingress)
		Expect(sparkUntil).ToNot(Equal(ingressUntil))
	})
	It("should track NodePools independently", func() {
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		Expect(backedOff(spark)).To(BeTrue())
		Expect(backedOff(ingress)).To(BeFalse())
	})
	It("should drop a NodePool's back-off with the NodePool, so one recreated under its name starts healthy", func() {
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeTrue())
		ExpectDeleted(ctx, env.Client, spark)
		recreated := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: spark.Name}})
		ExpectApplied(ctx, env.Client, recreated)

		level, _, b := backoff.Get(recreated)
		Expect(level).To(Equal(0))
		Expect(b).To(BeFalse())
		// A failure of the deleted NodePool's attempt doesn't back off the recreated one.
		Expect(backoff.Fail(spark, env.Clock.Now())).To(BeFalse())
		Expect(backedOff(recreated)).To(BeFalse())
	})
})
