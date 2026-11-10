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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("ReplayValidator", func() {
	var nodePool *v1.NodePool
	var single *disruption.SingleNodeConsolidation
	var replay *disruption.ReplayValidator

	nodeOf := func(it string, cpu string) (*v1.NodeClaim, *corev1.Node) {
		nc, n := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: it,
				v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			}},
			Status: v1.NodeClaimStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourcePods: resource.MustParse("100")}},
		})
		nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		return nc, n
	}
	commands := func() []disruption.Command {
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, single.ShouldDisrupt, disruption.GracefulDisruptionClass, queue)
		Expect(err).ToNot(HaveOccurred())
		cmds, err := single.ComputeCommands(ctx, map[string]int{nodePool.Name: 10}, candidates...)
		Expect(err).ToNot(HaveOccurred())
		return cmds
	}

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			Budgets:             []v1.Budget{{Nodes: "100%"}},
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		}}})
		c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)
		single = disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{}))
		replay = disruption.NewSingleReplayValidator(c)
	})

	It("accepts a replacement command whose placement still holds, and refuses it once the candidate's pods are gone", func() {
		nc, n := nodeOf(mostExpensiveInstance.Name, "32")
		pod := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}})
		ExpectApplied(ctx, env.Client, nodePool, nc, n, pod)
		ExpectManualBinding(ctx, env.Client, pod, n)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{n}, []*v1.NodeClaim{nc})

		cmds := commands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Replacements).To(HaveLen(1))
		result := replay.Check(ctx, cmds[0])
		Expect(result.Err).ToNot(HaveOccurred())
		Expect(result.Accepted).To(BeTrue())
		Expect(result.Path).To(Equal(disruption.ReplayPathReplay))

		ExpectDeleted(ctx, env.Client, pod)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
		result = replay.Check(ctx, cmds[0])
		Expect(result.Accepted).To(BeFalse())
		Expect(result.Reason).To(Equal(disruption.ReplayReasonCandidate))
	})

	It("refuses a delete command once its destination fills up, and accepts it again by search when another node has room", func() {
		ncA, nA := nodeOf(mostExpensiveInstance.Name, "4")
		ncB, nB := nodeOf(mostExpensiveInstance.Name, "4")
		small := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}})
		busy := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}}})
		ExpectApplied(ctx, env.Client, nodePool, ncA, nA, ncB, nB, small, busy)
		ExpectManualBinding(ctx, env.Client, small, nA)
		ExpectManualBinding(ctx, env.Client, busy, nB)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{nA, nB}, []*v1.NodeClaim{ncA, ncB})

		cmds := commands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Candidates).To(HaveLen(1))
		Expect(cmds[0].Replacements).To(BeEmpty())
		dest := lo.Ternary(cmds[0].Candidates[0].Name() == nA.Name, nB, nA)
		Expect(replay.Check(ctx, cmds[0]).Accepted).To(BeTrue())

		// Fill the destination, then add C with room: the pinned placement fails, and the fallback finds C.
		filler := test.Pod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}}})
		ExpectApplied(ctx, env.Client, filler)
		ExpectManualBinding(ctx, env.Client, filler, dest)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(dest))
		result := replay.Check(ctx, cmds[0])
		Expect(result.Accepted).To(BeFalse())
		Expect(result.Reason).To(Equal(disruption.ReplayReasonReplacementCount))

		ncC, nC := nodeOf(mostExpensiveInstance.Name, "4")
		ExpectApplied(ctx, env.Client, ncC, nC)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{nC}, []*v1.NodeClaim{ncC})
		result = replay.Check(ctx, cmds[0])
		Expect(result.Err).ToNot(HaveOccurred())
		Expect(result.Accepted).To(BeTrue())
		Expect(result.Path).To(Equal(disruption.ReplayPathFallback))
	})
})
