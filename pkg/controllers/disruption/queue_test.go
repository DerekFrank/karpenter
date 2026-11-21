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
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"

	"sigs.k8s.io/karpenter/pkg/cluster/virtualpods"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var (
	nodeClaim1, nodeClaim2 *v1.NodeClaim
	nodePool               *v1.NodePool
	node1, node2           *corev1.Node
)

var _ = Describe("Queue", func() {
	BeforeEach(func() {
		nodePool = test.NodePool()
		nodeClaim1, node1 = test.NodeClaimAndNode(
			v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
						v1.CapacityTypeLabelKey:        cloudProvider.InstanceTypes[0].Offerings.Cheapest().Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       cloudProvider.InstanceTypes[0].Offerings.Cheapest().Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					ProviderID:  test.RandomProviderID(),
					Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
				},
			},
		)
		nodeClaim2, node2 = test.NodeClaimAndNode(
			v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
						v1.CapacityTypeLabelKey:        cloudProvider.InstanceTypes[0].Offerings.Cheapest().Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       cloudProvider.InstanceTypes[0].Offerings.Cheapest().Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					ProviderID:  test.RandomProviderID(),
					Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
				},
			},
		)
		node1.Spec.Taints = append(node1.Spec.Taints, v1.DisruptedNoScheduleTaint)
		node2.Spec.Taints = append(node2.Spec.Taints, v1.DisruptedNoScheduleTaint)
	})
	Context("Limits", func() {
		var nodeEntry karpentercluster.NodeClaimEntry
		var command func() *disruption.Command
		BeforeEach(func() {
			node1.Spec.Taints = nil
			nodeClaim1.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32")}
			command = func() *disruption.Command {
				nct := scheduling.NewNodeClaimTemplate(nodePool)
				nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
				return &disruption.Command{
					Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
					CreationTimestamp: env.Clock.Now(),
					ID:                uuid.New(),
					Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
					Replacements:      []*disruption.Replacement{{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}}},
				}
			}
		})
		setup := func(cpuLimit resource.Quantity) {
			nodePool.Spec.Limits = v1.Limits{corev1.ResourceCPU: cpuLimit}
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry = nodeClaimEntryOf(nodeClaim1)
		}
		// fitsInPlace is a CPU limit the candidate and the largest replacement don't fit together, but the replacement
		// fits in the candidate's place.
		fitsInPlace := func() resource.Quantity {
			largest := provisioning.MaxCapacity(cloudProvider.InstanceTypes)[corev1.ResourceCPU]
			return lo.Ternary(largest.Cmp(resource.MustParse("32")) > 0, largest, resource.MustParse("32"))
		}
		It("should start a command whose replacement fits once its candidate stops counting", func() {
			setup(fitsInPlace())
			Expect(queue.StartCommand(ctx, command())).To(Succeed())
			Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))
		})
		It("should refuse a command whose replacement doesn't fit before marking its candidate", func() {
			largest := provisioning.MaxCapacity(cloudProvider.InstanceTypes)[corev1.ResourceCPU]
			largest.Sub(resource.MustParse("1"))
			setup(largest)
			Expect(queue.StartCommand(ctx, command())).ToNot(Succeed())

			Expect(ExpectNodeExists(ctx, env.Client, node1.Name).Spec.Taints).ToNot(ContainElement(v1.DisruptedNoScheduleTaint))
			Expect(ExpectExists(ctx, env.Client, nodeClaim1).StatusConditions().Get(v1.ConditionTypeDisruptionReason)).To(BeNil())
			Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(1))
			Expect(cs.NodePoolState.Fields(nodePool.Name).ReservedResources).To(BeEmpty())
		})
	})
	Context("Reconcile", func() {
		It("should delete a candidate's NodeClaim without writing the NodeClaim cluster state holds", func() {
			// The finalizer keeps the NodeClaim in storage after the delete, so the client decodes the server's response
			// into the object it deletes. In a running controller, cluster state holds the informer's own NodeClaim.
			nodeClaim1.Finalizers = []string{v1.TerminationFinalizer}
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)
			held := nodeEntry.Object
			want := held.DeepCopy()

			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())
			ExpectObjectReconciled(ctx, env.Client, queue, want)

			Expect(ExpectExists(ctx, env.Client, nodeClaim1).DeletionTimestamp.IsZero()).To(BeFalse())
			Expect(held).To(Equal(want))
		})
		It("should keep nodes tainted when replacements haven't finished initialization", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})

			nct := scheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements := []*disruption.Replacement{
				{
					NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct},
				},
			}

			nodeEntry := entryOfNode(node1)
			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      replacements,
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())

			node1 = ExpectNodeExists(ctx, env.Client, node1.Name)
			Expect(node1.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))

			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)

			// Update state
			Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))
			node1 = ExpectNodeExists(ctx, env.Client, node1.Name)
			Expect(node1.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
		})
		It("should not return an error when handling commands before the timeout", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)

			nct := scheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements := []*disruption.Replacement{
				{
					NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct},
				},
			}

			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      replacements,
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())
			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			Expect(queue.HasAny(nodeEntry.Object.Name)).To(BeTrue()) // Expect the command to still be in the queue
		})
		It("should not return an error when the NodeClaim doesn't exist but the NodeCliam is in cluster state", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)

			nct := scheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements := []*disruption.Replacement{
				{
					NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct},
				},
			}

			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      replacements,
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())

			replacementNodeClaim := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd.Replacements[0].Name}, replacementNodeClaim))
			ExpectNodeClaimDeployedOrFailed(ctx, env.Client, cloudProvider, replacementNodeClaim)
			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			Expect(queue.HasAny(nodeEntry.Object.Name)).To(BeTrue()) // Expect the command to still be in the queue
		})
		It("should untaint nodes when a command times out", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)

			nct := scheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements := []*disruption.Replacement{
				{
					NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct},
				},
			}

			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      replacements,
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())

			// Step the clock to trigger the timeout.
			env.Clock.Step(11 * time.Minute)

			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			node1 = ExpectNodeExists(ctx, env.Client, node1.Name)
			Expect(node1.Spec.Taints).ToNot(ContainElement(v1.DisruptedNoScheduleTaint))
		})
		It("should emit the consolidation type in the failures metric when a command fails", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)

			nct := scheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements := []*disruption.Replacement{
				{
					NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct},
				},
			}

			// A single-node consolidation command's Decision() is "replace" (it has both
			// candidates and replacements) while its ConsolidationType() is "single". The
			// failures metric must report the consolidation type, not the decision.
			c := disruption.MakeConsolidation(env.Clock, stores, cs.NodePoolState, env.Overlay, prov, cloudProvider, recorder, queue)
			cmd := &disruption.Command{
				Method:            disruption.NewSingleNodeConsolidation(c),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      replacements,
			}
			Expect(cmd.Decision()).To(Equal(disruption.ReplaceDecision))
			Expect(cmd.ConsolidationType()).To(Equal(disruption.SingleNodeConsolidationType.Name))
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())

			// This metric isn't reset between specs, so clear it to isolate this failure.
			disruption.DisruptionQueueFailuresTotal.Reset()

			// Step the clock to trigger the timeout so the command fails.
			env.Clock.Step(11 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)

			ExpectMetricCounterValue(disruption.DisruptionQueueFailuresTotal, 1, map[string]string{
				disruption.ConsolidationTypeLabel: disruption.SingleNodeConsolidationType.Name,
			})
		})
		It("should fully handle a command when replacements are initialized", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)

			nct := scheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements := []*disruption.Replacement{
				{
					NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct},
				},
			}

			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      replacements,
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())

			replacementNodeClaim := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd.Replacements[0].Name}, replacementNodeClaim))
			replacementNodeClaim, replacementNode := ExpectNodeClaimDeployedOrFailed(ctx, env.Client, cloudProvider, replacementNodeClaim)

			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			// Get the command
			Expect(cmd.Replacements[0].Initialized).To(BeFalse())

			Expect(recorder.DetectedEvent(disruptionevents.Launching(replacementNodeClaim, string(cmd.Reason())).Message)).To(BeTrue())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(replacementNodeClaim).Message)).To(BeTrue())

			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock,
				[]*corev1.Node{replacementNode}, []*v1.NodeClaim{replacementNodeClaim})

			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			Expect(cmd.Replacements[0].Initialized).To(BeTrue())

			terminatingEvents := disruptionevents.Terminating(node1, nodeClaim1, string(cmd.Reason()), "")
			Expect(recorder.DetectedEvent(terminatingEvents[0].Message)).To(BeTrue())
			Expect(recorder.DetectedEvent(terminatingEvents[1].Message)).To(BeTrue())

			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
			// And expect the nodeClaim and node to be deleted
			ExpectNotFound(ctx, env.Client, nodeClaim1, node1)
		})
		It("should only finish a command when all replacements are initialized", func() {
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim1, node1)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)

			nct := scheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			nct2 := scheduling.NewNodeClaimTemplate(nodePool)
			nct2.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements := []*disruption.Replacement{
				{
					NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct},
				},
				{
					NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct2},
				},
			}

			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      replacements,
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())

			replacementNodeClaim1 := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd.Replacements[0].Name}, replacementNodeClaim1))
			replacementNodeClaim1, replacementNode1 := ExpectNodeClaimDeployedOrFailed(ctx, env.Client, cloudProvider, replacementNodeClaim1)
			replacementNodeClaim2 := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd.Replacements[1].Name}, replacementNodeClaim2))
			replacementNodeClaim2, replacementNode2 := ExpectNodeClaimDeployedOrFailed(ctx, env.Client, cloudProvider, replacementNodeClaim2)

			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			Expect(cmd.Replacements[0].Initialized).To(BeFalse())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(nodeClaim1).Message)).To(BeTrue())
			Expect(cmd.Replacements[1].Initialized).To(BeFalse())

			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{replacementNode1}, []*v1.NodeClaim{replacementNodeClaim1})

			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			Expect(cmd.Replacements[0].Initialized).To(BeTrue())
			Expect(cmd.Replacements[1].Initialized).To(BeFalse())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(nodeClaim1).Message)).To(BeTrue())

			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{replacementNode2}, []*v1.NodeClaim{replacementNodeClaim2})

			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			Expect(cmd.Replacements[0].Initialized).To(BeTrue())
			Expect(cmd.Replacements[1].Initialized).To(BeTrue())

			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
			// And expect the nodeClaim and node to be deleted
			ExpectNotFound(ctx, env.Client, nodeClaim1, node1)
		})
		It("should not wait for replacements when none are needed", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)

			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      nil,
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())

			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)

			terminatingEvents := disruptionevents.Terminating(node1, nodeClaim1, string(cmd.Reason()), "")
			Expect(recorder.DetectedEvent(terminatingEvents[0].Message)).To(BeTrue())
			Expect(recorder.DetectedEvent(terminatingEvents[1].Message)).To(BeTrue())

			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
			// And expect the nodeClaim and node to be deleted
			ExpectNotFound(ctx, env.Client, nodeClaim1, node1)
		})
		DescribeTable("should publish the termination event",
			func(reason disruption.TerminateFirstReason, suffix string) {
				ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
				ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
				nodeEntry := nodeClaimEntryOf(nodeClaim1)

				cmd := &disruption.Command{
					Method:               disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
					CreationTimestamp:    env.Clock.Now(),
					ID:                   uuid.New(),
					Results:              scheduling.Results{},
					Candidates:           []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
					TerminateFirstReason: reason,
				}
				Expect(queue.StartCommand(ctx, cmd)).To(BeNil())
				ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)

				Expect(recorder.DetectedEvent("Disrupting Node: Drifted" + suffix)).To(BeTrue())
				Expect(recorder.DetectedEvent("Disrupting NodeClaim: Drifted" + suffix)).To(BeTrue())
			},
			Entry("for a node that doesn't go before its replacement", disruption.TerminateFirstReason(""), ""),
			Entry("for a node that goes before its replacement because its capacity reservation is full", disruption.TerminateFirstNoReservedCapacity,
				", terminating it before its replacement because its capacity reservation is full"),
			Entry("for a node that goes before its replacement because its NodePool is at its node limit", disruption.TerminateFirstStaticAtLimit,
				", terminating it before its replacement because its NodePool is at its node limit"),
		)
		It("should finish two commands in order as replacements are initialized", func() {
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim1, node1, nodeClaim2, node2)
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1, node2}, []*v1.NodeClaim{nodeClaim1, nodeClaim2})
			nodeEntry := nodeClaimEntryOf(nodeClaim1)
			nodeEntry2 := nodeClaimEntryOf(nodeClaim2)

			nct := scheduling.NewNodeClaimTemplate(nodePool)
			nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements := []*disruption.Replacement{{
				NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct},
			}}
			nct2 := scheduling.NewNodeClaimTemplate(nodePool)
			nct2.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
			replacements2 := []*disruption.Replacement{{
				NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct2},
			}}

			cmd := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
				Replacements:      replacements,
			}
			Expect(queue.StartCommand(ctx, cmd)).To(BeNil())
			cmd2 := &disruption.Command{
				Method:            disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff),
				CreationTimestamp: env.Clock.Now(),
				ID:                uuid.New(),
				Results:           scheduling.Results{},
				Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry2, nodePool)},
				Replacements:      replacements2,
			}
			Expect(queue.StartCommand(ctx, cmd2)).To(BeNil())

			replacementNodeClaim1 := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd.Replacements[0].Name}, replacementNodeClaim1))
			replacementNodeClaim2 := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd2.Replacements[0].Name}, replacementNodeClaim2))

			replacementNodeClaim1, replacementNode1 := ExpectNodeClaimDeployedOrFailed(ctx, env.Client, cloudProvider, replacementNodeClaim1)
			replacementNodeClaim2, replacementNode2 := ExpectNodeClaimDeployedOrFailed(ctx, env.Client, cloudProvider, replacementNodeClaim2)

			// Reconcile the first command and expect nothing to be initialized
			ExpectObjectReconciled(ctx, env.Client, queue, nodeEntry.Object)
			Expect(cmd.Replacements[0].Initialized).To(BeFalse())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(nodeClaim1).Message)).To(BeTrue())
			Expect(cmd2.Replacements[0].Initialized).To(BeFalse())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(nodeClaim2).Message)).To(BeTrue())

			// Make the first command's node initialized
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{replacementNode1}, []*v1.NodeClaim{replacementNodeClaim1})
			// Reconcile the second command and expect nothing to be initialized
			ExpectObjectReconciled(ctx, env.Client, queue, cmd2.Candidates[0].NodeClaim)
			Expect(cmd.Replacements[0].Initialized).To(BeFalse())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(nodeClaim1).Message)).To(BeTrue())
			Expect(cmd2.Replacements[0].Initialized).To(BeFalse())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(nodeClaim2).Message)).To(BeTrue())

			// Reconcile the first command and expect the replacement to be initialized
			ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
			Expect(cmd.Replacements[0].Initialized).To(BeTrue())
			Expect(cmd2.Replacements[0].Initialized).To(BeFalse())

			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
			ExpectNotFound(ctx, env.Client, nodeClaim1, node1)

			// Make the second command's node initialized
			ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{replacementNode2}, []*v1.NodeClaim{replacementNodeClaim2})

			// Reconcile the second command and expect the replacement to be initialized
			ExpectObjectReconciled(ctx, env.Client, queue, cmd2.Candidates[0].NodeClaim)
			Expect(cmd.Replacements[0].Initialized).To(BeTrue())
			Expect(cmd2.Replacements[0].Initialized).To(BeTrue())

			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim2)
			// And expect the nodeClaim and node to be deleted
			ExpectNotFound(ctx, env.Client, nodeClaim2, node2)
		})
		Context("Drift back-off", func() {
			BeforeEach(func() {
				ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodePoolDriftBackoff: new(true)}}))
			})
			backedOff := func() bool {
				_, _, b := driftBackoff.Get(nodePool)
				return b
			}
			driftMethod := func() disruption.Method {
				return disruption.NewDrift(env.Overlay, prov, recorder, env.Clock, driftBackoff)
			}
			emptinessMethod := func() disruption.Method {
				return disruption.NewEmptiness(disruption.MakeConsolidation(env.Clock, stores, cs.NodePoolState, env.Overlay, prov, cloudProvider, recorder, queue))
			}
			withReplacement := func(nodePool *v1.NodePool) []*disruption.Replacement {
				nct := scheduling.NewNodeClaimTemplate(nodePool)
				nct.InstanceTypeOptions = append([]*cloudprovider.InstanceType{}, cloudProvider.InstanceTypes...)
				return []*disruption.Replacement{{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}}}
			}

			// completeVia* drive a started command to its terminal state, encapsulating the mechanic
			// that produces each outcome and asserting on the StartCommand result.
			completeViaTimeout := func(cmd *disruption.Command) {
				Expect(queue.StartCommand(ctx, cmd)).To(BeNil())
				// Step past the command timeout so the replacement never initializes -> unrecoverable failure.
				env.Clock.Step(11 * time.Minute)
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
			}
			completeViaLaunchFailure := func(cmd *disruption.Command) {
				// A provisioner whose NodeClaim creates fail, so createReplacementNodeClaims (and therefore StartCommand)
				// fails before the command ever enters the queue: the failure is never observed by Queue.Reconcile, so
				// StartCommand itself must arm back-off.
				failing := &nodeClaimCreateErrorClient{Client: env.Overlay}
				p := provisioning.NewProvisioner(failing, recorder, cloudProvider, provisioning.StoresOf(cluster), cs.NodePoolState, cs.CapacityQuotaState, env.Clock, draController, virtualpods.NewVirtualPodCache(failing), store, cs.Nomination, cs.BufferPods, cs.PodScheduling)
				q := disruption.NewQueue(failing, recorder, cluster.NodeClaims, env.Clock, p, cs.Deletion, driftBackoff)
				err := q.StartCommand(ctx, cmd)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("launching replacement nodeclaim"))
				// The launch failure arms back-off and increments the counter exactly once (the pool's
				// name is unique per spec, so the counter is deterministic).
				ExpectMetricCounterValue(disruption.DriftBackoffsTotal, 1, map[string]string{metrics.NodePoolLabel: nodePool.Name})
			}
			completeViaSuccess := func(cmd *disruption.Command) {
				Expect(queue.StartCommand(ctx, cmd)).To(BeNil())
				if len(cmd.Replacements) > 0 {
					replacementNodeClaim := &v1.NodeClaim{}
					Expect(env.Client.Get(ctx, types.NamespacedName{Name: cmd.Replacements[0].Name}, replacementNodeClaim)).To(Succeed())
					replacementNodeClaim, replacementNode := ExpectNodeClaimDeployedOrFailed(ctx, env.Client, cloudProvider, replacementNodeClaim)
					ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{replacementNode}, []*v1.NodeClaim{replacementNodeClaim})
				}
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
			}

			DescribeTable("observing a completed command's outcome",
				func(newMethod func() disruption.Method, replacements func(*v1.NodePool) []*disruption.Replacement, preArm bool, complete func(*disruption.Command), wantBackedOff bool) {
					ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
					ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
					nodeEntry := nodeClaimEntryOf(nodeClaim1)

					if preArm {
						// Pre-arm back-off so we can observe whether the command's outcome resets it.
						Expect(driftBackoff.Fail(nodePool, env.Clock.Now())).To(BeTrue())
						Expect(backedOff()).To(BeTrue())
					} else {
						Expect(backedOff()).To(BeFalse())
					}

					var repl []*disruption.Replacement
					if replacements != nil {
						repl = replacements(nodePool)
					}
					cmd := &disruption.Command{
						Method:            newMethod(),
						CreationTimestamp: env.Clock.Now(),
						ID:                uuid.New(),
						Results:           scheduling.Results{},
						Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
						Replacements:      repl,
					}
					complete(cmd)

					Expect(backedOff()).To(Equal(wantBackedOff))
				},
				Entry("arms back-off when a drift replacement fails unrecoverably (timeout)",
					driftMethod, withReplacement, false, completeViaTimeout, true),
				Entry("arms back-off when a drift replacement fails to launch",
					driftMethod, withReplacement, false, completeViaLaunchFailure, true),
				Entry("leaves back-off untouched when a drift command succeeds without a replacement",
					driftMethod, nil, true, completeViaSuccess, true),
				Entry("resets back-off when a drift replacement succeeds",
					driftMethod, withReplacement, true, completeViaSuccess, false),
				Entry("leaves back-off untouched for a successful non-drift (consolidation) command",
					emptinessMethod, nil, true, completeViaSuccess, true),
			)
			// startRefused starts a drift command whose replacement the provisioner p refuses against limits.
			startRefused := func(p *provisioning.Provisioner) {
				ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
				ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
				q := disruption.NewQueue(env.Overlay, recorder, cluster.NodeClaims, env.Clock, p, cs.Deletion, driftBackoff)
				err := q.StartCommand(ctx, &disruption.Command{
					Method:            driftMethod(),
					CreationTimestamp: env.Clock.Now(),
					ID:                uuid.New(),
					Results:           scheduling.Results{},
					Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeClaimEntryOf(nodeClaim1), nodePool)},
					Replacements:      withReplacement(nodePool),
				})
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("reserving replacements against limits"))
			}
			It("arms back-off when a drift replacement doesn't fit its NodePool's limits", func() {
				nodePool.Spec.Limits = v1.Limits{corev1.ResourceCPU: resource.MustParse("1m")}
				startRefused(prov)
				Expect(driftBackedOff(nodePool)).To(BeTrue())
			})
			It("doesn't back off when the CapacityQuotas haven't synced", func() {
				ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodePoolDriftBackoff: new(true), CapacityQuota: new(true)}}))
				// A CapacityQuota store whose source hasn't synced, as before the informer's first List.
				unsynced := karpentercluster.NewCapacityQuotaState(ctx, env.Clock, statetest.NewUnsyncedObjects(), cs.NodeClaims, cs.Nodes)
				p := provisioning.NewProvisioner(env.Overlay, recorder, cloudProvider, provisioning.StoresOf(cluster), cs.NodePoolState, unsynced, env.Clock, draController, virtualpods.NewVirtualPodCache(env.Overlay), store, cs.Nomination, cs.BufferPods, cs.PodScheduling)
				startRefused(p)
				Expect(driftBackedOff(nodePool)).To(BeFalse())
			})
			It("does not back off the NodePool when NodePoolDriftBackoff is disabled", func() {
				ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
				ExpectMakeNodesAndNodeClaimsInitialized(ctx, env.Client, env.Clock, []*corev1.Node{node1}, []*v1.NodeClaim{nodeClaim1})
				nodeEntry := nodeClaimEntryOf(nodeClaim1)

				cmd := &disruption.Command{
					Method:            driftMethod(),
					CreationTimestamp: env.Clock.Now(),
					ID:                uuid.New(),
					Results:           scheduling.Results{},
					Candidates:        []*disruption.Candidate{disruption.NewTestCandidate(nodeEntry, nodePool)},
					Replacements:      withReplacement(nodePool),
				}
				ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodePoolDriftBackoff: new(false)}}))
				completeViaTimeout(cmd)

				Expect(backedOff()).To(BeFalse())
			})
		})
		Context("CalculateRetryDuration", func() {
			DescribeTable("should calculate correct timeout based on queue length",
				func(numCommands int, expectedDuration time.Duration) {
					q := disruption.NewQueue(env.Overlay, recorder, cluster.NodeClaims, env.Clock, prov, cs.Deletion, driftBackoff)
					q.Lock()
					for i := range numCommands {
						q.NodeClaimToCommand[strconv.Itoa(i)] = &disruption.Command{}
					}
					q.Unlock()
					actualDuration := q.GetMaxRetryDuration()
					Expect(actualDuration).To(Equal(expectedDuration))
				},
				Entry("very small queue - 100 commands", 100, 10*time.Minute),                  // max(100*80ms, 10min) = 10min
				Entry("small queue - 4000 commands", 4000, 10*time.Minute),                     // max(4000*80ms, 10min) = 10min
				Entry("medium queue - 10000 commands", 10000, 13*time.Minute+20*time.Second),   // 10000*80ms = 13min 20sec
				Entry("large queue - 40000 commands", 40000, 53*time.Minute+20*time.Second),    // 40000*80ms = 53min 20sec
				Entry("very large queue - 80000 commands (capped)", 80000, 1*time.Hour),        // min(80000*80ms, 1hr) = 1hr
				Entry("extremely large queue - 100000 commands (capped)", 100000, 1*time.Hour), // min(100000*80ms, 1hr) = 1hr
			)
		})
	})
})

// nodeClaimCreateErrorClient fails every NodeClaim create, as a launch that can't create its NodeClaim does.
type nodeClaimCreateErrorClient struct {
	client.Client
}

func (c *nodeClaimCreateErrorClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*v1.NodeClaim); ok {
		return errors.New("injected NodeClaim create failure")
	}
	return c.Client.Create(ctx, obj, opts...)
}
