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
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	staticdeprovisioning "sigs.k8s.io/karpenter/pkg/controllers/static/deprovisioning"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

var _ = Describe("StaticDrift", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node

	BeforeEach(func() {
		nodePool = test.StaticNodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Replicas: new(int64(1)), // Static NodePool with 3 replicas
				Disruption: v1.Disruption{
					// Disrupt away!
					Budgets: []v1.Budget{{
						Nodes: "100%",
					}},
				},
			},
		})
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("12"),
				},
			},
		})
	})

	Context("Budgets", func() {
		var numNodes = 5
		var nodeClaims []*v1.NodeClaim
		var nodes []*corev1.Node

		BeforeEach(func() {
			nodePool.Spec.Replicas = new(int64(numNodes))
		})

		It("should respect disruption budgets (Nodes Count) for static drift", func() {
			nodeClaims, nodes = test.NodeClaimsAndNodes(numNodes, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			nodePool.Spec.Disruption.Budgets = []v1.Budget{
				{Reasons: []v1.DisruptionReason{v1.DisruptionReasonDrifted}, Nodes: "2"},
			}

			ExpectApplied(ctx, env.Client, nodePool)
			for i := range numNodes {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			// Should only allow 2 nodes to be disrupted because of budget
			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 2, map[string]string{
				metrics.NodePoolLabel: nodePool.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, numNodes, 2, 0)

			// Execute commands, should only delete 2 nodes
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(2))
			for _, cmd := range cmds {
				ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmd)
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
				// Cascade any deletion of the nodeClaims to the nodes
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmd.Candidates[0].NodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim))
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, numNodes, 0, 0)

			Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(numNodes))
			ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 2, map[string]string{
				metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
		})
		It("should respect disruption budgets (Nodes Percentage) for static drift", func() {
			nodeClaims, nodes = test.NodeClaimsAndNodes(numNodes, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			nodePool.Spec.Disruption.Budgets = []v1.Budget{
				{Reasons: []v1.DisruptionReason{v1.DisruptionReasonDrifted}, Nodes: "20%"},
			}

			ExpectApplied(ctx, env.Client, nodePool)
			for i := range numNodes {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			// Should only allow 1 nodes to be disrupted because of budget
			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 1, map[string]string{
				metrics.NodePoolLabel: nodePool.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, numNodes, 1, 0)

			// Execute commands, should only delete 1 nodes
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			for _, cmd := range cmds {
				ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmd)
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)

				// Cascade any deletion of the nodeClaims to the nodes
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmd.Candidates[0].NodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim))
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, numNodes, 0, 0)

			Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(numNodes))
			ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 1, map[string]string{
				metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
		})
		It("should respect budgets for multiple static nodepools", func() {
			// Create 3 static NodePools with different configurations
			nodePool1 := test.StaticNodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{Name: "static-nodepool-1"},
				Spec: v1.NodePoolSpec{
					Replicas: new(int64(4)),
					Disruption: v1.Disruption{
						Budgets: []v1.Budget{{
							Reasons: []v1.DisruptionReason{v1.DisruptionReasonDrifted},
							Nodes:   "1", // Only 1 allowed
						}},
					},
				},
			})
			nodePool2 := test.StaticNodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{Name: "static-nodepool-2"},
				Spec: v1.NodePoolSpec{
					Replicas: new(int64(3)),
					Disruption: v1.Disruption{
						Budgets: []v1.Budget{{
							Reasons: []v1.DisruptionReason{v1.DisruptionReasonDrifted},
							Nodes:   "2", // 2 allowed
						}},
					},
				},
			})
			nodePool3 := test.StaticNodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{Name: "static-nodepool-3"},
				Spec: v1.NodePoolSpec{
					Replicas: new(int64(2)),
					Disruption: v1.Disruption{
						Budgets: []v1.Budget{{
							Reasons: []v1.DisruptionReason{v1.DisruptionReasonDrifted},
							Nodes:   "100%", // Unlimited budget
						}},
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool1, nodePool2, nodePool3)

			// Create 2 nodes for each NodePool
			nodeClaims1, nodes1 := test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool1.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			nodeClaims2, nodes2 := test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool2.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			nodeClaims3, nodes3 := test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool3.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			// Mark all nodes as drifted
			allNodeClaims := append(append(nodeClaims1, nodeClaims2...), nodeClaims3...)
			for _, nc := range allNodeClaims {
				nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nc)
			}
			allNodes := append(append(nodes1, nodes2...), nodes3...)
			for _, n := range allNodes {
				ExpectApplied(ctx, env.Client, n)
			}

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, allNodes, allNodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 1, map[string]string{
				metrics.NodePoolLabel: nodePool1.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 2, map[string]string{
				metrics.NodePoolLabel: nodePool2.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 2, map[string]string{
				metrics.NodePoolLabel: nodePool3.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool1.Name, 2, 1, 0)
			ExpectStateNodePoolCount(cluster, nodePool2.Name, 2, 2, 0)
			ExpectStateNodePoolCount(cluster, nodePool3.Name, 2, 2, 0)

			// Execute commands, should drift 5 nodes total
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(5))

			// Verify commands are from the correct NodePools
			nodePoolNames := make(map[string]int)
			for _, cmd := range cmds {
				nodePoolNames[cmd.Candidates[0].NodePool.Name]++
			}
			Expect(nodePoolNames[nodePool1.Name]).To(Equal(1))
			Expect(nodePoolNames[nodePool2.Name]).To(Equal(2))
			Expect(nodePoolNames[nodePool3.Name]).To(Equal(2))

			// Execute the commands
			for _, cmd := range cmds {
				ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmd)
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
				// Cascade any deletion of the nodeClaims to the nodes
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmd.Candidates[0].NodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim))
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool1.Name, 2, 0, 0)
			ExpectStateNodePoolCount(cluster, nodePool2.Name, 2, 0, 0)
			ExpectStateNodePoolCount(cluster, nodePool3.Name, 2, 0, 0)

			Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(len(allNodes)))

			ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 5, map[string]string{
				metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
		})
	})
	Context("Limits and Scaling", func() {
		var numNodes = 3
		var nodeClaims []*v1.NodeClaim
		var nodes []*corev1.Node

		It("should not drift nodes when we cannot acquire limits", func() {
			nodePool.Spec.Replicas = new(int64(5))
			nodePool.Spec.Limits = v1.Limits{
				resources.Node: resource.MustParse(strconv.Itoa(5)),
			}
			nodeClaims, nodes = test.NodeClaimsAndNodes(5, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool)
			for i := range 5 {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 5, 0, 0)

			// Since we cannot acquire limits, we should not have any drifts
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(0))
			// The budget-allowed candidate is reported Blocked rather than silently skipped
			Expect(recorder.Calls(events.DisruptionBlocked)).To(BeNumerically(">", 0))
		})
		It("should drift nodes when we can acquire limits", func() {
			nodePool.Spec.Replicas = new(int64(5))
			nodePool.Spec.Limits = v1.Limits{
				resources.Node: resource.MustParse(strconv.Itoa(5)),
			}
			nodeClaims, nodes = test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool)
			for i := range 2 {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 2, map[string]string{
				metrics.NodePoolLabel: nodePool.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 2, 0)

			// Should drift nodes since we can scale up to target
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(2))
			Expect(recorder.Calls(events.DisruptionBlocked)).To(Equal(0))
			for _, cmd := range cmds {
				ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmd)
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
				// Cascade any deletion of the nodeClaims to the nodes
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmd.Candidates[0].NodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim))
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

			Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(2))
			ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 2, map[string]string{
				metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
		})
		It("should drift partially when we can acquire some limits", func() {
			nodePool.Spec.Replicas = new(int64(5))
			nodePool.Spec.Limits = v1.Limits{
				resources.Node: resource.MustParse(strconv.Itoa(7)),
			}
			nodeClaims, nodes = test.NodeClaimsAndNodes(5, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool)
			for i := range 5 {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 5, map[string]string{
				metrics.NodePoolLabel: nodePool.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 5, 2, 0)

			// Should drift 2 nodes since we can only acquire 2 limit
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(2))
			for _, cmd := range cmds {
				ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmd)
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
				// Cascade any deletion of the nodeClaims to the nodes
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmd.Candidates[0].NodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim))
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 5, 0, 0)

			Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(5))
			ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 2, map[string]string{
				metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
		})
		It("should keep drifting partially when we can acquire some limits for each reconcile", func() {
			nodePool.Spec.Replicas = new(int64(5))
			nodePool.Spec.Limits = v1.Limits{
				resources.Node: resource.MustParse(strconv.Itoa(6)),
			}
			nodeClaims, nodes = test.NodeClaimsAndNodes(5, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool)
			for i := range 5 {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

			for i := range 5 {
				ExpectSingletonReconciled(ctx, disruptionController)

				// Verify StateNodePool Has been updated
				ExpectStateNodePoolCount(cluster, nodePool.Name, 5, 1, 0)

				cmds := queue.GetCommands()
				Expect(cmds).To(HaveLen(1))
				for _, cmd := range cmds {
					ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmd)
					// Any point in time we should not go over limit
					Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(6))
					ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
					// Cascade any deletion of the nodeClaims to the nodes
					ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmd.Candidates[0].NodeClaim)
					ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim))
				}

				// Verify StateNodePool Has been updated
				ExpectStateNodePoolCount(cluster, nodePool.Name, 5, 0, 0)

				Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(5))
				ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, float64(i+1), map[string]string{
					metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
				})
			}
		})
		It("should wait until nodes are deprovisioned when we are overscaled", func() {
			nodePool.Spec.Replicas = new(int64(1)) // Target 1, but have 2
			numNodes = 2
			nodeClaims, nodes = test.NodeClaimsAndNodes(numNodes, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						v1.NodeInitializedLabelKey:     "true",
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool)
			for i := 0; i < numNodes; i++ {
				nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
			}

			// Create pod on drifted node to prevent deprovisioning controller from decommisioning the drifted NC
			pods := test.Pods(1, test.PodOptions{
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU: resource.MustParse("1"),
					},
				},
				ObjectMeta: metav1.ObjectMeta{},
			})

			ExpectApplied(ctx, env.Client, pods[0])
			ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])

			// 1st node is drifted
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 2, 0, 0)

			// Should not disrupt drifted nodes
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(0))

			// Deprovision nodes
			ExpectDeleted(ctx, env.Client, nodeClaims[1], nodes[1])

			// reconcile
			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaims[1])
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(nodes[1]))
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaims[1]))

			remainingNodeClaims := &v1.NodeClaimList{}
			remainingNodes := &corev1.NodeList{}
			Expect(env.Client.List(ctx, remainingNodeClaims)).To(Succeed())
			Expect(env.Client.List(ctx, remainingNodes)).To(Succeed())
			Expect(remainingNodeClaims.Items).To(HaveLen(1))
			Expect(remainingNodes.Items).To(HaveLen(1))

			ExpectSingletonReconciled(ctx, disruptionController)

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 1, 0)

			// Should drift nodes since we already scaled down nodes to replicas
			cmds = queue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			for _, cmd := range cmds {
				ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmd)
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
				// Cascade any deletion of the nodeClaims to the nodes
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmd.Candidates[0].NodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim))
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

			Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(1))
			ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 1, map[string]string{
				metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
		})
	})
	Context("Multiple NodePools", func() {
		It("should handle drift for multiple static NodePools independently", func() {
			// Create three static NodePools
			nodePool1 := test.StaticNodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{Name: "nodepool-1"},
				Spec: v1.NodePoolSpec{
					Replicas: new(int64(3)),
					Disruption: v1.Disruption{
						Budgets: []v1.Budget{{Nodes: "100%"}},
					},
				},
			})
			nodePool2 := test.StaticNodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{Name: "nodepool-2"},
				Spec: v1.NodePoolSpec{
					Replicas: new(int64(2)),
					Disruption: v1.Disruption{
						Budgets: []v1.Budget{{Nodes: "100%"}},
					},
					Limits: v1.Limits{
						resources.Node: resource.MustParse(strconv.Itoa(2)), // no allowed drifts
					},
				},
			})
			nodePool3 := test.StaticNodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{Name: "nodepool-3"},
				Spec: v1.NodePoolSpec{
					Replicas: new(int64(2)),
					Disruption: v1.Disruption{
						Budgets: []v1.Budget{{Nodes: "100%"}},
					},
				},
			})

			ExpectApplied(ctx, env.Client, nodePool1, nodePool2, nodePool3)

			// Create nodes for each NodePool
			nodeClaims1, nodes1 := test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool1.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			nodeClaims2, nodes2 := test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool2.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			nodeClaims3, nodes3 := test.NodeClaimsAndNodes(2, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool3.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})

			// Mark all as drifted
			for i := range nodeClaims1 {
				nodeClaims1[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims1[i], nodes1[i])
			}
			for i := range nodeClaims2 {
				nodeClaims2[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims2[i], nodes2[i])
			}
			for i := range nodeClaims3 {
				nodeClaims3[i].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims3[i], nodes3[i])
			}

			allNodes := append(nodes1, nodes2...)
			allNodes = append(allNodes, nodes3...)
			allNodeClaims := append(nodeClaims1, nodeClaims2...)
			allNodeClaims = append(allNodeClaims, nodeClaims3...)

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, allNodes, allNodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 2, map[string]string{
				metrics.NodePoolLabel: nodePool1.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 2, map[string]string{
				metrics.NodePoolLabel: nodePool2.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
			ExpectMetricGaugeValue(disruption.NodePoolAllowedDisruptions, 2, map[string]string{
				metrics.NodePoolLabel: nodePool3.Name,
				metrics.ReasonLabel:   strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})

			// Execute commands, should drift 4 nodes total
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(4))

			// Verify commands are from the correct NodePools
			nodePoolNames := make(map[string]int)
			for _, cmd := range cmds {
				nodePoolNames[cmd.Candidates[0].NodePool.Name]++
			}
			Expect(nodePoolNames[nodePool1.Name]).To(Equal(2))
			Expect(nodePoolNames[nodePool3.Name]).To(Equal(2))

			// Execute the commands
			for _, cmd := range cmds {
				ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmd)
				ExpectObjectReconciled(ctx, env.Client, queue, cmd.Candidates[0].NodeClaim)
				// Cascade any deletion of the nodeClaims to the nodes
				ExpectNodeClaimsCascadeDeletion(ctx, env.Client, cmd.Candidates[0].NodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(cmd.Candidates[0].NodeClaim))
			}

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool1.Name, 2, 0, 0)
			ExpectStateNodePoolCount(cluster, nodePool2.Name, 2, 0, 0)
			ExpectStateNodePoolCount(cluster, nodePool3.Name, 2, 0, 0)

			Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(len(allNodes)))

			ExpectMetricCounterValue(disruption.DecisionsPerformedTotal, 4, map[string]string{
				metrics.ReasonLabel: strings.ToLower(string(v1.DisruptionReasonDrifted)),
			})
		})
	})
	Context("Edge Cases", func() {
		It("should handle zero replicas", func() {
			nodePool.Spec.Replicas = new(int64(0))
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			ExpectSingletonReconciled(ctx, disruptionController)

			// Should not drift any nodes when target is 0
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(0))
		})
		It("should handle missing node limits gracefully", func() {
			nodePool.Spec.Replicas = new(int64(5))
			nodePool.Spec.Limits = nil // No limits set

			nodeClaims, nodes := test.NodeClaimsAndNodes(5, v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            nodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:  resource.MustParse("32"),
						corev1.ResourcePods: resource.MustParse("100"),
					},
				},
			})
			ExpectApplied(ctx, env.Client, nodePool)

			for i, nc := range nodeClaims {
				nc.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
				ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])

			}

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
			ExpectSingletonReconciled(ctx, disruptionController)

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 5, 5, 0)

			// Should drift the node since no limits are constraining it
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(5))
		})
		It("should ignore nodes without the drifted status condition", func() {
			_ = nodeClaim.StatusConditions().Clear(v1.ConditionTypeDrifted)
			nodePool.Spec.Replicas = new(int64(1))
			ExpectApplied(ctx, env.Client, nodeClaim, node, nodePool)

			// inform cluster state about nodes and nodeclaims
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			ExpectSingletonReconciled(ctx, disruptionController)

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

			// Should not drift any nodes
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(0))
		})
		It("should ignore nodes with the drifted status condition set to false", func() {
			nodePool.Spec.Replicas = new(int64(1))
			nodeClaim.StatusConditions().SetFalse(v1.ConditionTypeDrifted, "NotDrifted", "NotDrifted")
			ExpectApplied(ctx, env.Client, nodeClaim, node, nodePool)

			// inform cluster state about nodes and nodeclaims
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			ExpectSingletonReconciled(ctx, disruptionController)

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

			// Should not drift any nodes
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(0))
		})
		It("should ignore nodes with the karpenter.sh/do-not-disrupt annotation", func() {
			nodePool.Spec.Replicas = new(int64(1))
			node.Annotations = lo.Assign(node.Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
			ExpectApplied(ctx, env.Client, nodeClaim, node, nodePool)

			// inform cluster state about nodes and nodeclaims
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

			ExpectSingletonReconciled(ctx, disruptionController)

			// Verify StateNodePool Has been updated
			ExpectStateNodePoolCount(cluster, nodePool.Name, 1, 0, 0)

			// Should not drift any nodes
			cmds := queue.GetCommands()
			Expect(cmds).To(HaveLen(0))
		})
	})
})

// nodeClaimCreateHookClient runs a hook after each NodeClaim Create, or fails the Create, so a test can run other
// controllers at the point in StartCommand where the replacement exists but the candidate isn't marked for deletion.
type nodeClaimCreateHookClient struct {
	client.Client
	failCreate  bool
	afterCreate func(*v1.NodeClaim)
}

func (c *nodeClaimCreateHookClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	nodeClaim, ok := obj.(*v1.NodeClaim)
	if ok && c.failCreate {
		return fmt.Errorf("simulated error creating nodeclaim")
	}
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if ok && c.afterCreate != nil {
		c.afterCreate(nodeClaim)
	}
	return nil
}

var _ = Describe("StaticDrift NodePoolState accounting", func() {
	const replicas = 10
	var nodePool *v1.NodePool
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var candidate *v1.NodeClaim
	var hookClient *nodeClaimCreateHookClient
	var hookQueue *disruption.Queue
	var hookDisruptionController *disruption.Controller
	var deprovisioner *staticdeprovisioning.Controller

	BeforeEach(func() {
		nodePool = test.StaticNodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Replicas: new(int64(replicas)),
				Limits:   v1.Limits{resources.Node: resource.MustParse("12")},
				Disruption: v1.Disruption{
					Budgets: []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
		nodeClaims, nodes = test.NodeClaimsAndNodes(replicas, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		candidate = nodeClaims[0]
		candidate.StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		ExpectApplied(ctx, env.Client, nodePool)
		for i := range nodeClaims {
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectStateNodePoolCount(cluster, nodePool.Name, replicas, 0, 0)

		// Run StartCommand through a provisioner whose NodeClaim Create can be intercepted
		hookClient = &nodeClaimCreateHookClient{Client: env.Client}
		hookProv := provisioning.NewProvisioner(hookClient, recorder, cloudProvider, cluster, env.Clock, draController, virtualpods.NewVirtualPodCache(env.Client))
		hookQueue = disruption.NewQueue(env.Client, recorder, cluster, env.Clock, hookProv)
		hookDisruptionController = disruption.NewController(ctx, env.Clock, env.Client, hookProv, cloudProvider, recorder, cluster, hookQueue, clusterCost,
			disruption.WithMethods(disruption.NewStaticDrift(cluster, hookProv, cloudProvider, recorder)))
		deprovisioner = staticdeprovisioning.NewController(env.Client, cluster, cloudProvider, env.Clock, recorder)
	})

	// Simulates the informer seeing the candidate and the replacement, then static.deprovisioning reconciling, all
	// before the queue marks the candidate for deletion. observeCandidate is the informer's view of the candidate.
	runDeprovisionerMidCommand := func(observeCandidate func()) {
		hookClient.afterCreate = func(replacement *v1.NodeClaim) {
			defer GinkgoRecover()
			observeCandidate()
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(replacement))
			ExpectStateNodePoolCount(cluster, nodePool.Name, replicas, 0, 1)
			ExpectObjectReconciled(ctx, env.Client, deprovisioner, nodePool)
		}
	}

	expectCommandCompletes := func() {
		cmds := hookQueue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		replacement := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: cmds[0].Replacements[0].Name}}
		// static.deprovisioning must not have deleted the replacement
		ExpectExists(ctx, env.Client, replacement)
		ExpectStateNodePoolCount(cluster, nodePool.Name, replicas, 1, 0)

		ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmds[0])
		ExpectObjectReconciled(ctx, env.Client, hookQueue, cmds[0].Candidates[0].NodeClaim)
		Expect(cmds[0].Succeeded).To(BeTrue())
		ExpectNodeClaimsCascadeDeletion(ctx, env.Client, candidate)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(candidate))
		ExpectStateNodePoolCount(cluster, nodePool.Name, replicas, 0, 0)
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(replicas))
	}

	It("should not let static deprovisioning delete the replacement after the informer sees the DisruptionReason patch", func() {
		runDeprovisionerMidCommand(func() {
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(candidate))
		})
		ExpectSingletonReconciled(ctx, hookDisruptionController)
		expectCommandCompletes()
	})
	It("should not let static deprovisioning delete the replacement after a stale read of the candidate", func() {
		stale := ExpectExists(ctx, env.Client, candidate).DeepCopy()
		Expect(stale.StatusConditions().Get(v1.ConditionTypeDisruptionReason).IsTrue()).To(BeFalse())
		runDeprovisionerMidCommand(func() {
			// The informer's periodic requeue reads the cache before it has caught up to the status patch
			cluster.UpdateNodeClaim(stale)
		})
		ExpectSingletonReconciled(ctx, hookDisruptionController)
		expectCommandCompletes()
	})

	Context("Self-healing", func() {
		blockDisruption := func() {
			nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
			ExpectApplied(ctx, env.Client, nodePool)
		}
		DescribeTable("should return the candidate to active when StartCommand fails to create the replacement",
			func(informerSawCondition bool) {
				hookClient.failCreate = true
				Expect(ExpectSingletonReconcileFailed(ctx, hookDisruptionController)).To(MatchError(ContainSubstring("simulated error creating nodeclaim")))
				Expect(ExpectExists(ctx, env.Client, candidate).StatusConditions().Get(v1.ConditionTypeDisruptionReason).IsTrue()).To(BeTrue())
				if informerSawCondition {
					ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(candidate))
				}
				ExpectStateNodePoolCount(cluster, nodePool.Name, replicas-1, 0, 1)

				// The next disruption loop's sweep clears the condition, since no command owns the candidate
				blockDisruption()
				ExpectSingletonReconciled(ctx, disruptionController)
				Expect(ExpectExists(ctx, env.Client, candidate).StatusConditions().Get(v1.ConditionTypeDisruptionReason).IsTrue()).To(BeFalse())
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(candidate))
				ExpectStateNodePoolCount(cluster, nodePool.Name, replicas, 0, 0)
			},
			Entry("after the informer saw the DisruptionReason condition", true),
			Entry("before the informer saw the DisruptionReason condition", false),
		)
		It("should return the candidate to active when the command fails in the queue", func() {
			ExpectSingletonReconciled(ctx, hookDisruptionController)
			cmds := hookQueue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(candidate))
			ExpectStateNodePoolCount(cluster, nodePool.Name, replicas, 1, 0)

			// Time the command out before the replacement initializes
			env.Clock.Step(11 * time.Minute)
			ExpectObjectReconciled(ctx, env.Client, hookQueue, candidate)
			Expect(cmds[0].Succeeded).To(BeFalse())
			Expect(hookQueue.HasAny(cmds[0].Candidates[0].ProviderID())).To(BeFalse())

			// The queue cleared the condition and unmarked the candidate. Observing the cleared condition makes it
			// active without waiting for another disruption loop.
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(candidate))
			ExpectStateNodePoolCount(cluster, nodePool.Name, replicas+1, 0, 0)
		})
		It("should count a candidate as pending disruption after a restart, then return it to active", func() {
			ExpectSingletonReconciled(ctx, hookDisruptionController)
			cmds := hookQueue.GetCommands()
			Expect(cmds).To(HaveLen(1))
			// Launch the replacement so cluster state can sync after the restart
			ExpectMakeNewNodeClaimsReady(ctx, env.Client, env.Clock, cluster, cloudProvider, cmds[0])

			// Restart: in-memory state and commands are gone, the DisruptionReason condition persists
			cluster.Reset()
			*queue = lo.FromPtr(disruption.NewQueue(env.Client, recorder, cluster, env.Clock, prov))
			for _, n := range ExpectNodes(ctx, env.Client) {
				ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
			}
			for _, nc := range ExpectNodeClaims(ctx, env.Client) {
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
			}
			ExpectStateNodePoolCount(cluster, nodePool.Name, replicas, 0, 1)

			blockDisruption()
			ExpectSingletonReconciled(ctx, disruptionController)
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(candidate))
			ExpectStateNodePoolCount(cluster, nodePool.Name, replicas+1, 0, 0)
		})
	})
})
