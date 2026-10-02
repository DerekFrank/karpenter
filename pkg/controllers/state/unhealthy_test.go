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

package state_test

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Unhealthy Nodes", func() {
	badNode := func(status corev1.ConditionStatus) corev1.NodeCondition {
		return corev1.NodeCondition{Type: "BadNode", Status: status, LastTransitionTime: metav1.NewTime(env.Clock.Now())}
	}
	managedNode := func(nodePoolName string, conditions ...corev1.NodeCondition) (*v1.NodeClaim, *corev1.Node) {
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey:            nodePoolName,
			corev1.LabelInstanceTypeStable: cloudProvider.InstanceTypes[0].Name,
		}}})
		node.Status.Conditions = append(node.Status.Conditions, conditions...)
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		return nodeClaim, node
	}
	setConditions := func(node *corev1.Node, conditions ...corev1.NodeCondition) {
		node = ExpectExists(ctx, env.Client, node)
		node.Status.Conditions = conditions
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
	}
	unhealthyNames := func() []string {
		return lo.Map(cluster.GetUnhealthyNodes(), func(n *state.StateNode, _ int) string { return n.Node.Name })
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(true)}}))
		// BadNode=False is repaired after 30m; the index must still include a node that has not yet waited out the toleration.
		cloudProvider.RepairPolicy = []cloudprovider.RepairPolicy{
			{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Action: cloudprovider.ReplaceNode},
			{ConditionType: corev1.NodeReady, ConditionStatus: corev1.ConditionUnknown, ReasonRegex: ".*", TolerationDuration: 10 * time.Minute, Action: cloudprovider.ReplaceNode},
		}
	})

	It("should index a node whose condition matches a repair policy regardless of toleration", func() {
		_, unhealthy := managedNode(nodePool.Name, badNode(corev1.ConditionFalse))
		managedNode(nodePool.Name)

		Expect(unhealthyNames()).To(ConsistOf(unhealthy.Name))
	})
	It("should index a node matching any policy's condition type and status", func() {
		_, unknown := managedNode(nodePool.Name, corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionUnknown})
		managedNode(nodePool.Name, corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionFalse})

		Expect(unhealthyNames()).To(ConsistOf(unknown.Name))
	})
	It("should index unhealthy nodes in a newly constructed cluster", func() {
		_, node := managedNode(nodePool.Name, badNode(corev1.ConditionFalse))
		fresh := state.NewCluster(env.Clock, env.Client, cloudProvider)

		Expect(fresh.UpdateNode(ctx, ExpectExists(ctx, env.Client, node))).To(Succeed())
		Expect(lo.Map(fresh.GetUnhealthyNodes(), func(n *state.StateNode, _ int) string { return n.Node.Name })).To(ConsistOf(node.Name))
	})
	It("should not index nodes when node repair is disabled", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: lo.ToPtr(false)}}))
		managedNode(nodePool.Name, badNode(corev1.ConditionFalse))

		Expect(cluster.GetUnhealthyNodes()).To(BeEmpty())
	})
	It("should not index a node whose condition matches a repair policy type but not its status", func() {
		managedNode(nodePool.Name, badNode(corev1.ConditionTrue))

		Expect(cluster.GetUnhealthyNodes()).To(BeEmpty())
	})
	It("should remove a node from the index when its condition recovers", func() {
		_, node := managedNode(nodePool.Name, badNode(corev1.ConditionFalse))
		Expect(unhealthyNames()).To(ConsistOf(node.Name))

		setConditions(node, badNode(corev1.ConditionTrue))
		Expect(cluster.GetUnhealthyNodes()).To(BeEmpty())

		setConditions(node, badNode(corev1.ConditionFalse))
		Expect(unhealthyNames()).To(ConsistOf(node.Name))
	})
	It("should keep a node indexed while it is updated and still unhealthy", func() {
		_, node := managedNode(nodePool.Name, badNode(corev1.ConditionFalse))
		managedNode(nodePool.Name)

		setConditions(node, badNode(corev1.ConditionFalse), corev1.NodeCondition{Type: "Other", Status: corev1.ConditionTrue})
		Expect(unhealthyNames()).To(ConsistOf(node.Name))
		setConditions(node, badNode(corev1.ConditionFalse))
		Expect(unhealthyNames()).To(ConsistOf(node.Name))
	})
	It("should remove a node from the index when the Node is deleted but its NodeClaim remains", func() {
		_, node := managedNode(nodePool.Name, badNode(corev1.ConditionFalse))
		_, other := managedNode(nodePool.Name, badNode(corev1.ConditionFalse))
		managedNode(nodePool.Name)

		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 3)
		Expect(unhealthyNames()).To(ConsistOf(other.Name))
		Expect(cluster.UnhealthyNodeCount()).To(Equal(1))
	})
	It("should remove a node from the index when both the Node and NodeClaim are deleted", func() {
		nodeClaim, node := managedNode(nodePool.Name, badNode(corev1.ConditionFalse))

		ExpectDeleted(ctx, env.Client, nodeClaim)
		ExpectReconcileSucceeded(ctx, nodeClaimController, client.ObjectKeyFromObject(nodeClaim))
		Expect(unhealthyNames()).To(ConsistOf(node.Name))
		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 0)
		Expect(cluster.UnhealthyNodeCount()).To(Equal(0))
	})
	It("should index an unhealthy unmanaged node", func() {
		node := test.Node(test.NodeOptions{ProviderID: test.RandomProviderID()})
		node.Status.Conditions = append(node.Status.Conditions, badNode(corev1.ConditionFalse))
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		Expect(unhealthyNames()).To(ConsistOf(node.Name))
	})
	It("should re-key an unhealthy node when its providerID is populated", func() {
		node := test.Node()
		node.Spec.ProviderID = ""
		node.Status.Conditions = append(node.Status.Conditions, badNode(corev1.ConditionFalse))
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		Expect(unhealthyNames()).To(ConsistOf(node.Name))

		node = ExpectExists(ctx, env.Client, node)
		node.Spec.ProviderID = test.RandomProviderID()
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		Expect(cluster.UnhealthyNodeCount()).To(Equal(1))
		unhealthy := cluster.GetUnhealthyNodes()
		Expect(unhealthy).To(HaveLen(1))
		Expect(unhealthy[0].ProviderID()).To(Equal(node.Spec.ProviderID))
	})
	It("should return copies that do not alias cluster state", func() {
		_, node := managedNode(nodePool.Name, badNode(corev1.ConditionFalse))

		cluster.GetUnhealthyNodes()[0].Node.Status.Conditions = nil
		unhealthy := cluster.GetUnhealthyNodes()
		Expect(unhealthy).To(HaveLen(1))
		Expect(unhealthy[0].Node.Status.Conditions).To(ContainElement(HaveField("Type", corev1.NodeConditionType("BadNode"))))
		Expect(unhealthy[0].Node.Name).To(Equal(node.Name))
	})
	It("should be safe to read while unhealthy nodes are updated", func() {
		var wg sync.WaitGroup
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = cluster.GetUnhealthyNodes()
				}
			}
		}()
		// Enough updates to trigger a DATA RACE under -race
		for range 100 {
			managedNode(nodePool.Name, badNode(corev1.ConditionFalse))
		}
		close(done)
		wg.Wait()
		Expect(cluster.GetUnhealthyNodes()).To(HaveLen(100))
	})
	It("should clear the index on Reset", func() {
		managedNode(nodePool.Name, badNode(corev1.ConditionFalse))

		cluster.Reset()
		Expect(cluster.UnhealthyNodeCount()).To(Equal(0))
	})
})
