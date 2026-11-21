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
	"fmt"
	"math/rand/v2"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
)

// recountNodePoolStats recounts what NodePoolStats derives, from every node and every Node: a launched NodeClaim
// counts once it's initialized or rebooting, until its instance is terminating; it's disrupting while it's marked for
// deletion, deleting or rebooting; and otherwise it consumes every budget but repair's while the tracked Node whose
// owner reference names it is missing or isn't Ready.
//
//nolint:gocyclo
func recountNodePoolStats(nodeClaims *karpentercluster.NodeClaims, nodeObjects map[string]*corev1.Node, reason v1.DisruptionReason) (numNodes, disrupting map[string]int) {
	numNodes = map[string]int{}
	disrupting = map[string]int{}
	// The NodeClaims are copies, since StatusConditions sets the missing conditions.
	for _, e := range nodeClaims.List() {
		if !karpentercluster.Launched(e) {
			continue
		}
		node := karpentercluster.ViewOfNodeClaim(e)
		nodeClaim := e.Object
		if !nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue() && !node.RebootInProgress() {
			continue
		}
		if nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			continue
		}
		nodePool := nodeClaim.Labels[v1.NodePoolLabelKey]
		numNodes[nodePool]++
		if karpentercluster.Deleting(e) || node.RebootInProgress() {
			disrupting[nodePool]++
			continue
		}
		owned, ok := lo.Find(lo.Values(nodeObjects), func(n *corev1.Node) bool {
			return karpentercluster.Tracked(n) && lo.ContainsBy(n.OwnerReferences, func(o metav1.OwnerReference) bool {
				return o.Kind == "NodeClaim" && o.Name == nodeClaim.Name
			})
		})
		if reason != v1.DisruptionReasonUnhealthy && (!ok || nodeutils.GetCondition(owned, corev1.NodeReady).Status != corev1.ConditionTrue) {
			disrupting[nodePool]++
		}
	}
	return numNodes, disrupting
}

var _ = Describe("NodePoolStats", func() {
	// Applies random Node and NodeClaim updates, deletes, NodePool moves, reboots, readiness changes and deletion marks
	// through the API server, and checks the derived disruption counts against the loop over every node after each
	// change.
	//
	//nolint:gocyclo
	It("should match a recount over every node after each change", func() {
		r := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // deterministic seed for reproducible test inputs
		nodePools := []string{"a", "b", ""}
		// remove deletes the object, finishing a deletion its finalizer holds.
		remove := func(obj client.Object) {
			if err := env.Client.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				Expect(client.IgnoreNotFound(err)).To(Succeed())
				return
			}
			ExpectFinalizersRemoved(ctx, env.Client, obj)
			ExpectDeleted(ctx, env.Client, obj)
		}
		// deleting returns whether the object exists and is being deleted, which an update can't undo.
		deleting := func(obj client.Object) bool {
			current := obj.DeepCopyObject().(client.Object)
			return env.Client.Get(ctx, client.ObjectKeyFromObject(obj), current) == nil && !current.GetDeletionTimestamp().IsZero()
		}
		nodeObjects := func() map[string]*corev1.Node {
			nodes := map[string]*corev1.Node{}
			for _, n := range ExpectNodes(ctx, env.Client) {
				nodes[n.Name] = n
			}
			return nodes
		}
		for i := range 2000 {
			id := r.IntN(30)
			name, providerID := fmt.Sprintf("node-%d", id), fmt.Sprintf("fake:///node-%d", id)
			nodePool := nodePools[r.IntN(len(nodePools))]
			switch op := r.IntN(12); {
			case op == 0:
				remove(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}})
			case op == 1:
				remove(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
			case op == 2:
				cs.Deletion.Mark(name)
			case op == 3:
				cs.Deletion.Unmark(name)
			case op < 7:
				nc := test.NodeClaim(v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1.NodePoolLabelKey: nodePool}},
					Status:     v1.NodeClaimStatus{ProviderID: providerID},
				})
				if deleting(nc) {
					remove(nc)
					break
				}
				deleted := r.IntN(5) == 0
				if r.IntN(4) == 0 {
					if r.IntN(2) == 0 {
						nc.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonRequested, "reboot requested")
					} else {
						nc.StatusConditions().SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonSucceeded, "done")
					}
				}
				if r.IntN(6) == 0 {
					nc.StatusConditions().SetTrue(v1.ConditionTypeInstanceTerminating)
				}
				if r.IntN(3) != 0 {
					nc.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
				}
				ExpectApplied(ctx, env.Client, nc)
				if deleted {
					ExpectDeletionTimestampSet(ctx, env.Client, nc)
				}
			default:
				labels := map[string]string{corev1.LabelInstanceTypeStable: "default-instance-type"}
				// Some Nodes are unmanaged: they carry no NodePool label and no owner reference.
				if r.IntN(5) != 0 {
					labels[v1.NodePoolLabelKey] = nodePool
				}
				if r.IntN(4) != 0 {
					labels[v1.NodeRegisteredLabelKey] = "true"
				}
				if r.IntN(3) != 0 {
					labels[v1.NodeInitializedLabelKey] = "true"
				}
				node := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
					Spec:       corev1.NodeSpec{ProviderID: providerID},
				}
				if deleting(node) {
					remove(node)
					break
				}
				// Registration links some Nodes to their NodeClaim by owner reference.
				if r.IntN(2) == 0 {
					uid := types.UID("never-created")
					if nc := (&v1.NodeClaim{}); env.Client.Get(ctx, types.NamespacedName{Name: name}, nc) == nil {
						uid = nc.UID
					}
					node.OwnerReferences = []metav1.OwnerReference{{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: name, UID: uid}}
				}
				switch r.IntN(3) {
				case 0:
					node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
				case 1:
					node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
				}
				ExpectApplied(ctx, env.Client, node)
				if r.IntN(8) == 0 {
					ExpectDeletionTimestampSet(ctx, env.Client, node)
				}
			}
			nodes := nodeObjects()
			for _, reason := range []v1.DisruptionReason{v1.DisruptionReasonUnderutilized, v1.DisruptionReasonDrifted, v1.DisruptionReasonEmpty, v1.DisruptionReasonUnhealthy} {
				wantNodes, wantDisrupting := recountNodePoolStats(cs.NodeClaims, nodes, reason)
				// NodePool names are never empty, so nodes without the label count toward none.
				for _, np := range []string{"a", "b"} {
					numNodes, disrupting := disruption.NodePoolStats(cs.NodePoolState, np, reason)
					Expect([]int{numNodes, disrupting}).To(Equal([]int{wantNodes[np], wantDisrupting[np]}),
						"after change %d, NodePool %s for %s counts %d nodes and %d disrupting, want %d and %d", i, np, reason, numNodes, disrupting, wantNodes[np], wantDisrupting[np])
				}
			}
		}
	})
})

var _ = Describe("ValidateNodeDisruptable", func() {
	nodeEntry := func(rebootStatus metav1.ConditionStatus) karpentercluster.NodeClaimEntry {
		nc := test.NodeClaim(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: "default"}}})
		n := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey:        "default",
			v1.NodeRegisteredLabelKey:  "true",
			v1.NodeInitializedLabelKey: "true",
		}}})
		if rebootStatus == metav1.ConditionTrue {
			nc.StatusConditions().SetTrueWithReason(v1.ConditionTypeRebooting, v1.RebootReasonRequested, "reboot requested")
		} else {
			nc.StatusConditions().SetFalse(v1.ConditionTypeRebooting, v1.RebootReasonSucceeded, "done")
		}
		return karpentercluster.NodeClaimEntry{Object: nc, Derived: karpentercluster.NodeClaimDerived{Node: n}}
	}
	It("should reject a rebooting node", func() {
		err := disruption.ValidateNodeDisruptable(nodeEntry(metav1.ConditionTrue), clock.RealClock{})
		Expect(err).To(MatchError(ContainSubstring("node is rebooting")), "ValidateNodeDisruptable on a rebooting node should error that the node is rebooting")
	})
	It("should accept a node done rebooting", func() {
		Expect(disruption.ValidateNodeDisruptable(nodeEntry(metav1.ConditionFalse), clock.RealClock{})).To(Succeed(), "ValidateNodeDisruptable on a node done rebooting")
	})
})
