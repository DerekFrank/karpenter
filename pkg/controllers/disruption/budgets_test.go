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
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	fakecp "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
)

// recountNodePoolStats recounts what NodePoolStats derives, from every node and every Node: a launched NodeClaim
// counts once it's initialized or rebooting, until its instance is terminating; it's disrupting while it's marked for
// deletion, deleting or rebooting; and otherwise it consumes every budget but repair's while the tracked Node whose
// owner reference names it is missing or isn't Ready.
//
//nolint:gocyclo
func recountNodePoolStats(nodes []*karpentercluster.StateNode, nodeObjects map[string]*corev1.Node, reason v1.DisruptionReason) (numNodes, disrupting map[string]int) {
	numNodes = map[string]int{}
	disrupting = map[string]int{}
	for _, node := range nodes {
		if !node.Managed() || (!node.NodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue() && !node.RebootInProgress()) {
			continue
		}
		if node.NodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			continue
		}
		nodePool := node.NodeClaim.Labels[v1.NodePoolLabelKey]
		numNodes[nodePool]++
		if node.MarkedForDeletion() || node.RebootInProgress() {
			disrupting[nodePool]++
			continue
		}
		owned, ok := lo.Find(lo.Values(nodeObjects), func(n *corev1.Node) bool {
			return karpentercluster.Tracked(n) && lo.ContainsBy(n.OwnerReferences, func(o metav1.OwnerReference) bool {
				return o.Kind == "NodeClaim" && o.Name == node.NodeClaim.Name
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
	// through cluster state, and checks the derived disruption counts against the loop over every node after each
	// change.
	//
	//nolint:gocyclo
	It("should match a recount over every node after each change", func() {
		statsCtx := options.ToContext(context.Background(), test.Options())
		r := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // deterministic seed for reproducible test inputs
		statsClient := fake.NewClientBuilder().WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).Build()
		clk := clocktesting.NewFakeClock(time.Now())
		cs := karpentercluster.New(statsCtx, clk, nil, nil, fakecp.NewCloudProvider())
		c := state.NewCluster(clk, statsClient, cs.Nodes, cs.NodePoolState, cs.CapacityQuotaState, cs.PodScheduling)
		nodePools := []string{"a", "b", ""}
		var rv uint64
		nextRV := func() string { rv++; return strconv.FormatUint(rv, 10) }
		uids := map[string]types.UID{}
		nodeObjects := map[string]*corev1.Node{}
		uid := func(kind, name string) types.UID {
			if _, ok := uids[kind+name]; !ok {
				uids[kind+name] = types.UID(fmt.Sprintf("%s-%s-%d", kind, name, rv))
			}
			return uids[kind+name]
		}
		for i := range 20000 {
			id := r.IntN(30)
			name, providerID := fmt.Sprintf("node-%d", id), fmt.Sprintf("fake:///node-%d", id)
			nodePool := nodePools[r.IntN(len(nodePools))]
			switch op := r.IntN(12); {
			case op == 0:
				c.NodeClaimStore.Forget(name)
				delete(uids, "nc"+name)
			case op == 1:
				c.NodeStore.Forget(name)
				delete(uids, "node"+name)
				delete(nodeObjects, name)
			case op == 2:
				cs.Deletion.Mark(name)
			case op == 3:
				cs.Deletion.Unmark(name)
			case op < 7:
				nc := &v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid("nc", name), ResourceVersion: nextRV(), Labels: map[string]string{v1.NodePoolLabelKey: nodePool}},
					Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
					Status:     v1.NodeClaimStatus{ProviderID: providerID},
				}
				if r.IntN(5) == 0 {
					nc.DeletionTimestamp = &metav1.Time{Time: time.Unix(1, 0)}
					nc.Finalizers = []string{v1.TerminationFinalizer}
				}
				if r.IntN(4) == 0 {
					nc.Status.Conditions = append(nc.Status.Conditions, status.Condition{Type: v1.ConditionTypeRebooting, Status: []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse}[r.IntN(2)]})
				}
				if r.IntN(6) == 0 {
					nc.Status.Conditions = append(nc.Status.Conditions, status.Condition{Type: v1.ConditionTypeInstanceTerminating, Status: metav1.ConditionTrue})
				}
				if r.IntN(3) != 0 {
					nc.Status.Conditions = append(nc.Status.Conditions, status.Condition{Type: v1.ConditionTypeInitialized, Status: metav1.ConditionTrue})
				}
				c.NodeClaimStore.Observe(nc)
			default:
				labels := map[string]string{corev1.LabelInstanceTypeStable: "default-instance-type"}
				// Some Nodes are unmanaged: they carry no NodePool label and no NodeClaim joins them, unless a
				// NodeClaim with the same providerID exists.
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
					ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid("node", name), ResourceVersion: nextRV(), Labels: labels},
					Spec:       corev1.NodeSpec{ProviderID: providerID},
				}
				// Registration links some Nodes to their NodeClaim by owner reference.
				if r.IntN(2) == 0 {
					node.OwnerReferences = []metav1.OwnerReference{{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: name}}
				}
				switch r.IntN(3) {
				case 0:
					node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
				case 1:
					node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
				}
				if r.IntN(8) == 0 {
					node.DeletionTimestamp = &metav1.Time{Time: time.Unix(1, 0)}
					node.Finalizers = []string{v1.TerminationFinalizer}
				}
				c.NodeStore.Observe(node)
				nodeObjects[name] = node
			}
			for _, reason := range []v1.DisruptionReason{v1.DisruptionReasonUnderutilized, v1.DisruptionReasonDrifted, v1.DisruptionReasonEmpty, v1.DisruptionReasonUnhealthy} {
				wantNodes, wantDisrupting := recountNodePoolStats(c.DeepCopyNodes(), nodeObjects, reason)
				// NodePool names are never empty, so nodes without the label count toward none.
				for _, np := range []string{"a", "b"} {
					numNodes, disrupting := disruption.NodePoolStats(c, np, reason)
					Expect([]int{numNodes, disrupting}).To(Equal([]int{wantNodes[np], wantDisrupting[np]}),
						"after change %d, NodePool %s for %s counts %d nodes and %d disrupting, want %d and %d", i, np, reason, numNodes, disrupting, wantNodes[np], wantDisrupting[np])
				}
			}
		}
	})
})

var _ = Describe("ValidateNodeDisruptable", func() {
	stateNode := func(rebootStatus metav1.ConditionStatus) *karpentercluster.StateNode {
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
		return &karpentercluster.StateNode{Node: n, NodeClaim: nc}
	}
	It("should reject a rebooting node", func() {
		err := disruption.ValidateNodeDisruptable(stateNode(metav1.ConditionTrue), clock.RealClock{})
		Expect(err).To(MatchError(ContainSubstring("node is rebooting")), "ValidateNodeDisruptable on a rebooting node should error that the node is rebooting")
	})
	It("should accept a node done rebooting", func() {
		Expect(disruption.ValidateNodeDisruptable(stateNode(metav1.ConditionFalse), clock.RealClock{})).To(Succeed(), "ValidateNodeDisruptable on a node done rebooting")
	})
})
