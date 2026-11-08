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
	"strings"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	fakecp "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
)

// recountNodePoolStats is the loop over every node that NodePoolStats replaced, kept as the reference the derived
// counts must match. It differs only in reading a missing Node as not Ready, where the loop read its Ready condition
// unconditionally; a missing Node only reaches that read when it's rebooting, which counts it as disrupting either way.
func recountNodePoolStats(nodes []*state.StateNode, reason v1.DisruptionReason) (numNodes, disrupting map[string]int) {
	numNodes = map[string]int{}
	disrupting = map[string]int{}
	for _, node := range nodes {
		if !node.Managed() || (!node.Initialized() && !node.RebootInProgress()) {
			continue
		}
		if node.NodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			continue
		}
		nodePool := node.Labels()[v1.NodePoolLabelKey]
		numNodes[nodePool]++
		notReady := node.Node == nil || nodeutils.GetCondition(node.Node, corev1.NodeReady).Status != corev1.ConditionTrue
		if node.MarkedForDeletion() || node.RebootInProgress() || (notReady && reason != v1.DisruptionReasonUnhealthy) {
			disrupting[nodePool]++
		}
	}
	return numNodes, disrupting
}

// TestNodePoolStatsMatchARecount applies random Node and NodeClaim updates, deletes, NodePool moves, reboots,
// readiness changes and deletion marks through cluster state, and checks the derived disruption counts against the
// loop over every node after each change.
//
//nolint:gocyclo
func TestNodePoolStatsMatchARecount(t *testing.T) {
	ctx := options.ToContext(context.Background(), test.Options())
	r := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // deterministic seed for reproducible test inputs
	kubeClient := fake.NewClientBuilder().WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
		return []string{o.(*corev1.Pod).Spec.NodeName}
	}).Build()
	c := state.NewCluster(ctx, clocktesting.NewFakeClock(time.Now()), kubeClient, fakecp.NewCloudProvider())
	nodePools := []string{"a", "b", ""}
	var rv uint64
	nextRV := func() string { rv++; return strconv.FormatUint(rv, 10) }
	uids := map[string]types.UID{}
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
			c.DeleteNodeClaim(name)
			delete(uids, "nc"+name)
		case op == 1:
			c.DeleteNode(name)
			delete(uids, "node"+name)
		case op == 2:
			c.MarkForDeletion(providerID)
		case op == 3:
			c.UnmarkForDeletion(providerID)
		case op < 7:
			nc := &v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid("nc", name), ResourceVersion: nextRV(), Labels: map[string]string{v1.NodePoolLabelKey: nodePool}},
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
			c.UpdateNodeClaim(nc)
		default:
			labels := map[string]string{corev1.LabelInstanceTypeStable: "default-instance-type"}
			// Some Nodes are unmanaged: they carry no NodePool label and no NodeClaim joins them, unless a NodeClaim
			// with the same providerID exists.
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
			if err := c.UpdateNode(ctx, node); err != nil {
				t.Fatalf("updating node, %v", err)
			}
		}
		for _, reason := range []v1.DisruptionReason{v1.DisruptionReasonUnderutilized, v1.DisruptionReasonDrifted, v1.DisruptionReasonEmpty, v1.DisruptionReasonUnhealthy} {
			wantNodes, wantDisrupting := recountNodePoolStats(c.DeepCopyNodes(), reason)
			// NodePool names are never empty, so nodes without the label count toward none.
			for _, np := range []string{"a", "b"} {
				if numNodes, disrupting := disruption.NodePoolStats(c, np, reason); numNodes != wantNodes[np] || disrupting != wantDisrupting[np] {
					t.Fatalf("after change %d, NodePool %s for %s counts %d nodes and %d disrupting, want %d and %d", i, np, reason, numNodes, disrupting, wantNodes[np], wantDisrupting[np])
				}
			}
		}
	}
}

func TestValidateNodeDisruptableRejectsARebootingNode(t *testing.T) {
	stateNode := func(rebootStatus metav1.ConditionStatus) *state.StateNode {
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
		return &state.StateNode{Node: n, NodeClaim: nc}
	}
	if err := disruption.ValidateNodeDisruptable(stateNode(metav1.ConditionTrue), clock.RealClock{}); err == nil || !strings.Contains(err.Error(), "node is rebooting") {
		t.Errorf("ValidateNodeDisruptable on a rebooting node = %v, want an error that the node is rebooting", err)
	}
	if err := disruption.ValidateNodeDisruptable(stateNode(metav1.ConditionFalse), clock.RealClock{}); err != nil {
		t.Errorf("ValidateNodeDisruptable on a node done rebooting = %v, want nil", err)
	}
}
