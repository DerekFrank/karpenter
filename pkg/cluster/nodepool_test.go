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
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/test"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

var _ = Describe("NodePool", func() {
	It("should keep the contribution laws for NodePool contributions", func() {
		active := nodeClaimContribution{
			usage: nodeResources("2"), capacityCounted: true, counts: NodeClaimCounts{Active: 1},
			disruption: NodePoolDisruption{Nodes: 1},
		}
		notReady := nodeClaimContribution{
			usage: nodeResources("4"), capacityCounted: true, counts: NodeClaimCounts{Active: 1},
			disruption: NodePoolDisruption{Nodes: 1, NotReady: 1},
		}
		// A NodeClaim being deleted counts only as deleting, and as disrupting.
		deleting := nodeClaimContribution{counts: NodeClaimCounts{Deleting: 1}, disruption: NodePoolDisruption{Nodes: 1, Disrupting: 1}}
		unlaunched := nodeClaimContribution{usage: corev1.ResourceList{resources.Node: resource.MustParse("1")}, counts: NodeClaimCounts{Active: 1}}
		for _, b := range []NodePoolFields{{}, contributionBase()} {
			for _, pair := range [][2]nodeClaimContribution{{active, notReady}, {notReady, deleting}, {unlaunched, active}, {deleting, unlaunched}} {
				statetest.ExpectContribution(GinkgoT(), b, pair[0], pair[1])
			}
		}
		// A NodeClaim's whole usage leaving drops the resources it zeroes, and an empty NodePool's fields are zero.
		f := active.Delete(active.Update(NodePoolFields{}, nodeClaimContribution{}))
		Expect(equality.Semantic.DeepEqual(f, NodePoolFields{})).To(BeTrue(), "fields = %+v once its only NodeClaim left, want zero", f)
		Expect(f.Usage).To(BeNil(), "fields = %+v once its only NodeClaim left, want zero", f)
	})
	// Changes NodeClaims and Nodes at random, concurrently, and checks each NodePool's fields against a recount of the
	// stores by the definitions of the stores they replace: NodePoolUsage's usage, NodePoolState's counts, the
	// disruption counts, and the per NodePool node health.
	DescribeTable("should keep fields that match a recount of their definitions",
		func(seed uint64) {
			s := newState(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, fake.NewCloudProvider(), WithRepairPolicyMatcher(testMatcher()))
			var wg sync.WaitGroup
			for w := range 8 {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(seed, uint64(w))) //nolint:gosec // a seeded source makes failures reproducible
					for i := range 400 {
						changeAtRandom(r, s, fmt.Sprint(w*10000+i+1))
					}
				})
			}
			wg.Wait()
			want := recountNodePools(s)
			got := map[string]NodePoolFields{}
			for _, name := range []string{"a", "b", "c"} {
				got[name] = s.NodePoolState.Get(name)
			}
			for _, name := range lo.Uniq(append(slices.Collect(maps.Keys(got)), slices.Collect(maps.Keys(want))...)) {
				Expect(equality.Semantic.DeepEqual(nonzeroFields(got[name]), nonzeroFields(want[name]))).To(BeTrue(),
					"NodePool %q fields = %+v, want the recount %+v", name, got[name], want[name])
			}
		},
		Entry("seed=0", uint64(0)),
		Entry("seed=1", uint64(1)),
		Entry("seed=2", uint64(2)),
		Entry("seed=3", uint64(3)),
		Entry("seed=4", uint64(4)),
	)
})

// nodeResources is a node's resources with q CPU, as a NodeClaim contributes them.
func nodeResources(q string) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(q), resources.Node: resource.MustParse("1")}
}

// contributionBase is a NodePool's fields with every field set, for the contribution laws to hold against.
func contributionBase() NodePoolFields {
	return NodePoolFields{
		Usage: nodeResources("8"), NodeClaims: NodeClaimCounts{Active: 3, Deleting: 1},
		Disruption: NodePoolDisruption{Nodes: 4, Disrupting: 1, NotReady: 2}, NodeHealth: NodeCounts{Nodes: 3, Unhealthy: 1}, Reserved: 2,
		ReservedResources: nodeResources("4"),
	}
}

// changeAtRandom makes one random change to a NodeClaim, its Node or its deletion mark.
//
//nolint:gocyclo
func changeAtRandom(r *rand.Rand, s State, rv string) {
	// Each NodeClaim keeps one NodePool label; the NodePool members spec covers NodeClaims that move. Its Node sometimes
	// carries another, which moves the node's health but not its NodeClaim's counts.
	pools := []string{"a", "b", "c"}
	i := r.IntN(30)
	name, pool := fmt.Sprint("nc-", i), pools[i%3]
	switch r.IntN(10) {
	case 0:
		s.Nodes.NodeClaimStore.Remove(name)
	case 1:
		s.Nodes.NodeStore.Remove("node-" + name)
	case 2:
		s.Deletion.Mark(name)
	case 3:
		s.Deletion.Unmark(name)
	case 4, 5, 6:
		nc := &v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: rv, Labels: map[string]string{v1.NodePoolLabelKey: pool}},
			Spec:       v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
			Status: v1.NodeClaimStatus{Capacity: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse(fmt.Sprint(1 + r.IntN(4))), corev1.ResourceMemory: resource.MustParse("0"),
			}},
		}
		if r.IntN(4) != 0 {
			nc.Status.ProviderID = "fake://" + name
		}
		if r.IntN(5) == 0 {
			nc.DeletionTimestamp, nc.Finalizers = &metav1.Time{Time: time.Unix(1, 0)}, []string{"test"}
		}
		for _, condition := range []string{v1.ConditionTypeInitialized, v1.ConditionTypeDisruptionReason, v1.ConditionTypeRebooting, v1.ConditionTypeInstanceTerminating} {
			if r.IntN(3) == 0 {
				nc.Status.Conditions = append(nc.Status.Conditions, status.Condition{
					Type: condition, Status: lo.Ternary(r.IntN(4) == 0, metav1.ConditionFalse, metav1.ConditionTrue), Reason: condition,
				})
			}
		}
		s.Nodes.NodeClaimStore.Put(nc)
	default:
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-" + name, UID: types.UID("node-" + name), ResourceVersion: rv,
				Labels: map[string]string{v1.NodePoolLabelKey: lo.Ternary(r.IntN(5) == 0, pools[(i+1)%3], pool), corev1.LabelInstanceTypeStable: "small"},
			},
			Spec: corev1.NodeSpec{ProviderID: "fake://" + name},
			Status: corev1.NodeStatus{
				Capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(fmt.Sprint(1 + r.IntN(4)))},
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: lo.Ternary(r.IntN(3) == 0, corev1.ConditionFalse, corev1.ConditionTrue)},
					{Type: "BadNode", Status: lo.Ternary(r.IntN(3) == 0, corev1.ConditionFalse, corev1.ConditionTrue)},
				},
			},
		}
		if r.IntN(2) == 0 {
			node.Labels[v1.NodeInitializedLabelKey] = "true"
		}
		if r.IntN(5) != 0 {
			node.OwnerReferences = []metav1.OwnerReference{{APIVersion: apis.Group + "/v1", Kind: "NodeClaim", Name: name, UID: types.UID(name)}}
		}
		s.Nodes.NodeStore.Put(node)
	}
}

// recountNodePools recounts each NodePool's fields from the NodeClaim and Node stores, by the definitions the fields
// replace.
//
//nolint:gocyclo
func recountNodePools(s State) map[string]NodePoolFields {
	want := map[string]NodePoolFields{}
	update := func(name string, fn func(NodePoolFields) NodePoolFields) {
		if name != "" {
			want[name] = fn(want[name])
		}
	}
	for _, f := range s.Nodes.NodeClaimStore.List() {
		nc := f.Object
		node := s.Nodes.NodeStore.linked(f)
		pool := nc.Labels[v1.NodePoolLabelKey]
		disrupted := lo.ContainsBy(nc.Status.Conditions, func(c status.Condition) bool {
			return c.Type == v1.ConditionTypeDisruptionReason && c.Status == metav1.ConditionTrue
		})
		// NodePoolUsage: every NodeClaim's status.capacity and one node, until it's deleting or disrupted.
		if nc.DeletionTimestamp.IsZero() && !disrupted {
			update(pool, func(n NodePoolFields) NodePoolFields {
				n.Usage = resources.Merge(n.Usage, nc.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")})
				return n
			})
		}
		// NodePoolState: active or deleting.
		update(pool, func(n NodePoolFields) NodePoolFields {
			if !nc.DeletionTimestamp.IsZero() || disrupted {
				n.NodeClaims.Deleting++
			} else {
				n.NodeClaims.Active++
			}
			return n
		})
		if nc.Status.ProviderID == "" {
			continue
		}
		// nodePoolDisruption.
		if conditionTrue(nc, v1.ConditionTypeInstanceTerminating) || (!conditionTrue(nc, v1.ConditionTypeInitialized) && !rebooting(nc)) {
			continue
		}
		update(pool, func(n NodePoolFields) NodePoolFields {
			n.Disruption.Nodes++
			switch {
			case f.Intent.Deletion.Value || deleted(nc) || rebooting(nc):
				n.Disruption.Disrupting++
			case node == nil || nodeutils.GetCondition(node, corev1.NodeReady).Status != corev1.ConditionTrue:
				n.Disruption.NotReady++
			}
			return n
		})
	}
	// The NodePool's node health, from the Node store.
	for _, e := range s.Nodes.NodeStore.List() {
		node := e.Object
		if !Tracked(node) {
			continue
		}
		update(node.Labels[v1.NodePoolLabelKey], func(n NodePoolFields) NodePoolFields {
			n.NodeHealth.Nodes++
			n.NodeHealth.Unhealthy += lo.Ternary(len(s.Nodes.matcher.Match(node)) > 0, 1, 0)
			return n
		})
	}
	return want
}

// nonzeroFields drops the zero quantities a sum leaves, so that fields compare by what they count.
func nonzeroFields(f NodePoolFields) NodePoolFields {
	f.Usage = nonzero(f.Usage)
	return f
}
