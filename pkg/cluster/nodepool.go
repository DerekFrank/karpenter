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
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// NodePool is a NodePool with the fields Karpenter derives about it.
type NodePool = state.View[*v1.NodePool, NodePoolDerived]

// NodePoolDerived are the fields Karpenter derives about a NodePool.
type NodePoolDerived struct {
	// Usage sums the status.capacity of the NodePool's NodeClaims, and counts each NodeClaim as one resources.Node. A
	// NodeClaim stops counting once it's being deleted or disrupted.
	Usage corev1.ResourceList
}

// NodePools is the state accessor for NodePools: read NodePools only through it.
type NodePools = state.Accessor[*v1.NodePool, *v1.NodePoolList, NodePoolDerived]

// NewNodePools returns the state accessor for NodePools, reading them through kubeClient.
func NewNodePools(kubeClient client.Reader, usage *NodePoolUsage) *NodePools {
	return state.NewAccessor(kubeClient,
		func() *v1.NodePool { return &v1.NodePool{} },
		func() *v1.NodePoolList { return &v1.NodePoolList{} },
		func(l *v1.NodePoolList) []*v1.NodePool {
			items := make([]*v1.NodePool, len(l.Items))
			for i := range l.Items {
				items[i] = &l.Items[i]
			}
			return items
		},
		func(np *v1.NodePool) NodePoolDerived {
			derived, _ := usage.store.Get(np.Name)
			return derived
		})
}

// NodePoolUsage keeps NodePoolDerived.Usage from the NodePools' NodeClaims.
type NodePoolUsage struct {
	store *state.Store[string, NodePoolDerived]
	// NodeClaims holds the NodeClaims the usage is derived from. It must run as a manager runnable.
	NodeClaims *state.Store[types.NamespacedName, *v1.NodeClaim]
}

// NewNodePoolUsage returns a NodePoolUsage that follows the NodeClaims once its NodeClaims store starts.
func NewNodePoolUsage(kubeClient client.Client, informers cache.Informers) *NodePoolUsage {
	nodeClaims := state.NewObjectStore[*v1.NodeClaim](kubeClient, informers)
	nodePools := state.NewStore[string, NodePoolDerived]()
	state.Derive(nodeClaims, nodePools, nodePoolOf, func(_ types.NamespacedName, nodeClaim *v1.NodeClaim) nodeClaimUsage {
		return nodeClaimUsage(lo.Assign(nodeClaim.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")}))
	})
	return &NodePoolUsage{store: nodePools, NodeClaims: nodeClaims}
}

// nodePoolOf is the NodePool a NodeClaim counts against, until it's being deleted or disrupted.
func nodePoolOf(_ types.NamespacedName, nodeClaim *v1.NodeClaim) string {
	if !nodeClaim.DeletionTimestamp.IsZero() || lo.ContainsBy(nodeClaim.Status.Conditions, func(c status.Condition) bool {
		return c.Type == v1.ConditionTypeDisruptionReason && c.Status == metav1.ConditionTrue
	}) {
		return ""
	}
	return nodeClaim.Labels[v1.NodePoolLabelKey]
}

// nodeClaimUsage is a NodeClaim's usage of its NodePool: its status.capacity, and one resources.Node.
type nodeClaimUsage corev1.ResourceList

func (u nodeClaimUsage) Update(f NodePoolDerived, prev nodeClaimUsage) NodePoolDerived {
	usage := resources.Merge(f.Usage, corev1.ResourceList(u))
	resources.SubtractFrom(usage, corev1.ResourceList(prev))
	f.Usage = nonzero(usage)
	return f
}

func (u nodeClaimUsage) Delete(f NodePoolDerived) NodePoolDerived {
	usage := resources.Merge(f.Usage)
	resources.SubtractFrom(usage, corev1.ResourceList(u))
	f.Usage = nonzero(usage)
	return f
}

func (u nodeClaimUsage) Equal(o nodeClaimUsage) bool {
	if len(u) != len(o) {
		return false
	}
	for name, q := range u {
		if other, ok := o[name]; !ok || !q.Equal(other) {
			return false
		}
	}
	return true
}

// nonzero drops usage's zero quantities, and returns nil for usage that is all zero.
func nonzero(usage corev1.ResourceList) corev1.ResourceList {
	usage = lo.OmitBy(usage, func(_ corev1.ResourceName, q resource.Quantity) bool { return q.IsZero() })
	if len(usage) == 0 {
		return nil
	}
	return usage
}

// Synced returns whether the usage counts the NodeClaims the NodeClaims store listed when it started.
func (u *NodePoolUsage) Synced() bool { return u.store.Synced() }

// Subscribe calls fn with the name of each NodePool whose usage changes. fn must not block.
func (u *NodePoolUsage) Subscribe(fn func(nodePool string)) {
	u.store.Subscribe(func(c state.Change[string, NodePoolDerived]) { fn(c.Key) })
}
