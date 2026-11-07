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

// Package state holds the state accessors: readers of API objects with the fields Karpenter derives about them. See
// pkg/state/store/AGENTS.md.
package state

import (
	"context"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// NodePool is a NodePool with the fields Karpenter derives about it.
type NodePool = View[*v1.NodePool, NodePoolDerived]

// NodePoolDerived are the fields Karpenter derives about a NodePool.
type NodePoolDerived struct {
	// Usage sums the status.capacity of the NodePool's NodeClaims, and counts each NodeClaim as one resources.Node. A
	// NodeClaim stops counting once it's being deleted or disrupted.
	Usage corev1.ResourceList
}

// NodePools is the state accessor for NodePools: read NodePools only through it.
type NodePools = Accessor[*v1.NodePool, *v1.NodePoolList, NodePoolDerived]

// NewNodePools returns the state accessor for NodePools, reading them through kubeClient.
func NewNodePools(kubeClient client.Reader, usage *NodePoolUsage) *NodePools {
	return NewAccessor(kubeClient,
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
	store      *Store[string, NodePoolDerived]
	NodeClaims *Contributor[string, *v1.NodeClaim, NodePoolDerived, corev1.ResourceList]
}

// NewNodePoolUsage returns a NodePoolUsage that follows the NodeClaims once its NodeClaims Contributor starts.
func NewNodePoolUsage(kubeClient client.Client, informers cache.Informers) *NodePoolUsage {
	nodePools := NewStore[string, NodePoolDerived]()
	return &NodePoolUsage{
		store: nodePools,
		NodeClaims: Contribute(nodePools, kubeClient, informers, &v1.NodeClaimList{},
			func(nodeClaim *v1.NodeClaim) string {
				if !nodeClaim.DeletionTimestamp.IsZero() || lo.ContainsBy(nodeClaim.Status.Conditions, func(c status.Condition) bool {
					return c.Type == v1.ConditionTypeDisruptionReason && c.Status == metav1.ConditionTrue
				}) {
					return ""
				}
				return nodeClaim.Labels[v1.NodePoolLabelKey]
			},
			func(_ context.Context, nodeClaim *v1.NodeClaim) (corev1.ResourceList, error) {
				return lo.Assign(nodeClaim.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")}), nil
			},
			func(f NodePoolDerived, prev, next corev1.ResourceList) NodePoolDerived {
				usage := resources.Merge(f.Usage, next)
				resources.SubtractFrom(usage, prev)
				f.Usage = lo.OmitBy(usage, func(_ corev1.ResourceName, q resource.Quantity) bool { return q.IsZero() })
				if len(f.Usage) == 0 {
					f.Usage = nil
				}
				return f
			},
		),
	}
}

// Subscribe calls fn with the name of each NodePool whose usage changes. fn must not block.
func (u *NodePoolUsage) Subscribe(fn func(nodePool string)) {
	u.store.Subscribe(func(name string, _, _ NodePoolDerived) { fn(name) })
}
