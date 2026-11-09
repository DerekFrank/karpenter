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
	"sync"

	"github.com/awslabs/operatorpkg/option"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	autoscalingv1beta1 "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

// These tests build cluster state without an API server: each object store follows statetest.Objects, which a test
// fills with Put and Remove as the informers would, through the store's accessor.

// sourcesOf are the Objects each accessor's store follows, by the accessor.
var sourcesOf sync.Map

// newState builds cluster state over statetest.Sources, as New does over the overlay client.
func newState(ctx context.Context, clk clock.Clock, cloudProvider cloudprovider.CloudProvider, opts ...option.Function[Options]) State {
	srcs := statetest.NewSources()
	s := New(ctx, clk, nil, cloudProvider, append(opts, WithSources(func(obj client.Object) state.Newest { return srcs.For(obj) }))...)
	for _, accessor := range []any{s.Nodes, s.Nodes.NodeClaimStore, s.Nodes.NodeStore, s.Nodes.PodStore, s.Nodes.CSINodeStore, s.NodePoolState, s.CapacityQuotaState} {
		sourcesOf.Store(accessor, srcs)
	}
	return s
}

// newNodeState builds node state over statetest.Sources, as NewNodeState does over the overlay client.
func newNodeState(ctx context.Context, clk clock.Clock, cloudProvider cloudprovider.CloudProvider) (*NodeState, *statetest.Sources) {
	srcs := statetest.NewSources()
	s := NewNodeState(ctx, clk, func(obj client.Object) state.Newest { return srcs.For(obj) }, cloudProvider, nil)
	for _, accessor := range []any{s, s.NodeClaimStore, s.NodeStore, s.PodStore, s.CSINodeStore} {
		sourcesOf.Store(accessor, srcs)
	}
	return s, srcs
}

func sources(accessor any) *statetest.Sources {
	srcs, ok := sourcesOf.Load(accessor)
	if !ok {
		panic("the accessor wasn't built by newState")
	}
	return srcs.(*statetest.Sources)
}

// Put shows the NodeClaim store a version of the NodeClaim, as the informer would.
func (n *NodeClaims) Put(nodeClaim *v1.NodeClaim) { sources(n).Put(nodeClaim) }

// Remove shows the NodeClaim store that the NodeClaim named name is gone, as the informer would.
func (n *NodeClaims) Remove(name string) {
	sources(n).Remove(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

func (n *Nodes) Put(node *corev1.Node) { sources(n).Put(node) }

func (n *Nodes) Remove(name string) {
	sources(n).Remove(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

func (c *CSINodes) Put(csiNode *storagev1.CSINode) { sources(c).Put(csiNode) }

func (c *CSINodes) Remove(name string) {
	sources(c).Remove(&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

func (p *Pods) Put(pod *corev1.Pod) { sources(p).Put(pod) }

func (p *Pods) Remove(name types.NamespacedName) {
	sources(p).Remove(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: name.Namespace, Name: name.Name}})
}

func (n *NodePoolState) Put(nodePool *v1.NodePool) { sources(n).Put(nodePool) }

func (n *NodePoolState) Remove(name string) {
	sources(n).Remove(&v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

// Put shows the CapacityQuota store a version of the CapacityQuota, as the informer would, and enforces it, as the
// quota controller would on the store's change.
func (s *CapacityQuotaState) Put(capacityQuota *autoscalingv1beta1.CapacityQuota) {
	sources(s).Put(capacityQuota)
	s.rebuild(capacityQuota.Name)
}

// Remove shows the CapacityQuota store that the CapacityQuota named name is gone, and stops enforcing it, as the quota
// controller would.
func (s *CapacityQuotaState) Remove(name string) {
	sources(s).Remove(&autoscalingv1beta1.CapacityQuota{ObjectMeta: metav1.ObjectMeta{Name: name}})
	s.rebuild(name)
}
