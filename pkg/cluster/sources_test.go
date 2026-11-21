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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	autoscalingv1beta1 "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

// The specs that race many writers against a recount, or that start from sources that haven't synced, build cluster
// state without an API server: each object store follows statetest.Objects, which a spec fills with Put and Remove as
// the informers would, through the store's accessor. Every write applies, with each Derive of it, before Put returns.

// sourcesOf are the Objects each accessor's store follows, by the accessor.
var sourcesOf sync.Map

// newState builds cluster state over statetest.Sources, as New does over the overlay client.
func newState(ctx context.Context, clk clock.Clock, cloudProvider cloudprovider.CloudProvider, opts ...option.Function[Options]) State {
	srcs := statetest.NewSources()
	s := New(ctx, clk, nil, cloudProvider, append(opts, WithSources(func(obj client.Object) state.Newest { return srcs.For(obj) }))...)
	registerSources(s, srcs)
	return s
}

// registerSources records srcs as what each of s's stores follows, for Put and Remove.
func registerSources(s State, srcs *statetest.Sources) {
	for _, accessor := range []any{s.NodeClaims, s.Nodes, s.Pods, s.CSINodes, s.NodePoolState, s.CapacityQuotaState} {
		sourcesOf.Store(accessor, srcs)
	}
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

func (n *NodePoolState) Put(nodePool *v1.NodePool) { sources(n).Put(nodePool) }

// Put shows the CapacityQuota store a version of the CapacityQuota, as the informer would, and enforces it, as the
// quota controller would on the store's change.
func (s *CapacityQuotaState) Put(capacityQuota *autoscalingv1beta1.CapacityQuota) {
	sources(s).Put(capacityQuota)
	s.rebuild(capacityQuota.Name)
}
