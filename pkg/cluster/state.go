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

// Package cluster holds the cluster's state: an accessor for each kind Karpenter reads, returning the objects with the
// fields Karpenter derives about them, built from the primitives in pkg/state. One file per kind. See
// pkg/state/AGENTS.md.
package cluster

import (
	"context"
	"fmt"

	"github.com/awslabs/operatorpkg/option"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	autoscalingv1beta1 "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/overlay"
)

// State holds every accessor, the stores behind them and the intent handles. Build it once, with New, where the
// operator is wired, start its stores with Register, and give each controller the accessors it reads and the intent
// handles it writes, never State itself, so that each constructor says what it reads and writes.
type State struct {
	// NodePools is the NodePool accessor, which reads each NodePool with NodePoolState's fields.
	NodePools *NodePools
	// NodePoolState keeps the fields of each NodePool, by name, and the reservations against its node limit.
	NodePoolState *NodePoolState
	// CapacityQuotaState keeps what counts against each CapacityQuota, by name, and the reservations against them.
	CapacityQuotaState *CapacityQuotaState
	// Nodes holds the stores behind the nodes Karpenter schedules onto and disrupts.
	Nodes *NodeState
	// PodScheduling keeps what the provisioner decides about each pod.
	PodScheduling *PodScheduling

	// Deletion is the handle of the deletion marks on NodeClaims, for the disruption queue, their one writer.
	Deletion *Deletion
	// Nomination is the handle of the nominations of NodeClaims for pending pods, for the provisioner, their one
	// writer.
	Nomination *Nomination
	// BufferPods is the handle of the buffer pod counts on NodeClaims, for the provisioner, their one writer.
	BufferPods *BufferPods
	// Registrations is the handle of the launch outcomes recorded on NodePools, for the NodeClaim lifecycle controller,
	// their one writer.
	Registrations *Registrations
}

// Options configure the state New builds.
type Options struct {
	repairPolicyMatcher *repair.RepairPolicyMatcher
	sources             Sources
}

// WithRepairPolicyMatcher makes the node state match each Node against the repair policies as it changes. Node repair
// reads the same matcher back through NodeState.RepairPolicyMatcher.
func WithRepairPolicyMatcher(matcher *repair.RepairPolicyMatcher) option.Function[Options] {
	return func(o *Options) { o.repairPolicyMatcher = matcher }
}

// Sources returns where cluster state reads the newest version of each object of obj's kind, or nil for a kind it
// doesn't follow.
type Sources func(obj client.Object) state.Newest

// WithSources reads each kind's newest versions from sources rather than the overlay client, for a unit test that
// builds cluster state without an API server and fills statetest.Objects as an informer would.
func WithSources(sources Sources) option.Function[Options] {
	return func(o *Options) { o.sources = sources }
}

// Kinds are the kinds cluster state follows, which the overlay client must overlay: each object store reads its kind's
// newest version through the overlay. CapacityQuotas are overlaid, and so followed, only with the CapacityQuota feature
// gate, since their CRD is installed only with it.
func Kinds(ctx context.Context) []client.Object {
	kinds := []client.Object{&v1.NodeClaim{}, &v1.NodePool{}, &corev1.Node{}, &corev1.Pod{}, &storagev1.CSINode{}}
	if options.FromContext(ctx).FeatureGates.CapacityQuota {
		kinds = append(kinds, &autoscalingv1beta1.CapacityQuota{})
	}
	return kinds
}

// New builds every store and accessor, in dependency order. Each object store follows its kind's newest versions
// through kubeClient, an overlay.Client that overlays Kinds, and the NodePools accessor reads NodePools through it.
// ctx carries the options derived fields read, and cloudProvider's node classes decide which NodeClaims the node state
// holds.
func New(ctx context.Context, clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, opts ...option.Function[Options]) State {
	o := option.Resolve(opts...)
	sources := o.sources
	if sources == nil {
		overlaid, ok := kubeClient.(*overlay.Client)
		if !ok {
			panic("cluster state reads each kind's newest versions through an overlay.Client")
		}
		sources = func(obj client.Object) state.Newest {
			src, err := overlaid.Source(obj)
			if err != nil {
				return nil
			}
			return src
		}
	}
	quotas := sources(&autoscalingv1beta1.CapacityQuota{})
	if quotas == nil {
		quotas = none{}
	}
	required := sources
	sources = func(obj client.Object) state.Newest {
		src := required(obj)
		if src == nil {
			panic(fmt.Sprintf("cluster state follows %T, which kubeClient doesn't overlay", obj))
		}
		return src
	}
	nodes := NewNodeState(ctx, clk, sources, cloudProvider, o.repairPolicyMatcher)
	nodePools := NewNodePoolState(clk, sources(&v1.NodePool{}), nodes)
	return State{
		NodePools:          NewNodePools(kubeClient, nodePools),
		NodePoolState:      nodePools,
		CapacityQuotaState: NewCapacityQuotaState(ctx, clk, quotas, nodes),
		Nodes:              nodes,
		PodScheduling:      NewPodScheduling(),
		Deletion:           newDeletion(nodes.NodeClaimStore),
		Nomination:         newNomination(clk, nodes.NodeClaimStore),
		BufferPods:         newBufferPods(nodes.NodeClaimStore),
		Registrations:      newRegistrations(nodePools),
	}
}

// none is the source of CapacityQuotas when the overlay doesn't overlay them, as without their feature gate: it holds
// nothing, and is synced.
type none struct{}

func (none) Get(types.NamespacedName) (client.Object, bool) { return nil, false }
func (none) Keys() []types.NamespacedName                   { return nil }
func (none) Subscribe(func(types.NamespacedName)) func()    { return func() {} }
func (none) HasSynced() bool                                { return true }
func (none) Holds(types.NamespacedName, types.UID) bool     { return false }

// Register adds every shared object store as a manager runnable, once, so that each applies what its source holds
// once the manager's caches sync. A controller reading them declares it with state.Requires, which holds
// its workers until they're synced. A controller adds only the stores it builds itself.
func (s State) Register(m manager.Manager) error {
	stores := []manager.Runnable{s.Nodes.NodeClaimStore.store, s.Nodes.NodeStore.store, s.Nodes.PodStore.store, s.Nodes.CSINodeStore.store,
		s.NodePoolState.store}
	// The CapacityQuota store follows the informer only with the CapacityQuota feature gate.
	if s.CapacityQuotaState.follows() {
		stores = append(stores, s.CapacityQuotaState.quotas)
	}
	for _, store := range stores {
		if err := m.Add(store); err != nil {
			return fmt.Errorf("adding cluster state store, %w", err)
		}
	}
	// The quota controller enforces each CapacityQuota the store holds, as it changes.
	if s.CapacityQuotaState.follows() {
		if err := s.CapacityQuotaState.register(m); err != nil {
			return fmt.Errorf("adding capacityquota controller, %w", err)
		}
	}
	return nil
}
