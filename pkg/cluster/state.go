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

// Package cluster holds the cluster's state: a store for each kind Karpenter reads, whose Reader returns the objects
// with the fields Karpenter derives about them, built from the primitives in pkg/state. One file per kind. See
// pkg/state/AGENTS.md.
package cluster

import (
	"context"
	"fmt"

	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

// State holds every store's Reader, the indexes on them and the intent handles. Build it once, with New, where the
// operator is wired, start its stores with Register, give each controller the Readers it reads and the intent handles
// it writes, never State itself, so that each constructor says what it reads and writes, and Close it once the manager
// stops.
type State struct {
	// NodePoolState keeps the fields of each NodePool, by name, and the reservations against its node limit.
	NodePoolState *NodePoolState
	// CapacityQuotaState keeps what counts against each CapacityQuota, by name, and the reservations against them.
	CapacityQuotaState *CapacityQuotaState
	// NodeClaims are the managed nodes, by NodeClaim name: each NodeClaim with its Node, that Node's usage and the
	// intent kept about the node.
	NodeClaims *NodeClaims
	// Nodes are the Nodes, by name, with the usage of the pods bound to each. Unmanaged reads the unmanaged ones.
	Nodes *Nodes
	// Pods are the pods, by namespaced name, with what the provisioner decided about each.
	Pods *Pods
	// CSINodes are the CSINodes, by name, whose drivers count on their Nodes' usage.
	CSINodes *CSINodes
	// PVCs are the PVCs' resolved CSI drivers, which the PVC driver controller feeds.
	PVCs *PVCs
	// Cluster is the cluster-wide counts.
	Cluster *Clusters
	// Cost keeps the cost of each NodePool's NodeClaims, priced by the pricing controller.
	Cost *ClusterCost
	// RepairPolicyMatcher is the matcher the Node store matches each Node's conditions with, or nil when node repair is
	// disabled.
	RepairPolicyMatcher *repair.PolicyMatcher

	// NodeClaimsByProviderID indexes the NodeClaims by providerID, for code that starts from a cloud instance.
	NodeClaimsByProviderID *NodeClaimIndex[string]
	// NodeClaimsByNodePool indexes the NodeClaims by their NodePool label.
	NodeClaimsByNodePool *NodeClaimIndex[string]
	// PodsByNode indexes the pods by spec.nodeName.
	PodsByNode *PodIndex[string]
	// PodsByDaemonSet indexes the pods by the DaemonSet that controls them, whose newest pod DaemonSetPod samples.
	PodsByDaemonSet *PodIndex[types.NamespacedName]
	// PodsBySimulatedNodeClaim indexes the pods by the NodeClaim the provisioner's most recent scheduling simulation of
	// each placed it on.
	PodsBySimulatedNodeClaim *PodIndex[string]

	// Deletion is the handle of the deletion marks on NodeClaims, for the disruption queue, their one writer.
	Deletion *Deletion
	// Nomination is the handle of the nominations of NodeClaims for pending pods, for the provisioner, their one
	// writer.
	Nomination *Nomination
	// BufferPods is the handle of the buffer pod counts on NodeClaims, for the provisioner, their one writer.
	BufferPods *BufferPods
	// PodScheduling is the handle of what the provisioner decides about each pod as it schedules it, for the
	// provisioner and the provisioning pod controller, which acks the pods it triggers the provisioner for.
	PodScheduling *PodScheduling
	// Registrations is the handle of the launch outcomes recorded on NodePools, for the NodeClaim lifecycle controller,
	// their one writer.
	Registrations *Registrations
	// DriftBackoffs is the handle of the drift back-off on NodePools, for disruption, its one writer.
	DriftBackoffs *DriftBackoffs

	// stores are every store New built, which Close closes.
	stores []interface{ Close() }
}

// Options configure the state New builds.
type Options struct {
	repairPolicyMatcher *repair.PolicyMatcher
	sources             Sources
}

// WithRepairPolicyMatcher makes the Node store match each Node against the repair policies as it changes. Node repair
// reads the same matcher back through State.RepairPolicyMatcher.
func WithRepairPolicyMatcher(matcher *repair.PolicyMatcher) option.Function[Options] {
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

// New builds every store, in dependency order. Each object store follows its kind's newest versions through
// kubeClient, an overlay.Client that overlays Kinds. ctx carries the options derived fields read, such as the feature
// gates that decide a pod's disruption cost, and cloudProvider's node classes decide which NodeClaims the NodeClaim
// store holds.
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
	managed := lo.Map(cloudProvider.GetSupportedNodeClasses(), func(nodeClass status.Object, _ int) schema.GroupKind {
		return object.GVK(nodeClass).GroupKind()
	})
	pods := newPods(sources(&corev1.Pod{}))
	nodeClaims := newNodeClaims(sources(&v1.NodeClaim{}), managed)
	nodes := newNodes(sources(&corev1.Node{}))
	csiNodes := newCSINodes(sources(&storagev1.CSINode{}))
	pvcs := newPVCs()
	clusters := newClusters()
	if o.repairPolicyMatcher != nil {
		nodes.deriveRepairMatches(o.repairPolicyMatcher)
	}
	csiNodes.deriveDrivers(nodes)
	pvcs.dependOn(pods)
	// The pods' usage on the Node they're bound to, then each tracked Node's entry on its NodeClaim's.
	state.Derive(pods.Reader, nodes.store, podNodeKey, podContributionOf(ctx, pvcs))
	nodeClaims.deriveNodes(nodes)
	clusters.derive(nodeClaims)
	nodePools := NewNodePoolState(clk, sources(&v1.NodePool{}), nodeClaims, nodes, o.repairPolicyMatcher)
	quotaState := NewCapacityQuotaState(ctx, clk, quotas, nodeClaims, nodes)
	cost := newClusterCost(ctx, cloudProvider, nodeClaims)
	return State{
		NodePoolState:            nodePools,
		CapacityQuotaState:       quotaState,
		NodeClaims:               nodeClaims,
		Nodes:                    nodes,
		Pods:                     pods,
		CSINodes:                 csiNodes,
		PVCs:                     pvcs,
		Cluster:                  clusters,
		Cost:                     cost,
		RepairPolicyMatcher:      o.repairPolicyMatcher,
		NodeClaimsByProviderID:   nodeClaims.byProviderID,
		NodeClaimsByNodePool:     nodeClaims.byNodePool,
		PodsByNode:               pods.byNode,
		PodsByDaemonSet:          pods.byDaemonSet,
		PodsBySimulatedNodeClaim: pods.bySimulatedNodeClaim,
		Deletion:                 newDeletion(nodeClaims),
		Nomination:               newNomination(clk, nodeClaims),
		BufferPods:               newBufferPods(nodeClaims),
		PodScheduling:            newPodScheduling(clk, pods),
		Registrations:            newRegistrations(nodePools),
		DriftBackoffs:            newDriftBackoffs(clk, nodePools),
		stores: []interface{ Close() }{nodeClaims.store, nodes.store, pods.store, csiNodes.store, pvcs.store, clusters.store,
			nodePools.store, quotaState.quotas, quotaState.pending, cost.offerings, cost.prices},
	}
}

// Close stops every store New built: each stops following the overlay, each Derive into it stops, and every intent
// lapse and claim expiry timer stops. The stores keep the entries they hold. The operator closes State once the
// manager stops; a suite closes the State it builds for each spec. It's idempotent.
func (s *State) Close() {
	for _, store := range s.stores {
		store.Close()
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
	stores := []manager.Runnable{s.NodeClaims.store, s.Nodes.store, s.Pods.store, s.CSINodes.store, s.NodePoolState.store}
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
