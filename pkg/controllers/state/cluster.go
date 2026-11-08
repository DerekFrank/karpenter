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

package state

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

// Cluster maintains cluster state that is often needed but expensive to compute. It keeps each node, the pods bound
// to it and the NodePools' usage as fields in state stores, which follow the Node, NodeClaim and Pod events that the
// cluster state informers apply.
type Cluster struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	// ctx carries the options derived fields read. Derived fields are computed on whatever goroutine changes their
	// source, so they read configuration fixed at construction rather than a request's.
	ctx       context.Context
	clock     clock.Clock
	hasSynced atomic.Bool

	// nodes is a join: each Node and its NodeClaim, by providerID, read together with the intent kept about them.
	nodes *karpenterstate.Store[string, nodeFields]
	// nodeClaims and pods hold the NodeClaims and pods, by name.
	nodeClaims *karpenterstate.Store[types.NamespacedName, *v1.NodeClaim]
	pods       *karpenterstate.Store[types.NamespacedName, *corev1.Pod]
	// nodeUsage keeps the fields derived about each node from its pods, by node name, as StateNode.Derived. They can lag
	// nodes. podsOnNodes is the Derive from pods, which knows the pods bound to each node.
	nodeUsage   *karpenterstate.Store[string, podFields]
	podsOnNodes *karpenterstate.Derivation[types.NamespacedName, string, *corev1.Pod, podFields, podContribution]
	// pvcDrivers keeps the CSI driver of each PersistentVolumeClaim, by namespaced name, which counts its volume against
	// the driver's limit on the node a pod mounts it on.
	pvcDrivers *karpenterstate.Store[types.NamespacedName, pvcFields]
	// nodePoolResources keeps the capacity derived about each NodePool from its nodes, by NodePool name.
	nodePoolResources *karpenterstate.Store[string, corev1.ResourceList]
	nodeCount         atomic.Int64
	// daemonSetPods keeps the newest pod derived about each DaemonSet from its pods, by DaemonSet.
	daemonSetPods *karpenterstate.Store[types.NamespacedName, daemonSetFields]
	// nodePoolMembers indexes each NodePool's nodes and unlaunched NodeClaims, by NodePool name.
	nodePoolMembers *karpenterstate.Store[string, nodePoolMembers]

	repairPolicyMatcher *health.RepairPolicyMatcher // nil when node repair is disabled

	NodePoolState *NodePoolState

	podAcks                         sync.Map // pod namespaced name -> time when Karpenter first saw the pod as pending
	podsSchedulingAttempted         sync.Map // pod namespaced name -> time when Karpenter tried to schedule a pod
	podsSchedulableTimes            sync.Map // pod namespaced name -> time when it was first marked as able to fit to a node
	podHealthyNodePoolScheduledTime sync.Map // pod namespaced name -> time when pod scheduled to a nodePool that has NodeRegistrationHealthy=true, is marked as able to fit to a node
	podToNodeClaim                  sync.Map // pod namespaced name -> nodeClaim name

	clusterStateMu sync.RWMutex // Separate mutex as this is called in some places that mu is held
	// A monotonically increasing timestamp representing the time state of the
	// cluster with respect to consolidation. This increases when something has
	// changed about the cluster that might make consolidation possible. By recording
	// the state, interested disruption methods can check to see if this has changed to
	// optimize and not try to disrupt if nothing about the cluster has changed.
	clusterState time.Time

	unsyncedTimeMu      sync.Mutex
	unsyncedStartTime   time.Time
	lastUnsyncedLogTime time.Time

	// bufferPodCounts tracks how many virtual CapacityBuffer pods the provisioner's
	// scheduling simulation placed on each node (keyed by providerID). This is
	// rebuilt wholesale after every provisioning pass — it is NOT incremental.
	//
	// Used by:
	//   - disruption/emptiness.go: prevents empty-consolidation of nodes that host
	//     buffer capacity (HasBufferPods check in ShouldDisrupt).
	//
	// NOT used by consolidation — consolidation naturally accounts for buffer pods
	// because SimulateScheduling calls GetPendingPods which injects virtual pods
	// into the pending set. Any replacement must fit both real and virtual pods.
	bufferPodCountsMu sync.RWMutex
	bufferPodCounts   map[string]int
}

// nodeFields are the fields kept for each node, keyed by its providerID, or by its name for an unmanaged Node without
// one. The Node and its NodeClaim each join themselves.
type nodeFields struct {
	Node                *corev1.Node
	NodeClaim           *v1.NodeClaim
	RepairPolicyMatches []health.RepairPolicyMatch
	// MarkedForDeletion and NominatedUntil are set by the disruption and provisioning controllers. They go with the
	// node once neither its Node nor its NodeClaim is left.
	MarkedForDeletion bool
	NominatedUntil    metav1.Time
}

// podFields are the fields derived about each node from the pods bound to it, keyed by the node's name. They exist as
// soon as a pod binds to the node, whether or not cluster state has seen the Node yet.
type podFields struct {
	Node              *corev1.Node
	Pods              int
	Requests          corev1.ResourceList
	Limits            corev1.ResourceList
	DaemonSetRequests corev1.ResourceList
	DaemonSetLimits   corev1.ResourceList
	// DisruptionCost sums the eviction costs of the non-DaemonSet pods with a positive cost.
	DisruptionCost float64
	HostPorts      *scheduling.HostPortUsage
	// PVCs counts the pods that mount each PersistentVolumeClaim. Its volume counts against the limit of its driver in
	// pvcDrivers.
	PVCs             map[types.NamespacedName]int
	AntiAffinityPods map[types.NamespacedName]*corev1.Pod
	// CSIDrivers are the drivers of the node's CSINode, read when the Node last changed and set by UpdateNode.
	CSIDrivers []storagev1.CSINodeDriver
}

// pvcFields are a PVC's CSI driver once resolved; Driver is empty for a PVC whose volume no driver limits.
type pvcFields struct {
	Resolved bool
	Driver   string
}

// nodeOnNode is a Node's contribution to the fields derived about it from its pods: the Node itself.
type nodeOnNode struct{ node *corev1.Node }

func (n nodeOnNode) Update(f podFields, _ nodeOnNode) podFields { f.Node = n.node; return f }
func (n nodeOnNode) Delete(f podFields) podFields               { f.Node = nil; return f }
func (n nodeOnNode) Equal(o nodeOnNode) bool                    { return n.node == o.node }

// podContribution is a pod's contribution to the node it's bound to.
type podContribution struct {
	pod            *corev1.Pod
	requests       corev1.ResourceList
	limits         corev1.ResourceList
	daemonSet      bool
	disruptionCost float64
	hostPorts      []scheduling.HostPort
	pvcs           []types.NamespacedName
	antiAffinity   bool
}

func (c *Cluster) newPodContribution(_ types.NamespacedName, pod *corev1.Pod) podContribution {
	// The same as RequestsForPods and LimitsForPods, from one Ceiling.
	ceiling := resources.Ceiling(pod)
	requests, limits := resources.Merge(ceiling.Requests), resources.Merge(ceiling.Limits)
	requests[corev1.ResourcePods] = *resource.NewQuantity(1, resource.DecimalExponent)
	limits[corev1.ResourcePods] = *resource.NewQuantity(1, resource.DecimalExponent)
	p := podContribution{
		pod:          pod,
		requests:     requests,
		limits:       limits,
		daemonSet:    podutils.IsOwnedByDaemonSet(pod),
		hostPorts:    scheduling.GetHostPorts(pod),
		antiAffinity: podutils.HasRequiredPodAntiAffinity(pod),
	}
	for _, volume := range pod.Spec.Volumes {
		if name := volumeutil.PersistentVolumeClaimName(pod, volume); name != "" {
			p.pvcs = append(p.pvcs, types.NamespacedName{Namespace: pod.Namespace, Name: name})
		}
	}
	if !p.daemonSet {
		p.disruptionCost = max(0, disruptionutils.EvictionCost(c.ctx, pod))
	}
	return p
}

// Update replaces prev's contribution with p's, copying each map at most once.
//
//nolint:gocyclo
func (p podContribution) Update(f podFields, prev podContribution) podFields {
	if prev.pod == nil {
		f.Pods++
		f.Requests = resources.Merge(f.Requests, p.requests)
		f.Limits = resources.Merge(f.Limits, p.limits)
	} else {
		f.Requests = resources.Merge(f.Requests, p.requests)
		resources.SubtractFrom(f.Requests, prev.requests)
		f.Limits = resources.Merge(f.Limits, p.limits)
		resources.SubtractFrom(f.Limits, prev.limits)
	}
	if prev.daemonSet || p.daemonSet {
		f.DaemonSetRequests = resources.Merge(f.DaemonSetRequests, lo.Ternary(p.daemonSet, p.requests, nil))
		f.DaemonSetLimits = resources.Merge(f.DaemonSetLimits, lo.Ternary(p.daemonSet, p.limits, nil))
		if prev.daemonSet {
			resources.SubtractFrom(f.DaemonSetRequests, prev.requests)
			resources.SubtractFrom(f.DaemonSetLimits, prev.limits)
		}
	}
	f.DisruptionCost += p.disruptionCost - prev.disruptionCost
	key := client.ObjectKeyFromObject(p.pod)
	if len(prev.hostPorts) > 0 || len(p.hostPorts) > 0 {
		f.HostPorts = lo.TernaryF(f.HostPorts == nil, scheduling.NewHostPortUsage, f.HostPorts.DeepCopy)
		f.HostPorts.DeletePod(key)
		if len(p.hostPorts) > 0 {
			f.HostPorts.Add(p.pod, p.hostPorts)
		}
	}
	if len(prev.pvcs) > 0 || len(p.pvcs) > 0 {
		f.PVCs = maps.Clone(f.PVCs)
		f.PVCs = countPVCs(countPVCs(f.PVCs, prev.pvcs, -1), p.pvcs, 1)
	}
	if prev.antiAffinity || p.antiAffinity {
		f.AntiAffinityPods = maps.Clone(f.AntiAffinityPods)
		delete(f.AntiAffinityPods, key)
		if p.antiAffinity {
			f.AntiAffinityPods = lo.Assign(f.AntiAffinityPods, map[types.NamespacedName]*corev1.Pod{key: p.pod})
		}
	}
	return f
}

func (p podContribution) Delete(f podFields) podFields {
	key := client.ObjectKeyFromObject(p.pod)
	f.Pods--
	f.Requests = resources.Subtract(f.Requests, p.requests)
	f.Limits = resources.Subtract(f.Limits, p.limits)
	if p.daemonSet {
		f.DaemonSetRequests = resources.Subtract(f.DaemonSetRequests, p.requests)
		f.DaemonSetLimits = resources.Subtract(f.DaemonSetLimits, p.limits)
	}
	f.DisruptionCost -= p.disruptionCost
	if len(p.hostPorts) > 0 {
		f.HostPorts = f.HostPorts.DeepCopy()
		f.HostPorts.DeletePod(key)
	}
	if len(p.pvcs) > 0 {
		f.PVCs = countPVCs(maps.Clone(f.PVCs), p.pvcs, -1)
	}
	if p.antiAffinity {
		f.AntiAffinityPods = maps.Clone(f.AntiAffinityPods)
		delete(f.AntiAffinityPods, key)
	}
	if f.Pods == 0 {
		return podFields{Node: f.Node, CSIDrivers: f.CSIDrivers}
	}
	return f
}

// countPVCs adds by to the count of each of pvcs in counts, which it modifies, and drops the PVCs it counts to zero.
func countPVCs(counts map[types.NamespacedName]int, pvcs []types.NamespacedName, by int) map[types.NamespacedName]int {
	for _, pvc := range pvcs {
		if counts == nil {
			counts = map[types.NamespacedName]int{}
		}
		if counts[pvc] += by; counts[pvc] == 0 {
			delete(counts, pvc)
		}
	}
	return lo.Ternary(len(counts) == 0, nil, counts)
}

// Equal reports whether Update would leave the fields the same with either contribution.
//
//nolint:gocyclo
func (p podContribution) Equal(o podContribution) bool {
	if (p.pod == nil) != (o.pod == nil) || (p.pod != nil && client.ObjectKeyFromObject(p.pod) != client.ObjectKeyFromObject(o.pod)) {
		return false
	}
	// A pod with required anti-affinity holds itself in the fields.
	if p.antiAffinity && p.pod != o.pod {
		return false
	}
	return p.daemonSet == o.daemonSet && p.antiAffinity == o.antiAffinity && p.disruptionCost == o.disruptionCost &&
		resourcesEqual(p.requests, o.requests) && resourcesEqual(p.limits, o.limits) &&
		slices.EqualFunc(p.hostPorts, o.hostPorts, func(a, b scheduling.HostPort) bool {
			return a.Port == b.Port && a.Protocol == b.Protocol && a.IP.Equal(b.IP)
		}) &&
		slices.Equal(p.pvcs, o.pvcs)
}

func resourcesEqual(a, b corev1.ResourceList) bool {
	return maps.EqualFunc(a, b, func(x, y resource.Quantity) bool { return x.Cmp(y) == 0 })
}

// nodePoolCapacity is a node's contribution to its NodePool's resources: its capacity, and one resources.Node.
type nodePoolCapacity corev1.ResourceList

func (n nodePoolCapacity) Update(total corev1.ResourceList, prev nodePoolCapacity) corev1.ResourceList {
	sum := resources.Merge(total, corev1.ResourceList(n))
	resources.SubtractFrom(sum, corev1.ResourceList(prev))
	return nonzero(sum)
}

func (n nodePoolCapacity) Delete(total corev1.ResourceList) corev1.ResourceList {
	sum := resources.Merge(total)
	resources.SubtractFrom(sum, corev1.ResourceList(n))
	return nonzero(sum)
}

func (n nodePoolCapacity) Equal(o nodePoolCapacity) bool {
	return resourcesEqual(corev1.ResourceList(n), corev1.ResourceList(o))
}

func nonzero(sum corev1.ResourceList) corev1.ResourceList {
	sum = lo.OmitBy(sum, func(_ corev1.ResourceName, q resource.Quantity) bool { return q.IsZero() })
	return lo.Ternary(len(sum) == 0, nil, sum)
}

// newestPod is a pod's contribution to its DaemonSet: the DaemonSet's newest pod, as a sample of its current template.
// A pod going away leaves the sample in place until a newer one arrives or the DaemonSet goes, so Delete doesn't undo
// Update, unlike a Contribution that counts.
type newestPod struct{ pod *corev1.Pod }

func (n newestPod) Update(f daemonSetFields, _ newestPod) daemonSetFields {
	if f.Pod == nil || n.pod.UID == f.Pod.UID || n.pod.CreationTimestamp.After(f.Pod.CreationTimestamp.Time) {
		f.Pod = n.pod
	}
	return f
}

func (n newestPod) Delete(f daemonSetFields) daemonSetFields { return f }
func (n newestPod) Equal(o newestPod) bool                   { return n.pod == o.pod }

type ClusterOptions struct {
	repairPolicyMatcher *health.RepairPolicyMatcher
}

// WithRepairPolicyMatcher makes cluster state match each Node against the repair policies as it changes. Node repair
// reads the same matcher back through RepairPolicyMatcher.
func WithRepairPolicyMatcher(matcher *health.RepairPolicyMatcher) option.Function[ClusterOptions] {
	return func(o *ClusterOptions) {
		o.repairPolicyMatcher = matcher
	}
}

// NewCluster returns cluster state. ctx carries the options its derived fields read, such as the feature gates that
// decide a pod's disruption cost.
func NewCluster(ctx context.Context, clk clock.Clock, client client.Client, cloudProvider cloudprovider.CloudProvider, opts ...option.Function[ClusterOptions]) *Cluster {
	o := option.Resolve(opts...)
	c := &Cluster{
		repairPolicyMatcher: o.repairPolicyMatcher,
		clock:               clk,
		kubeClient:          client,
		cloudProvider:       cloudProvider,
		bufferPodCounts:     map[string]int{},
		ctx:                 ctx,
	}
	c.newStores()
	return c
}

func (c *Cluster) newStores() {
	c.nodes = karpenterstate.NewJoinStore(nil, nil,
		karpenterstate.Join(func(nodeClaim *v1.NodeClaim) string { return nodeClaim.Status.ProviderID }, func(f nodeFields, nodeClaim *v1.NodeClaim) nodeFields {
			f.NodeClaim = nodeClaim
			return f.orNothing()
		}),
		karpenterstate.Join(nodeKey, c.setNode))
	c.nodeClaims = karpenterstate.NewObjectStore[*v1.NodeClaim](nil, nil)
	c.pods = karpenterstate.NewObjectStore[*corev1.Pod](nil, nil)
	c.nodeUsage = karpenterstate.NewStore[string, podFields]()
	c.pvcDrivers = karpenterstate.NewStore[types.NamespacedName, pvcFields]()
	c.nodePoolResources = karpenterstate.NewStore[string, corev1.ResourceList]()
	c.daemonSetPods = karpenterstate.NewStore[types.NamespacedName, daemonSetFields]()
	c.nodePoolMembers = karpenterstate.NewStore[string, nodePoolMembers]()
	c.NodePoolState = NewNodePoolState(c.clock, c.nodeClaims)

	c.podsOnNodes = karpenterstate.Derive(c.pods, c.nodeUsage,
		func(_ types.NamespacedName, pod *corev1.Pod) string {
			return lo.Ternary(podutils.IsTerminal(pod), "", pod.Spec.NodeName)
		},
		c.newPodContribution)
	karpenterstate.Derive(c.nodes, c.nodeUsage,
		func(_ string, f nodeFields) string {
			return lo.TernaryF(f.Node == nil, func() string { return "" }, func() string { return f.Node.Name })
		},
		func(_ string, f nodeFields) nodeOnNode { return nodeOnNode{f.Node} })
	karpenterstate.Derive(c.pods, c.daemonSetPods,
		func(_ types.NamespacedName, pod *corev1.Pod) types.NamespacedName { return daemonSetKey(pod) },
		func(_ types.NamespacedName, pod *corev1.Pod) newestPod { return newestPod{pod} })
	// A node marked for deletion doesn't count against its NodePool.
	karpenterstate.Derive(c.nodes, c.nodePoolResources,
		func(_ string, f nodeFields) string {
			n := &StateNode{Node: f.Node, NodeClaim: f.NodeClaim, markedForDeletion: f.MarkedForDeletion}
			if (f.Node == nil && f.NodeClaim == nil) || n.MarkedForDeletion() {
				return ""
			}
			return n.Labels()[v1.NodePoolLabelKey]
		},
		func(_ string, f nodeFields) nodePoolCapacity {
			n := &StateNode{Node: f.Node, NodeClaim: f.NodeClaim}
			return nodePoolCapacity(lo.Assign(n.Capacity(), corev1.ResourceList{resources.Node: resource.MustParse("1")}))
		})
	karpenterstate.Derive(c.nodes, c.nodePoolMembers, nodePoolOfNode, func(k string, _ nodeFields) nodePoolNode { return nodePoolNode(k) })
	karpenterstate.Derive(c.nodeClaims, c.nodePoolMembers, nodePoolOfUnlaunched,
		func(k types.NamespacedName, _ *v1.NodeClaim) unlaunchedNodeClaim { return unlaunchedNodeClaim(k) })
	c.nodes.Subscribe(func(change karpenterstate.Change[string, nodeFields]) {
		if change.Existed != change.Exists {
			ClusterStateNodesCount.Set(float64(c.nodeCount.Add(int64(lo.Ternary(change.Exists, 1, -1)))), nil)
		}
		old, new := change.Old, change.New
		before := &StateNode{Node: old.Node, NodeClaim: old.NodeClaim, markedForDeletion: old.MarkedForDeletion}
		after := &StateNode{Node: new.Node, NodeClaim: new.NodeClaim, markedForDeletion: new.MarkedForDeletion}
		if change.Existed != change.Exists || (change.Exists && (before.Initialized() != after.Initialized() || before.MarkedForDeletion() != after.MarkedForDeletion())) {
			c.MarkUnconsolidated()
		}
	})
	c.nodeUsage.Subscribe(func(change karpenterstate.Change[string, podFields]) {
		if change.Old.Pods != change.New.Pods {
			c.MarkUnconsolidated()
		}
	})
}

// clearStores empties the stores in place, so that their subscribers stay subscribed.
func (c *Cluster) clearStores() {
	for _, s := range []interface{ Clear() }{c.nodes, c.nodeClaims, c.pods, c.nodeUsage, c.pvcDrivers, c.nodePoolResources, c.daemonSetPods, c.nodePoolMembers} {
		s.Clear()
	}
	c.NodePoolState.Reset()
	c.nodeCount.Store(0)
}

// orNothing drops the intent kept about a node once neither its Node nor its NodeClaim is left.
func (f nodeFields) orNothing() nodeFields {
	if f.Node == nil && f.NodeClaim == nil {
		return nodeFields{}
	}
	return f
}

// setNode joins a Node into its fields, kept under its name as its providerID if it has none, with the repair policies
// it matches.
func (c *Cluster) setNode(f nodeFields, node *corev1.Node) nodeFields {
	f.Node, f.RepairPolicyMatches = node, nil
	if node != nil {
		if node.Spec.ProviderID == "" {
			f.Node = node.DeepCopy()
			f.Node.Spec.ProviderID = node.Name
		}
		if c.repairPolicyMatcher != nil {
			f.RepairPolicyMatches = c.repairPolicyMatcher.Match(node)
		}
	}
	return f.orNothing()
}

// nodeKey returns the key a Node is kept under: its providerID, or its name for an unmanaged Node without one. A
// managed Node is kept once it has a providerID and either its instance type label or has initialized.
func nodeKey(node *corev1.Node) string {
	managed := node.Labels[v1.NodePoolLabelKey] != ""
	if node.Spec.ProviderID == "" {
		return lo.Ternary(managed, "", node.Name)
	}
	if managed && node.Labels[corev1.LabelInstanceTypeStable] == "" && node.Labels[v1.NodeInitializedLabelKey] == "" {
		return ""
	}
	return node.Spec.ProviderID
}

// stateNode builds the StateNode for a node's fields, joined with the fields of the pods bound to it. A StateNode
// shares the Node and NodeClaim with cluster state, so they must not be modified. copyUsage copies the host port
// usage, which the scheduler adds simulated pods to; the volume usage is always the StateNode's own.
func (c *Cluster) stateNode(f nodeFields, copyUsage bool) *StateNode {
	n := &StateNode{
		Node:                f.Node,
		NodeClaim:           f.NodeClaim,
		markedForDeletion:   f.MarkedForDeletion,
		nominatedUntil:      f.NominatedUntil,
		repairPolicyMatches: f.RepairPolicyMatches,
	}
	var pods podFields
	if f.Node != nil {
		pods, _ = c.nodeUsage.Get(f.Node.Name)
	}
	n.Derived.PodRequests, n.Derived.PodLimits = pods.Requests, pods.Limits
	n.Derived.DaemonSetRequests, n.Derived.DaemonSetLimits = pods.DaemonSetRequests, pods.DaemonSetLimits
	n.Derived.DisruptionCost = pods.DisruptionCost
	n.Derived.HostPorts = lo.TernaryF(pods.HostPorts == nil, scheduling.NewHostPortUsage, func() *scheduling.HostPortUsage {
		return lo.Ternary(copyUsage, pods.HostPorts.DeepCopy(), pods.HostPorts)
	})
	n.Derived.Volumes = scheduling.NewVolumeUsage()
	if len(pods.PVCs) > 0 {
		volumes := scheduling.Volumes{}
		for pvc := range pods.PVCs {
			if driver, _ := c.pvcDrivers.Get(pvc); driver.Driver != "" {
				volumes.Add(driver.Driver, pvc.String())
			}
		}
		n.Derived.Volumes.Insert(volumes)
	}
	for _, driver := range pods.CSIDrivers {
		if driver.Allocatable == nil || driver.Allocatable.Count == nil {
			n.Derived.Volumes.AddUnbounded(driver.Name)
			continue
		}
		n.Derived.Volumes.AddLimit(driver.Name, int(*driver.Allocatable.Count))
	}
	return n
}

// Synced validates that the NodeClaims and the Nodes that are stored in the apiserver
// have the same representation in the cluster state. This is to ensure that our view
// of the cluster is as close to correct as it can be when we begin to perform operations
// utilizing the cluster state as our source of truth
//
//nolint:gocyclo
func (c *Cluster) Synced(ctx context.Context) (synced bool) {
	// Set the metric depending on the result of the Synced() call
	defer func() {
		c.unsyncedTimeMu.Lock()
		defer c.unsyncedTimeMu.Unlock()

		if synced {
			c.unsyncedStartTime = time.Time{}
			c.lastUnsyncedLogTime = time.Time{}
			ClusterStateUnsyncedTimeSeconds.Set(0, nil)
		} else {
			if c.unsyncedStartTime.IsZero() {
				c.unsyncedStartTime = c.clock.Now()
			}
			// We want to log every 10s when the cluster hasn't synced for 30s which is long enough for us to think there is an issue
			if c.clock.Since(c.unsyncedStartTime) > time.Second*30 && c.clock.Since(c.lastUnsyncedLogTime) > time.Second*10 {
				c.lastUnsyncedLogTime = c.clock.Now()
				log.FromContext(ctx).Error(
					fmt.Errorf("waiting on cluster sync"),
					"cluster is waiting on sync for extended duration",
					"duration", c.clock.Since(c.unsyncedStartTime).Truncate(time.Second))
			}
			ClusterStateUnsyncedTimeSeconds.Set(c.clock.Since(c.unsyncedStartTime).Seconds(), nil)
		}
	}()
	// Set the metric to whatever the result of the Synced() call is
	defer func() {
		ClusterStateSynced.Set(lo.Ternary[float64](synced, 1, 0), nil)
	}()

	stateNodeClaimNames := sets.New[string]()
	for name, nodeClaim := range c.nodeClaims.List() {
		// Check to see if any node claim doesn't have a provider ID. If it doesn't, then the nodeclaim hasn't been
		// launched, and we need to wait to see what the resolved values are before continuing.
		if nodeClaim.Status.ProviderID == "" {
			return false
		}
		stateNodeClaimNames.Insert(name.Name)
	}
	// If the cluster state has already synced once, then we assume that objects are kept internally consistent
	// with each other to avoid having to continually re-check that we have fully captured the same view
	// of cluster state that controller-runtime has
	if c.hasSynced.Load() {
		return true
	}

	// If we haven't synced before, then we need to make sure that our internal cache is fully hydrated
	// before we start doing operations against the state
	// Because we get so many NodeClaims from this response, we are not DeepCopying the cached data here
	// DO NOT MUTATE NodeClaims in this function as this will affect the underlying cached NodeClaim
	nodeClaims, err := nodeclaimutils.ListManaged(ctx, c.kubeClient, c.cloudProvider, client.UnsafeDisableDeepCopy)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed checking cluster state sync")
		return false
	}
	nodeList := &corev1.NodeList{}
	// Because we get so many Nodes from this response, we are not DeepCopying the cached data here
	// DO NOT MUTATE Nodes in this function as this will affect the underlying cached Node
	if err := c.kubeClient.List(ctx, nodeList, client.UnsafeDisableDeepCopy); err != nil {
		log.FromContext(ctx).Error(err, "failed checking cluster state sync")
		return false
	}
	stateNodeNames := sets.New[string]()
	for _, f := range c.nodes.List() {
		if f.Node != nil {
			stateNodeNames.Insert(f.Node.Name)
		}
	}

	nodeClaimNames := sets.New[string]()
	for _, nodeClaim := range nodeClaims {
		nodeClaimNames.Insert(nodeClaim.Name)
	}
	nodeNames := sets.New[string]()
	for _, node := range nodeList.Items {
		nodeNames.Insert(node.Name)
	}
	// The names tracked in-memory should at least have all the data that is in the api-server
	// This doesn't ensure that the two states are exactly aligned (we could still not be tracking a node
	// that exists in the cluster state but not in the apiserver) but it ensures that we have a state
	// representation for every node/nodeClaim that exists on the apiserver
	synced = stateNodeClaimNames.IsSuperset(nodeClaimNames) && stateNodeNames.IsSuperset(nodeNames)
	if synced {
		c.hasSynced.Store(true)
	}
	return synced
}

// ForPodsWithAntiAffinity calls the supplied function once for each pod with required anti affinity terms that is
// currently bound to a node. The pod returned may not be up-to-date with respect to status, however since the
// anti-affinity terms can't be modified, they will be correct.
func (c *Cluster) ForPodsWithAntiAffinity(fn func(p *corev1.Pod, n *corev1.Node) bool) {
	for _, f := range c.nodeUsage.List() {
		if f.Node == nil {
			continue
		}
		for _, pod := range f.AntiAffinityPods {
			if !fn(pod, f.Node) {
				return
			}
		}
	}
}

// Nodes returns an iterator over the state nodes in the cluster. The StateNodes share their Node, NodeClaim and usage
// with cluster state, so they must not be modified; use DeepCopyNodes for StateNodes to modify.
func (c *Cluster) Nodes() iter.Seq[*StateNode] {
	return func(yield func(*StateNode) bool) {
		for _, f := range c.nodes.List() {
			if !yield(c.stateNode(f, false)) {
				return
			}
		}
	}
}

// DeepCopyNodes returns the state nodes in the cluster, with their own copies of the usage that the scheduler adds
// simulated pods to. They share their Node and NodeClaim with cluster state, which must not be modified.
func (c *Cluster) DeepCopyNodes() StateNodes {
	var nodes StateNodes
	for _, f := range c.nodes.List() {
		nodes = append(nodes, c.stateNode(f, true))
	}
	return nodes
}

// IsNodeNominated returns true if the given node was expected to have a pod bound to it during a recent scheduling
// batch
func (c *Cluster) IsNodeNominated(providerID string) bool {
	f, ok := c.nodes.Get(providerID)
	return ok && f.NominatedUntil.After(c.clock.Now())
}

// NominateNodeForPod records that a node was the target of a pending pod during a scheduling batch
func (c *Cluster) NominateNodeForPod(ctx context.Context, providerID string) {
	until := metav1.Time{Time: c.clock.Now().Add(nominationWindow(ctx))}
	c.nodes.Update(providerID, func(f nodeFields) nodeFields {
		if f.Node != nil || f.NodeClaim != nil {
			f.NominatedUntil = until // extends nomination window if already nominated
		}
		return f
	})
}

// UnmarkForDeletion removes the marking on the node as a node the controller intends to delete
func (c *Cluster) UnmarkForDeletion(providerIDs ...string) {
	for _, id := range providerIDs {
		c.nodes.Update(id, func(f nodeFields) nodeFields { f.MarkedForDeletion = false; return f })
	}
}

// MarkForDeletion marks the node as pending deletion in the internal cluster state
func (c *Cluster) MarkForDeletion(providerIDs ...string) {
	for _, id := range providerIDs {
		c.nodes.Update(id, func(f nodeFields) nodeFields {
			if f.Node != nil || f.NodeClaim != nil {
				f.MarkedForDeletion = true
			}
			return f
		})
	}
}

func (c *Cluster) UpdateNodeClaim(nodeClaim *v1.NodeClaim) {
	c.nodes.Observe(nodeClaim)
	c.nodeClaims.Observe(nodeClaim)
}

func (c *Cluster) DeleteNodeClaim(name string) {
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
	c.nodes.Forget(nodeClaim)
	c.nodeClaims.Forget(nodeClaim)
}

// UpdateNode applies the Node, re-reading its CSINode, and re-reads the pods bound to it, which heals pod events
// cluster state missed.
func (c *Cluster) UpdateNode(ctx context.Context, node *corev1.Node) error {
	c.nodes.Observe(node)
	if nodeKey(node) == "" {
		return nil
	}
	var csiNode storagev1.CSINode
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: node.Name}, &csiNode); client.IgnoreNotFound(err) != nil {
		return serrors.Wrap(fmt.Errorf("getting CSINode to determine volume limit, %w", err), "CSINode", klog.KRef("", node.Name))
	}
	c.nodeUsage.Update(node.Name, func(f podFields) podFields { f.CSIDrivers = csiNode.Spec.Drivers; return f })
	var pods corev1.PodList
	if err := c.kubeClient.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node.Name}, client.UnsafeDisableDeepCopy); err != nil {
		return fmt.Errorf("listing pods, %w", err)
	}
	listed := make([]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		listed[i] = &pods.Items[i]
	}
	if err := c.resolvePVCDrivers(ctx, listed...); err != nil {
		return err
	}
	karpenterstate.Resync(c.pods, listed, c.podsOnNodes.Sources(node.Name))
	return nil
}

func (c *Cluster) DeleteNode(name string) {
	c.nodes.Forget(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	c.nodeUsage.Update(name, func(f podFields) podFields { f.CSIDrivers = nil; return f })
}

// UpdatePod applies the pod, resolving the CSI drivers of the PVCs it mounts that cluster state hasn't seen.
func (c *Cluster) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	err := c.resolvePVCDrivers(ctx, pod)
	c.pods.Observe(pod)
	return err
}

// resolvePVCDrivers resolves the drivers of the PVCs the pods mount that pvcDrivers doesn't hold, for a PVC cluster
// state sees through a pod before its own event. UpdatePersistentVolumeClaim keeps them current.
func (c *Cluster) resolvePVCDrivers(ctx context.Context, pods ...*corev1.Pod) error {
	var errs error
	for _, pod := range pods {
		for _, volume := range pod.Spec.Volumes {
			name := volumeutil.PersistentVolumeClaimName(pod, volume)
			if name == "" {
				continue
			}
			if _, ok := c.pvcDrivers.Get(types.NamespacedName{Namespace: pod.Namespace, Name: name}); ok {
				continue
			}
			pvc := &corev1.PersistentVolumeClaim{}
			if err := c.kubeClient.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: name}, pvc); err != nil {
				// A PVC that's gone was deleted and its finalizer removed, so its volume no longer counts.
				if client.IgnoreNotFound(err) != nil {
					errs = multierr.Append(errs, fmt.Errorf("tracking volume usage, %w", err))
				}
				continue
			}
			errs = multierr.Append(errs, c.UpdatePersistentVolumeClaim(ctx, pvc))
		}
	}
	return errs
}

// UpdatePersistentVolumeClaim resolves the PVC's CSI driver from its PersistentVolume or StorageClass.
func (c *Cluster) UpdatePersistentVolumeClaim(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	driver, err := scheduling.ResolveDriver(ctx, c.kubeClient, nil, "", pvc, lo.FromPtr(pvc.Spec.StorageClassName))
	if err != nil {
		return fmt.Errorf("tracking volume usage, %w", err)
	}
	c.pvcDrivers.Update(client.ObjectKeyFromObject(pvc), func(pvcFields) pvcFields { return pvcFields{Resolved: true, Driver: driver} })
	return nil
}

// DeletePersistentVolumeClaim drops the PVC's driver.
func (c *Cluster) DeletePersistentVolumeClaim(key types.NamespacedName) {
	c.pvcDrivers.Update(key, func(pvcFields) pvcFields { return pvcFields{} })
}

func (c *Cluster) NodeClaimExists(nodeClaimName string) bool {
	_, ok := c.nodeClaims.Get(types.NamespacedName{Name: nodeClaimName})
	return ok
}

func (c *Cluster) UnlaunchedNodeClaimExists(nodeClaimName string) bool {
	nodeClaim, ok := c.nodeClaims.Get(types.NamespacedName{Name: nodeClaimName})
	return ok && nodeClaim.Status.ProviderID == ""
}

func (c *Cluster) DeletePod(podKey types.NamespacedName) {
	c.pods.Forget(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: podKey.Namespace, Name: podKey.Name}})
	c.ClearPodSchedulingMappings(podKey)
	c.MarkUnconsolidated()
}

func (c *Cluster) NodePoolResourcesFor(nodePoolName string) corev1.ResourceList {
	resources, _ := c.nodePoolResources.Get(nodePoolName)
	return maps.Clone(resources)
}

// Reset the cluster state for unit testing
func (c *Cluster) Reset() {
	c.unsyncedTimeMu.Lock()
	defer c.unsyncedTimeMu.Unlock()
	c.clusterState = time.Time{}
	c.unsyncedStartTime = time.Time{}
	c.lastUnsyncedLogTime = time.Time{}
	c.hasSynced.Store(false)
	c.clearStores()
	c.podAcks = sync.Map{}
	c.podsSchedulingAttempted = sync.Map{}
	c.podsSchedulableTimes = sync.Map{}
	c.podToNodeClaim = sync.Map{}
	c.bufferPodCountsMu.Lock()
	c.bufferPodCounts = map[string]int{}
	c.bufferPodCountsMu.Unlock()
}

// RepairPolicyMatcher returns the matcher cluster state matches Nodes with, or nil when node repair is disabled.
func (c *Cluster) RepairPolicyMatcher() *health.RepairPolicyMatcher {
	return c.repairPolicyMatcher
}

// UpdateBufferPodCounts replaces the entire buffer pod count mapping with the
// new counts. Called by the provisioner after each scheduling pass to reflect
// which nodes are hosting virtual buffer pods. Nodes not in the map are cleared.
//
// The mapping is derived from Results.ExistingNodes — each ExistingNode.Pods
// entry that carries the fake-pod annotation is counted. When buffer capacity
// is consumed (real pods take the space), virtual pods move to other nodes or
// new NodeClaims, and this map updates accordingly on the next pass.
func (c *Cluster) UpdateBufferPodCounts(counts map[string]int) {
	c.bufferPodCountsMu.Lock()
	defer c.bufferPodCountsMu.Unlock()
	c.bufferPodCounts = counts
}

// HasBufferPods returns true if the node with the given providerID has at least
// one virtual buffer pod placed on it during the last provisioning pass.
func (c *Cluster) HasBufferPods(providerID string) bool {
	return c.BufferPodCount(providerID) > 0
}

// BufferPodCount returns the number of virtual buffer pods on the node.
func (c *Cluster) BufferPodCount(providerID string) int {
	c.bufferPodCountsMu.RLock()
	defer c.bufferPodCountsMu.RUnlock()
	return c.bufferPodCounts[providerID]
}

// AckPods marks the pod as acknowledged for scheduling from the provisioner. This is only done once per-pod.
func (c *Cluster) AckPods(pods ...*corev1.Pod) {
	now := c.clock.Now()
	for _, pod := range pods {
		// store the value as now only if it doesn't exist.
		c.podAcks.LoadOrStore(types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, now)
	}
}

// PodAckTime will return the time the pod was first seen in our cache.
func (c *Cluster) PodAckTime(podKey types.NamespacedName) time.Time {
	if ackTime, ok := c.podAcks.Load(podKey); ok {
		return ackTime.(time.Time)
	}
	return time.Time{}
}

// MarkPodSchedulingDecisions keeps track of when we first tried to schedule a pod to a node.
// It updates podHealthyNodePoolScheduledTime for pods scheduled against nodePool that have
// NodeRegistrationHealthy=true. This also marks when the pod is first seen as schedulable for pod metrics.
// We'll only emit a metric for a pod if we haven't done it before.
// nolint:gocyclo
func (c *Cluster) MarkPodSchedulingDecisions(ctx context.Context, podErrors map[*corev1.Pod]error, npPods map[string][]*corev1.Pod, ncPods map[string][]*corev1.Pod) {
	now := c.clock.Now()
	for pod := range podErrors {
		nn := client.ObjectKeyFromObject(pod)
		// delete podsSchedulableTimes and podHealthyNodePoolScheduledTime for pods that have pod errors
		c.podsSchedulableTimes.Delete(nn)
		_, alreadyExists := c.podsSchedulingAttempted.LoadOrStore(nn, now)
		// If we already attempted this, we don't need to emit another metric.
		if !alreadyExists {
			// We should have ACK'd the pod.
			if ackTime := c.PodAckTime(nn); !ackTime.IsZero() {
				PodSchedulingDecisionSeconds.Observe(c.clock.Since(ackTime).Seconds(), nil)
			}
		}
		c.podHealthyNodePoolScheduledTime.Delete(nn)
		c.podToNodeClaim.Delete(nn)
	}
	for nodePoolName, pods := range npPods {
		nodePool := &v1.NodePool{}
		if nodePoolName != "" {
			// Swallow errors if we can't get the nodepool
			_ = c.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool)
		}
		for _, p := range pods {
			nn := client.ObjectKeyFromObject(p)
			// Skip pods that are already bound to a node (e.g. pods from deleting nodes
			// included in the scheduling simulation for capacity planning). Storing a new
			// timestamp for already-bound pods would cause negative metric values since
			// their PodScheduled LastTransitionTime is in the past.
			if podutils.IsScheduled(p) {
				continue
			}
			c.podsSchedulableTimes.LoadOrStore(nn, now)
			_, alreadyExists := c.podsSchedulingAttempted.LoadOrStore(nn, now)
			// If we already attempted this, we don't need to emit another metric.
			if !alreadyExists {
				// We should have ACK'd the pod.
				if ackTime := c.PodAckTime(nn); !ackTime.IsZero() {
					PodSchedulingDecisionSeconds.Observe(c.clock.Since(ackTime).Seconds(), nil)
				}
			}
			// If the pod is scheduled to a nodePool and if the nodePool has NodeRegistrationHealthy=true
			// then mark the time when we thought it can schedule to now.
			if nodePoolName != "" && nodePool.StatusConditions().IsTrue(v1.ConditionTypeNodeRegistrationHealthy) {
				c.podHealthyNodePoolScheduledTime.LoadOrStore(nn, c.clock.Now())
			} else {
				// If the pod was scheduled to a healthy nodePool earlier but is now getting scheduled to an
				// unhealthy one then we need to delete its entry from the map because it will not schedule successfully
				c.podHealthyNodePoolScheduledTime.Delete(nn)
			}
		}
	}
	c.UpdatePodToNodeClaimMapping(ncPods)
}

func (c *Cluster) UpdatePodToNodeClaimMapping(ncPods map[string][]*corev1.Pod) {
	for ncName, pods := range ncPods {
		for _, p := range pods {
			c.podToNodeClaim.Store(client.ObjectKeyFromObject(p), ncName)
		}
	}
}

// PodSchedulingDecisionTime returns when Karpenter first decided if a pod could schedule a pod in scheduling simulations.
// This returns 0, false if Karpenter never made a decision on the pod.
func (c *Cluster) PodSchedulingDecisionTime(podKey types.NamespacedName) time.Time {
	if val, found := c.podsSchedulingAttempted.Load(podKey); found {
		return val.(time.Time)
	}
	return time.Time{}
}

// PodSchedulingSuccessTime returns when Karpenter first thought it could schedule a pod in its scheduling simulation.
// This returns 0, false if the pod was never considered in scheduling as a pending pod.
func (c *Cluster) PodSchedulingSuccessTime(podKey types.NamespacedName) time.Time {
	if val, found := c.podsSchedulableTimes.Load(podKey); found {
		return val.(time.Time)
	}
	return time.Time{}
}

// PodNodeClaimMapping returns the nodeClaim against which the pod is simulated to get scheduled
func (c *Cluster) PodNodeClaimMapping(podKey types.NamespacedName) string {
	if val, found := c.podToNodeClaim.Load(podKey); found {
		return val.(string)
	}
	return ""
}

// PodsMappedToNodeClaim returns the keys of the pods that were simulated to schedule against the given nodeClaim
// during the most recent scheduling decision for each pod
func (c *Cluster) PodsMappedToNodeClaim(nodeClaimName string) []types.NamespacedName {
	var podKeys []types.NamespacedName
	// TODO: store reverse mapping for perf
	c.podToNodeClaim.Range(func(k, v any) bool {
		if v.(string) == nodeClaimName {
			podKeys = append(podKeys, k.(types.NamespacedName))
		}
		return true
	})
	return podKeys
}

// PodSchedulingSuccessTimeRegistrationHealthyCheck returns when Karpenter first thought it could schedule a pod in its scheduling simulation.
// This returns 0, false if the pod was never considered in scheduling as a pending pod.
func (c *Cluster) PodSchedulingSuccessTimeRegistrationHealthyCheck(podKey types.NamespacedName) time.Time {
	if val, found := c.podHealthyNodePoolScheduledTime.Load(podKey); found {
		return val.(time.Time)
	}
	return time.Time{}
}

func (c *Cluster) ClearPodSchedulingMappings(podKey types.NamespacedName) {
	c.podAcks.Delete(podKey)
	c.podsSchedulableTimes.Delete(podKey)
	c.podsSchedulingAttempted.Delete(podKey)
	c.podHealthyNodePoolScheduledTime.Delete(podKey)
	c.podToNodeClaim.Delete(podKey)
}

// MarkUnconsolidated marks the cluster state as being unconsolidated.  This should be called in any situation where
// something in the cluster has changed such that the cluster may have moved from a non-consolidatable to a consolidatable
// state.
func (c *Cluster) MarkUnconsolidated() time.Time {
	newState := c.clock.Now()
	c.clusterStateMu.Lock()
	c.clusterState = newState
	c.clusterStateMu.Unlock()
	return newState
}

// ConsolidationState returns a timestamp of the last time that the cluster state with respect to consolidation changed.
// If nothing changes, this timestamp resets after five minutes to force watchers that use this to defer work to
// occasionally revalidate that nothing external (e.g. an instance type becoming available) has changed that now makes
// it possible for them to operate. Time was chosen as the type here as it allows comparisons using the built-in
// monotonic clock.
func (c *Cluster) ConsolidationState() time.Time {
	c.clusterStateMu.RLock()
	state := c.clusterState
	c.clusterStateMu.RUnlock()

	// time.Time uses a monotonic clock for these comparisons
	if c.clock.Since(state) < time.Minute*5 {
		return state
	}

	// This ensures that at least once every 5 minutes we consider consolidating our cluster in case something else has
	// changed (e.g. instance type availability) that we can't detect which would allow consolidation to occur.
	return c.MarkUnconsolidated()
}

// sets the cluster to be synced or unsynced for unit testing
func (c *Cluster) SetSynced(state bool) {
	c.unsyncedTimeMu.Lock()
	defer c.unsyncedTimeMu.Unlock()
	c.hasSynced.Store(state)
}

// daemonSetFields are the fields derived about a DaemonSet from its pods.
type daemonSetFields struct {
	// Pod is the DaemonSet's newest pod.
	Pod *corev1.Pod
}

func daemonSetKey(pod *corev1.Pod) types.NamespacedName {
	if owner := metav1.GetControllerOf(pod); owner != nil && owner.Kind == "DaemonSet" {
		return types.NamespacedName{Namespace: pod.Namespace, Name: owner.Name}
	}
	return types.NamespacedName{}
}

// GetDaemonSetPod returns a copy of the DaemonSet's newest pod, or nil if cluster state hasn't seen one.
func (c *Cluster) GetDaemonSetPod(daemonset *appsv1.DaemonSet) *corev1.Pod {
	f, _ := c.daemonSetPods.Get(client.ObjectKeyFromObject(daemonset))
	if f.Pod == nil {
		return nil
	}
	return f.Pod.DeepCopy()
}

// DeleteDaemonSet drops the DaemonSet's pod.
func (c *Cluster) DeleteDaemonSet(key types.NamespacedName) {
	c.daemonSetPods.Update(key, func(daemonSetFields) daemonSetFields { return daemonSetFields{} })
}

// HasSynced returns whether the cluster state has been synchronized at least once.
func (c *Cluster) HasSynced() bool {
	return c.hasSynced.Load()
}
