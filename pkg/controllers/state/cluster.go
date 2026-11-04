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
)

// Cluster maintains cluster state that is often needed but expensive to compute. It keeps each node, the pods bound
// to it and the NodePools' usage as fields in state stores, which follow the Node, NodeClaim and Pod events that the
// cluster state informers apply.
type Cluster struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	clock         clock.Clock
	hasSynced     atomic.Bool

	// nodes is a join: each Node and its NodeClaim, by providerID, read together with the intent kept about them.
	nodes *karpenterstate.Store[string, nodeFields]
	// pods keeps the fields derived about each node from its pods, by node name, as StateNode.Derived. They can lag
	// nodes.
	pods *karpenterstate.Store[string, podFields]
	// nodeClaimNames finds a NodeClaim's providerID by name.
	nodeClaimNames *karpenterstate.Store[string, nodeClaimFields]
	// nodePoolResources keeps the capacity derived about each NodePool from its nodes, by NodePool name.
	nodePoolResources *karpenterstate.Store[string, corev1.ResourceList]
	nodeCount         atomic.Int64

	nodeClaimsByProviderID *karpenterstate.Contributor[string, *v1.NodeClaim, nodeFields, *v1.NodeClaim]
	nodeClaimsByName       *karpenterstate.Contributor[string, *v1.NodeClaim, nodeClaimFields, string]
	nodesByProviderID      *karpenterstate.Contributor[string, *corev1.Node, nodeFields, nodeContribution]
	nodesByName            *karpenterstate.Contributor[string, *corev1.Node, podFields, *corev1.Node]
	podsByNodeName         *karpenterstate.Contributor[string, *corev1.Pod, podFields, podContribution]

	// daemonSetPods keeps the newest pod derived about each DaemonSet from its pods, by DaemonSet.
	daemonSetPods   *karpenterstate.Store[types.NamespacedName, daemonSetFields]
	podsByDaemonSet *karpenterstate.Contributor[types.NamespacedName, *corev1.Pod, daemonSetFields, *corev1.Pod]

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
// one. The Node and its NodeClaim each contribute themselves, which joins them.
type nodeFields struct {
	Node      *corev1.Node
	NodeClaim *v1.NodeClaim
	// CSIDrivers are the drivers of the Node's CSINode, read when the Node last changed.
	CSIDrivers          []storagev1.CSINodeDriver
	RepairPolicyMatches []health.RepairPolicyMatch
	// MarkedForDeletion and NominatedUntil are set by the disruption and provisioning controllers.
	MarkedForDeletion bool
	NominatedUntil    metav1.Time
}

type nodeContribution struct {
	node       *corev1.Node
	csiDrivers []storagev1.CSINodeDriver
	matches    []health.RepairPolicyMatch
}

// podFields are the fields kept for each node from the pods bound to it, keyed by the node's name. They exist as soon
// as a pod binds to the node, whether or not cluster state has seen the Node yet.
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
	// Volumes are the pods' volumes, read from their PVCs when each pod last changed.
	Volumes          *scheduling.VolumeUsage
	AntiAffinityPods map[types.NamespacedName]*corev1.Pod
}

type podContribution struct {
	pod            *corev1.Pod
	requests       corev1.ResourceList
	limits         corev1.ResourceList
	daemonSet      bool
	disruptionCost float64
	hostPorts      []scheduling.HostPort
	volumes        scheduling.Volumes
	antiAffinity   bool
}

// equal reports whether applyPod folds the two contributions the same way.
//
//nolint:gocyclo
func (p podContribution) equal(o podContribution) bool {
	if (p.pod == nil) != (o.pod == nil) || (p.pod != nil && client.ObjectKeyFromObject(p.pod) != client.ObjectKeyFromObject(o.pod)) {
		return false
	}
	return p.daemonSet == o.daemonSet && p.antiAffinity == o.antiAffinity && p.disruptionCost == o.disruptionCost &&
		resourcesEqual(p.requests, o.requests) && resourcesEqual(p.limits, o.limits) &&
		slices.EqualFunc(p.hostPorts, o.hostPorts, func(a, b scheduling.HostPort) bool {
			return a.Port == b.Port && a.Protocol == b.Protocol && a.IP.Equal(b.IP)
		}) &&
		maps.EqualFunc(p.volumes, o.volumes, func(a, b sets.Set[string]) bool { return a.Equal(b) })
}

func resourcesEqual(a, b corev1.ResourceList) bool {
	return maps.EqualFunc(a, b, func(x, y resource.Quantity) bool { return x.Cmp(y) == 0 })
}

type nodeClaimFields struct {
	ProviderID string
}

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

func NewCluster(clk clock.Clock, client client.Client, cloudProvider cloudprovider.CloudProvider, opts ...option.Function[ClusterOptions]) *Cluster {
	o := option.Resolve(opts...)
	c := &Cluster{
		repairPolicyMatcher: o.repairPolicyMatcher,
		clock:               clk,
		kubeClient:          client,
		cloudProvider:       cloudProvider,
		bufferPodCounts:     map[string]int{},
	}
	c.newStores()
	return c
}

//nolint:gocyclo
func (c *Cluster) newStores() {
	c.nodes = karpenterstate.NewStore[string, nodeFields]()
	c.pods = karpenterstate.NewStore[string, podFields]()
	c.nodeClaimNames = karpenterstate.NewStore[string, nodeClaimFields]()
	c.nodePoolResources = karpenterstate.NewStore[string, corev1.ResourceList]()
	c.nodeCount.Store(0)
	c.NodePoolState = NewNodePoolState(c.clock)
	c.daemonSetPods = karpenterstate.NewStore[types.NamespacedName, daemonSetFields]()
	c.podsByDaemonSet = karpenterstate.Contribute(c.daemonSetPods, nil, nil, &corev1.PodList{}, daemonSetKey,
		func(_ context.Context, pod *corev1.Pod) (*corev1.Pod, error) { return pod, nil },
		func(f daemonSetFields, _, next *corev1.Pod) daemonSetFields {
			// Keep the newest pod, as a sample of the DaemonSet's current template. A pod going away leaves the
			// sample in place until a newer one arrives or the DaemonSet goes.
			if next != nil && (f.Pod == nil || next.UID == f.Pod.UID || next.CreationTimestamp.After(f.Pod.CreationTimestamp.Time)) {
				f.Pod = next
			}
			return f
		})

	c.nodeClaimsByProviderID = karpenterstate.Contribute(c.nodes, nil, nil, &v1.NodeClaimList{},
		func(nodeClaim *v1.NodeClaim) string { return nodeClaim.Status.ProviderID },
		func(_ context.Context, nodeClaim *v1.NodeClaim) (*v1.NodeClaim, error) { return nodeClaim, nil },
		func(f nodeFields, _, next *v1.NodeClaim) nodeFields { f.NodeClaim = next; return f })
	c.nodeClaimsByName = karpenterstate.Contribute(c.nodeClaimNames, nil, nil, &v1.NodeClaimList{},
		func(nodeClaim *v1.NodeClaim) string { return nodeClaim.Name },
		func(_ context.Context, nodeClaim *v1.NodeClaim) (string, error) {
			return nodeClaim.Status.ProviderID, nil
		},
		func(f nodeClaimFields, _, next string) nodeClaimFields { f.ProviderID = next; return f })
	c.nodesByProviderID = karpenterstate.Contribute(c.nodes, nil, nil, &corev1.NodeList{}, nodeKey, c.nodeContribution,
		func(f nodeFields, _, next nodeContribution) nodeFields {
			f.Node, f.CSIDrivers, f.RepairPolicyMatches = next.node, next.csiDrivers, next.matches
			return f
		})
	c.nodesByName = karpenterstate.Contribute(c.pods, nil, nil, &corev1.NodeList{},
		func(node *corev1.Node) string { return lo.Ternary(nodeKey(node) == "", "", node.Name) },
		func(_ context.Context, node *corev1.Node) (*corev1.Node, error) { return node, nil },
		func(f podFields, _, next *corev1.Node) podFields { f.Node = next; return f })
	c.podsByNodeName = karpenterstate.Contribute(c.pods, nil, nil, &corev1.PodList{},
		func(pod *corev1.Pod) string { return lo.Ternary(podutils.IsTerminal(pod), "", pod.Spec.NodeName) },
		c.podContribution, applyPod).CompareWith(podContribution.equal)

	karpenterstate.Derive(c.nodes, c.nodePoolResources,
		func(_ string, f nodeFields) string {
			if f.Node == nil && f.NodeClaim == nil {
				return ""
			}
			return (&StateNode{Node: f.Node, NodeClaim: f.NodeClaim}).Labels()[v1.NodePoolLabelKey]
		},
		func(_ string, f nodeFields) corev1.ResourceList {
			n := &StateNode{Node: f.Node, NodeClaim: f.NodeClaim, markedForDeletion: f.MarkedForDeletion}
			if n.MarkedForDeletion() {
				return nil
			}
			return lo.Assign(n.Capacity(), corev1.ResourceList{resources.Node: resource.MustParse("1")})
		},
		func(total corev1.ResourceList, prev, next corev1.ResourceList) corev1.ResourceList {
			sum := resources.Merge(total, next)
			resources.SubtractFrom(sum, prev)
			sum = lo.OmitBy(sum, func(_ corev1.ResourceName, q resource.Quantity) bool { return q.IsZero() })
			return lo.Ternary(len(sum) == 0, nil, sum)
		})
	c.nodes.Subscribe(func(_ string, old, new nodeFields) {
		existed, exists := old.Node != nil || old.NodeClaim != nil, new.Node != nil || new.NodeClaim != nil
		if existed != exists {
			ClusterStateNodesCount.Set(float64(c.nodeCount.Add(int64(lo.Ternary(exists, 1, -1)))), nil)
		}
		before := &StateNode{Node: old.Node, NodeClaim: old.NodeClaim, markedForDeletion: old.MarkedForDeletion}
		after := &StateNode{Node: new.Node, NodeClaim: new.NodeClaim, markedForDeletion: new.MarkedForDeletion}
		if existed != exists || (exists && (before.Initialized() != after.Initialized() || before.MarkedForDeletion() != after.MarkedForDeletion())) {
			c.MarkUnconsolidated()
		}
	})
	c.pods.Subscribe(func(_ string, old, new podFields) {
		if old.Pods != new.Pods {
			c.MarkUnconsolidated()
		}
	})
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

func (c *Cluster) nodeContribution(ctx context.Context, node *corev1.Node) (nodeContribution, error) {
	if node.Spec.ProviderID == "" {
		node = node.DeepCopy()
		node.Spec.ProviderID = node.Name
	}
	contribution := nodeContribution{node: node}
	if c.repairPolicyMatcher != nil {
		contribution.matches = c.repairPolicyMatcher.Match(node)
	}
	var csiNode storagev1.CSINode
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: node.Name}, &csiNode); err != nil {
		return contribution, client.IgnoreNotFound(serrors.Wrap(fmt.Errorf("getting CSINode to determine volume limit, %w", err), "CSINode", klog.KRef("", node.Name)))
	}
	contribution.csiDrivers = csiNode.Spec.Drivers
	return contribution, nil
}

func (c *Cluster) podContribution(ctx context.Context, pod *corev1.Pod) (podContribution, error) {
	volumes, err := scheduling.GetVolumes(ctx, c.kubeClient, pod)
	if err != nil {
		return podContribution{}, fmt.Errorf("tracking volume usage, %w", err)
	}
	// The same as RequestsForPods and LimitsForPods, from one Ceiling.
	ceiling := resources.Ceiling(pod)
	requests, limits := resources.Merge(ceiling.Requests), resources.Merge(ceiling.Limits)
	requests[corev1.ResourcePods] = *resource.NewQuantity(1, resource.DecimalExponent)
	limits[corev1.ResourcePods] = *resource.NewQuantity(1, resource.DecimalExponent)
	contribution := podContribution{
		// The fields need only the pod's key, except for a pod with required anti-affinity, which they hold. The pod may
		// be a List's item that shares the cache's memory, so hold a copy.
		pod:          &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}},
		requests:     requests,
		limits:       limits,
		daemonSet:    podutils.IsOwnedByDaemonSet(pod),
		hostPorts:    scheduling.GetHostPorts(pod),
		volumes:      volumes,
		antiAffinity: podutils.HasRequiredPodAntiAffinity(pod),
	}
	if contribution.antiAffinity {
		contribution.pod = pod.DeepCopy()
	}
	if !contribution.daemonSet {
		contribution.disruptionCost = max(0, disruptionutils.EvictionCost(ctx, pod))
	}
	return contribution, nil
}

//nolint:gocyclo
func applyPod(f podFields, prev, next podContribution) podFields {
	if prev.pod != nil {
		key := client.ObjectKeyFromObject(prev.pod)
		f.Pods--
		f.Requests = resources.Subtract(f.Requests, prev.requests)
		f.Limits = resources.Subtract(f.Limits, prev.limits)
		if prev.daemonSet {
			f.DaemonSetRequests = resources.Subtract(f.DaemonSetRequests, prev.requests)
			f.DaemonSetLimits = resources.Subtract(f.DaemonSetLimits, prev.limits)
		}
		f.DisruptionCost -= prev.disruptionCost
		if len(prev.hostPorts) > 0 {
			f.HostPorts = f.HostPorts.DeepCopy()
			f.HostPorts.DeletePod(key)
		}
		if len(prev.volumes) > 0 {
			f.Volumes = f.Volumes.DeepCopy()
			f.Volumes.DeletePod(key)
		}
		if prev.antiAffinity {
			f.AntiAffinityPods = maps.Clone(f.AntiAffinityPods)
			delete(f.AntiAffinityPods, key)
		}
	}
	if next.pod != nil {
		f.Pods++
		f.Requests = resources.Merge(f.Requests, next.requests)
		f.Limits = resources.Merge(f.Limits, next.limits)
		if next.daemonSet {
			f.DaemonSetRequests = resources.Merge(f.DaemonSetRequests, next.requests)
			f.DaemonSetLimits = resources.Merge(f.DaemonSetLimits, next.limits)
		}
		f.DisruptionCost += next.disruptionCost
		if len(next.hostPorts) > 0 {
			f.HostPorts = lo.TernaryF(f.HostPorts == nil, scheduling.NewHostPortUsage, f.HostPorts.DeepCopy)
			f.HostPorts.Add(next.pod, next.hostPorts)
		}
		if len(next.volumes) > 0 {
			f.Volumes = lo.TernaryF(f.Volumes == nil, scheduling.NewVolumeUsage, f.Volumes.DeepCopy)
			f.Volumes.Add(next.pod, next.volumes)
		}
		if next.antiAffinity {
			f.AntiAffinityPods = lo.Assign(f.AntiAffinityPods, map[types.NamespacedName]*corev1.Pod{client.ObjectKeyFromObject(next.pod): next.pod})
		}
	}
	if f.Pods == 0 {
		return podFields{Node: f.Node}
	}
	return f
}

// stateNode builds the StateNode for a node's fields, joined with the fields of the pods bound to it. A StateNode
// shares the Node and NodeClaim with cluster state, so they must not be modified. copyUsage copies the host port and
// volume usage, which the scheduler adds simulated pods to.
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
		pods, _ = c.pods.Get(f.Node.Name)
	}
	n.Derived.PodRequests, n.Derived.PodLimits = pods.Requests, pods.Limits
	n.Derived.DaemonSetRequests, n.Derived.DaemonSetLimits = pods.DaemonSetRequests, pods.DaemonSetLimits
	n.Derived.DisruptionCost = pods.DisruptionCost
	n.Derived.HostPorts = lo.TernaryF(pods.HostPorts == nil, scheduling.NewHostPortUsage, func() *scheduling.HostPortUsage {
		return lo.Ternary(copyUsage, pods.HostPorts.DeepCopy(), pods.HostPorts)
	})
	if pods.Volumes == nil && len(f.CSIDrivers) == 0 {
		n.Derived.Volumes = scheduling.NewVolumeUsage()
		return n
	}
	n.Derived.Volumes = lo.TernaryF(pods.Volumes == nil, scheduling.NewVolumeUsage, pods.Volumes.DeepCopy)
	for _, driver := range f.CSIDrivers {
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
	for name, f := range c.nodeClaimNames.List() {
		// Check to see if any node claim doesn't have a provider ID. If it doesn't, then the nodeclaim hasn't been
		// launched, and we need to wait to see what the resolved values are before continuing.
		if f.ProviderID == "" {
			return false
		}
		stateNodeClaimNames.Insert(name)
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
	for name, f := range c.pods.List() {
		if f.Node != nil {
			stateNodeNames.Insert(name)
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
	for _, f := range c.pods.List() {
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
	ctx := context.Background()
	_ = c.nodeClaimsByProviderID.Recompute(ctx, nodeClaim)
	_ = c.nodeClaimsByName.Recompute(ctx, nodeClaim)
	_ = c.NodePoolState.nodeClaims.Recompute(ctx, nodeClaim)
}

func (c *Cluster) DeleteNodeClaim(name string) {
	c.nodeClaimsByProviderID.Forget(types.NamespacedName{Name: name})
	c.nodeClaimsByName.Forget(types.NamespacedName{Name: name})
	c.NodePoolState.nodeClaims.Forget(types.NamespacedName{Name: name})
}

// UpdateNode applies the Node, re-reading its CSINode, and re-reads the pods bound to it, which heals pod events
// cluster state missed.
func (c *Cluster) UpdateNode(ctx context.Context, node *corev1.Node) error {
	if err := c.nodesByProviderID.Recompute(ctx, node); err != nil {
		return err
	}
	if err := c.nodesByName.Recompute(ctx, node); err != nil {
		return err
	}
	if nodeKey(node) == "" {
		return nil
	}
	var pods corev1.PodList
	if err := c.kubeClient.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node.Name}, client.UnsafeDisableDeepCopy); err != nil {
		return fmt.Errorf("listing pods, %w", err)
	}
	listed := make([]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		listed[i] = &pods.Items[i]
	}
	return c.podsByNodeName.Resync(ctx, node.Name, listed)
}

func (c *Cluster) DeleteNode(name string) {
	c.nodesByProviderID.Forget(types.NamespacedName{Name: name})
	c.nodesByName.Forget(types.NamespacedName{Name: name})
}

// UpdatePod applies the pod, re-reading its volumes.
func (c *Cluster) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	// A pod's contribution reads its PVCs, which can change while the pod doesn't, so only such a pod recomputes the
	// version cluster state already holds.
	byNodeName := c.podsByNodeName.Observe
	if lo.ContainsBy(pod.Spec.Volumes, func(v corev1.Volume) bool { return v.PersistentVolumeClaim != nil || v.Ephemeral != nil }) {
		byNodeName = c.podsByNodeName.Recompute
	}
	return multierr.Combine(byNodeName(ctx, pod), c.podsByDaemonSet.Observe(ctx, pod))
}

func (c *Cluster) NodeClaimExists(nodeClaimName string) bool {
	_, ok := c.nodeClaimNames.Get(nodeClaimName)
	return ok
}

func (c *Cluster) UnlaunchedNodeClaimExists(nodeClaimName string) bool {
	f, ok := c.nodeClaimNames.Get(nodeClaimName)
	return ok && f.ProviderID == ""
}

func (c *Cluster) DeletePod(podKey types.NamespacedName) {
	c.podsByNodeName.Forget(podKey)
	c.podsByDaemonSet.Forget(podKey)
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
	c.newStores()
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
