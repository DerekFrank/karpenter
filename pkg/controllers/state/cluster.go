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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

// Cluster is cluster state: a façade over the stores of cluster.NodeState, whose reads it exposes (Nodes,
// NodePoolNodes, MarkForDeletion and the rest), that the cluster state informers feed through its Update and Delete
// methods. It also keeps whether cluster state has synced, what the provisioner decided about each pod for the pod
// metrics, each DaemonSet's newest pod and the consolidation state.
type Cluster struct {
	*cluster.NodeState

	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	clock         clock.Clock
	hasSynced     atomic.Bool

	// daemonSetPods keeps the newest pod derived about each DaemonSet from its pods, by DaemonSet.
	daemonSetPods *karpenterstate.Store[types.NamespacedName, daemonSetFields]

	NodePoolState *NodePoolState

	// podScheduling keeps what the provisioner decided about each pod, by namespaced name, for the pod metrics.
	podScheduling *karpenterstate.Store[types.NamespacedName, podSchedulingFields]
	// nodeClaimPods keeps the pods derived about each NodeClaim from podScheduling, by NodeClaim name: the pods last
	// simulated to schedule against it.
	nodeClaimPods *karpenterstate.Store[string, map[types.NamespacedName]struct{}]

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
}

// podSchedulingFields are the fields Karpenter decides about a pod as it schedules it, keyed by its namespaced name.
// Each time is set once, the first time; they go with the pod when it's deleted or has started.
type podSchedulingFields struct {
	// AckedAt is when the provisioner first saw the pod as pending.
	AckedAt time.Time
	// DecidedAt is when the provisioner first decided whether the pod could schedule.
	DecidedAt time.Time
	// SchedulableAt is when a scheduling simulation first fit the pod to a node, until one fails it.
	SchedulableAt time.Time
	// NodeClaim is the NodeClaim the most recent scheduling simulation placed the pod on.
	NodeClaim string
}

// podOnNodeClaim is a pod's contribution to the NodeClaim it was simulated to schedule against: the pod's key.
type podOnNodeClaim struct{ pod types.NamespacedName }

func (p podOnNodeClaim) Update(pods map[types.NamespacedName]struct{}, prev podOnNodeClaim) map[types.NamespacedName]struct{} {
	pods = maps.Clone(pods)
	if pods == nil {
		pods = map[types.NamespacedName]struct{}{}
	}
	if prev != (podOnNodeClaim{}) {
		delete(pods, prev.pod)
	}
	pods[p.pod] = struct{}{}
	return pods
}

func (p podOnNodeClaim) Delete(pods map[types.NamespacedName]struct{}) map[types.NamespacedName]struct{} {
	pods = maps.Clone(pods)
	delete(pods, p.pod)
	return lo.Ternary(len(pods) == 0, nil, pods)
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
	repairPolicyMatcher *repair.RepairPolicyMatcher
}

// WithRepairPolicyMatcher makes cluster state match each Node against the repair policies as it changes. Node repair
// reads the same matcher back through RepairPolicyMatcher.
func WithRepairPolicyMatcher(matcher *repair.RepairPolicyMatcher) option.Function[ClusterOptions] {
	return func(o *ClusterOptions) {
		o.repairPolicyMatcher = matcher
	}
}

// NewCluster returns cluster state. ctx carries the options its derived fields read, such as the feature gates that
// decide a pod's disruption cost.
func NewCluster(ctx context.Context, clk clock.Clock, client client.Client, cloudProvider cloudprovider.CloudProvider, opts ...option.Function[ClusterOptions]) *Cluster {
	o := option.Resolve(opts...)
	c := &Cluster{
		NodeState:     cluster.NewNodeState(ctx, clk, o.repairPolicyMatcher),
		clock:         clk,
		kubeClient:    client,
		cloudProvider: cloudProvider,
		daemonSetPods: karpenterstate.NewStore[types.NamespacedName, daemonSetFields](),
		podScheduling: karpenterstate.NewStore[types.NamespacedName, podSchedulingFields](),
		nodeClaimPods: karpenterstate.NewStore[string, map[types.NamespacedName]struct{}](),
	}
	c.NodePoolState = NewNodePoolState(c.clock, c.NodeClaimStore)
	karpenterstate.Derive(c.podScheduling, c.nodeClaimPods,
		func(_ types.NamespacedName, f podSchedulingFields) string { return f.NodeClaim },
		func(pod types.NamespacedName, _ podSchedulingFields) podOnNodeClaim { return podOnNodeClaim{pod} })
	karpenterstate.Derive(c.PodStore, c.daemonSetPods,
		func(_ types.NamespacedName, pod *corev1.Pod) types.NamespacedName { return daemonSetKey(pod) },
		func(_ types.NamespacedName, pod *corev1.Pod) newestPod { return newestPod{pod} })
	c.Subscribe(func() { c.MarkUnconsolidated() })
	return c
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
	for name, f := range c.NodeClaimStore.List() {
		if f.NodeClaim == nil {
			continue
		}
		// Check to see if any node claim doesn't have a provider ID. If it doesn't, then the nodeclaim hasn't been
		// launched, and we need to wait to see what the resolved values are before continuing.
		if f.NodeClaim.Status.ProviderID == "" {
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
	for name := range c.TrackedNodeNames() {
		stateNodeNames.Insert(name)
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

func (c *Cluster) UpdateNodeClaim(nodeClaim *v1.NodeClaim) {
	c.NodeClaimStore.Observe(nodeClaim)
}

func (c *Cluster) DeleteNodeClaim(name string) {
	c.NodeClaimStore.Forget(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

// UpdateNode applies the Node, re-reading its CSINode, and re-reads the pods bound to it, which heals pod events
// cluster state missed.
func (c *Cluster) UpdateNode(ctx context.Context, node *corev1.Node) error {
	c.NodeStore.Observe(node)
	if !cluster.Tracked(node) {
		return nil
	}
	var csiNode storagev1.CSINode
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: node.Name}, &csiNode); client.IgnoreNotFound(err) != nil {
		return serrors.Wrap(fmt.Errorf("getting CSINode to determine volume limit, %w", err), "CSINode", klog.KRef("", node.Name))
	}
	c.SetCSIDrivers(node.Name, csiNode.Spec.Drivers)
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
	karpenterstate.Resync(c.PodStore, listed, c.PodsOnNodes.Sources(node.Name))
	return nil
}

func (c *Cluster) DeleteNode(name string) {
	c.NodeStore.Forget(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	c.SetCSIDrivers(name, nil)
}

// UpdatePod applies the pod, resolving the CSI drivers of the PVCs it mounts that cluster state hasn't seen.
func (c *Cluster) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	err := c.resolvePVCDrivers(ctx, pod)
	c.PodStore.Observe(pod)
	return err
}

// resolvePVCDrivers resolves the drivers of the PVCs the pods mount that cluster state doesn't hold, for a PVC cluster
// state sees through a pod before its own event. UpdatePersistentVolumeClaim keeps them current.
func (c *Cluster) resolvePVCDrivers(ctx context.Context, pods ...*corev1.Pod) error {
	var errs error
	for _, pod := range pods {
		for _, volume := range pod.Spec.Volumes {
			name := volumeutil.PersistentVolumeClaimName(pod, volume)
			if name == "" {
				continue
			}
			if c.PVCDriverResolved(types.NamespacedName{Namespace: pod.Namespace, Name: name}) {
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
	c.SetPVCDriver(client.ObjectKeyFromObject(pvc), driver)
	return nil
}

// DeletePersistentVolumeClaim drops the PVC's driver.
func (c *Cluster) DeletePersistentVolumeClaim(key types.NamespacedName) {
	c.ForgetPVC(key)
}

func (c *Cluster) DeletePod(podKey types.NamespacedName) {
	c.PodStore.Forget(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: podKey.Namespace, Name: podKey.Name}})
	c.ClearPodSchedulingMappings(podKey)
	c.MarkUnconsolidated()
}

// Reset the cluster state for unit testing
func (c *Cluster) Reset() {
	c.unsyncedTimeMu.Lock()
	defer c.unsyncedTimeMu.Unlock()
	c.clusterState = time.Time{}
	c.unsyncedStartTime = time.Time{}
	c.lastUnsyncedLogTime = time.Time{}
	c.hasSynced.Store(false)
	// Clear empties the stores in place, so that their subscribers stay subscribed.
	c.Clear()
	for _, s := range []interface{ Clear() }{c.daemonSetPods, c.podScheduling, c.nodeClaimPods} {
		s.Clear()
	}
	c.NodePoolState.Reset()
}

// AckPods marks the pod as acknowledged for scheduling from the provisioner. This is only done once per-pod.
func (c *Cluster) AckPods(pods ...*corev1.Pod) {
	now := c.clock.Now()
	for _, pod := range pods {
		c.podScheduling.Update(client.ObjectKeyFromObject(pod), func(f podSchedulingFields) podSchedulingFields {
			f.AckedAt = lo.Ternary(f.AckedAt.IsZero(), now, f.AckedAt)
			return f
		})
	}
}

// PodAckTime will return the time the pod was first seen in our cache.
func (c *Cluster) PodAckTime(podKey types.NamespacedName) time.Time {
	f, _ := c.podScheduling.Get(podKey)
	return f.AckedAt
}

// MarkPodSchedulingDecisions keeps track of when we first tried to schedule a pod to a node, and when the pod is first
// seen as schedulable for pod metrics, and maps each pod to the NodeClaim it was simulated against. We'll only emit a
// metric for a pod if we haven't done it before.
func (c *Cluster) MarkPodSchedulingDecisions(_ context.Context, podErrors map[*corev1.Pod]error, npPods map[string][]*corev1.Pod, ncPods map[string][]*corev1.Pod) {
	now := c.clock.Now()
	for pod := range podErrors {
		c.markPodSchedulingDecision(client.ObjectKeyFromObject(pod), now, func(f podSchedulingFields) podSchedulingFields {
			f.SchedulableAt, f.NodeClaim = time.Time{}, ""
			return f
		})
	}
	for _, pods := range npPods {
		for _, p := range pods {
			// Skip pods that are already bound to a node (e.g. pods from deleting nodes
			// included in the scheduling simulation for capacity planning). Storing a new
			// timestamp for already-bound pods would cause negative metric values since
			// their PodScheduled LastTransitionTime is in the past.
			if podutils.IsScheduled(p) {
				continue
			}
			c.markPodSchedulingDecision(client.ObjectKeyFromObject(p), now, func(f podSchedulingFields) podSchedulingFields {
				f.SchedulableAt = lo.Ternary(f.SchedulableAt.IsZero(), now, f.SchedulableAt)
				return f
			})
		}
	}
	c.UpdatePodToNodeClaimMapping(ncPods)
}

// markPodSchedulingDecision applies the decision to the pod's fields, records now as when the pod was first decided
// on, and observes the decision latency from the pod's ack the first time.
func (c *Cluster) markPodSchedulingDecision(podKey types.NamespacedName, now time.Time, decide func(podSchedulingFields) podSchedulingFields) {
	var first bool
	var ackedAt time.Time
	c.podScheduling.Update(podKey, func(f podSchedulingFields) podSchedulingFields {
		f = decide(f)
		if first = f.DecidedAt.IsZero(); first {
			f.DecidedAt = now
		}
		ackedAt = f.AckedAt
		return f
	})
	// We should have ACK'd the pod.
	if first && !ackedAt.IsZero() {
		PodSchedulingDecisionSeconds.Observe(c.clock.Since(ackedAt).Seconds(), nil)
	}
}

func (c *Cluster) UpdatePodToNodeClaimMapping(ncPods map[string][]*corev1.Pod) {
	for ncName, pods := range ncPods {
		for _, p := range pods {
			c.podScheduling.Update(client.ObjectKeyFromObject(p), func(f podSchedulingFields) podSchedulingFields {
				f.NodeClaim = ncName
				return f
			})
		}
	}
}

// PodSchedulingDecisionTime returns when Karpenter first decided if a pod could schedule a pod in scheduling simulations.
// This returns 0, false if Karpenter never made a decision on the pod.
func (c *Cluster) PodSchedulingDecisionTime(podKey types.NamespacedName) time.Time {
	f, _ := c.podScheduling.Get(podKey)
	return f.DecidedAt
}

// PodSchedulingSuccessTime returns when Karpenter first thought it could schedule a pod in its scheduling simulation.
// This returns 0, false if the pod was never considered in scheduling as a pending pod.
func (c *Cluster) PodSchedulingSuccessTime(podKey types.NamespacedName) time.Time {
	f, _ := c.podScheduling.Get(podKey)
	return f.SchedulableAt
}

// PodNodeClaimMapping returns the nodeClaim against which the pod is simulated to get scheduled
func (c *Cluster) PodNodeClaimMapping(podKey types.NamespacedName) string {
	f, _ := c.podScheduling.Get(podKey)
	return f.NodeClaim
}

// PodsMappedToNodeClaim returns the keys of the pods that were simulated to schedule against the given nodeClaim
// during the most recent scheduling decision for each pod
func (c *Cluster) PodsMappedToNodeClaim(nodeClaimName string) []types.NamespacedName {
	pods, _ := c.nodeClaimPods.Get(nodeClaimName)
	return slices.Collect(maps.Keys(pods))
}

// ClearPodSchedulingMappings drops what Karpenter decided about the pod as it scheduled it.
func (c *Cluster) ClearPodSchedulingMappings(podKey types.NamespacedName) {
	c.podScheduling.Update(podKey, func(podSchedulingFields) podSchedulingFields { return podSchedulingFields{} })
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
