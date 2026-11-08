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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

// Cluster is cluster state: a façade over the stores of cluster.NodeState, whose reads it exposes (Nodes,
// NodePoolNodes, MarkForDeletion and the rest). The node state's object stores follow the API server themselves; the
// cluster state informers keep what needs I/O to read, such as each PVC's CSI driver. Cluster also keeps the pod
// scheduling telemetry, whether cluster state waits on launches, and the consolidation state.
type Cluster struct {
	*cluster.NodeState

	kubeClient client.Client
	clock      clock.Clock

	clusterStateMu sync.RWMutex // Separate mutex as this is called in some places that mu is held
	// A monotonically increasing timestamp representing the time state of the
	// cluster with respect to consolidation. This increases when something has
	// changed about the cluster that might make consolidation possible. By recording
	// the state, interested disruption methods can check to see if this has changed to
	// optimize and not try to disrupt if nothing about the cluster has changed.
	clusterState time.Time

	// unsyncedSince and lastUnsyncedLog time how long cluster state has waited on NodeClaims to launch.
	unsyncedMu      sync.Mutex
	unsyncedSince   time.Time
	lastUnsyncedLog time.Time
}

// NewCluster returns cluster state over the node state, which cluster.New builds.
func NewCluster(clk clock.Clock, kubeClient client.Client, nodes *cluster.NodeState) *Cluster {
	c := &Cluster{
		NodeState:  nodes,
		clock:      clk,
		kubeClient: kubeClient,
	}
	c.Subscribe(func() { c.MarkUnconsolidated() })
	return c
}

// NodeClaimsLaunched returns whether every NodeClaim has launched, for a decision that counts the cluster's nodes,
// which waits until none is launching since an unlaunched NodeClaim isn't a node yet. A controller calling it declares
// the node state with state.Requires and wakes on UnlaunchedNodeClaimStore. It records whether cluster state is
// synced: its stores synced, and every NodeClaim launched.
func (c *Cluster) NodeClaimsLaunched(ctx context.Context) bool {
	launched := !c.HasUnlaunchedNodeClaims()
	synced := launched && c.Synced()
	c.unsyncedMu.Lock()
	defer c.unsyncedMu.Unlock()
	ClusterStateSynced.Set(lo.Ternary[float64](synced, 1, 0), nil)
	if synced {
		c.unsyncedSince, c.lastUnsyncedLog = time.Time{}, time.Time{}
		ClusterStateUnsyncedTimeSeconds.Set(0, nil)
		return launched
	}
	if c.unsyncedSince.IsZero() {
		c.unsyncedSince = c.clock.Now()
	}
	// Log every 10s once cluster state hasn't synced for 30s, which is long enough to suggest a problem.
	if c.clock.Since(c.unsyncedSince) > 30*time.Second && c.clock.Since(c.lastUnsyncedLog) > 10*time.Second {
		c.lastUnsyncedLog = c.clock.Now()
		log.FromContext(ctx).Error(fmt.Errorf("waiting on cluster sync"), "cluster is waiting on sync for extended duration",
			"duration", c.clock.Since(c.unsyncedSince).Truncate(time.Second))
	}
	ClusterStateUnsyncedTimeSeconds.Set(c.clock.Since(c.unsyncedSince).Seconds(), nil)
	return launched
}

// UpdateNodeClaim applies a version of the NodeClaim that a caller just wrote, before the NodeClaim store sees it.
func (c *Cluster) UpdateNodeClaim(nodeClaim *v1.NodeClaim) {
	c.NodeClaimStore.Observe(nodeClaim)
}

// DeleteNodeClaim drops the NodeClaim, for a caller that finds an entry the NodeClaim store will never see deleted,
// such as one a seed applied after the NodeClaim's delete.
func (c *Cluster) DeleteNodeClaim(name string) {
	c.NodeClaimStore.Forget(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

// ResolvePVCDrivers resolves the drivers of the PVCs the pods mount that cluster state doesn't hold, for a PVC cluster
// state sees through a pod before its own event. UpdatePersistentVolumeClaim keeps them current.
func (c *Cluster) ResolvePVCDrivers(ctx context.Context, pods ...*corev1.Pod) error {
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

// Reset the cluster state for unit testing
func (c *Cluster) Reset() {
	c.clusterStateMu.Lock()
	c.clusterState = time.Time{}
	c.clusterStateMu.Unlock()
	c.unsyncedMu.Lock()
	c.unsyncedSince, c.lastUnsyncedLog = time.Time{}, time.Time{}
	c.unsyncedMu.Unlock()
	// Clear empties the stores in place, so that their subscribers stay subscribed.
	c.Clear()
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
