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

package disruption

// The disruption policy every disruption method and its callers share: which nodes and pods may be disrupted, the
// errors that say why not, and the taint and condition that mark a node while a command disrupts it.

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
)

// UninitializedNodeError tracks a special pod error for disruption where pods schedule to a node
// that hasn't been initialized yet, meaning that we can't be confident to make a disruption decision based off of it
type UninitializedNodeError struct {
	*scheduling.ExistingNode
}

func NewUninitializedNodeError(node *scheduling.ExistingNode) *UninitializedNodeError {
	return &UninitializedNodeError{ExistingNode: node}
}

func (u *UninitializedNodeError) Error() string {
	var info []string
	if u.NodeClaim != nil {
		info = append(info, fmt.Sprintf("nodeclaim/%s", u.NodeClaim.Name))
	}
	if u.Node != nil {
		info = append(info, fmt.Sprintf("node/%s", u.Node.Name))
	}
	return fmt.Sprintf("would schedule against uninitialized %s", strings.Join(info, ", "))
}

// RequireNoScheduleTaint will add/remove the karpenter.sh/disruption:NoSchedule taint from the candidates.
// This is used to enforce no taints at the beginning of disruption, and
// to add/remove taints while executing a disruption action.
func RequireNoScheduleTaint(ctx context.Context, kubeClient client.Client, addTaint bool, nodes ...*karpentercluster.StateNode) error {
	errs := make([]error, len(nodes))
	workqueue.ParallelizeUntil(ctx, len(nodes), len(nodes), func(i int) {
		// If the StateNode is Karpenter owned and only has a nodeclaim, or is not owned by
		// Karpenter, thus having no nodeclaim, don't touch the node.
		if nodes[i].Node == nil || nodes[i].NodeClaim == nil {
			return
		}
		node := &corev1.Node{}
		if err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
			if e := kubeClient.Get(ctx, client.ObjectKey{Name: nodes[i].Node.Name}, node); e != nil {
				return e
			}
			// If the node already has the taint, continue to the next
			_, hasTaint := lo.Find(node.Spec.Taints, func(taint corev1.Taint) bool {
				return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
			})
			// Node is being deleted, so no need to remove taint as the node will be gone soon.
			// This ensures that the disruption controller doesn't modify taints that the Termination
			// controller is also modifying
			if hasTaint && !node.DeletionTimestamp.IsZero() {
				return nil
			}
			stored := node.DeepCopy()
			// If the taint is present and we want to remove the taint, remove it.
			if !addTaint {
				node.Spec.Taints = lo.Reject(node.Spec.Taints, func(taint corev1.Taint, _ int) bool {
					return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
				})
				// otherwise, add it.
			} else if addTaint && !hasTaint {
				// If the taint key is present (but with a different value or effect), remove it.
				node.Spec.Taints = lo.Reject(node.Spec.Taints, func(taint corev1.Taint, _ int) bool {
					return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
				})
				node.Spec.Taints = append(node.Spec.Taints, v1.DisruptedNoScheduleTaint)
			}
			if !equality.Semantic.DeepEqual(stored, node) {
				// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch
				// can cause races due to the fact that it fully replaces the list on a change
				// Here, we are updating the taint list
				return kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
			}
			return nil
		}); err != nil {
			errs[i] = client.IgnoreNotFound(fmt.Errorf("getting node, %w", err))
			return
		}
	})
	return multierr.Combine(errs...)
}

// ClearNodeClaimsCondition will remove the conditionType from the NodeClaim status of the provided statenodes
func ClearNodeClaimsCondition(ctx context.Context, kubeClient client.Client, clk clock.Clock, conditionType string, nodes ...*karpentercluster.StateNode) error {
	errs := make([]error, len(nodes))
	workqueue.ParallelizeUntil(ctx, len(nodes), len(nodes), func(i int) {
		if !nodes[i].Initialized() || nodes[i].NodeClaim == nil {
			return
		}
		nodeClaim := &v1.NodeClaim{}
		if err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
			if e := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodes[i].NodeClaim), nodeClaim); e != nil {
				return e
			}
			stored := nodeClaim.DeepCopy()
			_ = nodeClaim.StatusConditions(status.WithClock(clk)).Clear(conditionType)
			if !equality.Semantic.DeepEqual(stored, nodeClaim) {
				return kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
			}
			return nil
		}); err != nil {
			errs[i] = client.IgnoreNotFound(err)
			return
		}

	})
	return multierr.Combine(errs...)
}

type PodBlockEvictionError struct {
	error
}

func NewPodBlockEvictionError(err error) *PodBlockEvictionError {
	return &PodBlockEvictionError{error: err}
}

func IsPodBlockEvictionError(err error) bool {
	if err == nil {
		return false
	}
	var podBlockEvictionError *PodBlockEvictionError
	return stderrors.As(err, &podBlockEvictionError)
}

func IgnorePodBlockEvictionError(err error) error {
	if IsPodBlockEvictionError(err) {
		return nil
	}
	return err
}

// NodeDoNotDisruptError is returned by ValidateNodeDisruptable when a node carries the do-not-disrupt annotation.
// It is a distinct type so the repair path can selectively ignore it: repair is not discretionary disruption, so
// do-not-disrupt must not block it (that would be a breaking behavior change and conflate two intents). Repair
// instead honors the separate do-not-repair annotation.
type NodeDoNotDisruptError struct {
	error
}

func NewNodeDoNotDisruptError(err error) *NodeDoNotDisruptError {
	return &NodeDoNotDisruptError{error: err}
}

func IsNodeDoNotDisruptError(err error) bool {
	if err == nil {
		return false
	}
	var nodeDoNotDisruptError *NodeDoNotDisruptError
	return stderrors.As(err, &nodeDoNotDisruptError)
}

func IgnoreNodeDoNotDisruptError(err error) error {
	if IsNodeDoNotDisruptError(err) {
		return nil
	}
	return err
}

// NodeUninitializedError is returned when a registered node hasn't initialized. Repair ignores it, since a node that
// never initializes is otherwise only replaced by a human.
type NodeUninitializedError struct {
	error
}

func NewNodeUninitializedError(err error) *NodeUninitializedError {
	return &NodeUninitializedError{error: err}
}

func IsNodeUninitializedError(err error) bool {
	if err == nil {
		return false
	}
	var nodeUninitializedError *NodeUninitializedError
	return stderrors.As(err, &nodeUninitializedError)
}

func IgnoreNodeUninitializedError(err error) error {
	if IsNodeUninitializedError(err) {
		return nil
	}
	return err
}

// ValidateNodeDisruptable returns an error if the StateNode cannot be disrupted
// This checks all associated StateNode internals, node labels, and do-not-disrupt annotations on the node.
// ValidateNodeDisruptable takes in a recorder to emit events on the nodeclaims when the state node is not a candidate
func ValidateNodeDisruptable(in *karpentercluster.StateNode, clk clock.Clock) error {
	if in.NodeClaim == nil {
		return fmt.Errorf("node isn't managed by karpenter")
	}
	if in.Node == nil {
		return fmt.Errorf("nodeclaim does not have an associated node")
	}
	// A rebooting node must not be picked up by other disruption methods. This covers the drain window
	// too, where the node is still Initialized but a reboot is already committed.
	if in.RebootInProgress() {
		return fmt.Errorf("node is rebooting")
	}
	if in.MarkedForDeletion() {
		return fmt.Errorf("node is deleting or marked for deletion")
	}
	// Checked ahead of nomination, which provisioning keeps renewing on an uninitialized node while pods wait for it.
	if !in.Initialized() {
		// Liveness's registration timeout owns a node until it registers, so only a registered node is ignorable.
		if !in.Registered() {
			return fmt.Errorf("node isn't initialized nor registered")
		}
		return NewNodeUninitializedError(fmt.Errorf("node isn't initialized"))
	}
	// skip the node if it is nominated by a recent provisioning pass to be the target of a pending pod.
	if in.Nominated(clk) {
		return fmt.Errorf("node is nominated for a pending pod")
	}
	if in.Annotations()[v1.DoNotDisruptAnnotationKey] == "true" {
		return NewNodeDoNotDisruptError(fmt.Errorf("disruption is blocked through the %q annotation", v1.DoNotDisruptAnnotationKey))
	}
	// check whether the node has the NodePool label
	if _, ok := in.Labels()[v1.NodePoolLabelKey]; !ok {
		return serrors.Wrap(fmt.Errorf("node doesn't have required label"), "label", v1.NodePoolLabelKey)
	}
	return nil
}

// ValidatePodsDisruptable returns the pods on the StateNode, and an error if one of them cannot be disrupted. contains a pod that cannot be disrupted
// This checks associated PDBs and do-not-disrupt annotations for each pod on the node.
// ValidatePodsDisruptable takes in a recorder to emit events on the nodeclaims when the state node is not a candidate
func ValidatePodsDisruptable(ctx context.Context, kubeClient client.Client, in *karpentercluster.StateNode, pdbs pdb.Limits, clk clock.Clock, recorder events.Recorder) ([]*corev1.Pod, error) {
	pods, err := in.Pods(ctx, kubeClient)
	if err != nil {
		return nil, err
	}
	for _, po := range pods {
		// We only consider pods that are actively running for "karpenter.sh/do-not-disrupt"
		// This means that we will allow Mirror Pods and DaemonSets to block disruption using this annotation
		if !pod.IsDisruptable(po, clk, recorder) {
			return pods, NewPodBlockEvictionError(serrors.Wrap(fmt.Errorf(`pod has "karpenter.sh/do-not-disrupt" annotation`), "Pod", klog.KObj(po)))
		}
	}
	if pdbKeys, ok := pdbs.CanEvictPods(pods, clk, recorder); !ok {
		if len(pdbKeys) > 1 {
			return pods, NewPodBlockEvictionError(serrors.Wrap(fmt.Errorf("eviction does not support multiple PDBs"), "PodDisruptionBudget(s)", pdbKeys))
		}
		return pods, NewPodBlockEvictionError(serrors.Wrap(fmt.Errorf("pdb prevents pod evictions"), "PodDisruptionBudget", pdbKeys))
	}

	return pods, nil
}
