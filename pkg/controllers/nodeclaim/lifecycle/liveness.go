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

package lifecycle

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/serrors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/metrics"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

type Liveness struct {
	clock         clock.Clock
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	// registrations records the NodeClaim's launch outcome on its NodePool.
	registrations *karpentercluster.Registrations
}

// registrationTimeout is a heuristic time that we expect the node to register within
// If we don't see the node within this time, then we should delete the NodeClaim and try again

const (
	registrationTimeout = time.Minute * 15
	// Sourced from pkg/metrics so the documented reason values stay in one place.
	registrationTimeoutReason = metrics.RegistrationTimeoutReason
	launchTimeoutReason       = metrics.LaunchTimeoutReason
)

// LaunchTimeout is a heuristic time that we expect to be able to launch within
// If we don't launch within this time, then we should delete the NodeClaim and try again
var LaunchTimeout = time.Minute * 5

//nolint:gocyclo
func (l *Liveness) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	registered := nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered)
	if registered.IsTrue() {
		return reconcile.Result{}, nil
	}
	launched := nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched)
	if launched == nil {
		return reconcile.Result{Requeue: true}, nil
	}
	if !launched.IsTrue() {
		if timeUntilTimeout := LaunchTimeout - l.clock.Since(launched.LastTransitionTime.Time); timeUntilTimeout > 0 {
			// This should never occur because if we failed to launch we requeue the object with error instead of this requeueAfter
			return reconcile.Result{RequeueAfter: timeUntilTimeout}, nil //storelint:deadline launch timeout
		}
		if err := l.recordFailure(ctx, nodeClaim); client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
		if err := l.deleteNodeClaimForTimeout(ctx, LaunchTimeout, launchTimeoutReason, nodeClaim); err != nil {
			if client.IgnoreNotFound(err) != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, nil
		}
	}
	if registered == nil {
		return reconcile.Result{Requeue: true}, nil
	}
	// If the Registered statusCondition hasn't gone True during the timeout since we first updated it, we should terminate the NodeClaim
	// NOTE: Timeout has to be stored and checked in the same place since l.clock can advance after the check causing a race
	if timeUntilTimeout := registrationTimeout - l.clock.Since(registered.LastTransitionTime.Time); timeUntilTimeout > 0 {
		return reconcile.Result{RequeueAfter: timeUntilTimeout}, nil //storelint:deadline registration timeout
	}
	if err := l.recordFailure(ctx, nodeClaim); client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}
	// Delete the NodeClaim if we believe the NodeClaim won't register since we haven't seen the node
	if err := l.deleteNodeClaimForTimeout(ctx, registrationTimeout, registrationTimeoutReason, nodeClaim); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

// recordFailure records that the NodeClaim failed to launch or register, with the launch failure's reason and message
// when it didn't launch.
func (l *Liveness) recordFailure(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	outcome := karpentercluster.RegistrationOutcome{Reason: "RegistrationFailed", Message: "Failed to register node"}
	if launched := nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched); !launched.IsTrue() {
		outcome.Reason, outcome.Message = launched.Reason, launched.Message
	}
	return recordLaunchOutcome(ctx, l.kubeClient, l.cloudProvider, l.registrations, nodeClaim, outcome)
}

func (l *Liveness) deleteNodeClaimForTimeout(ctx context.Context, timeout time.Duration, reason string, nodeClaim *v1.NodeClaim) error {
	if err := l.kubeClient.Delete(ctx, nodeClaim); err != nil {
		return serrors.Wrap(fmt.Errorf("deleting nodeclaim for timeout, %w", err), "NodeClaim", klog.KObj(nodeClaim))
	}
	log.FromContext(ctx).V(1).WithValues("timeout", timeout, "reason", reason).Info("terminating due to timeout")
	metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
		metrics.ReasonLabel:              reason,
		metrics.NodePoolLabel:            nodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel:        nodeClaim.Labels[v1.CapacityTypeLabelKey],
		metrics.ConsolidationPolicyLabel: "",
		metrics.TerminationModeLabel:     nodeclaimutils.DisruptionTerminationMode(nodeClaim),
	})
	return nil
}
