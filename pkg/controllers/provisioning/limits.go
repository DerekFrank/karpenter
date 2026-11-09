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

package provisioning

import (
	"context"
	"fmt"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	scheduler "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// ReserveLimits holds each NodeClaim's place against its NodePool's resource limits and, with the CapacityQuota
// feature gate, against every CapacityQuota it may match, and adds the reservations to the NodeClaim, so that creating
// it or dropping it ends them. replacing names NodeClaims that count now and stop counting before these launch, such as
// the candidates a disruption command replaces, so what they count is credited. If any NodeClaim doesn't fit, it
// releases what it reserved for all of them and returns an error.
func (p *Provisioner) ReserveLimits(ctx context.Context, nodeClaims []*scheduler.NodeClaim, replacing []string) error {
	var reserved scheduler.Reservations
	var marked []*scheduler.NodeClaim
	for _, n := range nodeClaims {
		if n.LimitsReserved {
			continue
		}
		nodePool := &v1.NodePool{}
		err := p.kubeClient.Get(ctx, types.NamespacedName{Name: n.NodePoolName}, nodePool)
		var reservations scheduler.Reservations
		if err == nil {
			reservations, err = p.reserveLimits(ctx, nodePool, n, replacing)
		}
		if err != nil {
			// Release only what this call reserved; a NodeClaim's other reservations stay with its caller, who releases
			// them when it drops the NodeClaim.
			reserved.Release()
			for _, m := range marked {
				m.LimitsReserved = false
			}
			return err
		}
		n.LimitsReserved = true
		marked = append(marked, n)
		for _, r := range reservations {
			n.AddReservation(r)
		}
		reserved = append(reserved, reservations...)
	}
	return nil
}

// reserveLimits reserves n's place against nodePool's resource limits, then against the CapacityQuotas it may match,
// holding the most n could launch with, and returns the reservations. The NodePool and each quota admit under their
// own store's lock, one at a time in that order, so a NodeClaim that fits its NodePool but not a quota releases what it
// reserved. Meanwhile another caller can be refused by a reservation that is about to be released; that caller retries
// on its next round, so the limits are never exceeded, though a NodeClaim near several limits can wait a round.
func (p *Provisioner) reserveLimits(ctx context.Context, nodePool *v1.NodePool, n *scheduler.NodeClaim, replacing []string) (scheduler.Reservations, error) {
	value := MaxCapacity(n.InstanceTypeOptions)
	var reservations scheduler.Reservations
	// A static NodePool only limits its node count, which its NodeClaims' node count reservations hold.
	if nodePool.Spec.Replicas == nil && len(nodePool.Spec.Limits) > 0 {
		reservation := p.cluster.NodePoolState.ReserveResources(nodePool.Name, corev1.ResourceList(nodePool.Spec.Limits), value, replacing)
		if reservation == nil {
			return nil, serrors.Wrap(fmt.Errorf("all available instance types exceed limits for nodepool"), "NodePool", klog.KObj(nodePool))
		}
		reservations = append(reservations, reservation)
	}
	if !options.FromContext(ctx).FeatureGates.CapacityQuota {
		return reservations, nil
	}
	if !p.cluster.CapacityQuotaState.Synced() {
		reservations.Release()
		return nil, fmt.Errorf("waiting for capacityquotas to sync")
	}
	quotaReservations, refused := p.cluster.CapacityQuotaState.Reserve(karpentercluster.LaunchRequirements(n.Requirements, n.InstanceTypeOptions), value, replacing)
	if refused != "" {
		reservations.Release()
		return nil, serrors.Wrap(fmt.Errorf("all available instance types exceed limits for capacityquota"), "NodePool", klog.KObj(nodePool), "CapacityQuota", klog.KRef("", refused))
	}
	for _, r := range quotaReservations {
		reservations = append(reservations, r)
	}
	return reservations, nil
}

// MaxCapacity is the most a NodeClaim with these instance type options could count against a limit once it launches:
// the largest capacity of each resource among them, and one resources.Node.
func MaxCapacity(instanceTypes []*cloudprovider.InstanceType) corev1.ResourceList {
	return lo.Assign(resources.MaxResources(lo.Map(instanceTypes, func(it *cloudprovider.InstanceType, _ int) corev1.ResourceList { return it.Capacity })...),
		corev1.ResourceList{resources.Node: resource.MustParse("1")})
}
