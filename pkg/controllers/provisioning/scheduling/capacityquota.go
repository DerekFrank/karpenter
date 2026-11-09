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

package scheduling

import (
	"fmt"
	"slices"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// quotaLimit is what a CapacityQuota has left during a solve. It starts at the quota's limits less what is held for
// NodeClaims that haven't launched, and each existing node the quota selects and each new NodeClaim that may match it
// spends from it, as a NodePool's remaining resources do.
type quotaLimit struct {
	quota     *karpentercluster.Quota
	remaining corev1.ResourceList
}

// newQuotaLimits returns what each enforced quota has left before the solve spends any, and, for each NodeClaim
// template, the quotas each of its instance types may match.
func newQuotaLimits(cluster *state.Cluster, templates []*NodeClaimTemplate) ([]*quotaLimit, []map[string][]int) {
	limits := lo.Map(cluster.CapacityQuotaState.Quotas(), func(q *karpentercluster.Quota, _ int) *quotaLimit {
		return &quotaLimit{quota: q, remaining: resources.Subtract(q.Limits, cluster.CapacityQuotaState.Reserved(q))}
	})
	matches := lo.Map(templates, func(t *NodeClaimTemplate, _ int) map[string][]int {
		byInstanceType := map[string][]int{}
		for _, it := range t.InstanceTypeOptions {
			requirements := karpentercluster.LaunchRequirements(t.Requirements, []*cloudprovider.InstanceType{it})[0]
			for i, l := range limits {
				if l.quota.MayMatch(requirements) {
					byInstanceType[it.Name] = append(byInstanceType[it.Name], i)
				}
			}
		}
		return byInstanceType
	})
	return limits, matches
}

// filterByQuotas drops the instance types of template that would exceed a quota a node of them may be selected by,
// given the pod's requirements, and returns an error naming the quotas if none are left.
func (s *Scheduler) filterByQuotas(template int, instanceTypes []*cloudprovider.InstanceType, podRequirements scheduling.Requirements) ([]*cloudprovider.InstanceType, error) {
	if len(s.quotaLimits) == 0 {
		return instanceTypes, nil
	}
	exceeded := map[string]struct{}{}
	filtered := lo.Filter(instanceTypes, func(it *cloudprovider.InstanceType, _ int) bool {
		for _, i := range s.quotaMatches[template][it.Name] {
			if fitsRemaining(lo.Assign(it.Capacity, corev1.ResourceList{resources.Node: *resources.Quantity("1")}), s.quotaLimits[i].remaining) {
				continue
			}
			// The template may match the quota; the pod may rule it out, such as by requiring another zone.
			requirements := karpentercluster.LaunchRequirements(s.nodeClaimTemplates[template].Requirements, []*cloudprovider.InstanceType{it})[0]
			requirements.Add(podRequirements.Values()...)
			if s.quotaLimits[i].quota.MayMatch(requirements) {
				exceeded[s.quotaLimits[i].quota.Name] = struct{}{}
				return false
			}
		}
		return true
	})
	if len(filtered) == 0 {
		// Name the first quota an instance type exceeds, in name order, so the error is stable across solves.
		names := lo.Keys(exceeded)
		slices.Sort(names)
		return nil, serrors.Wrap(fmt.Errorf("all available instance types exceed limits for capacityquota"),
			"NodePool", klog.KRef("", s.nodeClaimTemplates[template].NodePoolName), "CapacityQuota", klog.KRef("", lo.FirstOr(names, "")))
	}
	return filtered, nil
}

// fitsRemaining returns whether capacity fits what is left of each limited resource.
func fitsRemaining(capacity, remaining corev1.ResourceList) bool {
	for name, left := range remaining {
		if resources.Cmp(capacity[name], left) > 0 {
			return false
		}
	}
	return true
}

// spendQuotas spends, from each quota a new NodeClaim may match, the most it could launch with among the instance types
// that may match the quota, and one resources.Node.
func (s *Scheduler) spendQuotas(nodeClaim *NodeClaim) {
	if len(s.quotaLimits) == 0 {
		return
	}
	launches := karpentercluster.LaunchRequirements(nodeClaim.Requirements, nodeClaim.InstanceTypeOptions)
	for _, l := range s.quotaLimits {
		matching := lo.Filter(nodeClaim.InstanceTypeOptions, func(_ *cloudprovider.InstanceType, j int) bool {
			return l.quota.MayMatch(launches[j])
		})
		if len(matching) > 0 {
			l.remaining = subtractMax(l.remaining, matching)
			l.remaining = resources.Subtract(l.remaining, corev1.ResourceList{resources.Node: *resources.Quantity("1")})
		}
	}
}

// spendQuotasForNode spends an existing node's capacity, and one resources.Node, from each quota that selects it.
func (s *Scheduler) spendQuotasForNode(node *karpentercluster.StateNode) {
	for _, l := range s.quotaLimits {
		if l.quota.Matches(node.Labels()) {
			l.remaining = resources.Subtract(l.remaining, lo.Assign(node.Capacity(), corev1.ResourceList{resources.Node: *resources.Quantity("1")}))
		}
	}
}
