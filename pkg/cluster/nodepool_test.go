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

package cluster

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/karpenter/pkg/state/statetest"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

func TestNodeClaimUsageKeepsTheContributionLaws(t *testing.T) {
	usage := func(cpu, memory string) nodeClaimUsage {
		u := nodeClaimUsage{resources.Node: resource.MustParse("1"), corev1.ResourceCPU: resource.MustParse(cpu)}
		if memory != "" {
			u[corev1.ResourceMemory] = resource.MustParse(memory)
		}
		return u
	}
	for _, base := range []NodePoolDerived{{}, {Usage: corev1.ResourceList(usage("3", "1Gi"))}} {
		statetest.ExpectContribution(t, base, usage("2", ""), usage("4", "2Gi"))
		statetest.ExpectContribution(t, base, usage("4", "2Gi"), usage("2", ""))
	}
}

func TestNodeClaimUsageDropsResourcesItZeroes(t *testing.T) {
	u := nodeClaimUsage{resources.Node: resource.MustParse("1"), corev1.ResourceCPU: resource.MustParse("2")}
	if f := u.Delete(u.Update(NodePoolDerived{}, nil)); f.Usage != nil {
		t.Fatalf("usage = %v once its only NodeClaim left, want nil", f.Usage)
	}
}
