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

import (
	"testing"
	"time"

	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
)

func TestRepairDecisionLogValues_EffectiveTerminationGracePeriod(t *testing.T) {
	tests := []struct {
		name      string
		policyTGP *time.Duration
		ncTGP     *time.Duration
		expected  *time.Duration
	}{
		{name: "policy bound capped by the NodeClaim bound", policyTGP: lo.ToPtr(5 * time.Minute), ncTGP: lo.ToPtr(2 * time.Minute), expected: lo.ToPtr(2 * time.Minute)},
		{name: "policy bound below the NodeClaim bound", policyTGP: lo.ToPtr(time.Minute), ncTGP: lo.ToPtr(2 * time.Minute), expected: lo.ToPtr(time.Minute)},
		{name: "no policy bound inherits the NodeClaim bound", ncTGP: lo.ToPtr(2 * time.Minute), expected: lo.ToPtr(2 * time.Minute)},
		{name: "no bound at all is unbounded", expected: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := mockCandidate("node-1")
			c.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nc-1"}}
			if tt.ncTGP != nil {
				c.NodeClaim.Spec.TerminationGracePeriod = &metav1.Duration{Duration: *tt.ncTGP}
			}
			c.RepairPolicyResult = health.RepairResult{TerminationGracePeriod: tt.policyTGP}
			c.TerminationGracePeriod = effectiveDrainBound(c, c.RepairPolicyResult)

			values := repairDecisionLogValues(c)
			got, found := logValue(values, "effective-termination-grace-period")
			if tt.expected == nil {
				if found {
					t.Errorf("expected no effective-termination-grace-period, got %v", got)
				}
				return
			}
			if !found || got != *tt.expected {
				t.Errorf("effective-termination-grace-period = %v (found %v), want %v", got, found, *tt.expected)
			}
			// The policy's own bound is still reported alongside the effective one.
			if tt.policyTGP != nil {
				if policy, ok := logValue(values, "termination-grace-period"); !ok || policy != *tt.policyTGP {
					t.Errorf("termination-grace-period = %v (found %v), want %v", policy, ok, *tt.policyTGP)
				}
			}
		})
	}
}

func logValue(values []any, key string) (any, bool) {
	for i := 0; i+1 < len(values); i += 2 {
		if values[i] == key {
			return values[i+1], true
		}
	}
	return nil, false
}
