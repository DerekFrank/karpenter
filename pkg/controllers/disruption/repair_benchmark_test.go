//go:build test_performance

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
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// This benchmark isolates the repair-specific per-pass work that scales with fleet size: ordering the eligible
// candidates by the repair score (which resolves the matching policy + per-reason onset merge per candidate) and the
// restraint dial bookkeeping (observe/resetIdleDomains/admit). It deliberately does NOT exercise SimulateScheduling —
// that is shared provisioning code already covered by the scheduling benchmark, and repair invokes it at most once per
// pass (for the single winning candidate), whereas the scoring/restraint work is O(candidates) every pass.
//
// Run:
//   go test -tags=test_performance -run=XXX -bench=BenchmarkRepairOrdering ./pkg/controllers/disruption/
//   go test -tags=test_performance -run=RepairProfile ./pkg/controllers/disruption/   # writes repair.cpuprofile/.heapprofile

// benchPolicies returns n reason-keyed repair policies (the F1 path, which compiles a regex per reason match).
func benchPolicies(withReasonMatcher bool) []cloudprovider.RepairPolicy {
	matcher := ""
	if withReasonMatcher {
		matcher = "NvidiaXid.*"
	}
	return []cloudprovider.RepairPolicy{
		{ConditionType: "BadNode", ConditionStatus: corev1.ConditionFalse, TolerationDuration: 30 * time.Minute, Priority: 10, ReasonMatcher: matcher},
	}
}

// benchCandidates builds n eligible repair candidates spread across a few zones, each with an unhealthy condition whose
// reason carries a past onset (so they are eligible and exercise the onset parser).
func benchCandidates(n int, onset time.Time) []*Candidate {
	zones := []string{"z1", "z2", "z3"}
	cands := make([]*Candidate, n)
	for i := 0; i < n; i++ {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   fmt.Sprintf("node-%d", i),
				Labels: map[string]string{v1.NodePoolLabelKey: "np", corev1.LabelTopologyZone: zones[i%len(zones)]},
			},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type:               "BadNode",
				Status:             corev1.ConditionFalse,
				Reason:             fmt.Sprintf("NvidiaXid79@%d", onset.Unix()),
				LastTransitionTime: metav1.Time{Time: onset},
			}}},
		}
		cands[i] = &Candidate{
			StateNode: &state.StateNode{Node: node, NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nc-%d", i)}, Status: v1.NodeClaimStatus{ProviderID: fmt.Sprintf("pid-%d", i)}}},
			NodePool:  &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np"}},
			zone:      zones[i%len(zones)],
		}
	}
	return cands
}

// resolveAndScore mirrors ComputeCommands' per-candidate hot work: match the policy, merge reasons, and score.
func resolveAndScore(r *Repair, policies []cloudprovider.RepairPolicy, ranks map[int]int, cands []*Candidate) {
	for _, c := range cands {
		policy, cond := matchingPolicy(policies, c.Node)
		if policy == nil {
			continue
		}
		merged, _ := resolveReasonPolicy(policies, *cond, r.clock.Now())
		_ = r.score(*cond, policy.TolerationDuration, merged, ranks, c.NodePool)
	}
}

// benchmarkRepairOrdering measures the repair resolve+score path over n candidates.
func benchmarkRepairOrdering(b *testing.B, n int) {
	cp := fake.NewCloudProvider()
	cp.RepairPolicy = benchPolicies(true)
	r := NewRepair(nil, nil, nil, cp, nil, clock.RealClock{}, nil)
	policies := cp.RepairPolicies()
	ranks := denseRanks(policies)
	cands := benchCandidates(n, time.Now().Add(-time.Hour))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resolveAndScore(r, policies, ranks, cands)
	}
	b.ReportMetric(float64(n), "candidates")
}

func BenchmarkRepairOrdering100(b *testing.B)   { benchmarkRepairOrdering(b, 100) }
func BenchmarkRepairOrdering1000(b *testing.B)  { benchmarkRepairOrdering(b, 1000) }
func BenchmarkRepairOrdering10000(b *testing.B) { benchmarkRepairOrdering(b, 10000) }

// TestRepairProfile writes CPU and heap profiles of the repair ordering path at 10k candidates for `go tool pprof`.
func TestRepairProfile(t *testing.T) {
	cpuf, err := os.Create("repair.cpuprofile")
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpuf); err != nil {
		t.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	cp := fake.NewCloudProvider()
	cp.RepairPolicy = benchPolicies(true)
	r := NewRepair(nil, nil, nil, cp, nil, clock.RealClock{}, nil)
	policies := cp.RepairPolicies()
	ranks := denseRanks(policies)
	cands := benchCandidates(10000, time.Now().Add(-time.Hour))
	for i := 0; i < 200; i++ { // ~200 disruption passes over a 10k-candidate fleet
		resolveAndScore(r, policies, ranks, cands)
	}

	heapf, err := os.Create("repair.heapprofile")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pprof.WriteHeapProfile(heapf); _ = heapf.Close() }()
}
