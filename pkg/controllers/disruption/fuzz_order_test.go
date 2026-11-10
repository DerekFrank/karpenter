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

package disruption_test

import (
	"fmt"
	"math"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

// fuzzOrderPrediction is whether a plan's hard topology spreads hold in every drain order, by the escrow worst case.
type fuzzOrderPrediction struct {
	Robust     bool     `json:"robust"`
	Violations []string `json:"violations,omitempty"`
}

// predictOrder checks each pod plan moves off a candidate against the worst order a drain can take for it: every other
// pod the plan moves out of a domain has left, except out of the pod's own destination, every pod the plan moves into
// the destination has landed, and none has landed anywhere else. If the pod's spread allows it there, it allows it in
// any order. The check reads kube-scheduler's rules: a domain is the zone or node of each node matching the pod's
// node selector, tainted ones included (nodeTaintsPolicy Ignore).
func (w *fuzzWorld) predictOrder(cmd disruption.Command, plan *pscheduling.Results) *fuzzOrderPrediction {
	candidates := sets.New(candidateNames(cmd)...)
	replacementName := fmt.Sprintf("fuzz-replacement-%d", w.seed)
	zoneOf := map[string]string{}
	for name, node := range w.nodes {
		zoneOf[name] = node.Labels[corev1.LabelTopologyZone]
	}
	if zone, ok := w.launchZone(cmd); ok {
		zoneOf[replacementName] = zone
	}
	dest := map[string]string{}
	var moved []*corev1.Pod
	for _, en := range plan.ExistingNodes {
		for _, p := range en.Pods {
			if candidates.Has(p.Spec.NodeName) {
				dest[p.Name] = en.Name()
				moved = append(moved, p)
			}
		}
	}
	if len(plan.NewNodeClaims) > 0 {
		for _, p := range plan.NewNodeClaims[0].Pods {
			if candidates.Has(p.Spec.NodeName) {
				dest[p.Name] = replacementName
				moved = append(moved, p)
			}
		}
	}
	bound := w.boundPods()
	prediction := &fuzzOrderPrediction{Robust: true}
	for _, p := range moved {
		wl := w.workloadOf(p)
		for _, tsc := range wl.template.Spec.TopologySpreadConstraints {
			if tsc.WhenUnsatisfiable != corev1.DoNotSchedule {
				continue
			}
			domainOf := func(node string) string {
				return lo.Ternary(tsc.TopologyKey == corev1.LabelHostname, node, zoneOf[node])
			}
			eligible := func(node string) bool {
				zone, ok := p.Spec.NodeSelector[corev1.LabelTopologyZone]
				_, known := zoneOf[node]
				return known && (!ok || zoneOf[node] == zone)
			}
			c0, out, in := map[string]int{}, map[string]int{}, map[string]int{}
			domains := sets.New[string]()
			for node := range zoneOf {
				if eligible(node) {
					domains.Insert(domainOf(node))
				}
			}
			for _, q := range bound {
				if q.Labels["fuzz-workload"] == wl.name && eligible(q.Spec.NodeName) {
					c0[domainOf(q.Spec.NodeName)]++
				}
			}
			for _, q := range moved {
				if q.Labels["fuzz-workload"] == wl.name {
					out[domainOf(q.Spec.NodeName)]++
					in[domainOf(dest[q.Name])]++
				}
			}
			d := domainOf(dest[p.Name])
			// The worst state p is placed into: d holds everything bound there and every other arrival, the other domains
			// have lost everything leaving them.
			before := func(domain string) int {
				if domain == d {
					return c0[d] + in[d] - 1
				}
				return c0[domain] - out[domain]
			}
			minCount := math.MaxInt
			for domain := range domains {
				minCount = min(minCount, before(domain))
			}
			if skew := before(d) + 1 - minCount; skew > int(tsc.MaxSkew) {
				prediction.Robust = false
				prediction.Violations = append(prediction.Violations, fmt.Sprintf("%s -> %s (%s %s): worst skew %d > %d", p.Name, dest[p.Name], wl.name, tsc.TopologyKey, skew, tsc.MaxSkew))
			}
		}
	}
	return prediction
}

// launchZone is the zone of the offering the ground truth launches for cmd's replacement.
func (w *fuzzWorld) launchZone(cmd disruption.Command) (string, bool) {
	if len(cmd.Replacements) == 0 {
		return "", false
	}
	replacement := cmd.Replacements[0]
	var zone string
	price := math.Inf(1)
	for _, option := range replacement.InstanceTypeOptions {
		for _, o := range w.instanceType(option.Name).Offerings.Available().Compatible(replacement.Requirements) {
			if o.Price < price {
				zone, price = o.Requirements.Get(corev1.LabelTopologyZone).Any(), o.Price
			}
		}
	}
	return zone, zone != ""
}
