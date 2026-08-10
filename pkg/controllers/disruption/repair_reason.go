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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// reasonMatcherCache memoizes compiled ReasonMatcher regexes. Matchers come from the finite RepairPolicy set and are
// stable, so compiling once avoids recompiling the regex per reason × per policy × per candidate × per disruption pass
// (profiling showed that recompilation dominated repair's per-pass allocations at fleet scale).
var reasonMatcherCache sync.Map // ReasonMatcher string -> *regexp.Regexp (nil for a matcher that failed to compile)

func compiledReasonMatcher(pattern string) *regexp.Regexp {
	if v, ok := reasonMatcherCache.Load(pattern); ok {
		re, _ := v.(*regexp.Regexp)
		return re
	}
	re, err := regexp.Compile("^(?:" + pattern + ")$")
	if err != nil {
		re = nil // cache the failure too, so we don't retry compiling an invalid matcher every pass
	}
	reasonMatcherCache.Store(pattern, re)
	return re
}

// Reason-level policy & matching (F1). Kubernetes hands repair a condition-granular signal (one NodeCondition per type
// with a single reason and a single lastTransitionTime), but correct repair needs reason-granular policy: within one
// condition, different reasons want different tolerations/priorities/drain-bounds. The signaler carries per-reason
// onset in the condition's reason field as an onset-bearing list — "token@onset;token@onset;..." — so repair reads a
// true per-reason onset. A bare single token (today's un-enriched signal) parses as one reason with a zero onset,
// falling back to the condition's own lastTransitionTime.

// reasonOnset is one reason on a node condition with its own onset time.
type reasonOnset struct {
	reason string
	onset  time.Time
}

// parseReasons splits an onset-bearing reason field ("token@epoch;token@epoch") into per-reason onsets. A bare token
// with no "@onset" yields a zero onset (the caller falls back to the condition's lastTransitionTime). Unparseable
// onsets also fall back to zero. Order is irrelevant — repair consumes the whole set.
func parseReasons(reason string) []reasonOnset {
	if reason == "" {
		return nil
	}
	out := make([]reasonOnset, 0, strings.Count(reason, ";")+1)
	for _, tok := range strings.Split(reason, ";") {
		name, onsetStr, hasOnset := strings.Cut(tok, "@")
		ro := reasonOnset{reason: name}
		if hasOnset {
			if epoch, err := strconv.ParseInt(onsetStr, 10, 64); err == nil {
				ro.onset = time.Unix(epoch, 0)
			}
		}
		out = append(out, ro)
	}
	return out
}

// matchesReason reports whether the policy's ReasonMatcher matches reason. An empty matcher means "any reason" (the
// condition-only superset). The match is whole-string, mirroring MNG's anchored .matches() semantics.
func matchesReason(policy cloudprovider.RepairPolicy, reason string) bool {
	if policy.ReasonMatcher == "" {
		return true
	}
	re := compiledReasonMatcher(policy.ReasonMatcher)
	if re == nil {
		return false
	}
	return re.MatchString(reason)
}

// mergedRepairPolicy is the result of merging every eligible reason on a condition. It is a safety-monotone lattice
// join (F1): toleration = min, TerminationGracePeriod = min (drain-ability is a conjunction), priority = max. The join
// is idempotent (min/max), so a correlated signal counted twice changes nothing — correlation cannot manufacture
// urgency. Only reasons past their own toleration participate ("eligible-only"), so every action traces to a configured
// policy for a reason that earned its confidence delay.
type mergedRepairPolicy struct {
	priority               int
	terminationGracePeriod *time.Duration
	tgpSet                 bool
}

// resolveReasonPolicy matches the node's condition + per-reason onsets against the repair policies and returns the
// merged policy over the reasons that are BOTH matched and past their own toleration ("eligible"), plus whether any
// reason was eligible at all. When no policy carries a ReasonMatcher, this collapses to the condition-only behavior.
func resolveReasonPolicy(policies []cloudprovider.RepairPolicy, cond corev1.NodeCondition, now time.Time) (mergedRepairPolicy, bool) {
	reasons := parseReasons(cond.Reason)
	// A condition with no parseable reasons still behaves condition-only: treat it as a single empty reason whose onset
	// is the condition's transition time.
	if len(reasons) == 0 {
		reasons = []reasonOnset{{reason: "", onset: cond.LastTransitionTime.Time}}
	}

	merged := mergedRepairPolicy{}
	eligible := false
	for _, policy := range policies {
		if policy.ConditionType != cond.Type || policy.ConditionStatus != cond.Status {
			continue
		}
		for _, ro := range reasons {
			if !matchesReason(policy, ro.reason) {
				continue
			}
			// Each reason's toleration runs from its own onset; fall back to the condition transition time when the
			// signal carried no per-reason onset.
			onset := ro.onset
			if onset.IsZero() {
				onset = cond.LastTransitionTime.Time
			}
			if now.Before(onset.Add(policy.TolerationDuration)) {
				continue // not yet past this reason's confidence delay — excluded from the eligible-only merge
			}
			eligible = true
			// priority = max
			if policy.Priority > merged.priority {
				merged.priority = policy.Priority
			}
			// TerminationGracePeriod = min (drain-ability is a conjunction: every eligible reason must permit it).
			if policy.TerminationGracePeriod != nil {
				if !merged.tgpSet || *policy.TerminationGracePeriod < *merged.terminationGracePeriod {
					merged.terminationGracePeriod = policy.TerminationGracePeriod
					merged.tgpSet = true
				}
			}
		}
	}
	return merged, eligible
}
