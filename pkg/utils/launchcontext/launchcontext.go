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

// Package launchcontext carries a NodeClaim's launch-time provenance: why it was created and, for a disruption
// replacement, which NodeClaim it superseded. It is serialized to the karpenter.sh/launch-context ANNOTATION — it is
// deliberately NOT an apis/v1 CRD field, so it is not subject to CRD field conventions (kube-api-linter, deepcopy-gen)
// and needs no CRD change. The correlated-failure repair loop reads it to identify its own in-flight replacements —
// and the failure domain they belong to — from cluster state alone, crash-safe on resync, with no in-memory ledger.
// (POC: a candidate to promote to a spec.launchContext field later.)
package launchcontext

import (
	"encoding/json"

	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Cause is why a NodeClaim was created. It is a superset of the DisruptionReasons (a replacement launched to supersede
// a disrupted NodeClaim carries that reason) plus Provisioned for a reactively-provisioned NodeClaim. The disruption
// values deliberately coincide with v1.DisruptionReason so CauseForReason is a plain cast.
type Cause string

const (
	// CauseProvisioned — reactive provisioning for pending pods (no NodeClaim replaced).
	CauseProvisioned Cause = "Provisioned"
	// CauseUnderutilized/Empty/Drifted — a consolidation/drift replacement.
	CauseUnderutilized Cause = "Underutilized"
	CauseEmpty         Cause = "Empty"
	CauseDrifted       Cause = "Drifted"
	// CauseUnhealthy — a repair replacement (pre-spun to supersede an unhealthy NodeClaim).
	CauseUnhealthy Cause = "Unhealthy"
)

// Context is launch-time provenance for a NodeClaim.
type Context struct {
	// Cause is why this NodeClaim was launched.
	Cause Cause `json:"cause"`
	// Replaces is the name of the NodeClaim this one was launched to replace. Empty for a reactively provisioned
	// NodeClaim (Cause=Provisioned).
	Replaces string `json:"replaces,omitempty"`
}

// ForReason maps a DisruptionReason to the Cause of a replacement launched to supersede it. The values coincide, so
// this is a cast — kept as a named function so intent is legible at call sites.
func ForReason(reason v1.DisruptionReason) Cause {
	return Cause(reason)
}

// Marshal renders the Context as the annotation value. A struct of strings never fails to marshal.
func (c Context) Marshal() string {
	b, _ := json.Marshal(c)
	return string(b)
}

// StampOn writes the launch-context annotation onto an object's annotations. It uses lo.Assign (copy-on-write) rather
// than mutating the existing map in place: the object's annotation map may be shared, and launches run concurrently
// (Provisioner.Create parallelizes), so an in-place write races (caught by -race).
func (c Context) StampOn(o metav1.Object) {
	o.SetAnnotations(lo.Assign(o.GetAnnotations(), map[string]string{
		v1.LaunchContextAnnotationKey: c.Marshal(),
	}))
}

// Get reads the launch-context annotation off an object. ok is false when the annotation is absent or unparseable (or
// carries no Cause) — a missing/garbled marker is simply treated as "no provenance", never an error, so a NodeClaim
// launched before this field existed is handled the same as one launched outside repair.
func Get(o metav1.Object) (Context, bool) {
	v, ok := o.GetAnnotations()[v1.LaunchContextAnnotationKey]
	if !ok {
		return Context{}, false
	}
	var c Context
	if err := json.Unmarshal([]byte(v), &c); err != nil || c.Cause == "" {
		return Context{}, false
	}
	return c, true
}
