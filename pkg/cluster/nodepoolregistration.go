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
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state"
)

const (
	// RegistrationBufferSize is how many of a NodePool's newest launch outcomes decide its registration health.
	RegistrationBufferSize = 4
	// RegistrationThresholdFalse is the share of failed outcomes at which NodeRegistrationHealthy is False.
	RegistrationThresholdFalse = 0.5
)

// RegistrationStatus is a NodePool's registration health, from its recent launch outcomes.
type RegistrationStatus int

const (
	RegistrationUnknown RegistrationStatus = iota
	RegistrationHealthy
	RegistrationUnhealthy
)

// RegistrationKey is the NodePool and the spec launch outcomes are of. Outcomes count only for the spec they're of, so
// a new NodePool or NodeClass spec starts with none.
type RegistrationKey struct {
	UID                 types.UID
	NodePoolGeneration  int64
	NodeClassGeneration int64
}

// RegistrationKeyOf returns the key of the NodePool's current spec, with its NodeClass at nodeClassGeneration.
func RegistrationKeyOf(nodePool *v1.NodePool, nodeClassGeneration int64) RegistrationKey {
	return RegistrationKey{UID: nodePool.UID, NodePoolGeneration: nodePool.Generation, NodeClassGeneration: nodeClassGeneration}
}

// RegistrationOutcome is one launch outcome: whether the NodeClaim registered, and why it didn't.
type RegistrationOutcome struct {
	Registered bool
	// Reason and Message say why a NodeClaim that didn't register failed.
	Reason, Message string
}

// Registration is the launch outcomes Karpenter records about a NodePool, intent kept on the NodePool's entry. The
// NodeClaim lifecycle controller is its one writer; the registration health controller derives the NodePool's
// NodeRegistrationHealthy condition from it. It goes once the NodePool does.
type Registration struct {
	// RegistrationKey is the NodePool and spec the outcomes are of. Outcomes recorded for an earlier NodePool of the same
	// name, or an earlier spec, don't count.
	RegistrationKey
	// Outcomes are the newest RegistrationBufferSize launch outcomes, oldest first: true for a NodeClaim that
	// registered, false for one that failed to launch or register. A stored slice is never modified.
	Outcomes []bool // bounded: RegistrationBufferSize
	// Reason and Message are the newest failed outcome's.
	Reason, Message string
}

// RegistrationHealth is a NodePool's registration health from its recorded outcomes, with the newest failure's reason
// and message once it's unhealthy.
type RegistrationHealth struct {
	Status          RegistrationStatus
	Reason, Message string
}

// outcomes returns the outcomes recorded for the key, ignoring those of another NodePool or spec.
func (r Registration) outcomes(key RegistrationKey) []bool {
	if r.RegistrationKey != key {
		return nil
	}
	return r.Outcomes
}

// Health returns the registration health of the recorded outcomes, whichever spec they're of.
func (r Registration) Health() RegistrationHealth {
	status := registrationStatus(r.Outcomes)
	if status != RegistrationUnhealthy {
		return RegistrationHealth{Status: status}
	}
	return RegistrationHealth{Status: status, Reason: r.Reason, Message: r.Message}
}

// RegistrationHealth returns the NodePool's registration health from the outcomes recorded for its current spec, with
// its NodeClass at nodeClassGeneration. With none recorded, such as after a restart or a spec change, it's
// RegistrationUnknown.
func (n *NodePoolState) RegistrationHealth(nodePool *v1.NodePool, nodeClassGeneration int64) RegistrationHealth {
	e, _ := n.store.Get(nodePool.Name)
	r := e.Intent.Registration.Value
	if r.outcomes(RegistrationKeyOf(nodePool, nodeClassGeneration)) == nil {
		return RegistrationHealth{}
	}
	return r.Health()
}

// Registrations is the handle of the launch outcomes recorded on NodePools, which the NodeClaim lifecycle controller,
// their one writer, holds.
type Registrations struct {
	intent *state.Intent[string, *v1.NodePool, NodePoolFields, NodePoolIntent, Registration]
}

func newRegistrations(n *NodePoolState) *Registrations {
	return &Registrations{intent: state.NewIntent(n.store, func(i *NodePoolIntent) *state.Intended[Registration] { return &i.Registration })}
}

// Record records a launch outcome of the NodePool's current spec, with its NodeClass at nodeClassGeneration. It drops
// the oldest outcome once there are RegistrationBufferSize, and the outcomes of any other spec. It's atomic with the
// entry, so concurrent records for one NodePool lose none.
func (h *Registrations) Record(nodePool *v1.NodePool, nodeClassGeneration int64, outcome RegistrationOutcome) {
	key := RegistrationKeyOf(nodePool, nodeClassGeneration)
	h.intent.Update(nodePool.Name, func(r Registration) Registration {
		next := Registration{RegistrationKey: key, Outcomes: record(r.outcomes(key), outcome.Registered)}
		if r.RegistrationKey == key {
			next.Reason, next.Message = r.Reason, r.Message
		}
		if !outcome.Registered {
			next.Reason, next.Message = outcome.Reason, outcome.Message
		}
		return next
	})
}

// record returns outcomes with the outcome appended, keeping the newest RegistrationBufferSize, in a new slice.
func record(outcomes []bool, success bool) []bool {
	next := make([]bool, 0, RegistrationBufferSize)
	if len(outcomes) >= RegistrationBufferSize {
		outcomes = outcomes[len(outcomes)-RegistrationBufferSize+1:]
	}
	return append(append(next, outcomes...), success)
}

// registrationStatus is unhealthy once the failures reach RegistrationThresholdFalse of RegistrationBufferSize, healthy
// below that once a NodeClaim registered, and unknown otherwise: a success is what turns a NodePool healthy, and enough
// failures what turns it unhealthy.
func registrationStatus(outcomes []bool) RegistrationStatus {
	var registered, failed int
	for _, success := range outcomes {
		if success {
			registered++
		} else {
			failed++
		}
	}
	switch {
	case float64(failed)/float64(RegistrationBufferSize) >= RegistrationThresholdFalse:
		return RegistrationUnhealthy
	case registered > 0:
		return RegistrationHealthy
	default:
		return RegistrationUnknown
	}
}
