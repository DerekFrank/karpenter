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

// Package nodepoolhealth keeps the recent launch outcomes of each NodePool, which decide its NodeRegistrationHealthy
// condition.
package nodepoolhealth

import (
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state"
)

const (
	BufferSize     = 4
	ThresholdFalse = 0.5 // 50% of 0s for NodeRegistrationHealthy=False
)

type Status int

const (
	StatusUnknown Status = iota
	StatusHealthy
	StatusUnhealthy
)

// Fields are what Karpenter records about a NodePool's launches, keyed by the NodePool's name. The lifecycle and
// registration health controllers set them with Store.Update, and they go once the NodePool does.
type Fields struct {
	// UID is the NodePool the outcomes are of. Outcomes recorded for an earlier NodePool of the same name don't count.
	UID types.UID
	// Outcomes are the NodePool's newest BufferSize launch outcomes, oldest first: true for a NodeClaim that
	// registered, false for one that failed to launch or register. A stored slice is never modified.
	Outcomes []bool
}

// State keeps the Fields of each NodePool.
type State struct {
	store *state.Store[string, Fields]
}

func NewState() *State {
	return &State{store: state.NewStore[string, Fields]()}
}

// outcomes returns the outcomes recorded for the NodePool, ignoring those of an earlier NodePool with its name.
func (f Fields) outcomes(nodePool *v1.NodePool) []bool {
	if f.UID != nodePool.UID {
		return nil
	}
	return f.Outcomes
}

// Status returns the NodePool's registration health from its recorded outcomes.
func (s *State) Status(nodePool *v1.NodePool) Status {
	f, _ := s.store.Get(nodePool.Name)
	return status(f.outcomes(nodePool))
}

// Update records a launch outcome of the NodePool, dropping its oldest once it holds BufferSize.
func (s *State) Update(nodePool *v1.NodePool, success bool) {
	s.store.Update(nodePool.Name, func(f Fields) Fields {
		return Fields{UID: nodePool.UID, Outcomes: record(f.outcomes(nodePool), success)}
	})
}

// DryRun returns the NodePool's status as if it had recorded the outcome, without recording it.
func (s *State) DryRun(nodePool *v1.NodePool, success bool) Status {
	f, _ := s.store.Get(nodePool.Name)
	return status(record(f.outcomes(nodePool), success))
}

// SetStatus replaces the NodePool's outcomes with the fewest that give the status. StatusUnknown drops them.
func (s *State) SetStatus(nodePool *v1.NodePool, st Status) {
	var outcomes []bool
	switch st {
	case StatusHealthy:
		outcomes = []bool{true}
	case StatusUnhealthy:
		outcomes = make([]bool, int(BufferSize*ThresholdFalse))
	}
	s.store.Update(nodePool.Name, func(Fields) Fields {
		if outcomes == nil {
			return Fields{}
		}
		return Fields{UID: nodePool.UID, Outcomes: outcomes}
	})
}

// Delete drops the outcomes of the NodePool with the name, once it's gone.
func (s *State) Delete(name string) {
	s.store.Update(name, func(Fields) Fields { return Fields{} })
}

// Get returns the Fields recorded under the NodePool name, and whether there are any.
func (s *State) Get(name string) (Fields, bool) {
	return s.store.Get(name)
}

// record returns outcomes with the outcome appended, keeping the newest BufferSize, in a new slice.
func record(outcomes []bool, success bool) []bool {
	next := make([]bool, 0, BufferSize)
	if len(outcomes) >= BufferSize {
		outcomes = outcomes[len(outcomes)-BufferSize+1:]
	}
	return append(append(next, outcomes...), success)
}

func status(outcomes []bool) Status {
	if len(outcomes) == 0 {
		return StatusUnknown
	}
	var unhealthy int
	for _, success := range outcomes {
		if !success {
			unhealthy++
		}
	}
	if float64(unhealthy)/float64(BufferSize) >= ThresholdFalse {
		return StatusUnhealthy
	}
	return StatusHealthy
}
