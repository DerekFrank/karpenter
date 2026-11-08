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

package state

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

func TestContributionsKeepTheContributionLaws(t *testing.T) {
	base := NodePoolFields{NodeClaims: NodeClaimCounts{Active: 3, Deleting: 1}, Reserved: 2}
	statetest.ExpectContribution(t, base, NodeClaimCounts{Active: 1}, NodeClaimCounts{Deleting: 1})
	statetest.ExpectContribution(t, base, reservedNodes(1), reservedNodes(2))
	podKey := func(name string) podOnNodeClaim {
		return podOnNodeClaim{types.NamespacedName{Namespace: "default", Name: name}}
	}
	for _, base := range []map[types.NamespacedName]struct{}{nil, {podKey("other").pod: {}}} {
		statetest.ExpectContribution(t, base, podKey("a"), podKey("a"))
		statetest.ExpectContribution(t, base, podKey("a"), podKey("b"))
	}
}
