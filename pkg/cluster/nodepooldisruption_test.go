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

	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

func TestNodePoolDisruptionKeepsTheContributionLaws(t *testing.T) {
	statetest.ExpectContribution(t, NodePoolDisruption{Nodes: 4, Disrupting: 1, NotReady: 2}, NodePoolDisruption{Nodes: 1}, NodePoolDisruption{Nodes: 1, Disrupting: 1})
	statetest.ExpectContribution(t, NodePoolDisruption{Nodes: 4, Disrupting: 1, NotReady: 2}, NodePoolDisruption{NotReady: 1}, NodePoolDisruption{NotReady: 1})
}
