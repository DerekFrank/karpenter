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

package storelint_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"sigs.k8s.io/karpenter/hack/storelint"
)

// TestStoresAreBuiltAndStartedWhereTheDesignSays checks both rules: stores are built only in pkg/cluster, pkg/state, a
// controller's Register or a test, and a shared store is added to the manager only by cluster.State.Register.
func TestStoresAreBuiltAndStartedWhereTheDesignSays(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), storelint.Analyzer, "controllers", "sigs.k8s.io/karpenter/pkg/cluster", "sigs.k8s.io/karpenter/pkg/state")
}
