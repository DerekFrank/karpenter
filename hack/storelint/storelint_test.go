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

// TestStoresAreBuiltAndStartedWhereTheDesignSays checks each rule with a passing and a failing case: where stores are
// built, constructors that subscribe or derive, shared stores added to the manager, held objects written, RequeueAfter
// without a deadline, Contributions without a law test, unbounded collections in a store's fields, raw stores
// exported from pkg/cluster, and Derives read only as a source.
func TestStoresAreBuiltAndStartedWhereTheDesignSays(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), storelint.Analyzer, "controllers", "sigs.k8s.io/karpenter/pkg/cluster", "sigs.k8s.io/karpenter/pkg/state")
}
