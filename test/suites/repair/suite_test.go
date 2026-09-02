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

package repair

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck
	. "github.com/onsi/gomega"    //nolint:staticcheck
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/pkg/environment/common"
)

var (
	env       *common.Environment
	nodeClass *unstructured.Unstructured
)

// option is the deployed-build identity for the run: each implementation option is a different Karpenter image
// (breaker-20pct / restraint-unpinned / restraint-pinned). The e2e suite is the trunk; the deployed build is the
// variable. We only read the option name here (from REPAIR_OPTION) to label the metric rows — the suite is otherwise
// implementation-agnostic and never imports pkg/controllers/disruption.
var option = optionName()

func optionName() string {
	if v, ok := os.LookupEnv("REPAIR_OPTION"); ok && v != "" {
		return v
	}
	return "deployed"
}

func TestRepair(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeSuite(func() {
		env = common.NewEnvironment(t)
	})
	AfterSuite(func() {
		env.Stop()
	})
	RunSpecs(t, "RepairPerformance")
}

var _ = BeforeEach(func() {
	env.BeforeEach()
	// Each cell builds its own NodePool(s) (topology axis), so we only stamp a fresh, uniquely-named default
	// NodeClass here; the per-cell NodePools reference it via newNodePool().
	nodeClass = env.DefaultNodeClass.DeepCopy()
	nodeClass.SetName(nodeClass.GetName() + "-" + test.RandomName())
})

var _ = AfterEach(func() {
	env.Cleanup()
	env.AfterEach()
})
