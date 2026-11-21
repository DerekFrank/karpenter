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

package cluster_test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeoverlay"
	"sigs.k8s.io/karpenter/pkg/controllers/nodepool/pricing"
	"sigs.k8s.io/karpenter/pkg/controllers/persistentvolumeclaim/driver"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *test.Environment
var cs karpentercluster.State
var cluster karpentercluster.State
var clusterCost *karpentercluster.ClusterCost
var pvcDriverController *driver.Controller
var pricingController *pricing.Controller
var nodeOverlayStore *nodeoverlay.InstanceTypeStore
var nodeOverlayController *nodeoverlay.Controller
var cloudProvider *fake.CloudProvider
var nodePool *v1.NodePool

const csiProvider = "fake.csi.provider"

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cluster")
}

// The suite runs against the envtest API server: it builds cluster state over env.Overlay, as the operator does, and
// writes through env.Client.
var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...), test.WithConfigOptions(func(config *rest.Config) {
		config.QPS = -1
	}))
	ctx = options.ToContext(ctx, test.Options())
	cloudProvider = fake.NewCloudProvider()
	cs = karpentercluster.New(ctx, env.Clock, env.Overlay, cloudProvider)
	cluster = cs
	clusterCost = cs.Cost
	pvcDriverController = driver.NewController(env.Overlay, cluster.PVCs)
	pricingController = pricing.NewController(env.Clock, cs.NodePoolState, clusterCost)
	nodeOverlayStore = nodeoverlay.NewInstanceTypeStore()
	nodeOverlayController = nodeoverlay.NewController(env.Clock, env.Overlay, cloudProvider, nodeOverlayStore)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	env.Clock.SetTime(time.Now())
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	nodePool = test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	ExpectApplied(ctx, env.Client, nodePool)
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
	cs.Reset()
	cloudProvider.Reset()
})
