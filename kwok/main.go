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

package main

import (
	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kwok "sigs.k8s.io/karpenter/kwok/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/overlay"
	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/controllers"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator"
)

func main() {
	lo.Must0(cloudprovider.SetCapacityTypeClassResolver(kwok.CapacityTypes))
	cloudprovider.SetNodeClaimPriceProvider(kwok.NodeClaimPriceProvider{})
	ctx, op := operator.NewOperator()
	instanceTypes, err := kwok.ConstructInstanceTypes(ctx)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed constructing instance types")
	}

	overlayUndecoratedCloudProvider := kwok.NewCloudProvider(ctx, op.GetClient(), instanceTypes)
	cloudProvider := overlay.Decorate(overlayUndecoratedCloudProvider, op.GetClient(), op.InstanceTypeStore)
	repairPolicyMatcher := lo.Must(repair.NewRepairPolicyMatcher(ctx, cloudProvider))
	s := cluster.New(ctx, op.Clock, op.GetClient(), op.GetCache(), cloudProvider, cluster.WithRepairPolicyMatcher(repairPolicyMatcher))
	lo.Must0(s.Register(op.Manager))
	clusterState := state.NewCluster(op.Clock, op.GetClient(), s.Nodes, s.NodePoolState, s.CapacityQuotaState, s.PodScheduling)
	op.
		WithControllers(ctx, controllers.NewControllers(
			ctx,
			op.Manager,
			op.Clock,
			op.GetClient(),
			op.EventRecorder,
			cloudProvider,
			overlayUndecoratedCloudProvider,
			s,
			clusterState,
			op.InstanceTypeStore,
			op.PredictionStore,
			controllers.WithVPAPrediction(),
		)...).Start(ctx)
}
