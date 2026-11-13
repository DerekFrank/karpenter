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

package scheduling_test

import (
	"context"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

// emptyState builds cluster state that holds nothing, without the API server, for a benchmark that schedules against
// no nodes.
func emptyState(ctx context.Context, clk clock.Clock, cloudProvider cloudprovider.CloudProvider) karpentercluster.State {
	return karpentercluster.New(ctx, clk, nil, cloudProvider,
		karpentercluster.WithSources(func(client.Object) karpenterstate.Newest { return statetest.NewObjects() }))
}
