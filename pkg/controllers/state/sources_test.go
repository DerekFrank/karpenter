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

package state_test

import (
	"context"

	"github.com/awslabs/operatorpkg/option"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/statetest"
)

// The benchmarks and the specs that drive cluster state from many goroutines build it in memory, without the API
// server: each object store follows statetest.Objects, which they fill with observe and forget as the informers would.

// inMemory builds cluster state over statetest.Sources, as cluster.New does over the overlay client, and returns the
// Sources with it. The caller holds them, so that they drop with the state they feed: a registry of them by cluster
// state would keep every object of every benchmark's fixture alive, which a benchmark that allocates pays for in GC.
func inMemory(ctx context.Context, clk clock.Clock, cloudProvider cloudprovider.CloudProvider, opts ...option.Function[karpentercluster.Options]) (karpentercluster.State, *statetest.Sources) {
	srcs := statetest.NewSources()
	cs := karpentercluster.New(ctx, clk, nil, cloudProvider,
		append(opts, karpentercluster.WithSources(func(obj client.Object) karpenterstate.Newest { return srcs.For(obj) }))...)
	return cs, srcs
}

// observe shows in-memory cluster state a version of each object, as the informers would.
func observe(srcs *statetest.Sources, objs ...client.Object) { srcs.Put(objs...) }

// forgetNodeClaim shows in-memory cluster state that the NodeClaim is gone, as the informer would.
func forgetNodeClaim(srcs *statetest.Sources, name string) {
	srcs.Remove(&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

// forgetPod shows in-memory cluster state that the pod is gone, as the informer would.
func forgetPod(srcs *statetest.Sources, name types.NamespacedName) {
	srcs.Remove(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: name.Namespace, Name: name.Name}})
}
