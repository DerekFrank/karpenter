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

package expectations

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2" //nolint:revive
	. "github.com/onsi/gomega"    //nolint:revive
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
)

// Under the manager, cluster state's object stores follow the informers themselves. A suite's cluster state has no
// informers, so a suite reconciles one of the informer stand-ins below to show cluster state an object as the informer
// would: the object as the API server has it, or that it's gone. Each then runs the cluster state informer controller
// it wraps, which keeps what needs I/O to read.

// NodeClaimInformer shows cluster state the NodeClaim it reconciles, then runs next.
func NodeClaimInformer(kubeClient client.Client, cluster *state.Cluster, next reconcile.Reconciler) reconcile.Reconciler {
	return informerStandIn{kubeClient: kubeClient, next: next, object: func() client.Object { return &v1.NodeClaim{} },
		observe: func(_ context.Context, obj client.Object) error { cluster.NodeClaimStore.Observe(obj); return nil },
		forget:  cluster.NodeClaimStore.Forget,
	}
}

// NodeInformer shows cluster state the Node it reconciles and the pods bound to it, then runs next.
func NodeInformer(kubeClient client.Client, cluster *state.Cluster, next reconcile.Reconciler) reconcile.Reconciler {
	return informerStandIn{kubeClient: kubeClient, next: next, object: func() client.Object { return &corev1.Node{} },
		observe: func(ctx context.Context, obj client.Object) error {
			return observeNode(ctx, kubeClient, cluster, obj.(*corev1.Node))
		},
		forget: cluster.NodeStore.Forget,
	}
}

// PodInformer shows cluster state the pod it reconciles, then runs next.
func PodInformer(kubeClient client.Client, cluster *state.Cluster, next reconcile.Reconciler) reconcile.Reconciler {
	return informerStandIn{kubeClient: kubeClient, next: next, object: func() client.Object { return &corev1.Pod{} },
		observe: func(_ context.Context, obj client.Object) error { cluster.PodStore.Observe(obj); return nil },
		forget:  cluster.PodStore.Forget,
	}
}

type informerStandIn struct {
	kubeClient client.Client
	next       reconcile.Reconciler
	object     func() client.Object
	observe    func(context.Context, client.Object) error
	forget     func(client.Object)
}

func (i informerStandIn) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	obj := i.object()
	if err := i.kubeClient.Get(ctx, req.NamespacedName, obj, client.UnsafeDisableDeepCopy); err != nil {
		if !errors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
		obj.SetNamespace(req.Namespace)
		obj.SetName(req.Name)
		i.forget(obj)
	} else if err := i.observe(ctx, obj); err != nil {
		return reconcile.Result{}, err
	}
	return i.next.Reconcile(ctx, req)
}

// ExpectNodeObserved shows cluster state the Node and the pods bound to it, as reconciling NodeInformer does.
func ExpectNodeObserved(ctx context.Context, c client.Client, cluster *state.Cluster, node *corev1.Node) {
	GinkgoHelper()
	Expect(observeNode(ctx, c, cluster, node)).To(Succeed())
}

// observeNode shows cluster state the Node and the pods bound to it, with their PVCs' drivers, as the Node and pod
// informers and the pod informer controller would.
func observeNode(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, node *corev1.Node) error {
	cluster.NodeStore.Observe(node)
	if !karpentercluster.Tracked(node) {
		return nil
	}
	var pods corev1.PodList
	if err := kubeClient.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node.Name}, client.UnsafeDisableDeepCopy); err != nil {
		return fmt.Errorf("listing pods, %w", err)
	}
	listed := make([]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		listed[i] = &pods.Items[i]
	}
	if err := cluster.ResolvePVCDrivers(ctx, listed...); err != nil {
		return err
	}
	karpenterstate.Resync(cluster.PodStore, listed, cluster.PodsOnNodes.Sources(node.Name))
	return nil
}
