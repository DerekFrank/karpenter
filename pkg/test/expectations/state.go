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
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// Under the manager, cluster state's object stores follow the informers themselves. A suite's cluster state has no
// informers, so a suite reconciles one of the informer stand-ins below to show cluster state an object as the informer
// would: the object as the API server has it, or that it's gone. Each then runs the cluster state informer controller
// it wraps, which keeps what needs I/O to read.

// NodeClaimInformer shows cluster state the NodeClaim it reconciles, then runs next.
func NodeClaimInformer(kubeClient client.Client, cluster *state.Cluster, next reconcile.Reconciler) reconcile.Reconciler {
	return informerStandIn{kubeClient: kubeClient, next: next, object: func() client.Object { return &v1.NodeClaim{} },
		observe: func(_ context.Context, obj client.Object) error {
			cluster.NodeClaimStore.Observe(obj.(*v1.NodeClaim))
			return nil
		},
		forget: func(name types.NamespacedName) { cluster.NodeClaimStore.Forget(name.Name) },
	}
}

// NodeInformer shows cluster state the Node it reconciles, its CSINode and the pods bound to it, then runs next, if
// any.
func NodeInformer(kubeClient client.Client, cluster *state.Cluster, next reconcile.Reconciler) reconcile.Reconciler {
	return informerStandIn{kubeClient: kubeClient, next: next, object: func() client.Object { return &corev1.Node{} },
		observe: func(ctx context.Context, obj client.Object) error {
			return observeNode(ctx, kubeClient, cluster, obj.(*corev1.Node))
		},
		forget: func(name types.NamespacedName) {
			cluster.NodeStore.Forget(name.Name)
			cluster.CSINodeStore.Forget(name.Name)
		},
	}
}

// PodInformer shows cluster state the pod it reconciles, then runs next.
func PodInformer(kubeClient client.Client, cluster *state.Cluster, next reconcile.Reconciler) reconcile.Reconciler {
	return informerStandIn{kubeClient: kubeClient, next: next, object: func() client.Object { return &corev1.Pod{} },
		observe: func(_ context.Context, obj client.Object) error {
			cluster.PodStore.Observe(obj.(*corev1.Pod))
			return nil
		},
		forget: func(name types.NamespacedName) { cluster.PodStore.Forget(name) },
	}
}

type informerStandIn struct {
	kubeClient client.Client
	next       reconcile.Reconciler
	object     func() client.Object
	observe    func(context.Context, client.Object) error
	forget     func(types.NamespacedName)
}

func (i informerStandIn) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	obj := i.object()
	if err := i.kubeClient.Get(ctx, req.NamespacedName, obj, client.UnsafeDisableDeepCopy); err != nil {
		if !errors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
		i.forget(req.NamespacedName)
	} else if err := i.observe(ctx, obj); err != nil {
		return reconcile.Result{}, err
	}
	if i.next == nil {
		return reconcile.Result{}, nil
	}
	return i.next.Reconcile(ctx, req)
}

// ExpectNodeObserved shows cluster state the Node and the pods bound to it, as reconciling NodeInformer does.
func ExpectNodeObserved(ctx context.Context, c client.Client, cluster *state.Cluster, node *corev1.Node) {
	GinkgoHelper()
	Expect(observeNode(ctx, c, cluster, node)).To(Succeed())
}

// observeNode shows cluster state the Node, its CSINode and the pods bound to it, with their PVCs' drivers, as the Node,
// CSINode and pod informers and the pod informer controller would.
func observeNode(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, node *corev1.Node) error {
	cluster.NodeStore.Observe(node)
	csiNode := &storagev1.CSINode{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: node.Name}, csiNode); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("getting CSINode, %w", err)
		}
		cluster.CSINodeStore.Forget(node.Name)
	} else {
		cluster.CSINodeStore.Observe(csiNode)
	}
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
	cluster.PodStore.Resync(listed, cluster.PodsOnNodes.Sources(node.Name))
	return nil
}

// NodeClaimWrites returns kubeClient with the NodeClaims its writes return shown to cluster state as each write
// returns, as the overlay client's write notifications show them to the NodeClaim store under the manager. A suite
// gives it to the controller under test, whose writes the controller then reads back through cluster state.
func NodeClaimWrites(kubeClient client.Client, cluster *state.Cluster) client.Client {
	return nodeClaimWrites{Client: kubeClient, cluster: cluster}
}

type nodeClaimWrites struct {
	client.Client
	cluster *state.Cluster
}

func (w nodeClaimWrites) show(obj client.Object, err error) error {
	if nodeClaim, ok := obj.(*v1.NodeClaim); ok && err == nil {
		if nodeClaim.DeletionTimestamp != nil && len(nodeClaim.Finalizers) == 0 {
			w.cluster.NodeClaimStore.Forget(nodeClaim.Name)
		} else {
			w.cluster.NodeClaimStore.Observe(nodeClaim.DeepCopy())
		}
	}
	return err
}

func (w nodeClaimWrites) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return w.show(obj, w.Client.Create(ctx, obj, opts...))
}

func (w nodeClaimWrites) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return w.show(obj, w.Client.Update(ctx, obj, opts...))
}

func (w nodeClaimWrites) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return w.show(obj, w.Client.Patch(ctx, obj, patch, opts...))
}

// Delete shows cluster state the terminating NodeClaim a finalizer holds, or that the NodeClaim is gone.
func (w nodeClaimWrites) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	rv := obj.GetResourceVersion()
	if err := w.Client.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	if nodeClaim, ok := obj.(*v1.NodeClaim); ok && nodeClaim.ResourceVersion == rv {
		w.cluster.NodeClaimStore.Forget(nodeClaim.Name)
		return nil
	}
	return w.show(obj, nil)
}

func (w nodeClaimWrites) Status() client.SubResourceWriter { return w.SubResource("status") }

func (w nodeClaimWrites) SubResource(subResource string) client.SubResourceClient {
	return nodeClaimSubResourceWrites{SubResourceClient: w.Client.SubResource(subResource), parent: w}
}

type nodeClaimSubResourceWrites struct {
	client.SubResourceClient
	parent nodeClaimWrites
}

func (s nodeClaimSubResourceWrites) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return s.parent.show(obj, s.SubResourceClient.Update(ctx, obj, opts...))
}

func (s nodeClaimSubResourceWrites) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return s.parent.show(obj, s.SubResourceClient.Patch(ctx, obj, patch, opts...))
}
