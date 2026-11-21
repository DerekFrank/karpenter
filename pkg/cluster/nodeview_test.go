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
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Node model", func() {
	var s State
	BeforeEach(func() {
		s = newState(options.ToContext(context.Background(), test.Options()), clock.RealClock{}, fake.NewCloudProvider())
	})
	nodeClaim := func(name, providerID, rv string) *v1.NodeClaim {
		return &v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: rv, Labels: map[string]string{
				v1.NodePoolLabelKey: "default", "claim": "label",
			}},
			Spec:   v1.NodeClaimSpec{NodeClassRef: test.NodeClaim().Spec.NodeClassRef},
			Status: v1.NodeClaimStatus{ProviderID: providerID},
		}
	}
	// node is a Node with the NodePool label from kubelet registration, owned by owner if it's set.
	node := func(name, providerID, owner, rv string, extra map[string]string) *corev1.Node {
		labels := map[string]string{v1.NodePoolLabelKey: "default", corev1.LabelInstanceTypeStable: "x", "node": "label"}
		for k, v := range extra {
			labels[k] = v
		}
		n := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: rv, Labels: labels},
			Spec:       corev1.NodeSpec{ProviderID: providerID},
		}
		if owner != "" {
			n.OwnerReferences = []metav1.OwnerReference{{APIVersion: apis.Group + "/v1", Kind: "NodeClaim", Name: owner, UID: types.UID(owner)}}
		}
		return n
	}
	unmanagedNode := func(name, rv string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), ResourceVersion: rv}}
	}
	pod := func(name, nodeName, cpu string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name), ResourceVersion: "1"},
			Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			}}}},
		}
	}
	entry := func(name string) NodeClaimEntry {
		GinkgoHelper()
		e, ok := s.NodeClaims.Get(name, client.UnsafeDisableDeepCopy)
		Expect(ok).To(BeTrue())
		return e
	}
	unmanaged := func() []string {
		var names []string
		for name := range s.Nodes.Unmanaged() {
			names = append(names, name)
		}
		return names
	}

	It("should show a pod bound to a registered Node in its NodeClaim entry's usage", func() {
		s.NodeClaims.Put(nodeClaim("nc", "p1", "1"))
		Expect(InFlight(entry("nc"))).To(BeTrue(), "a NodeClaim without a Node is in flight")
		n := node("n1", "p1", "nc", "1", map[string]string{v1.NodeRegisteredLabelKey: "true"})
		s.Nodes.Put(n)
		e := entry("nc")
		Expect(e.Derived.Node).ToNot(BeNil())
		Expect(e.Derived.Node.Name).To(Equal("n1"))
		Expect(Registered(e)).To(BeTrue())
		Expect(e.Derived.Usage.Pods).To(BeZero())

		s.Pods.Put(pod("a", "n1", "1"))
		s.Pods.Put(pod("b", "n1", "500m"))
		e = entry("nc")
		Expect(e.Derived.Usage.Pods).To(Equal(2), "the pods bound to the Node, on its NodeClaim's entry")
		Expect(e.Derived.Usage.Requests.Cpu().String()).To(Equal("1500m"))
		requests := ViewOfNodeClaim(e).PodRequests()
		Expect(requests.Cpu().String()).To(Equal("1500m"))
		nodeEntry, _ := s.Nodes.Get("n1")
		Expect(nodeEntry.Derived.Usage.Pods).To(Equal(2), "the Node's own entry")

		s.Pods.Remove(types.NamespacedName{Namespace: "default", Name: "b"})
		Expect(entry("nc").Derived.Usage.Pods).To(Equal(1), "once a pod leaves")

		// Without the owner reference, the Node is registering again and the NodeClaim is in flight.
		s.Nodes.Put(node("n1", "p1", "", "2", map[string]string{v1.NodeRegisteredLabelKey: "true"}))
		e = entry("nc")
		Expect(e.Derived.Node).To(BeNil(), "the owner reference is gone")
		Expect(e.Derived.Usage.Pods).To(BeZero())
		Expect(InFlight(e)).To(BeTrue())

		s.Nodes.Put(node("n1", "p1", "nc", "3", map[string]string{v1.NodeRegisteredLabelKey: "true"}))
		Expect(entry("nc").Derived.Usage.Pods).To(Equal(1), "once the owner reference lands again")
		s.Nodes.Remove("n1")
		e = entry("nc")
		Expect(e.Derived.Node).To(BeNil(), "the Node is gone")
		Expect(e.Derived.Usage).To(Equal(NodeUsage{}))
	})
	It("should leave a registering Node out of Unmanaged, and read a Node with neither an owner nor the NodePool label as unmanaged", func() {
		s.NodeClaims.Put(nodeClaim("nc", "p1", "1"))
		s.Nodes.Put(node("registering", "p1", "", "1", nil))
		s.Nodes.Put(unmanagedNode("plain", "1"))
		Expect(unmanaged()).To(ConsistOf("plain"))
		Expect(InFlight(entry("nc"))).To(BeTrue(), "the registering Node's NodeClaim counts it, in flight")

		// A plain Node a NodeClaim's owner reference comes to name is managed.
		owned := unmanagedNode("plain", "2")
		owned.OwnerReferences = []metav1.OwnerReference{{APIVersion: apis.Group + "/v1", Kind: "NodeClaim", Name: "other", UID: "other"}}
		s.Nodes.Put(owned)
		Expect(unmanaged()).To(BeEmpty())
		s.Nodes.Put(unmanagedNode("plain", "3"))
		Expect(unmanaged()).To(ConsistOf("plain"))
		s.Nodes.Remove("plain")
		Expect(unmanaged()).To(BeEmpty())
	})
	It("should read a node's labels as capacity quotas do", func() {
		nc := nodeClaim("nc", "p1", "1")
		for _, c := range []struct {
			name string
			node *corev1.Node
		}{
			{"in flight", nil},
			{"unregistered Node", node("n", "p1", "nc", "1", nil)},
			{"registered Node", node("n", "p1", "nc", "1", map[string]string{v1.NodeRegisteredLabelKey: "true"})},
		} {
			view := ViewOfNodeClaim(NodeClaimEntry{Object: nc, Derived: NodeClaimDerived{Node: c.node}})
			Expect(view.Labels()).To(Equal(nodeLabels(nc, c.node)), c.name)
		}
		Expect(ViewOfNodeClaim(NodeClaimEntry{Object: nc}).Labels()).To(HaveKey("claim"), "in flight, the NodeClaim's labels")
		registered := node("n", "p1", "nc", "1", map[string]string{v1.NodeRegisteredLabelKey: "true"})
		Expect(ViewOfNodeClaim(NodeClaimEntry{Object: nc, Derived: NodeClaimDerived{Node: registered}}).Labels()).To(HaveKey("node"), "registered, the Node's labels")
		plain := unmanagedNode("plain", "1")
		plain.Labels = map[string]string{"unmanaged": "label"}
		Expect(ViewOfNode(NodeEntry{Object: plain}).Labels()).To(Equal(nodeLabels(nil, plain)), "an unmanaged Node")
	})
	It("should index the NodeClaims by providerID and the pods by their Node", func() {
		s.NodeClaims.Put(nodeClaim("a", "p1", "1"))
		s.NodeClaims.Put(nodeClaim("b", "", "1"))
		Expect(s.NodeClaimsByProviderID.Lookup("p1")).To(ConsistOf("a"))
		s.NodeClaims.Put(nodeClaim("b", "p2", "2"))
		Expect(s.NodeClaimsByProviderID.Lookup("p2")).To(ConsistOf("b"), "once b launches")
		s.NodeClaims.Remove("a")
		Expect(s.NodeClaimsByProviderID.Lookup("p1")).To(BeEmpty())

		s.Pods.Put(pod("x", "n1", "1"))
		s.Pods.Put(pod("y", "n1", "1"))
		s.Pods.Put(pod("z", "n2", "1"))
		names := func(pods []*corev1.Pod) []string {
			var out []string
			for _, p := range pods {
				out = append(out, p.Name)
			}
			return out
		}
		Expect(names(PodsOn(s.PodsByNode, "n1"))).To(ConsistOf("x", "y"))
		Expect(PodsOn(s.PodsByNode, "")).To(BeEmpty(), "a node in flight has no pods")
		held, _ := s.Pods.Get(types.NamespacedName{Namespace: "default", Name: "x"}, client.UnsafeDisableDeepCopy)
		for _, p := range PodsOn(s.PodsByNode, "n1") {
			Expect(p).ToNot(BeIdenticalTo(held.Object), "PodsOn copies")
		}
		moved := pod("y", "n2", "1")
		moved.ResourceVersion = "2"
		s.Pods.Put(moved)
		Expect(names(PodsOn(s.PodsByNode, "n1"))).To(ConsistOf("x"))
		Expect(names(PodsOn(s.PodsByNode, "n2"))).To(ConsistOf("y", "z"))
	})
	It("should recompute a pod's volumes on its Node and NodeClaim once its PVC's driver resolves", func() {
		s.NodeClaims.Put(nodeClaim("nc", "p1", "1"))
		s.Nodes.Put(node("n1", "p1", "nc", "1", map[string]string{v1.NodeRegisteredLabelKey: "true"}))
		p := pod("a", "n1", "1")
		p.Spec.Volumes = []corev1.Volume{{Name: "v", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "claim"}}}}
		s.Pods.Put(p)
		pvc := types.NamespacedName{Namespace: "default", Name: "claim"}
		Expect(entry("nc").Derived.Usage.Volumes).To(Equal(map[volume]int{{PVC: pvc}: 1}), "the PVC before its driver resolves")
		s.PVCs.store.Feed(pvc, PVCDriver{Driver: "csi", Resolved: true})
		Expect(entry("nc").Derived.Usage.Volumes).To(Equal(map[volume]int{{PVC: pvc, Driver: "csi"}: 1}), "on the NodeClaim once the driver resolves")
		n, _ := s.Nodes.Get("n1")
		Expect(n.Derived.Usage.Volumes).To(Equal(map[volume]int{{PVC: pvc, Driver: "csi"}: 1}), "on the Node")
		s.PVCs.Forget(pvc)
		Expect(entry("nc").Derived.Usage.Volumes).To(Equal(map[volume]int{{PVC: pvc}: 1}), "once the driver is forgotten")
	})
})
