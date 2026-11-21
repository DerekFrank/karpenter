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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Node model", func() {
	var s State
	BeforeEach(func() {
		s = newSuiteState(testContext())
	})
	nodeClaim := func(name, providerID string) *v1.NodeClaim {
		return test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1.NodePoolLabelKey: "default", "claim": "label"}},
			Status:     v1.NodeClaimStatus{ProviderID: providerID},
		})
	}
	unlaunched := func(name string) *v1.NodeClaim {
		nc := nodeClaim(name, "")
		nc.Status.ProviderID = ""
		return nc
	}
	// node is a Node with the NodePool label from kubelet registration, owned by owner if it's set.
	node := func(name, providerID, owner string, extra map[string]string) *corev1.Node {
		labels := map[string]string{v1.NodePoolLabelKey: "default", corev1.LabelInstanceTypeStable: "x", "node": "label"}
		for k, v := range extra {
			labels[k] = v
		}
		n := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Spec:       corev1.NodeSpec{ProviderID: providerID},
		}
		if owner != "" {
			n.OwnerReferences = []metav1.OwnerReference{{APIVersion: apis.Group + "/v1", Kind: "NodeClaim", Name: owner, UID: types.UID(owner)}}
		}
		return n
	}
	unmanagedNode := func(name string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	pod := func(name, nodeName, cpu string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Name: "c", Image: "c", Resources: corev1.ResourceRequirements{
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
		apply(nodeClaim("nc", "fake://p1"))
		Expect(InFlight(entry("nc"))).To(BeTrue(), "a NodeClaim without a Node is in flight")
		apply(node("n1", "fake://p1", "nc", map[string]string{v1.NodeRegisteredLabelKey: "true"}))
		e := entry("nc")
		Expect(e.Derived.Node).ToNot(BeNil())
		Expect(e.Derived.Node.Name).To(Equal("n1"))
		Expect(Registered(e)).To(BeTrue())
		Expect(e.Derived.Usage.Pods).To(BeZero())

		b := pod("b", "n1", "500m")
		apply(pod("a", "n1", "1"), b)
		e = entry("nc")
		Expect(e.Derived.Usage.Pods).To(Equal(2), "the pods bound to the Node, on its NodeClaim's entry")
		Expect(e.Derived.Usage.Requests.Cpu().String()).To(Equal("1500m"))
		requests := ViewOfNodeClaim(e).PodRequests()
		Expect(requests.Cpu().String()).To(Equal("1500m"))
		nodeEntry, _ := s.Nodes.Get("n1")
		Expect(nodeEntry.Derived.Usage.Pods).To(Equal(2), "the Node's own entry")

		remove(b)
		Expect(entry("nc").Derived.Usage.Pods).To(Equal(1), "once a pod leaves")

		// Without the owner reference, the Node is registering again and the NodeClaim is in flight.
		apply(node("n1", "fake://p1", "", map[string]string{v1.NodeRegisteredLabelKey: "true"}))
		e = entry("nc")
		Expect(e.Derived.Node).To(BeNil(), "the owner reference is gone")
		Expect(e.Derived.Usage.Pods).To(BeZero())
		Expect(InFlight(e)).To(BeTrue())

		n := node("n1", "fake://p1", "nc", map[string]string{v1.NodeRegisteredLabelKey: "true"})
		apply(n)
		Expect(entry("nc").Derived.Usage.Pods).To(Equal(1), "once the owner reference lands again")
		remove(n)
		e = entry("nc")
		Expect(e.Derived.Node).To(BeNil(), "the Node is gone")
		Expect(e.Derived.Usage).To(Equal(NodeUsage{}))
	})
	It("should leave a registering Node out of Unmanaged, and read a Node with neither an owner nor the NodePool label as unmanaged", func() {
		apply(nodeClaim("nc", "fake://p1"), node("registering", "fake://p1", "", nil), unmanagedNode("plain"))
		Expect(unmanaged()).To(ConsistOf("plain"))
		Expect(InFlight(entry("nc"))).To(BeTrue(), "the registering Node's NodeClaim counts it, in flight")

		// A plain Node a NodeClaim's owner reference comes to name is managed.
		owned := unmanagedNode("plain")
		owned.OwnerReferences = []metav1.OwnerReference{{APIVersion: apis.Group + "/v1", Kind: "NodeClaim", Name: "other", UID: "other"}}
		apply(owned)
		Expect(unmanaged()).To(BeEmpty())
		plain := unmanagedNode("plain")
		apply(plain)
		Expect(unmanaged()).To(ConsistOf("plain"))
		remove(plain)
		Expect(unmanaged()).To(BeEmpty())
	})
	It("should read a node's labels as capacity quotas do", func() {
		nc := nodeClaim("nc", "fake://p1")
		for _, c := range []struct {
			name string
			node *corev1.Node
		}{
			{"in flight", nil},
			{"unregistered Node", node("n", "fake://p1", "nc", nil)},
			{"registered Node", node("n", "fake://p1", "nc", map[string]string{v1.NodeRegisteredLabelKey: "true"})},
		} {
			view := ViewOfNodeClaim(NodeClaimEntry{Object: nc, Derived: NodeClaimDerived{Node: c.node}})
			Expect(view.Labels()).To(Equal(nodeLabels(nc, c.node)), c.name)
		}
		Expect(ViewOfNodeClaim(NodeClaimEntry{Object: nc}).Labels()).To(HaveKey("claim"), "in flight, the NodeClaim's labels")
		registered := node("n", "fake://p1", "nc", map[string]string{v1.NodeRegisteredLabelKey: "true"})
		Expect(ViewOfNodeClaim(NodeClaimEntry{Object: nc, Derived: NodeClaimDerived{Node: registered}}).Labels()).To(HaveKey("node"), "registered, the Node's labels")
		plain := unmanagedNode("plain")
		plain.Labels = map[string]string{"unmanaged": "label"}
		Expect(ViewOfNode(NodeEntry{Object: plain}).Labels()).To(Equal(nodeLabels(nil, plain)), "an unmanaged Node")
	})
	It("should index the NodeClaims by providerID and the pods by their Node", func() {
		a := nodeClaim("a", "fake://p1")
		apply(a, unlaunched("b"))
		Expect(s.NodeClaimsByProviderID.Lookup("fake://p1")).To(ConsistOf("a"))
		apply(nodeClaim("b", "fake://p2"))
		Expect(s.NodeClaimsByProviderID.Lookup("fake://p2")).To(ConsistOf("b"), "once b launches")
		remove(a)
		Expect(s.NodeClaimsByProviderID.Lookup("fake://p1")).To(BeEmpty())

		apply(pod("x", "n1", "1"), pod("y", "n1", "1"), pod("z", "n2", "1"))
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
		// A pod's spec.nodeName is set once, so a pod moves to another Node as a new pod under its name.
		y := pod("y", "n1", "1")
		remove(y)
		apply(pod("y", "n2", "1"))
		Expect(names(PodsOn(s.PodsByNode, "n1"))).To(ConsistOf("x"))
		Expect(names(PodsOn(s.PodsByNode, "n2"))).To(ConsistOf("y", "z"))
	})
	It("should recompute a pod's volumes on its Node and NodeClaim once its PVC's driver resolves", func() {
		apply(nodeClaim("nc", "fake://p1"), node("n1", "fake://p1", "nc", map[string]string{v1.NodeRegisteredLabelKey: "true"}))
		p := pod("a", "n1", "1")
		p.Spec.Volumes = []corev1.Volume{{Name: "v", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "claim"}}}}
		apply(p)
		pvc := types.NamespacedName{Namespace: "default", Name: "claim"}
		Expect(entry("nc").Derived.Usage.Volumes).To(Equal(map[volume]int{{PVC: pvc}: 1}), "the PVC before its driver resolves")
		// The PVC driver controller feeds the PVC store.
		s.PVCs.store.Feed(pvc, PVCDriver{Driver: "csi", Resolved: true})
		Expect(entry("nc").Derived.Usage.Volumes).To(Equal(map[volume]int{{PVC: pvc, Driver: "csi"}: 1}), "on the NodeClaim once the driver resolves")
		n, _ := s.Nodes.Get("n1")
		Expect(n.Derived.Usage.Volumes).To(Equal(map[volume]int{{PVC: pvc, Driver: "csi"}: 1}), "on the Node")
		s.PVCs.Forget(pvc)
		Expect(entry("nc").Derived.Usage.Volumes).To(Equal(map[volume]int{{PVC: pvc}: 1}), "once the driver is forgotten")
	})
})
