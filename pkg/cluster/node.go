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
	"maps"
	"slices"
	"strings"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

// newNodeStore returns the Node store: the Nodes by name. A Node is cluster-scoped, so its name is its key, whatever
// namespace an object built outside the API server carries.
func newNodeStore() *state.Store[string, *corev1.Node] {
	return state.NewJoinStore(nil, nil, state.Join(func(node *corev1.Node) string { return node.Name },
		func(_ *corev1.Node, node *corev1.Node) *corev1.Node { return node }))
}

// Tracked returns whether cluster state reads a Node: an unmanaged Node always, and a managed one (with the NodePool
// label) once it has a providerID and either its instance type label or has initialized, so that a Node that's
// registering isn't read before its labels propagate. An untracked Node links to no NodeClaim and has no usage read.
func Tracked(node *corev1.Node) bool {
	if node.Labels[v1.NodePoolLabelKey] == "" {
		return true
	}
	return node.Spec.ProviderID != "" && (node.Labels[corev1.LabelInstanceTypeStable] != "" || node.Labels[v1.NodeInitializedLabelKey] != "")
}

// ownerNodeClaim returns the name of the NodeClaim in the Node's owner references, which registration sets
// (nodeclaimutils.UpdateNodeOwnerReferences), or "" for none.
func ownerNodeClaim(node *corev1.Node) string {
	for _, o := range node.OwnerReferences {
		if o.Kind == "NodeClaim" && strings.HasPrefix(o.APIVersion, apis.Group+"/") {
			return o.Name
		}
	}
	return ""
}

// bare returns whether a Node has neither a NodeClaim owner reference nor the NodePool label. A bare Node is unmanaged
// unless a NodeClaim has its providerID; a Node that isn't bare and has no NodeClaim is orphaned, and isn't a node in
// Nodes.
func bare(node *corev1.Node) bool {
	return ownerNodeClaim(node) == "" && node.Labels[v1.NodePoolLabelKey] == ""
}

// nodeOwnerKey is the NodeClaim a tracked Node links to by its owner reference.
func nodeOwnerKey(_ string, node *corev1.Node) string {
	if !Tracked(node) {
		return ""
	}
	return ownerNodeClaim(node)
}

// nodeAtProviderID is a tracked Node's contribution to its providerID: its name, and whether it's bare.
type nodeAtProviderID struct {
	name string
	bare bool
}

func (n nodeAtProviderID) Update(f ProviderIDFields, prev nodeAtProviderID) ProviderIDFields {
	f.Nodes = insertName(removeName(slices.Clone(f.Nodes), prev.name), n.name)
	f.BareNodes += lo.Ternary(n.bare, 1, 0) - lo.Ternary(prev.bare, 1, 0)
	return f
}

func (n nodeAtProviderID) Delete(f ProviderIDFields) ProviderIDFields {
	f.Nodes = removeName(slices.Clone(f.Nodes), n.name)
	f.BareNodes -= lo.Ternary(n.bare, 1, 0)
	return f
}

func (n nodeAtProviderID) Equal(o nodeAtProviderID) bool { return n == o }

func nodeProviderIDKey(_ string, node *corev1.Node) string {
	if !Tracked(node) {
		return ""
	}
	return node.Spec.ProviderID
}

func nodeAtProviderIDOf(_ string, node *corev1.Node) nodeAtProviderID {
	return nodeAtProviderID{name: node.Name, bare: bare(node)}
}

// bareNode is a bare Node's contribution to the candidate unmanaged Nodes: one Node. The candidates themselves are the
// Derive's sources.
func bareNodeKey(_ string, node *corev1.Node) nodeCountKey {
	return lo.Ternary(bare(node), bareNodes, 0)
}

// nodeOnNode is a tracked Node's contribution to the fields kept by its name: the Node itself, and the repair policies
// its conditions match. Readers that hold a Node's name read it with its usage in one lookup. Each Node is the only
// source of its key.
type nodeOnNode struct {
	node    *corev1.Node
	matches []repair.RepairPolicyMatch
}

func (n nodeOnNode) Update(f podFields, _ nodeOnNode) podFields {
	f.Node, f.RepairPolicyMatches = n.node, n.matches
	return f
}

func (n nodeOnNode) Delete(f podFields) podFields {
	f.Node, f.RepairPolicyMatches = nil, nil
	return f
}

func (n nodeOnNode) Equal(o nodeOnNode) bool {
	return n.node == o.node && slices.EqualFunc(n.matches, o.matches, repair.RepairPolicyMatch.Equal)
}

func nodeOnNodeKey(_ string, node *corev1.Node) string {
	return lo.Ternary(Tracked(node), node.Name, "")
}

// podFields are the fields kept for each Node by its name: the Node and its repair policy matches, fed by the Node
// store, and the fields derived from the pods bound to it, fed by the pod store. They exist as soon as a pod binds to
// the Node, whether or not cluster state has seen the Node yet, so pods bound before a node registers count once its
// Node links to its NodeClaim.
type podFields struct {
	// Node is the Node, while cluster state tracks it.
	Node *corev1.Node
	// RepairPolicyMatches are the repair policies matching the Node's conditions, regardless of toleration.
	RepairPolicyMatches []repair.RepairPolicyMatch

	Pods              int
	Requests          corev1.ResourceList
	Limits            corev1.ResourceList
	DaemonSetRequests corev1.ResourceList
	DaemonSetLimits   corev1.ResourceList
	// DisruptionCost sums the eviction costs of the non-DaemonSet pods with a positive cost.
	DisruptionCost float64
	HostPorts      *scheduling.HostPortUsage
	// PVCs counts the pods that mount each PersistentVolumeClaim. Its volume counts against the limit of its driver in
	// pvcDrivers.
	PVCs             map[types.NamespacedName]int
	AntiAffinityPods map[types.NamespacedName]*corev1.Pod
	// CSIDrivers are the drivers of the Node's CSINode, read when the Node last changed and set by SetCSIDrivers.
	CSIDrivers []storagev1.CSINodeDriver
}

// pvcFields are a PVC's CSI driver once resolved; Driver is empty for a PVC whose volume no driver limits.
type pvcFields struct {
	Resolved bool
	Driver   string
}

// podContribution is a pod's contribution to the Node it's bound to.
type podContribution struct {
	pod            *corev1.Pod
	requests       corev1.ResourceList
	limits         corev1.ResourceList
	daemonSet      bool
	disruptionCost float64
	hostPorts      []scheduling.HostPort
	pvcs           []types.NamespacedName
	antiAffinity   bool
}

func (s *NodeState) newPodContribution(_ types.NamespacedName, pod *corev1.Pod) podContribution {
	// The same as RequestsForPods and LimitsForPods, from one Ceiling.
	ceiling := resources.Ceiling(pod)
	requests, limits := resources.Merge(ceiling.Requests), resources.Merge(ceiling.Limits)
	requests[corev1.ResourcePods] = *resource.NewQuantity(1, resource.DecimalExponent)
	limits[corev1.ResourcePods] = *resource.NewQuantity(1, resource.DecimalExponent)
	p := podContribution{
		pod:          pod,
		requests:     requests,
		limits:       limits,
		daemonSet:    podutils.IsOwnedByDaemonSet(pod),
		hostPorts:    scheduling.GetHostPorts(pod),
		antiAffinity: podutils.HasRequiredPodAntiAffinity(pod),
	}
	for _, volume := range pod.Spec.Volumes {
		if name := volumeutil.PersistentVolumeClaimName(pod, volume); name != "" {
			p.pvcs = append(p.pvcs, types.NamespacedName{Namespace: pod.Namespace, Name: name})
		}
	}
	if !p.daemonSet {
		p.disruptionCost = max(0, disruptionutils.EvictionCost(s.ctx, pod))
	}
	return p
}

// Update replaces prev's contribution with p's, copying each map at most once.
//
//nolint:gocyclo
func (p podContribution) Update(f podFields, prev podContribution) podFields {
	if prev.pod == nil {
		f.Pods++
		f.Requests = resources.Merge(f.Requests, p.requests)
		f.Limits = resources.Merge(f.Limits, p.limits)
	} else {
		f.Requests = resources.Merge(f.Requests, p.requests)
		resources.SubtractFrom(f.Requests, prev.requests)
		f.Limits = resources.Merge(f.Limits, p.limits)
		resources.SubtractFrom(f.Limits, prev.limits)
	}
	if prev.daemonSet || p.daemonSet {
		f.DaemonSetRequests = resources.Merge(f.DaemonSetRequests, lo.Ternary(p.daemonSet, p.requests, nil))
		f.DaemonSetLimits = resources.Merge(f.DaemonSetLimits, lo.Ternary(p.daemonSet, p.limits, nil))
		if prev.daemonSet {
			resources.SubtractFrom(f.DaemonSetRequests, prev.requests)
			resources.SubtractFrom(f.DaemonSetLimits, prev.limits)
		}
	}
	f.DisruptionCost += p.disruptionCost - prev.disruptionCost
	key := client.ObjectKeyFromObject(p.pod)
	if len(prev.hostPorts) > 0 || len(p.hostPorts) > 0 {
		f.HostPorts = lo.TernaryF(f.HostPorts == nil, scheduling.NewHostPortUsage, f.HostPorts.DeepCopy)
		f.HostPorts.DeletePod(key)
		if len(p.hostPorts) > 0 {
			f.HostPorts.Add(p.pod, p.hostPorts)
		}
	}
	if len(prev.pvcs) > 0 || len(p.pvcs) > 0 {
		f.PVCs = maps.Clone(f.PVCs)
		f.PVCs = countPVCs(countPVCs(f.PVCs, prev.pvcs, -1), p.pvcs, 1)
	}
	if prev.antiAffinity || p.antiAffinity {
		f.AntiAffinityPods = maps.Clone(f.AntiAffinityPods)
		delete(f.AntiAffinityPods, key)
		if p.antiAffinity {
			f.AntiAffinityPods = lo.Assign(f.AntiAffinityPods, map[types.NamespacedName]*corev1.Pod{key: p.pod})
		}
	}
	return f
}

func (p podContribution) Delete(f podFields) podFields {
	key := client.ObjectKeyFromObject(p.pod)
	f.Pods--
	f.Requests = resources.Subtract(f.Requests, p.requests)
	f.Limits = resources.Subtract(f.Limits, p.limits)
	if p.daemonSet {
		f.DaemonSetRequests = resources.Subtract(f.DaemonSetRequests, p.requests)
		f.DaemonSetLimits = resources.Subtract(f.DaemonSetLimits, p.limits)
	}
	f.DisruptionCost -= p.disruptionCost
	if len(p.hostPorts) > 0 {
		f.HostPorts = f.HostPorts.DeepCopy()
		f.HostPorts.DeletePod(key)
	}
	if len(p.pvcs) > 0 {
		f.PVCs = countPVCs(maps.Clone(f.PVCs), p.pvcs, -1)
	}
	if p.antiAffinity {
		f.AntiAffinityPods = maps.Clone(f.AntiAffinityPods)
		delete(f.AntiAffinityPods, key)
	}
	if f.Pods == 0 {
		return podFields{Node: f.Node, RepairPolicyMatches: f.RepairPolicyMatches, CSIDrivers: f.CSIDrivers}
	}
	return f
}

// countPVCs adds by to the count of each of pvcs in counts, which it modifies, and drops the PVCs it counts to zero.
func countPVCs(counts map[types.NamespacedName]int, pvcs []types.NamespacedName, by int) map[types.NamespacedName]int {
	for _, pvc := range pvcs {
		if counts == nil {
			counts = map[types.NamespacedName]int{}
		}
		if counts[pvc] += by; counts[pvc] == 0 {
			delete(counts, pvc)
		}
	}
	return lo.Ternary(len(counts) == 0, nil, counts)
}

// Equal reports whether Update would leave the fields the same with either contribution.
//
//nolint:gocyclo
func (p podContribution) Equal(o podContribution) bool {
	if (p.pod == nil) != (o.pod == nil) || (p.pod != nil && client.ObjectKeyFromObject(p.pod) != client.ObjectKeyFromObject(o.pod)) {
		return false
	}
	// A pod with required anti-affinity holds itself in the fields.
	if p.antiAffinity && p.pod != o.pod {
		return false
	}
	return p.daemonSet == o.daemonSet && p.antiAffinity == o.antiAffinity && p.disruptionCost == o.disruptionCost &&
		resourcesEqual(p.requests, o.requests) && resourcesEqual(p.limits, o.limits) &&
		slices.EqualFunc(p.hostPorts, o.hostPorts, func(a, b scheduling.HostPort) bool {
			return a.Port == b.Port && a.Protocol == b.Protocol && a.IP.Equal(b.IP)
		}) &&
		slices.Equal(p.pvcs, o.pvcs)
}

// podNodeKey is the Node a pod's usage counts on: the Node it's bound to, until it's terminal.
func podNodeKey(_ types.NamespacedName, pod *corev1.Pod) string {
	return lo.Ternary(podutils.IsTerminal(pod), "", pod.Spec.NodeName)
}

// unmanaged returns whether a Node is unmanaged: it's bare, and no NodeClaim has its providerID.
func (s *NodeState) unmanaged(node *corev1.Node) bool {
	if !bare(node) {
		return false
	}
	if node.Spec.ProviderID == "" {
		return true
	}
	f, _ := s.providerIDs.Get(node.Spec.ProviderID)
	return len(f.NodeClaims) == 0
}

// linkedNode returns the fields kept by the name of the NodeClaim's Node: its usage, and the Node and its repair policy
// matches. The Node is the one whose owner reference names the NodeClaim, which its entry holds, or else, in the window
// before registration sets the owner reference, the Node with its providerID that no other NodeClaim owns. The fields
// have a nil Node if the NodeClaim has none.
func (s *NodeState) linkedNode(name string, f NodeClaimFields) podFields {
	if f.Node != nil {
		usage, _ := s.nodeUsage.Get(f.Node.Name)
		usage.Node, usage.RepairPolicyMatches = f.Node, f.RepairPolicyMatches
		return usage
	}
	nodeClaim := f.NodeClaim
	if nodeClaim.Status.ProviderID == "" {
		return podFields{}
	}
	ids, _ := s.providerIDs.Get(nodeClaim.Status.ProviderID)
	for _, n := range ids.Nodes {
		if f, _ := s.nodeUsage.Get(n); f.Node != nil {
			if owner := ownerNodeClaim(f.Node); owner == "" || owner == name {
				return f
			}
		}
	}
	return podFields{}
}

// SetCSIDrivers sets the drivers of the Node's CSINode, which the caller read when the Node changed.
func (s *NodeState) SetCSIDrivers(nodeName string, drivers []storagev1.CSINodeDriver) {
	s.nodeUsage.Update(nodeName, func(f podFields) podFields { f.CSIDrivers = drivers; return f })
}

// PVCDriverResolved returns whether cluster state holds the CSI driver of the PVC.
func (s *NodeState) PVCDriverResolved(key types.NamespacedName) bool {
	_, ok := s.pvcDrivers.Get(key)
	return ok
}

// SetPVCDriver sets the CSI driver of the PVC, which the caller resolved; "" is a PVC whose volume no driver limits.
func (s *NodeState) SetPVCDriver(key types.NamespacedName, driver string) {
	s.pvcDrivers.Update(key, func(pvcFields) pvcFields { return pvcFields{Resolved: true, Driver: driver} })
}

// ForgetPVC drops the PVC's driver.
func (s *NodeState) ForgetPVC(key types.NamespacedName) {
	s.pvcDrivers.Update(key, func(pvcFields) pvcFields { return pvcFields{} })
}

// ForPodsWithAntiAffinity calls the supplied function once for each pod with required anti affinity terms that is
// currently bound to a tracked Node. The pod returned may not be up-to-date with respect to status, however since the
// anti-affinity terms can't be modified, they will be correct.
func (s *NodeState) ForPodsWithAntiAffinity(fn func(p *corev1.Pod, n *corev1.Node) bool) {
	for _, f := range s.nodeUsage.List() {
		if len(f.AntiAffinityPods) == 0 || f.Node == nil {
			continue
		}
		for _, pod := range f.AntiAffinityPods {
			if !fn(pod, f.Node) {
				return
			}
		}
	}
}

// RepairPolicyMatcher returns the matcher cluster state matches Nodes with, or nil when node repair is disabled.
func (s *NodeState) RepairPolicyMatcher() *repair.RepairPolicyMatcher {
	return s.matcher
}
