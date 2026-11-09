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
	"maps"
	"slices"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

// NodeUsage is what the pods bound to a Node use of it, on the Node's entry. A pod counts until it's terminal or gone.
type NodeUsage struct {
	Pods int
	// Terminating counts the pods being deleted, which no longer reschedule, and DoNotDisrupt the pods with the
	// do-not-disrupt annotation, which block the node's disruption.
	Terminating  int
	DoNotDisrupt int

	Requests          corev1.ResourceList // bounded: resource names
	Limits            corev1.ResourceList // bounded: resource names
	DaemonSetRequests corev1.ResourceList // bounded: resource names
	DaemonSetLimits   corev1.ResourceList // bounded: resource names
	// DisruptionCost sums the eviction costs of the non-DaemonSet pods with a positive cost.
	DisruptionCost float64
	HostPorts      *scheduling.HostPortUsage
	// Volumes counts the pods that mount each PersistentVolumeClaim, by the PVC and its CSI driver, which limits the
	// volumes a node attaches. A PVC whose driver isn't resolved yet has no driver until the PVC store resolves it.
	Volumes map[volume]int // bounded: PVCs the pods per node mount
	// AntiAffinityPods are the pods with required anti-affinity.
	AntiAffinityPods map[types.NamespacedName]*corev1.Pod // bounded: pods per node
}

// volume is a PersistentVolumeClaim a pod mounts, and its CSI driver.
type volume struct {
	PVC    types.NamespacedName
	Driver string
}

// podContribution is a pod's contribution to the Node it's bound to.
type podContribution struct {
	pod            *corev1.Pod
	requests       corev1.ResourceList
	limits         corev1.ResourceList
	daemonSet      bool
	terminating    bool
	doNotDisrupt   bool
	disruptionCost float64
	hostPorts      []scheduling.HostPort
	volumes        []volume
	antiAffinity   bool
}

// podContributionOf returns a pod's contribution to its Node. It reads the drivers of the pod's PVCs from pvcs, whose
// subscriber Touches the pod when one resolves. ctx carries the options that decide a pod's disruption cost.
func podContributionOf(ctx context.Context, pvcs *PVCs) func(types.NamespacedName, PodEntry) podContribution {
	return func(_ types.NamespacedName, e PodEntry) podContribution {
		pod := e.Object
		// The same as RequestsForPods and LimitsForPods, from one Ceiling.
		ceiling := resources.Ceiling(pod)
		requests, limits := resources.Merge(ceiling.Requests), resources.Merge(ceiling.Limits)
		requests[corev1.ResourcePods] = *resource.NewQuantity(1, resource.DecimalExponent)
		limits[corev1.ResourcePods] = *resource.NewQuantity(1, resource.DecimalExponent)
		_, doNotDisrupt := pod.Annotations[v1.DoNotDisruptAnnotationKey]
		p := podContribution{
			pod:          pod,
			requests:     requests,
			limits:       limits,
			daemonSet:    podutils.IsOwnedByDaemonSet(pod),
			terminating:  podutils.IsTerminating(pod),
			doNotDisrupt: doNotDisrupt,
			hostPorts:    scheduling.GetHostPorts(pod),
			antiAffinity: podutils.HasRequiredPodAntiAffinity(pod),
		}
		for _, v := range pod.Spec.Volumes {
			if name := volumeutil.PersistentVolumeClaimName(pod, v); name != "" {
				pvc := types.NamespacedName{Namespace: pod.Namespace, Name: name}
				p.volumes = append(p.volumes, volume{PVC: pvc, Driver: pvcs.driver(pvc)})
			}
		}
		if !p.daemonSet {
			p.disruptionCost = max(0, disruptionutils.EvictionCost(ctx, pod))
		}
		return p
	}
}

// Update replaces prev's contribution with p's, copying each map at most once.
//
//nolint:gocyclo
func (p podContribution) Update(d NodeDerived, prev podContribution) NodeDerived {
	f := d.Usage
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
	f.Terminating += lo.Ternary(p.terminating, 1, 0) - lo.Ternary(prev.terminating, 1, 0)
	f.DoNotDisrupt += lo.Ternary(p.doNotDisrupt, 1, 0) - lo.Ternary(prev.doNotDisrupt, 1, 0)
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
	if len(prev.volumes) > 0 || len(p.volumes) > 0 {
		f.Volumes = countVolumes(countVolumes(maps.Clone(f.Volumes), prev.volumes, -1), p.volumes, 1)
	}
	if prev.antiAffinity || p.antiAffinity {
		f.AntiAffinityPods = maps.Clone(f.AntiAffinityPods)
		delete(f.AntiAffinityPods, key)
		if p.antiAffinity {
			f.AntiAffinityPods = lo.Assign(f.AntiAffinityPods, map[types.NamespacedName]*corev1.Pod{key: p.pod})
		}
		f.AntiAffinityPods = lo.Ternary(len(f.AntiAffinityPods) == 0, nil, f.AntiAffinityPods)
	}
	d.Usage = f
	return d
}

func (p podContribution) Delete(d NodeDerived) NodeDerived {
	f := d.Usage
	key := client.ObjectKeyFromObject(p.pod)
	f.Pods--
	if f.Pods == 0 {
		d.Usage = NodeUsage{}
		return d
	}
	f.Requests = resources.Subtract(f.Requests, p.requests)
	f.Limits = resources.Subtract(f.Limits, p.limits)
	f.Terminating -= lo.Ternary(p.terminating, 1, 0)
	f.DoNotDisrupt -= lo.Ternary(p.doNotDisrupt, 1, 0)
	if p.daemonSet {
		f.DaemonSetRequests = resources.Subtract(f.DaemonSetRequests, p.requests)
		f.DaemonSetLimits = resources.Subtract(f.DaemonSetLimits, p.limits)
	}
	f.DisruptionCost -= p.disruptionCost
	if len(p.hostPorts) > 0 {
		f.HostPorts = f.HostPorts.DeepCopy()
		f.HostPorts.DeletePod(key)
	}
	if len(p.volumes) > 0 {
		f.Volumes = countVolumes(maps.Clone(f.Volumes), p.volumes, -1)
	}
	if p.antiAffinity {
		f.AntiAffinityPods = maps.Clone(f.AntiAffinityPods)
		delete(f.AntiAffinityPods, key)
		f.AntiAffinityPods = lo.Ternary(len(f.AntiAffinityPods) == 0, nil, f.AntiAffinityPods)
	}
	d.Usage = f
	return d
}

// countVolumes adds by to the count of each of volumes in counts, which it modifies, and drops the volumes it counts to
// zero.
func countVolumes(counts map[volume]int, volumes []volume, by int) map[volume]int {
	for _, v := range volumes {
		if counts == nil {
			counts = map[volume]int{}
		}
		if counts[v] += by; counts[v] == 0 {
			delete(counts, v)
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
		p.terminating == o.terminating && p.doNotDisrupt == o.doNotDisrupt &&
		resourcesEqual(p.requests, o.requests) && resourcesEqual(p.limits, o.limits) &&
		slices.EqualFunc(p.hostPorts, o.hostPorts, func(a, b scheduling.HostPort) bool {
			return a.Port == b.Port && a.Protocol == b.Protocol && a.IP.Equal(b.IP)
		}) &&
		slices.Equal(p.volumes, o.volumes)
}

// podNodeKey is the Node a pod's usage counts on: the Node it's bound to, until it's terminal.
func podNodeKey(_ types.NamespacedName, e PodEntry) string {
	return lo.Ternary(podutils.IsTerminal(e.Object), "", e.Object.Spec.NodeName)
}

// volumeUsage returns the node's volume usage: its pods' volumes by driver, against its CSINode's limits.
func (u NodeUsage) volumeUsage(drivers NodeDerived) *scheduling.VolumeUsage {
	usage := scheduling.NewVolumeUsage()
	if len(u.Volumes) > 0 {
		volumes := scheduling.Volumes{}
		for v := range u.Volumes {
			if v.Driver != "" {
				volumes.Add(v.Driver, v.PVC.String())
			}
		}
		usage.Insert(volumes)
	}
	for _, driver := range drivers.CSIDrivers {
		if driver.Allocatable == nil || driver.Allocatable.Count == nil {
			usage.AddUnbounded(driver.Name)
			continue
		}
		usage.AddLimit(driver.Name, int(*driver.Allocatable.Count))
	}
	return usage
}
