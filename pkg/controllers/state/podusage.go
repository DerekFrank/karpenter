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

package state

import (
	"context"
	"fmt"

	"github.com/awslabs/operatorpkg/serrors"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/scheduling"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// PodUsage is what the pods bound to a node use of it. Readers share it, so don't modify it; DeepCopy it instead.
type PodUsage struct {
	Requests corev1.ResourceList
	Limits   corev1.ResourceList
	// DaemonSetRequests and DaemonSetLimits are the part of Requests and Limits from DaemonSet pods, which lets users
	// of the node identify the resources future DaemonSets are expected to consume.
	DaemonSetRequests corev1.ResourceList
	DaemonSetLimits   corev1.ResourceList
	// DisruptionCost sums the positive eviction costs of the node's non-DaemonSet pods.
	DisruptionCost float64
	HostPorts      *scheduling.HostPortUsage
	Volumes        *scheduling.VolumeUsage
}

func newPodUsage() *PodUsage {
	return &PodUsage{HostPorts: scheduling.NewHostPortUsage(), Volumes: scheduling.NewVolumeUsage()}
}

func (u *PodUsage) DeepCopy() *PodUsage {
	return &PodUsage{
		Requests:          copyResources(u.Requests),
		Limits:            copyResources(u.Limits),
		DaemonSetRequests: copyResources(u.DaemonSetRequests),
		DaemonSetLimits:   copyResources(u.DaemonSetLimits),
		DisruptionCost:    u.DisruptionCost,
		HostPorts:         u.HostPorts.DeepCopy(),
		Volumes:           u.Volumes.DeepCopy(),
	}
}

func copyResources(in corev1.ResourceList) corev1.ResourceList {
	if in == nil {
		return nil
	}
	out := make(corev1.ResourceList, len(in))
	for name, quantity := range in {
		out[name] = quantity.DeepCopy()
	}
	return out
}

// podUsageFunc returns a func that returns the usage of the pods bound to a node, computing it if a pod or the node
// changed since it was last read. Computing reads the operator's options and logger from ctx, but outlives it. A failure
// to read a pod's volumes is logged, and leaves that pod's volumes out until the next read.
func (c *Cluster) podUsageFunc(ctx context.Context) func(nodeName string) *PodUsage {
	ctx = context.WithoutCancel(ctx)
	return func(nodeName string) *PodUsage {
		if nodeName == "" {
			return newPodUsage()
		}
		usage, err := c.podUsage.Get(ctx, nodeName)
		if err != nil {
			log.FromContext(ctx).WithValues("Node", klog.KRef("", nodeName)).Error(err, "failed computing pod usage")
		}
		return usage
	}
}

// computePodUsage sums the pods the cache has bound to the node, claiming each so that its delete, which only names
// it, invalidates this node.
func (c *Cluster) computePodUsage(ctx context.Context, nodeName string) (*PodUsage, error) {
	usage := newPodUsage()
	var errs error
	var csiNode storagev1.CSINode
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: nodeName}, &csiNode); client.IgnoreNotFound(err) != nil {
		errs = multierr.Append(errs, serrors.Wrap(fmt.Errorf("getting CSINode to determine volume limit, %w", err), "CSINode", klog.KRef("", nodeName)))
	}
	for _, driver := range csiNode.Spec.Drivers {
		if driver.Allocatable == nil || driver.Allocatable.Count == nil {
			usage.Volumes.AddUnbounded(driver.Name)
			continue
		}
		usage.Volumes.AddLimit(driver.Name, int(*driver.Allocatable.Count))
	}
	pods := &corev1.PodList{}
	if err := c.kubeClient.List(ctx, pods, client.MatchingFields{"spec.nodeName": nodeName}, client.UnsafeDisableDeepCopy); err != nil {
		return usage, multierr.Append(errs, fmt.Errorf("listing pods, %w", err))
	}
	counted, err := c.claim(ctx, nodeName, pods.Items)
	errs = multierr.Append(errs, err)
	for _, pod := range counted {
		requests, limits := resources.RequestsForPods(pod), resources.LimitsForPods(pod)
		usage.Requests = resources.MergeInto(usage.Requests, requests)
		usage.Limits = resources.MergeInto(usage.Limits, limits)
		if podutils.IsOwnedByDaemonSet(pod) {
			usage.DaemonSetRequests = resources.MergeInto(usage.DaemonSetRequests, requests)
			usage.DaemonSetLimits = resources.MergeInto(usage.DaemonSetLimits, limits)
		} else if cost := disruptionutils.EvictionCost(ctx, pod); cost > 0 {
			usage.DisruptionCost += cost
		}
		usage.HostPorts.Add(pod, scheduling.GetHostPorts(pod))
		volumes, err := scheduling.GetVolumes(ctx, c.kubeClient, pod)
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("tracking volume usage, %w", err))
			continue
		}
		usage.Volumes.Add(pod, volumes)
	}
	return usage, errs
}

// podUsageInputs are the fields of a bound pod its node's usage is computed from that can change while it's bound:
// its requests and limits follow in-place resizes, and its eviction cost follows its annotations. Its host ports and
// volumes come from its spec, which can't change.
type podUsageInputs struct {
	Phase        corev1.PodPhase
	Requests     corev1.ResourceList
	Limits       corev1.ResourceList
	DaemonSet    bool
	EvictionCost float64
}

func podUsageInputsFor(ctx context.Context, pod *corev1.Pod) (string, podUsageInputs) {
	return pod.Spec.NodeName, podUsageInputs{
		Phase:        pod.Status.Phase,
		Requests:     resources.RequestsForPods(pod),
		Limits:       resources.LimitsForPods(pod),
		DaemonSet:    podutils.IsOwnedByDaemonSet(pod),
		EvictionCost: disruptionutils.EvictionCost(ctx, pod),
	}
}

// claim returns the pods to count on the node: the cache's non-terminal pods bound to it. Claiming one cluster state
// doesn't track yet makes its delete invalidate the node; it counts only if the cache still holds it after that, since
// a delete before then went unnoticed.
func (c *Cluster) claim(ctx context.Context, nodeName string, pods []corev1.Pod) ([]*corev1.Pod, error) {
	var counted []*corev1.Pod
	var errs error
	for i := range pods {
		pod := &pods[i]
		if podutils.IsTerminal(pod) {
			continue
		}
		if c.pods.Claim(nodeName, pod) {
			if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				if errors.IsNotFound(err) {
					c.pods.Forget(client.ObjectKeyFromObject(pod))
					continue
				}
				errs = multierr.Append(errs, err)
			}
		}
		counted = append(counted, pod)
	}
	return counted, errs
}
