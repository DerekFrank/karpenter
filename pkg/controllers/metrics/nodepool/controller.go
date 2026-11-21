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

package nodepool

import (
	"context"
	"strings"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/metrics"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
)

// The NodePool gauges are built at scrape time from cluster state's NodePool entries and its cost, by the
// Collector Register registers.
var (
	Limit = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "limit",
			Help:      "Limits specified on the nodepool that restrict the quantity of resources provisioned. Labeled by nodepool name and resource type.",
		},
		[]opmetrics.Label{
			metrics.ResourceType,
			metrics.NodePool,
		},
		opmetrics.GA,
	)
	Usage = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "usage",
			Help:      "The amount of resources that have been provisioned for a nodepool. Labeled by nodepool name and resource type.",
		},
		[]opmetrics.Label{
			metrics.ResourceType,
			metrics.NodePool,
		},
		opmetrics.GA,
	)
	ClusterCost = metrics.NewCollectedGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "cost_total",
			Help:      "Total cost of the nodepool from Karpenter's perspective. Units are determined by the cloud provider. Not an authoritative source for billing. Includes modifications due to NodeOverlays",
		},
		[]opmetrics.Label{metrics.NodePool},
		opmetrics.Alpha,
	)
)

type Controller struct {
	nodePools     *karpentercluster.NodePoolState
	cloudProvider cloudprovider.CloudProvider
	clusterCost   *karpentercluster.ClusterCost
}

// NewController returns the NodePool metrics, which read cluster state's NodePools and their cost at scrape time.
func NewController(nodePools *karpentercluster.NodePoolState, cloudProvider cloudprovider.CloudProvider, clusterCost *karpentercluster.ClusterCost) *Controller {
	return &Controller{nodePools: nodePools, cloudProvider: cloudProvider, clusterCost: clusterCost}
}

func (c *Controller) Name() string {
	return "metrics.nodepool"
}

// Register registers the Collector. Nothing reconciles: each scrape reads the stores.
func (c *Controller) Register(_ context.Context, _ manager.Manager) error {
	return crmetrics.Registry.Register(c.collector())
}

// collector builds the NodePool gauges at scrape time, once the NodePool store is synced.
func (c *Controller) collector() *metrics.Collector {
	return metrics.NewCollector(c.nodePools.Synced, c.collect, Limit, Usage, ClusterCost)
}

// collect emits each managed NodePool's limits, usage and cost. It reads the store's NodePools, copying none.
func (c *Controller) collect(emit metrics.Emit) {
	for _, e := range c.nodePools.List(client.UnsafeDisableDeepCopy) {
		nodePool := e.Object
		if !nodepoolutils.IsManaged(nodePool, c.cloudProvider) {
			continue
		}
		emit.Gauge(ClusterCost, c.clusterCost.GetNodepoolCost(nodePool), nodePool.Name)
		for _, g := range []struct {
			gauge     *metrics.CollectedGauge
			resources corev1.ResourceList
		}{
			{Usage, nodePool.Status.Resources},
			{Limit, corev1.ResourceList(nodePool.Spec.Limits)},
		} {
			for k, v := range g.resources {
				emit.Gauge(g.gauge, lo.Ternary(k == corev1.ResourceCPU, float64(v.MilliValue())/float64(1000), float64(v.Value())),
					strings.ReplaceAll(strings.ToLower(string(k)), "-", "_"), nodePool.Name)
			}
		}
	}
}
