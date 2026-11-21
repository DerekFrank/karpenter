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

// Package pricing keeps the prices of the offerings each NodePool's NodeClaims count, for cluster cost.
package pricing

import (
	"context"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
)

// refreshPeriod is how long a NodePool's prices hold before the controller reads its instance types again, for
// prices that change with time, such as spot prices.
const refreshPeriod = time.Hour

var CostTrackingErrorsTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodePoolSubsystem,
		Name:      "cost_tracker_errors_total",
		Help:      "Number of errors encountered during cost tracking operations. Labeled by nodepool.",
	},
	[]opmetrics.Label{
		metrics.NodePool,
	},
	opmetrics.Alpha,
)

// Controller prices each NodePool's offerings from its instance types: when the NodePool appears, when the offerings
// its NodeClaims count include one its prices don't price yet, and every refreshPeriod. It drops a NodePool's prices
// once the NodePool is gone.
//
// This whole pricing controller only exists because CP's surface InstanceType information
// via a poll-method. Long term, this controller should likely be removed in favor of refactoring the
// cloudprovider interface to a push model for instance type information:
// https://github.com/kubernetes-sigs/karpenter/issues/2605
type Controller struct {
	clock         clock.Clock
	nodePoolState *cluster.NodePoolState
	cost          *cluster.ClusterCost
	// refreshedAt is when each NodePool's prices were last read. The controller reconciles one NodePool at a time.
	refreshedAt map[string]time.Time
}

func NewController(clk clock.Clock, nodePoolState *cluster.NodePoolState, cost *cluster.ClusterCost) *Controller {
	return &Controller{
		clock:         clk,
		nodePoolState: nodePoolState,
		cost:          cost,
		refreshedAt:   map[string]time.Time{},
	}
}

func (c *Controller) Name() string {
	return "nodepool.pricing"
}

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	nodePool, ok := c.nodePoolState.Get(req.Name)
	if !ok {
		c.cost.DropPrices(req.Name)
		delete(c.refreshedAt, req.Name)
		return reconcile.Result{}, nil
	}
	if refreshedAt, ok := c.refreshedAt[req.Name]; ok && c.cost.Priced(req.Name) && c.clock.Since(refreshedAt) < refreshPeriod {
		return reconcile.Result{RequeueAfter: refreshPeriod - c.clock.Since(refreshedAt)}, nil //storelint:poll the cloud provider's prices
	}
	if err := c.cost.RefreshPrices(ctx, nodePool.Object); err != nil {
		CostTrackingErrorsTotal.Inc(map[string]string{metrics.NodePoolLabel: req.Name})
		return reconcile.Result{}, err
	}
	c.refreshedAt[req.Name] = c.clock.Now()
	return reconcile.Result{RequeueAfter: refreshPeriod}, nil //storelint:poll the cloud provider's prices
}

// Register wakes the controller on a NodePool appearing or going, and on a change of the set of offerings a NodePool's
// NodeClaims count.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(c.nodePoolState.SourceOn(func(string, cluster.NodePoolEntry) any { return nil })).
		WatchesRawSource(c.cost.OfferingsSource()).
		Complete(c)
}
