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
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

var ClusterStateNodesCount = opmetrics.NewPrometheusGauge(
	crmetrics.Registry,
	prometheus.GaugeOpts{
		Namespace: metrics.Namespace,
		Subsystem: "cluster_state",
		Name:      "node_count",
		Help:      "Current count of nodes in cluster state",
	},
	[]opmetrics.Label{},
	opmetrics.GA,
)

// ClaimsExpiredTotal counts claims that expired: the caller that reserved them never bound them to an object cluster
// state saw, or released them, such as after an ambiguous create failure. A steady rate means a caller drops
// reservations it should release.
var ClaimsExpiredTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: "cluster_state",
		Name:      "claims_expired_total",
		Help:      "Number of claims against limits that expired because the caller that reserved them never bound them to an object cluster state saw, or released them. Labeled by store.",
	},
	[]opmetrics.Label{metrics.ClaimStore},
	opmetrics.Alpha,
)

// countExpiry returns an OnExpire func that counts each expiry against store.
func countExpiry[K, H any](store opmetrics.Value) func(K, H) {
	return func(K, H) { ClaimsExpiredTotal.Inc(map[string]string{metrics.ClaimStoreLabel: store.Name}) }
}
