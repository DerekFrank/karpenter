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

package cost

import (
	"context"
	"fmt"
	"maps"
	"slices"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/overlay"
)

// NecessaryLabels defines the set of required Kubernetes labels that must be present
// on NodeClaim objects for cost tracking to function properly.
var NecessaryLabels = []string{corev1.LabelInstanceTypeStable, v1.CapacityTypeLabelKey, corev1.LabelTopologyZone, v1.NodePoolLabelKey}

var (
	CostTrackingErrorsTotal = opmetrics.NewPrometheusCounter(
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
)

// ClusterCost tracks the cost of compute resources across all NodePools in a cluster.
// This is an alpha-level component and its API may change without notice.
//
// It keeps three stores, and combines the last two when it's read:
//   - nodeClaims, an object store of the NodeClaims the NodeClaim informer observes and forgets;
//   - offerings, a synthetic store derived from nodeClaims, each NodePool's NodeClaims by offering;
//   - prices, a fed store of each NodePool's price of the offerings it counts, fed by RefreshPrices.
type ClusterCost struct {
	nodeClaims *state.Store[types.NamespacedName, *v1.NodeClaim, struct{}, struct{}]
	offerings  *state.Store[string, string, NodePoolOfferings, struct{}]
	prices     *state.Store[string, NodePoolPrices, struct{}, struct{}]

	cloudProvider cloudprovider.CloudProvider
	client        client.Client
}

// OfferingKey uniquely identifies a specific compute offering by its zone,
// capacity type (e.g., spot/on-demand), and instance type name.
// This is not a hard invariant
type OfferingKey struct {
	Zone, CapacityType, InstanceName string
}

// NodePoolOfferings are a NodePool's NodeClaims, derived from the NodeClaims with every NecessaryLabel. A NodeClaim
// stops counting once it's forgotten, or once it loses one of the labels.
type NodePoolOfferings struct {
	// Offerings counts the NodeClaims priced by their offering, by offering.
	Offerings map[OfferingKey]int // bounded: offerings of the NodePool's instance types
	// Fixed counts the NodeClaims the NodeClaimPriceProvider priced from the NodeClaim alone, by that price. Price
	// refreshes don't change them.
	Fixed map[float64]int // bounded: prices the NodePool's NodeClaims carry
}

// NodePoolPrices are a NodePool's prices of the offerings its NodeClaims count, set by the pricing refresh.
type NodePoolPrices map[OfferingKey]OfferingPrice

// OfferingPrice is an offering's price, and whether the NodePool's instance types had it when they were last read.
type OfferingPrice struct {
	Price float64
	Found bool
}

// nodeClaimOffering is what one NodeClaim contributes to its NodePool's offerings: one of its offering, or one of a
// price it carries itself.
type nodeClaimOffering struct {
	offering OfferingKey
	fixed    bool
	price    float64
	// n is 1, so that the zero contribution, a source new to the NodePool, adds nothing.
	n int
}

func (o nodeClaimOffering) Update(f NodePoolOfferings, prev nodeClaimOffering) NodePoolOfferings {
	next := NodePoolOfferings{Offerings: maps.Clone(f.Offerings), Fixed: maps.Clone(f.Fixed)}
	return prev.add(o.add(next, o.n), -prev.n).orNothing()
}

func (o nodeClaimOffering) Delete(f NodePoolOfferings) NodePoolOfferings {
	return o.add(NodePoolOfferings{Offerings: maps.Clone(f.Offerings), Fixed: maps.Clone(f.Fixed)}, -o.n).orNothing()
}

// add adds n of o to f's maps, which it owns.
func (o nodeClaimOffering) add(f NodePoolOfferings, n int) NodePoolOfferings {
	if n == 0 {
		return f
	}
	if o.fixed {
		f.Fixed = addCount(f.Fixed, o.price, n)
	} else {
		f.Offerings = addCount(f.Offerings, o.offering, n)
	}
	return f
}

func addCount[K comparable](m map[K]int, k K, n int) map[K]int {
	if m == nil {
		m = map[K]int{}
	}
	if m[k] += n; m[k] == 0 {
		delete(m, k)
	}
	return m
}

// orNothing drops empty maps, so a NodePool nothing counts toward leaves the store.
func (f NodePoolOfferings) orNothing() NodePoolOfferings {
	if len(f.Offerings) == 0 {
		f.Offerings = nil
	}
	if len(f.Fixed) == 0 {
		f.Fixed = nil
	}
	return f
}

// Options configure the cost tracker NewClusterCost builds.
type Options struct {
	nodeClaims state.Newest
}

// WithNodeClaimSource reads the newest NodeClaims from src rather than from the overlay client, for a benchmark or unit
// test that tracks cost without an API server, as cluster.WithSources does for cluster state.
func WithNodeClaimSource(src state.Newest) option.Function[Options] {
	return func(o *Options) { o.nodeClaims = src }
}

// NewClusterCost creates and initializes a new ClusterCost instance for tracking
// compute costs across the cluster. It requires a cloud provider for accessing
// instance type and pricing information, and a Kubernetes client for NodePool lookups.
// ctx is the context NodeClaims are priced with by the NodeClaimPriceProvider.
func NewClusterCost(ctx context.Context, cloudProvider cloudprovider.CloudProvider, kubeClient client.Client, opts ...option.Function[Options]) *ClusterCost {
	nodeClaims := option.Resolve(opts...).nodeClaims
	if nodeClaims == nil {
		overlaid, ok := kubeClient.(*overlay.Client)
		if !ok {
			panic("cost reads NodeClaims through an overlay.Client")
		}
		nodeClaims = lo.Must(overlaid.Source(&v1.NodeClaim{}))
	}
	managed := lo.Map(cloudProvider.GetSupportedNodeClasses(), func(nodeClass status.Object, _ int) schema.GroupKind {
		return object.GVK(nodeClass).GroupKind()
	})
	cc := &ClusterCost{
		nodeClaims:    state.NewObjectStore[types.NamespacedName, *v1.NodeClaim, struct{}, struct{}](nodeClaims),                      //storelint:allow cost moves to pkg/cluster (review 18)
		offerings:     state.NewSyntheticStore[string, string, NodePoolOfferings, struct{}](func(name string) string { return name }), //storelint:allow cost moves to pkg/cluster (review 18)
		prices:        state.NewFedStore[string, NodePoolPrices, struct{}, struct{}](),                                                //storelint:allow cost moves to pkg/cluster (review 18)
		cloudProvider: cloudProvider,
		client:        kubeClient,
	}
	// The store holds every NodeClaim; one of a node class the cloud provider doesn't manage counts toward nothing.
	//storelint:allow cost moves to pkg/cluster (review 18)
	state.DeriveObject(cc.nodeClaims.Reader, cc.offerings,
		func(_ types.NamespacedName, nodeClaim *v1.NodeClaim) string {
			if nodeClaim.Spec.NodeClassRef == nil || !slices.Contains(managed, nodeClaim.Spec.NodeClassRef.GroupKind()) || nodeClaimMissingLabels(nodeClaim) {
				return ""
			}
			return nodeClaim.Labels[v1.NodePoolLabelKey]
		},
		func(_ types.NamespacedName, nodeClaim *v1.NodeClaim) nodeClaimOffering {
			// A price the NodeClaim carries itself, such as one recorded at launch, is tracked as is. Other NodeClaims
			// are priced by their offering.
			if price, ok := cloudprovider.PriceNodeClaim(ctx, nodeClaim, nil); ok {
				return nodeClaimOffering{fixed: true, price: price, n: 1}
			}
			return nodeClaimOffering{offering: offeringOf(nodeClaim), n: 1}
		})
	return cc
}

// UpdateNodeClaim refreshes the prices of a NodeClaim's NodePool if they don't price its offering yet. The cost tracker
// follows each NodeClaim's newest version through the overlay itself; a NodeClaim without every NecessaryLabel counts
// nothing until it has them.
func (cc *ClusterCost) UpdateNodeClaim(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	if nodeClaimMissingLabels(nodeClaim) {
		// not a failure mode, as we expect to retry once the labels are propagated
		return nil
	}
	nodePoolName := nodeClaim.Labels[v1.NodePoolLabelKey]
	offering := offeringOf(nodeClaim)
	if counted, _ := cc.offerings.Get(nodePoolName); counted.Derived.Offerings[offering] == 0 {
		return nil
	}
	if prices, _ := cc.prices.Get(nodePoolName); priced(prices.Object, offering) {
		return nil
	}
	np := &v1.NodePool{}
	if err := cc.client.Get(ctx, client.ObjectKey{Name: nodePoolName}, np); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		CostTrackingErrorsTotal.Inc(map[string]string{metrics.NodePoolLabel: nodePoolName})
		return serrors.Wrap(err, "NodeClaim", klog.KObj(nodeClaim), "NodePool", klog.KRef("", nodePoolName))
	}
	if err := cc.RefreshPrices(ctx, np); err != nil {
		CostTrackingErrorsTotal.Inc(map[string]string{metrics.NodePoolLabel: nodePoolName})
		return serrors.Wrap(err, "NodeClaim", klog.KObj(nodeClaim))
	}
	return nil
}

// priced returns whether prices holds offering, found or not.
func priced(prices NodePoolPrices, offering OfferingKey) bool {
	_, ok := prices[offering]
	return ok
}

// DeleteNodePool drops a deleted NodePool's prices. Its NodeClaims count until they're deleted too, those priced by
// their offering at nothing, since nothing prices the NodePool's offerings any more.
func (cc *ClusterCost) DeleteNodePool(_ context.Context, npName string) {
	cc.dropPrices(npName)
}

// RefreshPrices reads a NodePool's instance types and prices the offerings it counts with them. It's the only reader
// of instance types, and runs from the pricing controller and for a NodeClaim of an offering its NodePool hasn't priced.
func (cc *ClusterCost) RefreshPrices(ctx context.Context, np *v1.NodePool) error {
	instanceTypes, err := cc.cloudProvider.GetInstanceTypes(ctx, np)
	if err != nil {
		return serrors.Wrap(fmt.Errorf("getting instance types, %w", err), "NodePool", klog.KObj(np))
	}
	cc.UpdateOfferings(ctx, np, instanceTypes)
	return nil
}

// UpdateOfferings prices the offerings a NodePool counts with instanceTypes, the NodePool's instance types.
func (cc *ClusterCost) UpdateOfferings(ctx context.Context, np *v1.NodePool, instanceTypes []*cloudprovider.InstanceType) {
	byName := make(map[string]*cloudprovider.InstanceType, len(instanceTypes))
	for _, it := range instanceTypes {
		if it != nil {
			byName[it.Name] = it
		}
	}
	var missing []OfferingKey
	e, _ := cc.offerings.Get(np.Name)
	var prices NodePoolPrices
	if counted := e.Derived; len(counted.Offerings) > 0 {
		prices = make(NodePoolPrices, len(counted.Offerings))
		for offering := range counted.Offerings {
			var p OfferingPrice
			if it, ok := byName[offering.InstanceName]; ok {
				p.Price, p.Found = it.OfferingPrice(offering.Zone, offering.CapacityType)
			}
			if !p.Found {
				missing = append(missing, offering)
			}
			prices[offering] = p
		}
	}
	cc.prices.Feed(np.Name, prices)
	for _, o := range missing {
		log.FromContext(ctx).Error(fmt.Errorf("offering not found"), "offering price unknown, cost tracking will undercount for its nodeclaims until it's found",
			"NodePool", klog.KObj(np), "instance-type", o.InstanceName, "zone", o.Zone, "capacity-type", o.CapacityType)
	}
}

// RetainPrices drops the prices of every NodePool not in names, the NodePools that exist, so prices set for a NodePool
// as it was deleted don't stay.
func (cc *ClusterCost) RetainPrices(names sets.Set[string]) {
	for name := range cc.prices.List() {
		if !names.Has(name) {
			cc.dropPrices(name)
		}
	}
}

// dropPrices drops the NodePool's prices.
func (cc *ClusterCost) dropPrices(name string) {
	cc.prices.Feed(name, nil)
}

// Reset empties the cost tracker's stores in place. It's for tests, between specs; storelint reports a Reset outside a
// test.
func (cc *ClusterCost) Reset() {
	cc.nodeClaims.Clear()
	cc.offerings.Clear()
	cc.prices.Clear()
}

// GetClusterCost returns the total cost of all compute resources across
// all NodePools in the cluster.
func (cc *ClusterCost) GetClusterCost() float64 {
	total := 0.0
	for name, offerings := range cc.offerings.List() {
		prices, _ := cc.prices.Get(name)
		total += nodePoolCost(offerings.Derived, prices.Object)
	}
	return total
}

// Sources are the stores a NodePool's cost is computed from, as sources that enqueue a NodePool, by name, whenever its
// offerings or their prices change, which is whenever GetNodepoolCost may return something new.
func (cc *ClusterCost) Sources() []source.SyncingSource {
	return []source.SyncingSource{cc.offerings.Source(), cc.prices.Source()}
}

// GetNodepoolCost returns the total cost of compute resources for a specific
// NodePool. Returns 0 if the NodePool is not being tracked.
func (cc *ClusterCost) GetNodepoolCost(np *v1.NodePool) float64 {
	offerings, _ := cc.offerings.Get(np.Name)
	prices, _ := cc.prices.Get(np.Name)
	return nodePoolCost(offerings.Derived, prices.Object)
}

// nodePoolCost combines a NodePool's offerings with its prices. An offering without a price counts nothing.
func nodePoolCost(offerings NodePoolOfferings, prices NodePoolPrices) float64 {
	cost := 0.0
	for price, n := range offerings.Fixed {
		cost += price * float64(n)
	}
	for offering, n := range offerings.Offerings {
		cost += prices[offering].Price * float64(n)
	}
	return cost
}

func offeringOf(nodeClaim *v1.NodeClaim) OfferingKey {
	return OfferingKey{
		CapacityType: nodeClaim.Labels[v1.CapacityTypeLabelKey],
		Zone:         nodeClaim.Labels[corev1.LabelTopologyZone],
		InstanceName: nodeClaim.Labels[corev1.LabelInstanceTypeStable],
	}
}

func nodeClaimMissingLabels(nc *v1.NodeClaim) bool {
	for _, key := range NecessaryLabels {
		if _, exists := nc.Labels[key]; !exists {
			return true
		}
	}
	return false
}
