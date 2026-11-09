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
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	autoscalingv1beta1 "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// CapacityQuotaFields are the fields derived about each CapacityQuota, on its entry, by name.
type CapacityQuotaFields struct {
	// Used is what counts against the quota: for each NodeClaim the quota selects, its status.capacity and one
	// resources.Node, fed by the NodeClaim store; and for each Node without a NodePool label the quota selects, its
	// capacity and one resources.Node, fed by the Node store. A NodeClaim that hasn't launched counts as one
	// resources.Node against every quota it may match, and once launched, against the quotas that select its node's
	// labels. A NodeClaim stops counting once it has a deletionTimestamp or its DisruptionReason condition is true, as
	// it does in NodePoolFields.Usage.
	Used corev1.ResourceList // bounded: resource names
	// Reserved is what is held against the quota for NodeClaims a controller is creating and whose launched capacity
	// Used doesn't count yet: the most each could launch with, and one resources.Node, held by claims.
	Reserved corev1.ResourceList // bounded: resource names
}

// CapacityQuotaIntent is what the quota controller decided about a CapacityQuota: the quota it enforces.
type CapacityQuotaIntent struct {
	Enforced state.Intended[enforcement]
}

// enforcement is the version of a CapacityQuota the quota controller last enforced, including one that isn't
// enforced because it's invalid, and the quota it enforces from it, or nil for an invalid one.
type enforcement struct {
	applied quotaSpec
	quota   *Quota
}

// CapacityQuotaEntry is a CapacityQuota's entry.
type CapacityQuotaEntry = state.Entry[*autoscalingv1beta1.CapacityQuota, CapacityQuotaFields, CapacityQuotaIntent]

// quotaSpec is a CapacityQuota's identity and spec, to tell whether the enforced quota is built from the version the
// store holds.
type quotaSpec struct {
	exists bool
	uid    types.UID
	spec   autoscalingv1beta1.CapacityQuotaSpec
}

func specOf(capacityQuota *autoscalingv1beta1.CapacityQuota) quotaSpec {
	return quotaSpec{exists: true, uid: capacityQuota.UID, spec: capacityQuota.Spec}
}

func (q quotaSpec) Equal(o quotaSpec) bool {
	return q.exists == o.exists && q.uid == o.uid && equality.Semantic.DeepEqual(q.spec, o.spec)
}

// pendingQuotas is a quota whose spec isn't applied yet, counted in CapacityQuotaState.pending.
type pendingQuotas int

func (p pendingQuotas) Update(f pendingQuotas, prev pendingQuotas) pendingQuotas { return f + p - prev }
func (p pendingQuotas) Delete(f pendingQuotas) pendingQuotas                     { return f - p }

// Quota is a CapacityQuota as Karpenter enforces it: its limits and selector, with the Derives that count NodeClaims and
// Nodes against it and the claims held against it. Its selector is captured when it's built, so its Derives are pure;
// a quota whose spec changes is rebuilt.
type Quota struct {
	Name string
	// Limits are the quota's spec.limits.resources.
	Limits corev1.ResourceList

	uid  types.UID
	spec autoscalingv1beta1.CapacityQuotaSpec
	// selector matches the labels of a node that exists.
	selector labels.Selector
	// requirements are the selector as node requirements, to tell whether a NodeClaim that hasn't launched may match.
	requirements scheduling.Requirements

	nodeClaims *state.Derivation[string, string, CapacityQuotaFields, quotaUsage]
	nodes      *state.Derivation[string, string, CapacityQuotaFields, quotaUsage]
	claims     *state.Claims[string, string, CapacityQuotaFields, quotaUsage, quotaHeld]
}

// Matches returns whether the quota selects a node with these labels.
func (q *Quota) Matches(nodeLabels map[string]string) bool {
	return q.selector.Matches(labels.Set(nodeLabels))
}

// MayMatch returns whether a node launched with these requirements may be one the quota selects. A well known label
// the requirements leave open, such as a zone, may take any value, so a quota on it may match; a custom label they
// don't define won't be on the node.
func (q *Quota) MayMatch(requirements scheduling.Requirements) bool {
	return requirements.IsCompatible(q.requirements, scheduling.AllowUndefinedWellKnownLabels)
}

// MayMatchAny returns whether a node launched with any of these requirements may be one the quota selects.
func (q *Quota) MayMatchAny(requirements []scheduling.Requirements) bool {
	return lo.ContainsBy(requirements, q.MayMatch)
}

// LaunchRequirements returns the requirements of a node launched as each of these instance types under requirements,
// to tell which quotas a NodeClaim that hasn't launched may match.
func LaunchRequirements(requirements scheduling.Requirements, instanceTypes []*cloudprovider.InstanceType) []scheduling.Requirements {
	return lo.Map(instanceTypes, func(it *cloudprovider.InstanceType, _ int) scheduling.Requirements {
		r := scheduling.NewRequirements(requirements.Values()...)
		r.Add(it.Requirements.Values()...)
		return r
	})
}

// newQuota parses a CapacityQuota. An empty or missing selector selects every node.
func newQuota(capacityQuota *autoscalingv1beta1.CapacityQuota) (*Quota, error) {
	q := &Quota{
		Name:         capacityQuota.Name,
		Limits:       corev1.ResourceList{},
		uid:          capacityQuota.UID,
		spec:         capacityQuota.Spec,
		selector:     labels.Everything(),
		requirements: scheduling.NewRequirements(),
	}
	for name, quantity := range capacityQuota.Spec.Limits.Resources {
		q.Limits[corev1.ResourceName(name)] = quantity
	}
	if s := capacityQuota.Spec.Selector; s != nil {
		selector, err := metav1.LabelSelectorAsSelector(s)
		if err != nil {
			return nil, fmt.Errorf("parsing selector, %w", err)
		}
		q.selector = selector
		for key, value := range s.MatchLabels {
			q.requirements.Add(scheduling.NewRequirement(key, corev1.NodeSelectorOpIn, value))
		}
		for _, e := range s.MatchExpressions {
			q.requirements.Add(scheduling.NewRequirement(e.Key, corev1.NodeSelectorOperator(e.Operator), e.Values...))
		}
	}
	return q, nil
}

// nodeClaimKey is the quota a NodeClaim counts against: the quota's name while the NodeClaim counts and the quota
// selects its node's labels once it has launched capacity, or may select its requirements before.
func (q *Quota) nodeClaimKey(nodes *Nodes) func(string, NodeClaimEntry) string {
	return func(_ string, e NodeClaimEntry) string {
		nodeClaim := e.Object
		if !nodeClaim.DeletionTimestamp.IsZero() || conditionTrue(nodeClaim, v1.ConditionTypeDisruptionReason) {
			return ""
		}
		if len(nodeClaim.Status.Capacity) > 0 {
			return lo.Ternary(q.Matches((&StateNode{Node: nodes.linked(e), NodeClaim: nodeClaim}).Labels()), q.Name, "")
		}
		requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
		requirements.Add(scheduling.NewLabelRequirements(nodeClaim.Labels).Values()...)
		return lo.Ternary(q.MayMatch(requirements), q.Name, "")
	}
}

// nodeKey is the quota a Node Karpenter doesn't manage counts against: the quota's name if it selects the Node. A Node
// with a NodePool label counts through its NodeClaim.
func (q *Quota) nodeKey(_ string, node *corev1.Node) string {
	if node.Labels[v1.NodePoolLabelKey] != "" || !q.Matches(node.Labels) {
		return ""
	}
	return q.Name
}

func nodeClaimQuotaUsage(_ string, e NodeClaimEntry) quotaUsage {
	return quotaUsage{
		usage:           lo.Assign(e.Object.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")}),
		capacityCounted: len(e.Object.Status.Capacity) > 0,
	}
}

func nodeQuotaUsage(_ string, node *corev1.Node) quotaUsage {
	return quotaUsage{usage: lo.Assign(node.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")}), capacityCounted: true}
}

// CapacityQuotaState holds each CapacityQuota's entry, by name, and the reservations against them. Each enforced quota
// has its own Derives into the CapacityQuota store, and its own claims, so that a NodeClaim that may match several
// quotas is reserved against them one at a time, in sorted name order. The enforced quota and the spec it was built
// from are the quota's intent, which the quota controller sets as it rebuilds them from the CapacityQuota store.
type CapacityQuotaState struct {
	// quotas is the CapacityQuota object store, which follows the informer once State.Register starts it with the
	// CapacityQuota feature gate, and which each quota's Derives feed.
	quotas *state.Store[string, *autoscalingv1beta1.CapacityQuota, CapacityQuotaFields, CapacityQuotaIntent]
	// enforced is the handle of the quota intent, which the quota controller holds.
	enforced *state.Intent[string, *autoscalingv1beta1.CapacityQuota, CapacityQuotaFields, CapacityQuotaIntent, enforcement]
	// ctx carries the options, read once State.Register decides whether the CapacityQuota store follows the informer.
	ctx context.Context
	// pending counts, under the key true, the quotas whose spec isn't applied yet, derived from quotas.
	pending *state.Store[bool, bool, pendingQuotas, struct{}]
	nodes   *NodeState
	clock   clock.Clock
	log     logr.Logger

	// rebuilding serializes the controller's rebuilds. No reader takes it.
	rebuilding sync.Mutex
	// built are the quotas the controller built Derives for, by name, until it removes them: a quota's intent drops
	// with its CapacityQuota, and its Derives must still be removed. Only the controller reads it, under rebuilding.
	built map[string]*Quota // bounded: CapacityQuotas
}

// NewCapacityQuotaState returns the CapacityQuotas' entries, derived from the node state's NodeClaim and Node stores.
// Its CapacityQuota store follows from, the newest version of each CapacityQuota, and the controller State.Register
// adds enforces each quota it holds, which it does only with the CapacityQuota feature gate.
func NewCapacityQuotaState(ctx context.Context, clk clock.Clock, from state.Newest, nodes *NodeState) *CapacityQuotaState {
	quotas := state.NewObjectStore[string, *autoscalingv1beta1.CapacityQuota, CapacityQuotaFields, CapacityQuotaIntent](from, state.WithKey(state.ByName))
	s := &CapacityQuotaState{
		quotas:   quotas,
		enforced: state.NewIntent(quotas, func(i *CapacityQuotaIntent) *state.Intended[enforcement] { return &i.Enforced }),
		ctx:      ctx,
		pending:  state.NewSyntheticStore[bool, bool, pendingQuotas, struct{}](func(k bool) bool { return k }),
		nodes:    nodes,
		clock:    clk,
		log:      log.FromContext(ctx),
		built:    map[string]*Quota{},
	}
	state.Derive(quotas.Reader, s.pending,
		func(_ string, e CapacityQuotaEntry) bool {
			return !specOf(e.Object).Equal(e.Intent.Enforced.Value.applied)
		},
		func(string, CapacityQuotaEntry) pendingQuotas { return 1 })
	return s
}

// QuotaController enforces each CapacityQuota as the CapacityQuota store holds it, reconciling the quotas the store's
// source enqueues.
type QuotaController struct{ s *CapacityQuotaState }

func (c *QuotaController) Name() string { return "cluster.capacityquota" }

// Reconcile rebuilds the quota the request names, from the CapacityQuota store.
func (c *QuotaController) Reconcile(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
	c.s.rebuild(req.Name)
	return reconcile.Result{}, nil
}

// Controller returns the controller that enforces each CapacityQuota, which State.Register adds with the CapacityQuota
// store.
func (s *CapacityQuotaState) Controller() *QuotaController { return &QuotaController{s: s} }

func (s *CapacityQuotaState) register(m manager.Manager) error {
	c := s.Controller()
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(s.quotas.SourceOn(func(_ string, e CapacityQuotaEntry) any { return specOf(e.Object) })).
		Complete(c)
}

// rebuild makes the enforced quota named name match the CapacityQuota the store holds, rebuilding it from the store as
// it is now, rather than from a change, so that changes applied out of order still converge. A quota whose spec
// changed gets new Derives and claims, built before the old ones are removed, so that it never counts less than it
// should; the claims held against the old ones are released.
func (s *CapacityQuotaState) rebuild(name string) {
	s.rebuilding.Lock()
	defer s.rebuilding.Unlock()
	e, exists := s.quotas.Get(name)
	old := s.built[name]
	if exists && e.Intent.Enforced.Value.applied.Equal(specOf(e.Object)) {
		return
	}
	if !exists && old == nil {
		return
	}
	var q *Quota
	if exists {
		parsed, err := newQuota(e.Object)
		if err != nil {
			s.log.Error(err, "not enforcing capacityquota", "CapacityQuota", klog.KObj(e.Object))
		} else {
			q = parsed
			q.nodeClaims = state.Derive(s.nodes.NodeClaimStore.Reader, s.quotas, q.nodeClaimKey(s.nodes.NodeStore), nodeClaimQuotaUsage)
			q.nodes = state.DeriveObject(s.nodes.NodeStore.Reader, s.quotas, q.nodeKey, nodeQuotaUsage)
			// A NodeClaim's Used counts what a reservation held for it once it counts the NodeClaim's launched capacity;
			// one that stops counting first releases it. Until then the reservation's resources.Node counts twice.
			q.claims = state.NewClaims[string, string, CapacityQuotaFields, quotaUsage, quotaHeld](q.nodeClaims,
				func(c quotaUsage) bool { return c.capacityCounted }, ReservationTTL, s.clock).
				OnExpire(countExpiry[string, quotaHeld](metrics.CapacityQuotaClaimStore))
		}
		s.enforced.Set(name, enforcement{applied: specOf(e.Object), quota: q})
	}
	if q != nil {
		s.built[name] = q
	} else {
		delete(s.built, name)
	}
	if old != nil {
		old.nodeClaims.Remove()
		old.nodes.Remove()
	}
}

// follows returns whether State.Register starts the CapacityQuota store, which it does with the CapacityQuota feature
// gate.
func (s *CapacityQuotaState) follows() bool {
	return options.FromContext(s.ctx).FeatureGates.CapacityQuota
}

// Quotas returns the quotas Karpenter enforces, sorted by name. They're shared; never modify one.
func (s *CapacityQuotaState) Quotas() []*Quota {
	var quotas []*Quota
	for _, e := range s.quotas.List() {
		if q := e.Intent.Enforced.Value.quota; q != nil {
			quotas = append(quotas, q)
		}
	}
	slices.SortFunc(quotas, func(a, b *Quota) int { return strings.Compare(a.Name, b.Name) })
	return quotas
}

// Get returns the quota's derived fields.
func (s *CapacityQuotaState) Get(name string) CapacityQuotaFields {
	e, _ := s.quotas.Get(name)
	return e.Derived
}

// Reserved returns what is held against the quota for NodeClaims whose launched capacity Used doesn't count yet
// (CapacityQuotaFields.Reserved). It's shared; copy it to change it.
func (s *CapacityQuotaState) Reserved(q *Quota) corev1.ResourceList {
	return s.Get(q.Name).Reserved
}

// Synced returns whether the CapacityQuota store has applied its first List, every quota it holds is enforced as it
// holds it, counting everything that counts against it, and no quota it no longer holds is enforced.
func (s *CapacityQuotaState) Synced() bool {
	pending, _ := s.pending.Get(true)
	if !s.quotas.Synced() || !s.pending.Synced() || pending.Derived != 0 {
		return false
	}
	s.rebuilding.Lock()
	defer s.rebuilding.Unlock()
	for name := range s.built {
		if _, ok := s.quotas.Get(name); !ok {
			return false
		}
	}
	return true
}

// Clear stops enforcing every quota, ending every reservation, and empties the store in place, so that its subscribers
// stay subscribed. It's for tests, between specs; storelint reports a Clear outside a test.
func (s *CapacityQuotaState) Clear() {
	s.rebuilding.Lock()
	defer s.rebuilding.Unlock()
	for _, q := range s.built {
		q.nodeClaims.Remove()
		q.nodes.Remove()
	}
	s.built = map[string]*Quota{}
	s.quotas.Clear()
}

// QuotaReservation holds the most a NodeClaim could launch with, and one resources.Node, against one CapacityQuota it
// may match, until the quota's Used counts the NodeClaim's launched capacity. Bind it to the NodeClaim once created, or
// Release it if it won't be.
type QuotaReservation = *state.Claim[string, string, quotaHeld]

// QuotaReservations are the QuotaReservations one Reserve granted.
type QuotaReservations []QuotaReservation

// Release releases every reservation.
func (r QuotaReservations) Release() {
	for _, reservation := range r {
		reservation.Release()
	}
}

// Reserve holds value against every enforced quota a NodeClaim launched with any of requirements may match, for a
// NodeClaim a caller is about to create, without a quota's Used and Reserved exceeding a limit. It reserves the quotas
// one at a time in sorted name order and, if a quota refuses, releases what it reserved and returns the quota's name.
// replacing names NodeClaims that count now and stop counting before the new one launches, such as the candidates a
// disruption command replaces, so what they count is credited.
func (s *CapacityQuotaState) Reserve(requirements []scheduling.Requirements, value corev1.ResourceList, replacing []string) (QuotaReservations, string) {
	var reservations QuotaReservations
	for _, q := range s.Quotas() {
		if !q.MayMatchAny(requirements) {
			continue
		}
		claims := q.claims.ReserveReplacing(q.Name, 1, quotaHeld(value), replacing, func(f CapacityQuotaFields) bool {
			return withinLimits(q.Limits, f.Used, f.Reserved)
		})
		if len(claims) == 0 {
			reservations.Release()
			return nil, q.Name
		}
		reservations = append(reservations, claims[0])
	}
	return reservations, ""
}

// quotaUsage is a NodeClaim's or a Node's contribution to a quota's Used.
type quotaUsage struct {
	usage corev1.ResourceList
	// capacityCounted is whether usage counts the NodeClaim's launched capacity.
	capacityCounted bool
}

func (u quotaUsage) Update(f CapacityQuotaFields, prev quotaUsage) CapacityQuotaFields {
	f.Used = replaceResources(f.Used, u.usage, prev.usage)
	return f
}

func (u quotaUsage) Delete(f CapacityQuotaFields) CapacityQuotaFields {
	f.Used = replaceResources(f.Used, nil, u.usage)
	return f
}

func (u quotaUsage) Equal(o quotaUsage) bool {
	return u.capacityCounted == o.capacityCounted && resourcesEqual(u.usage, o.usage)
}

// quotaHeld is what a QuotaReservation holds against its quota.
type quotaHeld corev1.ResourceList

func (h quotaHeld) Update(f CapacityQuotaFields, prev quotaHeld) CapacityQuotaFields {
	f.Reserved = replaceResources(f.Reserved, corev1.ResourceList(h), corev1.ResourceList(prev))
	return f
}

func (h quotaHeld) Delete(f CapacityQuotaFields) CapacityQuotaFields {
	f.Reserved = replaceResources(f.Reserved, nil, corev1.ResourceList(h))
	return f
}

func (h quotaHeld) Equal(o quotaHeld) bool {
	return resourcesEqual(corev1.ResourceList(h), corev1.ResourceList(o))
}
