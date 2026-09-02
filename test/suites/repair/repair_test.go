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

package repair

// The e2e runner: each cell is realized as (Deployment sized per scale + PDB per drain axis + NodePool topology),
// provisioned against a DEPLOYED Karpenter/KWOK build; the fault is injected by patching KWOK node/pod conditions;
// recovery is MEASURED with Eventually (never a fake-clock step, because this is a real control plane on a real clock).
//
// The runner is UNIFIED: every cell is measured the same way — provision → armImpairment → injectFault → poll until the
// workload is fully healthy on good capacity OR the deadline — and records only RAW observables. There is no
// help/non-help branch and no interpreted verdict; whether repair "should" help is derived from the impairment in
// analysis, not measured here.
//
// Cell → mechanism map:
//   impairment → the correlation domain and its fault: base SimUnhealthy on the faulted nodes, plus a domain-scoped
//                replacement mechanism (node-broken re-flags in-domain replacement NODES unhealthy; workload-broken
//                gates in-domain replacement PODS never-ready; benign/uncorrelated leave replacements healthy).
//   scale      → fleet size (workload replicas, one pod per node) — makes the blast a meaningful FRACTION of the fleet.
//   blast      → how many nodes are faulted (single / ~10% / ~35% of the fleet).
//   drain      → PDB presence/strictness, and kubelet-dead (eviction won't succeed ⇒ forceful past the NodePool TGP).
//   strat      → replace-first (elastic NodePool, headroom) vs terminate-first (static/reserved; currently un-runnable).
//   topo       → single NodePool vs per-AZ vs per-AMI (arch) NodePools; sets the domains a repair's budget lines up with.
//
// Metrics recorded per (cell, option), scale-agnostic: mitigated%, time-to-recovery, disrupted-pods, cloud-provider
// calls (report.go).

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"sigs.k8s.io/karpenter/test/pkg/debug"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck
	. "github.com/onsi/gomega"    //nolint:staticcheck
	"github.com/awslabs/operatorpkg/object"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/pkg/environment/common"
)

const (
	// Timings are generous and real-clock. e2e is poll-based, so every wait is an Eventually with a wide ceiling.
	pollInterval = 2 * time.Second // sim observation + reflag/gate/clear cadence (was 5s; tightened for faster sims)

	// nodePoolTGP is the NodePool's TerminationGracePeriod — the customer-facing drain bound. It is set ~1h (effectively
	// nil / non-binding) on purpose: the repair drain bound now comes from the RepairPolicy's per-condition
	// TerminationGracePeriod (kwok/cloudprovider/cloudprovider.go, REPAIR_POLICY_TGP default 45s), so the effective bound
	// is min(policyTGP=45s, nodePoolTGP=1h) = 45s. This makes the drain axis exercise the POLICY knob rather than a pool
	// default, while still resolving kubelet-dead / blocking-PDB cells within the window (repair forces past 45s).
	nodePoolTGP = time.Hour

	// The deployed KWOK build's RepairPolicies (kwok/cloudprovider/cloudprovider.go RepairPolicies()) must include the
	// custom {ConditionType: "SimUnhealthy", ConditionStatus: True} policy with a short (seconds) TolerationDuration, so
	// the injected fault (see markNodeUnhealthy) is actionable in test time. This is a deployment-side change; the suite
	// only observes.
)

// recoveryTimeout is the fault-onset → workload-healthy ceiling. It MUST exceed the implementation's own eventual-
// consistency timescale, not just the toleration: an AIMD restraint only widens after a replacement holds healthy for
// the success dwell (provider-tunable via the KWOK provider's RepairTiming.Dwell, default 30s; 5m for
// realistic). At dwell=2m the slowest RECOVERING cell (escape-required zone-node-broken majority) completes ~9m, so the
// ceiling is sized just above that. Too short reports "unmitigated" for a fault that would recover (an earlier 4m default
// was wrong — it false-failed the ~9m escape cell); too long just makes NON-recovering cells hang. Right-size, don't
// hang: raise via REPAIR_RECOVERY_TIMEOUT (and bump REPAIR_DWELL) for realistic-timescale runs.
var recoveryTimeout = envDuration("REPAIR_RECOVERY_TIMEOUT", 12*time.Minute)

// provisionTimeout is the ceiling for fault-free topology build + workload-healthy. Env-tunable (REPAIR_PROVISION_TIMEOUT)
// because a large scale (n100 = 100 pinned NodeClaims/cell) takes longer to launch+register than the small cells.
var provisionTimeout = envDuration("REPAIR_PROVISION_TIMEOUT", 5*time.Minute)

// refDwell is the harness's reference for the controller's success dwell (default 30s = the KWOK fast default). It sizes
// the reflag-delay axis (tbelow=0.5×, tabove=2×, trand∈[0,2×)) relative to the dwell. Set REPAIR_REF_DWELL to match a
// non-default REPAIR_DWELL so the "within vs after the dwell" cases stay meaningful.
var refDwell = envDuration("REPAIR_REF_DWELL", 30*time.Second)

// reflagDelayFor is how long a node-broken in-domain replacement stays healthy before re-flagging, per the cell's reflag
// mode. trand draws a fresh uniform [0, 2×refDwell) per replacement, so one cell exercises a distribution straddling the
// dwell (the "indeterminate" case that stresses dwell jitter / optimistic crediting).
func reflagDelayFor(r reflagDelay) time.Duration {
	switch r {
	case reflagBelowDwell:
		return refDwell / 2
	case reflagAboveDwell:
		return 2 * refDwell
	case reflagRandom:
		if span := int64(2 * refDwell); span > 0 { // guard: REPAIR_REF_DWELL<=0 ⇒ treat as immediate (Int63n panics on <=0)
			return time.Duration(rand.Int63n(span))
		}
		return 0
	default: // reflagImmediate
		return 0
	}
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(name); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// simUnhealthyCondition is the custom repair-matching node condition the suite injects. Validated on-cluster: KWOK does
// not manage arbitrary conditions, so SimUnhealthy=True holds (unlike Ready, which node-initialize reverts), and the
// node stays Ready=True so it remains a valid voluntary-disruption candidate. The deployed KWOK RepairPolicies() must
// include {ConditionType: "SimUnhealthy", ConditionStatus: True}.
const simUnhealthyCondition corev1.NodeConditionType = "SimUnhealthy"

// simWorkloadReadyGate is a pod readiness gate used only by the workload-broken impairment. KWOK fakes container
// readiness, so a normal probe can't make a pod unready; a readiness gate makes Ready = AND(containers, gate), and
// nothing satisfies the gate unless we explicitly patch it. armImpairment patches it True on every workload pod to
// establish a healthy baseline; the workload-broken fault then flips it False on pods in the impaired domain so those
// pods are genuinely down while the NODES stay healthy (the detection blind spot — repair must correctly decline).
const simWorkloadReadyGate corev1.PodConditionType = "SimWorkloadReady"

// kwokZones / kwokArches mirror the domains the deployed KWOK provider generates (kwok/tools/gen_instance_types.go).
var (
	kwokZones  = []string{"test-zone-a", "test-zone-b", "test-zone-c", "test-zone-d"}
	kwokArches = []string{v1.ArchitectureAmd64, v1.ArchitectureArm64}
)

// fleet is what a cell's scenario created, retained so the runner can inject faults into it and observe recovery.
type fleet struct {
	nodePools   []*v1.NodePool
	deployment  *appsv1.Deployment
	selector    labels.Selector
	replicas    int
	nodes       []*corev1.Node // workload nodes at fault-injection time
	faultNodes  []*corev1.Node // the subset selected for the fault (count per blast, confined to the impaired domain)
	faultedPods sets.Set[string]
	faultOnset  time.Time // when the fault was injected (pins the sustainFault transition time; anchors TTR)
	// impairedKey/impairedValue record the impaired domain chosen at fault selection (e.g. topology.kubernetes.io/zone
	// = test-zone-a, or kubernetes.io/arch = amd64). "" for uncorrelated (no shared domain). The domain-scoped
	// mechanisms (re-flag / readiness gate) act ONLY on replacements whose zone/arch matches this.
	impairedKey   string
	impairedValue string
	// For the node-broken reflag-delay axis: when each in-domain replacement was first observed, and the per-replacement
	// healthy window it must survive before re-flagging (0 = immediate). Lets a replacement join healthy, then fail after t.
	replacementSeen  map[string]time.Time
	replacementDelay map[string]time.Duration
}

var _ = Describe("Repair/Performance", Label("repair-perf", debug.NoWatch), func() {
	// Full cross product — one It per cell (no skip; degeneracy is revealed empirically in the collapse report).
	// shardCells filters to this process's SHARD_INDEX/SHARD_COUNT slice when running the matrix across parallel clusters.
	for _, c := range shardCells(enumerateCells()) {
		c := c
		It("cell "+c.id(), func() {
			recordResult(c, option, runCell(c))
		})
	}
})

// runCell provisions the cell, injects its impairment, and measures recovery along the ONE unified path used for every
// cell: provision → armImpairment → injectFault → poll (sustainFault + domain-scoped re-flag/gate each tick) until the
// workload is fully healthy on good capacity OR the deadline. It records only raw observables — no help/non-help branch,
// no per-cell classification. timeToRecovery is nil unless the cell fully recovered; a partial recovery shows up as
// mitigatedFraction < 1.
func runCell(c cell) (m repairMetrics) {
	cellStart := time.Now()
	defer func() { m.wallTime = time.Since(cellStart) }() // named return so the defer sets it on the value returned

	f := provisionCell(c)

	// Arm the replacement mechanism BEFORE injecting so it affects replacements as they appear.
	armImpairment(c, &f)
	// Baseline the disruption-cost counters HERE — after the topology is built and the workload is healthy, and just
	// BEFORE the fault — so cp-calls and disrupted-pods measure ONLY the repair churn, not the one-time topology
	// provisioning. (No repair can have fired yet: the fault isn't injected until the next line.)
	cpBefore := scrapeCloudProviderCalls()
	disruptBefore := scrapePodsDisrupted()
	gracefulBefore := scrapePodsGracefullyDrained()
	forcedBefore := scrapePodsForceDeleted()
	injectFault(c, &f)

	if measureRecovery(c, &f) {
		d := time.Since(f.faultOnset)
		m.timeToRecovery = &d
	}
	// recovery% = healthy nodes / total nodes at the end (fleet-health fraction; workload-broken uses healthy pods /
	// total pods). See faultMitigated. On full recovery this is 1.0.
	m.mitigatedFraction = faultMitigated(c, &f)
	// Record the node-level outcome behind the metric (terminated / escaped-out-of-domain / reflagged-in-domain) as a
	// logged observable — the direct nodeclaim signal that distinguishes escape (unpinned) from in-zone churn (pinned).
	term, esc, refl := nodeOutcomes(&f)
	fmt.Fprintf(GinkgoWriter, "[repair] cell=%s option=%s node-outcomes: terminated=%d escaped=%d reflagged-in-domain=%d faulted=%d recovery=%.2f\n",
		c.id(), option, term, esc, refl, len(f.faultNodes), m.mitigatedFraction)
	// disrupted-pods = TOTAL pods disrupted during the cell (cumulative counter delta), so constant churn is penalized —
	// a slot re-disrupted N times counts N, not once.
	m.disruptedPods = maxZero(scrapePodsDisrupted() - disruptBefore)
	// Split the disruption cost: gracefully drained (eviction honored PDB+grace) vs force-deleted (bypassed them past the
	// drain bound). graceful+forced need not equal disruptedPods — the latter counts pods at disruption-DECISION time,
	// these count actual terminations — but together they characterize HOW the pods were disrupted.
	m.gracefulPods = maxZero(scrapePodsGracefullyDrained() - gracefulBefore)
	m.forcedPods = maxZero(scrapePodsForceDeleted() - forcedBefore)
	m.cpCalls = maxZero(scrapeCloudProviderCalls() - cpBefore)
	return m
}

// faultMitigated is the fraction of the FAULTED nodes that repair actually mitigated: of the len(faultNodes) nodes made
// unhealthy, how many now have their workload slot back on GOOD capacity (a node not flagged SimUnhealthy). It credits
// ESCAPE (a replacement that comes up healthy out-of-domain) and does NOT credit mere termination or an in-domain
// replacement that re-fails — so pinned (churns in-zone → 0 fixed) and unpinned (escapes → >0 fixed) separate. With
// one-pod-per-node, the never-faulted nodes contribute a constant `replicas - faultedCount` healthy pods (baseline), so
// (healthyOnGoodCapacity - baseline) is exactly the count of faulted slots recovered. Normalizing by faultedCount (not
// replicas) keeps the metric undiluted by the untouched majority: a tripped/no-op repair reads 0%, full recovery 100%.
func faultMitigated(c cell, f *fleet) float64 {
	n := len(f.faultNodes)
	if n == 0 {
		return 0
	}
	// Mitigated = fraction of the ORIGINALLY-unhealthy nodes that are now remediated:
	//   (initialFaulted - stillUnhealthy) / initialFaulted   (clamped 0..1)
	// Normalizing against the BLAST count (nodes unhealthy at the start), not the whole fleet, is what makes this a real
	// mitigation reading: an untouched fault reads 0% (not diluted UP by the healthy majority — a breaker that freezes a
	// 4-of-10 fault reads 0%, not 60%), a fully escaped/healed fault reads 100%, partial reads in between. stillUnhealthy
	// = everything currently SimUnhealthy on workload capacity = unrepaired originals + re-flagged in-domain replacements.
	// EXCEPTION: workload-broken (deferred) leaves the node healthy but the pod unready — measured on pods; when it is
	// re-enabled this should likewise normalize against the impaired-domain pod count.
	if c.impairment.workloadBroken() {
		return math.Max(0, math.Min(float64(healthyOnGoodCapacity(f))/float64(f.replicas), 1.0))
	}
	healthy, total := nodeHealth(f)
	stillUnhealthy := total - healthy
	return math.Max(0, math.Min(float64(n-stillUnhealthy)/float64(n), 1.0))
}

// nodeHealth counts the workload nodes at the end: total = karpenter-managed workload nodes not terminating; healthy =
// those not flagged SimUnhealthy. recovery% = healthy/total (see faultMitigated).
func nodeHealth(f *fleet) (healthy, total int) {
	list := &corev1.NodeList{}
	if err := env.Client.List(env.Context, list); err != nil {
		return 0, 0
	}
	for i := range list.Items {
		nd := &list.Items[i]
		if nd.Labels[v1.NodePoolLabelKey] == "" || nd.DeletionTimestamp != nil {
			continue // workload capacity only (skip EKS managed/control-plane nodes) and nodes on their way out
		}
		total++
		if !hasTrueSimUnhealthy(nd) {
			healthy++
		}
	}
	return healthy, total
}

// nodeOutcomes reports the NODE-level disposition behind the metric — how many faulted nodes repair terminated, and of
// the fresh replacements, how many landed OUT of the impaired domain (escaped → healthy) vs back IN it (reflagged →
// re-failed). This is the direct node/nodeclaim observable (independent of the pod-derived metric): pinned churns
// in-zone (escaped≈0), unpinned escapes (escaped>0), a tripped breaker terminates nothing. Recorded/logged here; in the
// deterministic CI variant these become hard assertions. Meaningful only for domained (zonal/ami) node-broken cells.
func nodeOutcomes(f *fleet) (terminated, escaped, reflaggedIn int) {
	for _, n := range f.faultNodes {
		if exists, _, _ := nodeState(n.Name); !exists {
			terminated++
		}
	}
	if f.impairedKey == "" {
		return terminated, 0, 0
	}
	orig := f.originalNodeNames()
	unhealthy := unhealthyNodeNames()
	for _, n := range env.Monitor.Nodes() {
		if orig.Has(n.Name) {
			continue // only fresh replacements
		}
		if n.Labels[f.impairedKey] == f.impairedValue {
			reflaggedIn++ // replacement re-created inside the impaired domain (node-broken re-flags it)
		} else if !unhealthy.Has(n.Name) {
			escaped++ // replacement placed outside the impaired domain and stayed healthy
		}
	}
	return terminated, escaped, reflaggedIn
}

// fractionRepaired is the fraction of the faulted nodes that repair has resolved (terminated/replaced) by the time the
// measurement window ends — computed BEFORE teardown, so a gone faulted node means repair took it, not cleanup. It turns
// a binary unmitigated into a graded signal: a majority fault that repair chipped 2 of 4 nodes off reads 50%, not 0.
func fractionRepaired(f *fleet) float64 {
	n := len(f.faultNodes)
	if n == 0 {
		return 0
	}
	remaining := 0
	for _, node := range f.faultNodes {
		if exists, _, _ := nodeState(node.Name); exists {
			remaining++
		}
	}
	return float64(n-remaining) / float64(n)
}

// provisionCell builds the DETERMINISTIC cluster topology (pinned NodeClaims — see buildTopology), then places the
// workload one-pod-per-built-node and waits for it to be healthy. The topology (which domains hold how many nodes, and
// which are faulted) is exact and reproducible — no reliance on pod scheduling constraints + Karpenter placement luck.
func provisionCell(c cell) fleet {
	f := fleet{}
	f.buildTopology(c) // sets nodePools, replicas, nodes, faultNodes, impairedKey/impairedValue; creates nodeClass+pools+nodeClaims

	f.deployment = newWorkload(c, f.replicas)
	f.selector = labels.SelectorFromSet(f.deployment.Spec.Selector.MatchLabels)
	objs := []client.Object{f.deployment}
	if pdb := newPDB(c, f.deployment.Spec.Selector.MatchLabels); pdb != nil {
		objs = append(objs, pdb)
	}
	env.ExpectCreated(objs...)

	env.EventuallyExpectHealthyPodCountWithTimeout(provisionTimeout, f.selector, f.replicas)
	f.faultedPods = podUIDsOnNodes(f.selector, f.faultNodes)
	return f
}

// buildTopology constructs the cell's cluster topology DIRECTLY by creating one pinned NodeClaim per intended node — each
// pinned via requirements to an exact (zone, arch, c-2x instance type) and owned by its NodePool — instead of shaping the
// topology indirectly through pod anti-affinity/spread and hoping Karpenter places nodes where we need. This removes the
// placement randomness (impaired-zone size, bin-packing) that made single runs unreliable and forced multi-trial
// averaging. Replacements (when repair deletes a faulted node → pending pod → Karpenter provisions) are STILL the DUT.
func (f *fleet) buildTopology(c cell) {
	f.replicas = c.scale.nodes()
	f.replacementSeen = map[string]time.Time{}
	f.replacementDelay = map[string]time.Duration{}
	count := c.blast.count(c.scale)
	if count > f.replicas {
		count = f.replicas
	}

	// Per-node (zone, arch) assignment + which are faulted. Default arch amd64; zones round-robin across all KWOK zones.
	zones := make([]string, f.replicas)
	arches := make([]string, f.replicas)
	faulted := make([]bool, f.replicas)
	for i := range zones {
		zones[i] = kwokZones[i%len(kwokZones)]
		arches[i] = v1.ArchitectureAmd64
	}
	switch {
	case c.blast == blastSingle:
		faulted[0] = true // one node, uncorrelated (no recorded domain)
	case c.impairment.domainKey() == corev1.LabelTopologyZone:
		// Correlated zonal outage: put exactly `count` faulted nodes in ONE impaired zone; spread the rest across the others.
		f.impairedKey, f.impairedValue = corev1.LabelTopologyZone, kwokZones[0]
		others, oi := kwokZones[1:], 0
		for i := 0; i < f.replicas; i++ {
			if i < count {
				zones[i], faulted[i] = kwokZones[0], true
			} else {
				zones[i] = others[oi%len(others)]
				oi++
			}
		}
	case c.impairment.domainKey() == corev1.LabelArchStable:
		// Correlated arch/AMI outage: `count` faulted nodes on the impaired arch; the rest on the other arch.
		f.impairedKey, f.impairedValue = corev1.LabelArchStable, kwokArches[0]
		for i := 0; i < f.replicas; i++ {
			if i < count {
				arches[i], faulted[i] = kwokArches[0], true
			} else {
				arches[i] = kwokArches[1]
			}
		}
	default: // uncorrelated multi-node: faulted nodes land in DISTINCT zones (round-robin), sharing no domain
		for i := 0; i < count; i++ {
			faulted[i] = true
		}
	}

	// NodePools per topology, created lazily and keyed by domain value (single = one shared pool).
	pools := map[string]*v1.NodePool{}
	poolFor := func(zone, arch string) *v1.NodePool {
		key, dom := "", map[string]string(nil)
		switch c.topo {
		case topoPerAZ:
			key, dom = zone, map[string]string{corev1.LabelTopologyZone: zone}
		case topoPerAMI:
			key, dom = arch, map[string]string{corev1.LabelArchStable: arch}
		}
		if p, ok := pools[key]; ok {
			return p
		}
		p := newNodePool(c, dom)
		pools[key] = p
		f.nodePools = append(f.nodePools, p)
		return p
	}

	// Build one pinned NodeClaim per intended node, owned (via the nodepool label) by its pool so the breaker's
	// per-NodePool accounting + disruption budgets apply.
	ncs := make([]*v1.NodeClaim, f.replicas)
	for i := 0; i < f.replicas; i++ {
		pool := poolFor(zones[i], arches[i])
		ncs[i] = test.NodeClaim(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: pool.Name}},
			Spec: v1.NodeClaimSpec{
				Requirements: []v1.NodeSelectorRequirementWithMinValues{
					{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"c-2x-" + arches[i] + "-linux"}},
					{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{arches[i]}},
					{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{zones[i]}},
					{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand}},
				},
				NodeClassRef: &v1.NodeClassReference{Group: object.GVK(nodeClass).Group, Kind: object.GVK(nodeClass).Kind, Name: nodeClass.GetName()},
				// Mirror the NodePool template's TGP onto the pinned NodeClaim. Without this, NewCandidate's
				// drainBoundedCandidate check (Spec.TerminationGracePeriod != nil) is false for a hand-created NodeClaim, so
				// a node whose pods block eviction (pdb-block / kubelet-dead) is REJECTED as a repair candidate and never
				// disrupted — the node wedges instead of demonstrating the TGP backstop. With the TGP set, repair admits the
				// node, drains up to the (policy-stamped) bound, then force-deletes past it.
				TerminationGracePeriod: &metav1.Duration{Duration: nodePoolTGP},
			},
		})
	}

	// nodeClass + NodePools are few — create them serially.
	base := []client.Object{nodeClass}
	for _, p := range f.nodePools {
		base = append(base, p)
	}
	env.ExpectCreated(base...)
	// The pinned NodeClaims can number in the thousands. env.ExpectCreated creates serially with a 10s per-object
	// deadline, which cannot survive a managed control plane's API Priority & Fairness throttling at that volume, so
	// fan the creates out with APF-tolerant retry instead.
	createPinnedNodeClaims(ncs)

	// Wait until every one of OUR NodeClaims has registered its node. A single List per poll (not one Get per
	// NodeClaim) keeps this cheap at thousands of nodes; join by name so any unrelated NodeClaims are ignored.
	Eventually(func(g Gomega) {
		list := &v1.NodeClaimList{}
		g.Expect(env.Client.List(env.Context, list, client.MatchingLabels{test.DiscoveryLabel: "unspecified"})).To(Succeed())
		registered := sets.New[string]()
		for i := range list.Items {
			if list.Items[i].Status.NodeName != "" {
				registered.Insert(list.Items[i].Name)
			}
		}
		missing := lo.CountBy(ncs, func(nc *v1.NodeClaim) bool { return !registered.Has(nc.Name) })
		g.Expect(missing).To(BeZero(), "%d/%d NodeClaims have not registered a node yet", missing, len(ncs))
	}).WithTimeout(provisionTimeout).WithPolling(pollInterval).Should(Succeed())

	// Map each NodeClaim to its Node via two Lists (not 2N Gets), preserving ncs order so the fault set is stable.
	ncList := &v1.NodeClaimList{}
	Expect(env.Client.List(env.Context, ncList, client.MatchingLabels{test.DiscoveryLabel: "unspecified"})).To(Succeed())
	nodeList := &corev1.NodeList{}
	Expect(env.Client.List(env.Context, nodeList)).To(Succeed())
	ncByName := map[string]*v1.NodeClaim{}
	for i := range ncList.Items {
		ncByName[ncList.Items[i].Name] = &ncList.Items[i]
	}
	nodeByName := map[string]*corev1.Node{}
	for i := range nodeList.Items {
		nodeByName[nodeList.Items[i].Name] = &nodeList.Items[i]
	}
	for i, nc := range ncs {
		fresh, ok := ncByName[nc.Name]
		Expect(ok).To(BeTrue(), "NodeClaim %s missing from list", nc.Name)
		node, ok := nodeByName[fresh.Status.NodeName]
		Expect(ok).To(BeTrue(), "Node %s (for NodeClaim %s) missing from list", fresh.Status.NodeName, nc.Name)
		f.nodes = append(f.nodes, node)
		if faulted[i] {
			f.faultNodes = append(f.faultNodes, node)
		}
	}
}

// createPinnedNodeClaims creates the pinned topology NodeClaims concurrently, tolerating API Priority & Fairness
// backpressure. env.ExpectCreated creates serially with a 10s per-object deadline; on a managed control plane that
// deadline is tripped once the topology reaches thousands of NodeClaims (the write storm gets 429-throttled and a
// starved create exceeds 10s). A bounded worker pool keeps many creates in flight and retries through 429s until the
// provision deadline, so large-scale topologies (n1000+) build reliably.
func createPinnedNodeClaims(ncs []*v1.NodeClaim) {
	GinkgoHelper()
	const workers = 50
	deadline := time.Now().Add(provisionTimeout)
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures int
	var lastErr error
	for _, nc := range ncs {
		// DiscoveryLabel is what env.Cleanup and the list-based registration wait select on; test.NodeClaim already
		// stamps it, but reaffirm it here so the concurrent path never depends on that default.
		nc.SetLabels(lo.Assign(nc.GetLabels(), map[string]string{test.DiscoveryLabel: "unspecified"}))
		wg.Add(1)
		sem <- struct{}{}
		go func(obj *v1.NodeClaim) {
			defer wg.Done()
			defer func() { <-sem }()
			for {
				err := env.Client.Create(env.Context, obj)
				if err == nil || apierrors.IsAlreadyExists(err) {
					return
				}
				// Retry only transient backpressure (429 / server-timeout / unavailable / internal). Anything else
				// (validation, forbidden, context-canceled) is permanent — record it immediately rather than spin
				// until the deadline, so a real defect fails fast instead of burning the whole provision budget.
				transient := apierrors.IsTooManyRequests(err) || apierrors.IsServerTimeout(err) ||
					apierrors.IsTimeout(err) || apierrors.IsServiceUnavailable(err) || apierrors.IsInternalError(err)
				if !transient || time.Now().After(deadline) {
					mu.Lock()
					failures++
					lastErr = err
					mu.Unlock()
					return
				}
				time.Sleep(time.Duration(100+rand.Intn(400)) * time.Millisecond) // jittered to avoid a thundering herd
			}
		}(nc)
	}
	wg.Wait()
	Expect(failures).To(BeZero(), "%d/%d NodeClaim creates failed before the provision deadline (last error: %v)", failures, len(ncs), lastErr)
}

// newWorkload builds a Deployment forced one-pod-per-node so faulting N nodes disrupts N pods, sized so Karpenter must
// provision `replicas` nodes. One-pod-per-node is guaranteed by the CPU request (1000m) exceeding half the KWOK node's
// allocatable (c-2x ≈ 1900m ⇒ a second pod never fits), so the hostname spread is only a placement nudge and is set to
// ScheduleAnyway: a hard DoNotSchedule spread strands a pod whenever provisioning is briefly uneven, and a pending pod
// keeps its target node "nominated", which makes Karpenter refuse to disrupt it — silently blocking repair on any
// multi-node fault.
func newWorkload(c cell, replicas int) *appsv1.Deployment {
	name := "repair-" + test.RandomName()
	// One pod per node is guaranteed by CAPACITY: the built nodes are all c-2x (2 vCPU, ~1.9 CPU allocatable) and each pod
	// requests 1.5 CPU, so a second pod never fits. No hostname anti-affinity or zonal topology-spread is needed — the
	// topology is built deterministically (buildTopology) with pinned NodeClaims, so pods just land one per existing node,
	// and repair-provisioned replacements (also c-2x) stay one-per-node too. This removes the scheduling hacks (and their
	// failure modes: bin-packing collapse, stranded-pending pods pinning nominations that silently blocked repair).
	opts := test.CreateDeploymentOptions(name, int32(replicas), "1500m", "1Gi")
	dep := test.Deployment(opts)
	if c.impairment.workloadBroken() {
		// workload-broken blind spot: nodes stay Ready+healthy to Karpenter, but the workload itself is down. A readiness
		// gate makes each pod's readiness depend on a custom condition KWOK does not set; armImpairment patches it True on
		// every pod to reach a healthy baseline, and the fault flips it False on the impaired domain's pods. No node
		// condition is injected — so a correct repair implementation sees only healthy nodes and declines.
		dep.Spec.Template.Spec.ReadinessGates = append(dep.Spec.Template.Spec.ReadinessGates,
			corev1.PodReadinessGate{ConditionType: simWorkloadReadyGate})
	}
	return dep
}

// newPDB returns a PDB per the drain axis, or nil when the axis places none.
func newPDB(c cell, matchLabels map[string]string) client.Object {
	if !c.drain.hasPDB() {
		return nil
	}
	var mu intstr.IntOrString
	if c.drain == drainNoPDBBlock {
		mu = intstr.FromInt(0) // fully blocking → drain stalls (wedged candidate)
	} else {
		mu = intstr.FromString("100%") // permissive → drain proceeds, PDB merely paces
	}
	return test.PodDisruptionBudget(test.PDBOptions{Labels: matchLabels, MaxUnavailable: &mu})
}

// newNodePool builds ONE NodePool constrained to the given domain (dom nil = spans all zones/arches). It pins the instance
// type to c-2x (one-pod-per-node by capacity), disables voluntary disruption (drift + consolidation) via per-reason
// budgets while leaving REPAIR ("Unhealthy") a full budget, and carries the NodePool TGP (the customer-facing drain
// bound). buildTopology decides how many pools a cell needs (single = 1; per-AZ = one per zone; per-AMI = one per arch).
// The pool is the provisioner for REPLACEMENTS when repair deletes a faulted node — that placement is the DUT.
func newNodePool(c cell, dom map[string]string) *v1.NodePool {
	np := env.DefaultNodePool(nodeClass)
	np.Spec.Limits = v1.Limits{}
	np.Spec.Template.Spec.TerminationGracePeriod = &metav1.Duration{Duration: nodePoolTGP}
	// PER-REASON disruption budgets: block DRIFT and CONSOLIDATION entirely (they would otherwise disrupt the
	// deterministically-built topology — a hand-created NodeClaim can read as drifted, and consolidation would reclaim
	// nodes), but leave REPAIR ("Unhealthy") a full budget. A blanket Budgets:0 cannot be used — repair is budget-gated
	// (rides the shared disruption budget; repair.go skips a NodePool whose budget is 0), so it would silently disable
	// the DUT. ConsolidateAfter:Never is redundant with the consolidation-reason budget but kept for clarity.
	np.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmpty
	np.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("Never")
	np.Spec.Disruption.Budgets = []v1.Budget{
		{Reasons: []v1.DisruptionReason{v1.DisruptionReasonDrifted, v1.DisruptionReasonUnderutilized, v1.DisruptionReasonEmpty}, Nodes: "0"},
		{Reasons: []v1.DisruptionReason{v1.DisruptionReasonUnhealthy}, Nodes: "100%"},
	}
	// Pin the instance type to c-2x (2 vCPU) so every node — initial AND repair-provisioned replacements — is the same
	// small shape. With a ~1.5-CPU workload pod this guarantees one-pod-per-node by capacity alone (a second pod never
	// fits), so no hostname anti-affinity / topology-spread scheduling hacks are needed.
	test.ReplaceRequirements(np, v1.NodeSelectorRequirementWithMinValues{
		Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn,
		Values: []string{"c-2x-amd64-linux", "c-2x-arm64-linux"},
	})
	for k, v := range dom {
		test.ReplaceRequirements(np, v1.NodeSelectorRequirementWithMinValues{
			Key: k, Operator: corev1.NodeSelectorOpIn, Values: []string{v},
		})
	}
	return np
}

// armImpairment sets up the replacement mechanism that must exist BEFORE the fault is injected. Only workload-broken
// needs pre-arming: its pods carry an unsatisfied SimWorkloadReady readiness gate (added to the template in
// newWorkload), so nothing is Ready until we gate every workload pod True to establish the healthy baseline. injectFault
// then flips the impaired domain's pods back False. benign / node-broken / uncorrelated arm nothing here — their
// mechanisms are either reactive (reflagInDomain, inside the poll) or need no replacement rig at all.
func armImpairment(c cell, f *fleet) {
	if !c.impairment.workloadBroken() {
		return
	}
	// Gate every workload pod Ready and confirm the healthy baseline (nodes are healthy and every pod is Ready).
	Expect(interruptibleEventually(provisionTimeout, func(g Gomega) {
		setWorkloadReadyGate(f, corev1.ConditionTrue, false)
		g.Expect(healthyOnGoodCapacity(f)).To(BeNumerically(">=", f.replicas))
	})).To(Succeed())
}

// injectFault arms the cell's fault at t=faultOnset. For the node-level impairments (uncorrelated / benign /
// node-broken) it marks the fault-domain nodes with the repair-matching SimUnhealthy condition, first blocking eviction
// on those nodes' workload pods for kubelet-dead (so a graceful drain can never complete). For workload-broken it
// injects NO node condition — the nodes stay healthy (Karpenter-blind) and the fault lives in the readiness-gated
// workload: the impaired domain's pods are flipped un-ready.
func injectFault(c cell, f *fleet) {
	f.faultOnset = time.Now()
	if c.impairment.workloadBroken() {
		// SCHEDULE-time fault: drive the in-domain workload pods NotReady (top-level Ready=False, see setWorkloadReadyGate)
		// so they go — and stay — un-ready while their nodes remain Ready. The poll re-applies this to fresh in-domain
		// replacements. Barrier/guard: the fault MUST actually reduce workload health — if it were a silent no-op (as the
		// readiness-gate approach was, KWOK ignoring gates), healthyOnGoodCapacity would stay == replicas and the cell
		// would fabricate an instant 100%/0s recovery. Require health to drop below full before measuring.
		Eventually(func(g Gomega) {
			setWorkloadReadyGate(f, corev1.ConditionFalse, true)
			g.Expect(healthyOnGoodCapacity(f)).To(BeNumerically("<", f.replicas), "workload-broken fault did not make any in-domain pod NotReady")
		}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
		return
	}
	if c.drain == drainNoKubeletDead {
		// "Eviction won't succeed": pin the faulted nodes' workload pods with the env's TestingFinalizer so an eviction
		// (a DELETE) is accepted but never completes — the pod sticks in Terminating, exactly as when a dead kubelet
		// never acks the graceful termination. The NodePool TGP then decides whether repair can force past it. Cleanup
		// strips TestingFinalizer, so this does not leak into teardown.
		blockEvictionOnFaultNodes(f)
	}
	// Inject the base fault ONCE. KWOK holds it durably natively — the owned node-heartbeat-with-lease stage skips nodes
	// carrying SimUnhealthy=True, so the condition is never wiped, no per-poll re-assertion needed. A flaky cell
	// (flakeTTL>0) additionally stamps a sim/fault-ttl annotation, and the node-fault-clear stage auto-clears the fault
	// after that TTL (modeling a flapping detector). See data/kwok-node-fault-design.md.
	for i, n := range f.faultNodes {
		markNodeUnhealthy(n, faultReason(c, i), c.flakeTTL)
	}
	// Barrier: do not begin measuring until every faulted node has been OBSERVED SimUnhealthy=True, so the controller
	// evaluates a COMPLETE blast rather than a set that grows node-by-node as the sequential patches land — which matters
	// for a >20%-gated breaker (RCA: injection-atomicity, run wmgadt9os). Latching (accumulate observed names) so a short
	// flakeTTL that self-clears a node before the whole blast is observed cannot deadlock the barrier.
	observed := sets.New[string]()
	Eventually(func(g Gomega) {
		observed = observed.Union(unhealthyNodeNames())
		for _, n := range f.faultNodes {
			g.Expect(observed.Has(n.Name)).To(BeTrue(), "faulted node %s not yet observed SimUnhealthy", n.Name)
		}
	}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
}

func hasTrueSimUnhealthy(n *corev1.Node) bool {
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == simUnhealthyCondition {
			return n.Status.Conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

// faultReason is the Reason stamped on the injected SimUnhealthy condition. For uncorrelated multi-node failures each
// node gets a DISTINCT reason (so they share no cause); the domained (zone-*/ami-*) impairments share one reason — the
// correlation being carried by the shared zone/arch label rather than the reason string.
func faultReason(c cell, i int) string {
	if c.blast != blastSingle && c.impairment == impUncorrelated {
		return fmt.Sprintf("RepairPerfUncorrelated-%d", i)
	}
	return "RepairPerfInjectedUnhealthy"
}

// blockEvictionOnFaultNodes adds the env TestingFinalizer to every workload pod on a faulted node, so graceful eviction
// can never finish. Idempotent; transient patch errors are ignored (the caller's measurement tolerates them).
func blockEvictionOnFaultNodes(f *fleet) {
	faulted := f.faultedNodeNames()
	list := &corev1.PodList{}
	if err := env.Client.List(env.Context, list, client.MatchingLabelsSelector{Selector: f.selector}); err != nil {
		return
	}
	for i := range list.Items {
		p := &list.Items[i]
		if !faulted.Has(p.Spec.NodeName) || lo.Contains(p.Finalizers, common.TestingFinalizer) {
			continue
		}
		stored := p.DeepCopy()
		p.Finalizers = append(p.Finalizers, common.TestingFinalizer)
		_ = env.Client.Patch(env.Context, p, client.MergeFrom(stored))
	}
}

// measureRecovery is the ONE poll every cell runs: each tick applies the domain-scoped replacement mechanisms, then
// checks whether the workload is fully healthy on good capacity. Returns whether full recovery was observed within
// recoveryTimeout. The base fault needs no re-assertion here — KWOK holds it durably (owned node-heartbeat stage skips
// faulted nodes); the poll only OBSERVES and flags fresh in-domain replacements as they appear.
func measureRecovery(c cell, f *fleet) (recovered bool) {
	// SUSTAINED-health requirement: full health must HOLD for confirmWindow(c) before it counts as recovery. For a
	// reflag-delay cell (tbelow/tabove/trand) this window spans the latest possible reflag, so a replacement that joins
	// healthy and re-fails after time t is caught (health dips → the confirmation restarts) instead of being credited a
	// spurious fast recovery. confirmWindow is 0 for non-reflag cells, preserving first-success there.
	confirm := confirmWindow(c)
	var healthySince time.Time
	err := interruptibleEventually(recoveryTimeout, func(g Gomega) {
		// Flaky detector: once flakeTTL elapses the signal self-heals (clear SimUnhealthy everywhere); while it is still
		// "firing" (or for a durable cell, always) keep re-flagging in-domain replacements.
		if !clearFlakeIfElapsed(c, f) {
			// node-broken: any fresh replacement NODE that landed in the impaired domain is re-born unhealthy, so an
			// in-domain replacement never proves out (repair helps only by escaping the domain).
			reflagInDomain(c, f)
		}
		// workload-broken: any fresh replacement POD that landed in the impaired domain keeps the unsatisfied readiness
		// gate, so it never becomes Ready even though its node is healthy.
		gateInDomain(c, f)

		// Recovered ⇔ ZERO unhealthy nodes remain (all faulted originals repaired and no re-flagged in-domain
		// replacement is currently unhealthy) — measured directly from node health, mechanism-agnostic. A dip (e.g. a
		// delayed reflag re-adding an unhealthy node) RESETS the sustained-health clock, so only durably-healthy state
		// counts. EXCEPTION: workload-broken flags no node (the pod's readiness gate is the fault), so it is measured by
		// workload readiness instead.
		recoveredNow := len(unhealthyNodeNames()) == 0
		if c.impairment.workloadBroken() {
			recoveredNow = healthyOnGoodCapacity(f) >= f.replicas
		}
		if !recoveredNow {
			healthySince = time.Time{}
			g.Expect(false).To(BeTrue(), "not yet recovered (unhealthy nodes remain / workload still unready)")
			return
		}
		if healthySince.IsZero() {
			healthySince = time.Now()
		}
		g.Expect(time.Since(healthySince)).To(BeNumerically(">=", confirm), "healthy but not yet sustained for the confirmation window")
	})
	return err == nil
}

// confirmWindow is how long full health must hold before recovery counts. It spans the latest possible reflag for a
// node-broken reflag-delay cell (so a "joins healthy, re-fails after t" replacement is observed re-failing before we'd
// declare recovery), and is 0 for every other cell (first-success — no delayed re-break is possible).
func confirmWindow(c cell) time.Duration {
	if !c.impairment.nodeBroken() {
		return 0
	}
	switch c.reflag {
	case reflagBelowDwell:
		return refDwell/2 + 2*pollInterval
	case reflagAboveDwell, reflagRandom:
		return 2*refDwell + 2*pollInterval
	default: // reflagImmediate — no healthy window, so no sustained-confirm needed
		return 0
	}
}

// healthyOnGoodCapacity counts workload pods that are Running, Ready, not terminating, and scheduled on good capacity —
// a node that exists and is not currently flagged SimUnhealthy. Requiring Ready (not just Running) is what makes a
// workload-broken pod, whose node is healthy but whose readiness gate is unsatisfied, correctly NOT count as recovered.
func healthyOnGoodCapacity(f *fleet) int {
	list := &corev1.PodList{}
	if err := env.Client.List(env.Context, list, client.MatchingLabelsSelector{Selector: f.selector}); err != nil {
		return 0
	}
	unhealthy := unhealthyNodeNames()
	n := 0
	for i := range list.Items {
		p := &list.Items[i]
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil && podReady(p) && !unhealthy.Has(p.Spec.NodeName) {
			n++
		}
	}
	return n
}

// podReady reports whether the pod's Ready condition is True (containers AND every readiness gate satisfied).
func podReady(p *corev1.Pod) bool {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == corev1.PodReady {
			return p.Status.Conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

// unhealthyNodeNames is the set of nodes currently carrying SimUnhealthy=True — the original faulted nodes plus any
// node-broken in-domain replacements. Pods on these nodes are not on "good capacity".
func unhealthyNodeNames() sets.Set[string] {
	s := sets.New[string]()
	list := &corev1.NodeList{}
	if err := env.Client.List(env.Context, list); err != nil {
		return s
	}
	for i := range list.Items {
		if hasTrueSimUnhealthy(&list.Items[i]) {
			s.Insert(list.Items[i].Name)
		}
	}
	return s
}

// --- repair-action observation (node signals) -----------------------------------------------------------------------

// nodeState reports the current disposition of a node by name: whether it still exists, carries the karpenter.sh/disrupted
// taint, or is terminating (has a deletion timestamp). A gone node reads as (false, false, false).
func nodeState(name string) (exists, disrupted, terminating bool) {
	n := &corev1.Node{}
	if err := env.Client.Get(env.Context, client.ObjectKey{Name: name}, n); err != nil {
		return false, false, false
	}
	for _, t := range n.Spec.Taints {
		if t.Key == v1.DisruptedTaintKey {
			disrupted = true
		}
	}
	return true, disrupted, n.DeletionTimestamp != nil
}

// clearFlakeIfElapsed models a FLAKY (transient/flapping) detector. For a flaky cell (flakeTTL>0), once flakeTTL has
// elapsed since fault onset it clears SimUnhealthy from EVERY node still carrying it (the originals plus any in-domain
// replacements) so the signal self-heals; the guarded node-heartbeat then resumes on those nodes and drops the condition,
// restoring them to healthy. Returns true once the flake has elapsed, so the caller stops re-flagging replacements. This
// exercises the detector-vs-toleration race: flakeTTL < RepairPolicy TolerationDuration ⇒ the fault self-heals before
// repair acts (repair should no-op); flakeTTL > TolerationDuration ⇒ repair fires before the signal clears. A one-shot
// clear, no re-assertion. (The node-fault-clear KWOK stage was meant to do this server-side but does not fire on this
// KWOK build — see data/RESUME-STATE.md — so the test drives it deterministically instead.) No-op for durable cells.
func clearFlakeIfElapsed(c cell, f *fleet) bool {
	if c.flakeTTL <= 0 || time.Since(f.faultOnset) < c.flakeTTL {
		return false
	}
	list := &corev1.NodeList{}
	if err := env.Client.List(env.Context, list); err != nil {
		return true // flake HAS elapsed; transient list error — skip this tick, caller still stops re-flagging
	}
	for i := range list.Items {
		n := &list.Items[i]
		if !hasTrueSimUnhealthy(n) {
			continue
		}
		stored := n.DeepCopy()
		n.Status.Conditions = lo.Reject(n.Status.Conditions, func(cond corev1.NodeCondition, _ int) bool {
			return cond.Type == simUnhealthyCondition
		})
		_ = env.Client.Status().Patch(env.Context, n, client.MergeFrom(stored))
	}
	return true
}

// reflagInDomain realizes the node-broken LAUNCH mechanism: any fresh replacement NODE that landed in the impaired
// domain is re-born SimUnhealthy, so an in-domain replacement never proves out (repair helps only by ESCAPING the
// domain). Domain-scoped — it touches ONLY replacements whose zone/arch matches the impaired domain. No-op for every
// other impairment.
func reflagInDomain(c cell, f *fleet) {
	if !c.impairment.nodeBroken() {
		return
	}
	orig := f.originalNodeNames()
	now := time.Now()
	for _, n := range env.Monitor.Nodes() {
		if orig.Has(n.Name) {
			continue // only fresh replacements, never the originally-faulted nodes
		}
		if n.Labels[f.impairedKey] != f.impairedValue {
			continue // out-of-domain replacement stays healthy (escaping the domain is how repair helps)
		}
		// Delayed reflag (reflag-delay axis): the replacement joins HEALTHY and only re-flags after its per-replacement
		// healthy window elapses (0 = immediate, the launch-failure model). This models "healthy for time t after joining,
		// then fails" — t<dwell re-fails during the confirmation window (AIMD should catch it), t>dwell re-fails after it
		// was credited proven (the retroactive claw-back case), trand straddles the dwell.
		seen, ok := f.replacementSeen[n.Name]
		if !ok {
			seen = now
			f.replacementSeen[n.Name] = seen
			f.replacementDelay[n.Name] = reflagDelayFor(c.reflag)
		}
		if now.Sub(seen) < f.replacementDelay[n.Name] {
			continue // still within its healthy window — leave it healthy so repair can (prematurely) credit it
		}
		markNodeUnhealthy(n, "RepairPerfNodeBroken", c.flakeTTL)
	}
}

// gateInDomain realizes the workload-broken SCHEDULE mechanism: any fresh replacement POD that landed in the impaired
// domain keeps the unsatisfied readiness gate, so it never becomes Ready even though its node is healthy. Domain-scoped
// to the impaired zone/arch. No-op for every other impairment.
func gateInDomain(c cell, f *fleet) {
	if !c.impairment.workloadBroken() {
		return
	}
	setWorkloadReadyGate(f, corev1.ConditionFalse, true)
}


// --- object + observation helpers -----------------------------------------------------------------------------------

func (f fleet) originalNodeNames() sets.Set[string] {
	s := sets.New[string]()
	for _, n := range f.nodes {
		s.Insert(n.Name)
	}
	return s
}

func (f fleet) faultedNodeNames() sets.Set[string] {
	s := sets.New[string]()
	for _, n := range f.faultNodes {
		s.Insert(n.Name)
	}
	return s
}

// markNodeUnhealthy injects the repair-matching fault as the CUSTOM SimUnhealthy=True condition (not Ready). Why not
// Ready: KWOK's node-initialize stage immediately reverts any Ready!=True back to Ready=True, AND a NotReady node isn't
// a clean voluntary-disruption candidate — so a Ready-based fault neither holds nor triggers repair. SimUnhealthy=True
// sticks while the node stays Ready=True (a valid candidate), exactly like a node-monitoring-agent condition: the owned
// node-heartbeat-with-lease stage skips faulted nodes so KWOK never wipes it. The deployed KWOK RepairPolicies() must
// include {SimUnhealthy, True}. Used for both the initial faulted nodes (injectFault) and node-broken in-domain
// replacements (reflagInDomain). If flakeTTL > 0 the node also gets a sim/fault-ttl annotation, which makes the
// node-fault-clear stage auto-clear the fault after flakeTTL (a flapping/transient detector); flakeTTL == 0 is durable.
func markNodeUnhealthy(n *corev1.Node, reason string, flakeTTL time.Duration) {
	GinkgoHelper()
	fresh := &corev1.Node{}
	if err := env.Client.Get(env.Context, client.ObjectKeyFromObject(n), fresh); err != nil {
		return
	}
	// Stamp the flake TTL first (metadata patch), so it is already present when the condition patch fires the event
	// node-fault-clear keys off. Durable cells (flakeTTL == 0) leave the annotation unset.
	if flakeTTL > 0 && fresh.Annotations["sim/fault-ttl"] != flakeTTL.String() {
		stored := fresh.DeepCopy()
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations["sim/fault-ttl"] = flakeTTL.String()
		_ = env.Client.Patch(env.Context, fresh, client.MergeFrom(stored))
	}
	stored := fresh.DeepCopy()
	found := false
	for i := range fresh.Status.Conditions {
		if fresh.Status.Conditions[i].Type == simUnhealthyCondition {
			fresh.Status.Conditions[i].Status = corev1.ConditionTrue
			fresh.Status.Conditions[i].Reason = reason
			fresh.Status.Conditions[i].LastHeartbeatTime = metav1.Now()
			// Do NOT touch LastTransitionTime on an existing condition — pin the repair toleration clock.
			found = true
		}
	}
	if !found {
		fresh.Status.Conditions = append(fresh.Status.Conditions, corev1.NodeCondition{
			Type: simUnhealthyCondition, Status: corev1.ConditionTrue, Reason: reason,
			LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now(),
		})
	}
	// Status subresource patch; ignore transient errors (measurement setup, retried by the Eventually caller).
	_ = env.Client.Status().Patch(env.Context, fresh, client.MergeFrom(stored))
}

// setWorkloadReadyGate patches the SimWorkloadReady readiness-gate condition on the workload pods to `status`. When
// inDomainOnly is true it patches ONLY pods on the blast's gated nodes — the faultNodes plus their fresh in-domain
// replacements (see gatedNodeNames) — so the fault is bounded by the blast, not the whole impaired domain; otherwise it
// patches every workload pod (establishing the healthy baseline in armImpairment). Transient patch errors are ignored —
// the Eventually caller retries.
func setWorkloadReadyGate(f *fleet, status corev1.ConditionStatus, inDomainOnly bool) {
	list := &corev1.PodList{}
	if err := env.Client.List(env.Context, list, client.MatchingLabelsSelector{Selector: f.selector}); err != nil {
		return
	}
	var inDomain sets.Set[string]
	if inDomainOnly {
		inDomain = f.gatedNodeNames()
	}
	for i := range list.Items {
		p := &list.Items[i]
		if inDomainOnly && !inDomain.Has(p.Spec.NodeName) {
			continue
		}
		stored := p.DeepCopy()
		p.Status.Conditions = upsertPodCondition(p.Status.Conditions, corev1.PodCondition{
			Type: simWorkloadReadyGate, Status: status, LastTransitionTime: metav1.Now(),
		})
		if status == corev1.ConditionFalse {
			// KWOK IGNORES readiness gates when it writes PodReady (the pod-ready stage force-sets Ready=True and even
			// sets our gate True itself), so flipping the gate alone never makes the pod NotReady. Drive the TOP-LEVEL
			// Ready (and ContainersReady) conditions directly. This persists because both the pod-ready and pod-unhealthy
			// stages only fire while `.status.podIP` does NOT exist (their selectors); a running (scheduled) workload pod
			// already has an IP, so nothing overwrites a directly-written Ready=False. gateInDomain re-asserts each poll
			// as a belt-and-suspenders against any reheal, mirroring sustainFault for nodes.
			p.Status.Conditions = upsertPodCondition(p.Status.Conditions, corev1.PodCondition{
				Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "SimWorkloadBroken",
				Message: "workload-broken fault", LastTransitionTime: metav1.Now(),
			})
			p.Status.Conditions = upsertPodCondition(p.Status.Conditions, corev1.PodCondition{
				Type: corev1.ContainersReady, Status: corev1.ConditionFalse, Reason: "SimWorkloadBroken",
				Message: "workload-broken fault", LastTransitionTime: metav1.Now(),
			})
		}
		_ = env.Client.Status().Patch(env.Context, p, client.MergeFrom(stored))
	}
}

// upsertPodCondition replaces any existing condition of the same type with `cond` (append-after-reject), returning the
// updated slice. Used to drive the workload-broken pods' Ready/ContainersReady/gate conditions.
func upsertPodCondition(conds []corev1.PodCondition, cond corev1.PodCondition) []corev1.PodCondition {
	return append(lo.Reject(conds, func(c corev1.PodCondition, _ int) bool { return c.Type == cond.Type }), cond)
}

// gatedNodeNames is the set of nodes whose workload pods the workload-broken fault keeps un-ready: the originally-faulted
// nodes (the `count` faultNodes chosen by the blast) PLUS any fresh replacement that landed in the impaired zone/arch.
// This mirrors node-broken's mechanism (sustainFault stamps the count faultNodes; reflagInDomain re-flags their in-domain
// replacements) so the blast bounds how much of the fleet breaks: a node merely CO-LOCATED in the impaired domain but not
// part of the fault keeps its pods healthy, and minority vs majority produce different broken-pod sets. Scoping the
// replacements to the impaired domain still guarantees no leak outside it. Empty domain ("" for uncorrelated) ⇒ just the
// faultNodes.
func (f *fleet) gatedNodeNames() sets.Set[string] {
	s := f.faultedNodeNames()
	if f.impairedKey == "" {
		return s
	}
	orig := f.originalNodeNames()
	list := &corev1.NodeList{}
	if err := env.Client.List(env.Context, list); err != nil {
		return s
	}
	for i := range list.Items {
		n := &list.Items[i]
		if orig.Has(n.Name) {
			continue // originals beyond the faultNodes are healthy co-located nodes — never gate them
		}
		if n.Labels[f.impairedKey] == f.impairedValue {
			s.Insert(n.Name) // a fresh in-domain replacement of a faulted node re-inherits the gate
		}
	}
	return s
}


func podUIDsOnNodes(selector labels.Selector, nodes []*corev1.Node) sets.Set[string] {
	names := sets.New[string]()
	for _, n := range nodes {
		names.Insert(n.Name)
	}
	s := sets.New[string]()
	list := &corev1.PodList{}
	if err := env.Client.List(env.Context, list, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return s
	}
	for i := range list.Items {
		if names.Has(list.Items[i].Spec.NodeName) {
			s.Insert(string(list.Items[i].UID))
		}
	}
	return s
}

// scrapeCloudProviderCalls sums the karpenter_cloudprovider_duration_seconds histogram's sample count across Create+Delete
// methods from the active Karpenter pod's /metrics (same API-server pod-proxy path the metrics poller uses). Failed calls are still
// counted (the histogram observes every attempt), so ICE/retry storms surface as call inflation.
//
// Returns 0 when the endpoint is unreachable; callers treat cp-calls as best-effort. This is preferred over the kwok
// provider exposing its own counters (it does not) — the deployed controller already publishes these.
// scrapeKarpenterMetrics fetches + parses the controller's /metrics into prometheus families (nil on any failure).
func scrapeKarpenterMetrics() map[string]*dto.MetricFamily {
	pod, err := env.FindActiveKarpenterPod(env.Context)
	if err != nil || pod == nil {
		return nil
	}
	data, err := env.KubeClient.CoreV1().Pods(pod.Namespace).ProxyGet("http", pod.Name, "8080", "/metrics", nil).DoRaw(env.Context)
	if err != nil {
		return nil
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		return nil
	}
	return families
}

func scrapeCloudProviderCalls() int {
	families := scrapeKarpenterMetrics()
	if families == nil {
		return 0
	}
	// The prometheus text parser groups a histogram's series (_bucket/_sum/_count) under the BASE family name; there is
	// no family literally named "..._count". Look up the base name and read each metric's histogram sample count.
	mf, ok := families["karpenter_cloudprovider_duration_seconds"]
	if !ok {
		return 0
	}
	total := 0.0
	for _, met := range mf.GetMetric() {
		method := labelValue(met, "method")
		if method == "Create" || method == "Delete" {
			total += float64(met.GetHistogram().GetSampleCount())
		}
	}
	return int(total)
}

// scrapePodsDisrupted reads karpenter_pods_disruption_initiated_total — the cumulative count of pods Karpenter has
// initiated disruption on. It's a COUNTER, so constant churn ACCUMULATES (a re-flagging slot re-disrupted N times adds
// N), which is the disruption-cost signal we want. It counts force-deletes too (unlike eviction requests) and is emitted
// by both the repair path (disruption queue) and the node.health breaker. Sampled as a delta across the cell.
func scrapePodsDisrupted() int {
	return scrapeCounterSum("karpenter_pods_disruption_initiated_total")
}

// scrapePodsGracefullyDrained reads karpenter_pods_drained_total — pods that completed a GRACEFUL eviction (eviction API
// accepted, PDB + grace honored). This is the "gracefully disrupted" half of the disruption-cost split.
func scrapePodsGracefullyDrained() int {
	return scrapeCounterSum("karpenter_pods_drained_total")
}

// scrapePodsForceDeleted reads karpenter_pods_force_deleted_total — pods force-deleted past their grace-period expiration
// (terminator DeleteExpiringPods), BYPASSING PDB and grace. This is the "disrupted without drain" half of the split.
// Absent in builds without the counter (e.g. the mainline :breaker image) → reads 0.
func scrapePodsForceDeleted() int {
	return scrapeCounterSum("karpenter_pods_force_deleted_total")
}

// scrapeCounterSum sums a counter family's samples across all label sets (0 if the family is absent).
func scrapeCounterSum(family string) int {
	families := scrapeKarpenterMetrics()
	if families == nil {
		return 0
	}
	total := 0.0
	if mf, ok := families[family]; ok {
		for _, met := range mf.GetMetric() {
			total += met.GetCounter().GetValue()
		}
	}
	return int(total)
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// interruptibleEventually runs an Eventually that succeeds when the condition holds, returning a (nil-on-success)
// error rather than failing the spec — a never-recovered run is a RESULT to record, not a hard failure. It uses a
// silent Gomega whose fail handler is a no-op, so a timeout returns false instead of aborting the spec.
func interruptibleEventually(timeout time.Duration, condition func(g Gomega)) error {
	silent := NewGomega(func(_ string, _ ...int) {})
	ok := silent.Eventually(func(g Gomega) { condition(g) }).WithTimeout(timeout).WithPolling(pollInterval).Should(Succeed())
	if !ok {
		return fmt.Errorf("condition not met within %s", timeout)
	}
	return nil
}

func maxZero(v int) int {
	if v < 0 {
		return 0
	}
	return v
}
