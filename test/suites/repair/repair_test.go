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
	"os"
	"time"

	"sigs.k8s.io/karpenter/test/pkg/debug"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck
	. "github.com/onsi/gomega"    //nolint:staticcheck
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	provisionTimeout = 5 * time.Minute // fault-free provisioning of the workload onto fresh nodes
	pollInterval     = 5 * time.Second

	// nodePoolTGP is the NodePool's TerminationGracePeriod — the customer-facing drain bound. Setting it on the NodePool
	// (NOT the RepairPolicy, which is an implementation detail) lets a correct implementation forcefully terminate a
	// node whose drain cannot complete (kubelet-dead / blocking-PDB), so those cells resolve within the window instead
	// of hanging forever. It must exceed the deployed toleration+dwell yet stay under recoveryTimeout.
	nodePoolTGP = 90 * time.Second

	// The deployed KWOK build's RepairPolicies (kwok/cloudprovider/cloudprovider.go RepairPolicies()) must include the
	// custom {ConditionType: "SimUnhealthy", ConditionStatus: True} policy with a short (seconds) TolerationDuration, so
	// the injected fault (see markNodeUnhealthy) is actionable in test time. This is a deployment-side change; the suite
	// only observes.
)

// recoveryTimeout is the fault-onset → workload-healthy ceiling. It MUST exceed the implementation's own eventual-
// consistency timescale, not just the toleration: an AIMD restraint only widens after a replacement holds healthy for
// the success dwell (repairDwell = 5m in the current build), so mitigating a correlated MAJORITY fault takes several
// dwell cycles (minutes to tens of minutes). Too short a ceiling reports "unmitigated" for a fault that would recover
// and hides the widening curve entirely. Overridable via REPAIR_RECOVERY_TIMEOUT (e.g. "25m") for deep/large-fault runs.
var recoveryTimeout = envDuration("REPAIR_RECOVERY_TIMEOUT", 6*time.Minute)

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
	kwokZones  = []string{"test-zone-a", "test-zone-b", "test-zone-c"}
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
func runCell(c cell) repairMetrics {
	m := repairMetrics{}

	cpBefore := scrapeCloudProviderCalls()
	f := provisionCell(c)

	// Arm the replacement mechanism BEFORE injecting so it affects replacements as they appear.
	armImpairment(c, &f)
	injectFault(c, &f)

	if measureRecovery(c, &f) {
		d := time.Since(f.faultOnset)
		m.timeToRecovery = &d
	}
	m.mitigatedFraction = fractionRepaired(&f)
	m.disruptedPods = countDisrupted(&f)
	m.cpCalls = maxZero(scrapeCloudProviderCalls() - cpBefore)
	return m
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

// provisionCell builds the topology NodePools + workload Deployment (+ PDB), waits for healthy provisioning, and
// records the workload nodes and the fault-domain subset (recording the impaired domain in the fleet).
func provisionCell(c cell) fleet {
	f := fleet{}
	f.nodePools = newNodePools(c)
	f.replicas = c.scale.nodes()

	f.deployment = newWorkload(c, f.replicas)
	f.selector = labels.SelectorFromSet(f.deployment.Spec.Selector.MatchLabels)

	objs := []client.Object{nodeClass}
	for _, np := range f.nodePools {
		objs = append(objs, np)
	}
	objs = append(objs, f.deployment)
	if pdb := newPDB(c, f.deployment.Spec.Selector.MatchLabels); pdb != nil {
		objs = append(objs, pdb)
	}
	env.ExpectCreated(objs...)

	env.EventuallyExpectHealthyPodCountWithTimeout(provisionTimeout, f.selector, f.replicas)
	f.nodes = env.EventuallyExpectCreatedNodeCount(">=", 1)
	f.selectFaultDomain(c)
	f.faultedPods = podUIDsOnNodes(f.selector, f.faultNodes)
	return f
}

// newWorkload builds a Deployment forced one-pod-per-node so faulting N nodes disrupts N pods, sized so Karpenter must
// provision `replicas` nodes. One-pod-per-node is guaranteed by the CPU request (1000m) exceeding half the KWOK node's
// allocatable (c-2x ≈ 1900m ⇒ a second pod never fits), so the hostname spread is only a placement nudge and is set to
// ScheduleAnyway: a hard DoNotSchedule spread strands a pod whenever provisioning is briefly uneven, and a pending pod
// keeps its target node "nominated", which makes Karpenter refuse to disrupt it — silently blocking repair on any
// multi-node fault.
func newWorkload(c cell, replicas int) *appsv1.Deployment {
	name := "repair-" + test.RandomName()
	opts := test.CreateDeploymentOptions(name, int32(replicas), "1", "1Gi")
	dep := test.Deployment(opts)
	// One pod per node is REQUIRED for the fault model (faulting N nodes must disrupt N pods, and blast is a fraction of
	// nodes). CPU sizing alone does NOT guarantee it — freed of a per-node constraint, Karpenter bin-packs onto larger
	// instances (e.g. 3× 1000m pods on a c-4x/3900m node), so `scale` nodes collapse to a third. Enforce it with
	// REQUIRED hostname anti-affinity: each pod needs a node with no other workload pod, so Karpenter provisions one node
	// per pod. This is disruption-FRIENDLY (unlike a DoNotSchedule topology spread, which strands pending pods and pins
	// nominations that block repair) — a disrupted pod reschedules onto its fresh replacement node, which is empty.
	sel := &metav1.LabelSelector{MatchLabels: dep.Spec.Selector.MatchLabels}
	dep.Spec.Template.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey: corev1.LabelHostname, LabelSelector: sel,
		}},
	}}
	// Also spread across ZONES (failure domains) so a correlated zonal blast lands on a zone's worth of pods. Soft
	// (ScheduleAnyway) — the anti-affinity already forces one-per-node; this only biases the zone distribution.
	dep.Spec.Template.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       corev1.LabelTopologyZone,
		WhenUnsatisfiable: corev1.ScheduleAnyway,
		LabelSelector:     sel,
	}}
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

// newNodePools realizes the topology axis. per-AZ pins each pool to a zone; per-AMI pins each to an arch; single is one
// elastic pool. terminate-first makes each pool static (fixed Replicas ⇒ reserved capacity, delete-then-refill).
func newNodePools(c cell) []*v1.NodePool {
	var domains []map[string]string
	switch c.topo {
	case topoPerAZ:
		for _, z := range kwokZones {
			domains = append(domains, map[string]string{corev1.LabelTopologyZone: z})
		}
	case topoPerAMI:
		for _, a := range kwokArches {
			domains = append(domains, map[string]string{corev1.LabelArchStable: a})
		}
	default:
		domains = []map[string]string{nil}
	}

	pools := make([]*v1.NodePool, 0, len(domains))
	for _, dom := range domains {
		np := env.DefaultNodePool(nodeClass)
		np.Spec.Limits = v1.Limits{}
		// Customer-facing drain bound: with a NodePool TGP, a node whose drain cannot complete (kubelet-dead, blocking
		// PDB) is forcefully terminated after the deadline rather than hanging. This is deliberately on the NodePool,
		// not the RepairPolicy (that would be an implementation detail the black-box rig must not encode).
		np.Spec.Template.Spec.TerminationGracePeriod = &metav1.Duration{Duration: nodePoolTGP}
		// Elastic (replace-first) posture: consolidation stays off so headroom nodes are not reclaimed mid-measurement.
		np.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmpty
		np.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("Never")
		np.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "100%"}}
		for k, v := range dom {
			test.ReplaceRequirements(np, v1.NodeSelectorRequirementWithMinValues{
				Key: k, Operator: corev1.NodeSelectorOpIn, Values: []string{v},
			})
		}
		if c.strat == stratTerminateFirst {
			// Static/reserved: a fixed node count with no elastic headroom → repair must delete then wait for refill.
			np.Spec.Replicas = lo.ToPtr(int64(c.scale.nodes()))
		}
		pools = append(pools, np)
	}
	return pools
}

// selectFaultDomain picks the nodes to fault per the blast (how many) and impairment (how they relate), and records the
// impaired domain (impairedKey/impairedValue) on the fleet so the domain-scoped mechanisms can target replacements:
//   - single (always uncorrelated): exactly one node; no shared domain.
//   - uncorrelated multi-node: `count` nodes chosen to MAXIMIZE domain diversity (distinct zone+arch), so they share
//     no domain.
//   - zone-* / ami-*: `count` nodes confined to one shared zone/arch domain (a correlated outage); that zone/arch is
//     recorded as the impaired domain.
//
// It selects the same node subset regardless of the domained impairment's mechanism (benign / node-broken /
// workload-broken) — the mechanism differs only in what injectFault and the poll do to those nodes' replacements.
func (f *fleet) selectFaultDomain(c cell) {
	count := c.blast.count(c.scale)
	if count > len(f.nodes) {
		count = len(f.nodes)
	}
	// blast=single is always uncorrelated (the coupling) — one node, no recorded domain. Every other blast (including a
	// minority/majority whose count rounds to 1) still runs the impairment switch so a domained cell RECORDS its impaired
	// zone/arch (impairedKey/impairedValue) — the domain-scoped re-flag/gate need it even when count == 1.
	if c.blast == blastSingle {
		f.faultNodes = capNodes(f.nodes, 1)
		return
	}
	switch c.impairment.domainKey() {
	case corev1.LabelTopologyZone:
		f.faultNodes = f.pickImpairedDomain(corev1.LabelTopologyZone, count)
	case corev1.LabelArchStable:
		f.faultNodes = f.pickImpairedDomain(corev1.LabelArchStable, count)
	default: // uncorrelated
		f.faultNodes = pickDiverseDomains(f.nodes, count)
	}
}

// pickImpairedDomain returns up to `count` nodes that all share the most-populated value of `key` (a correlated outage
// confined to one zone/arch) and records that domain on the fleet. If the largest group is smaller than count, it
// returns that whole group.
func (f *fleet) pickImpairedDomain(key string, count int) []*corev1.Node {
	byVal := map[string][]*corev1.Node{}
	for _, n := range f.nodes {
		byVal[n.Labels[key]] = append(byVal[n.Labels[key]], n)
	}
	bestVal, best := "", []*corev1.Node(nil)
	for v, group := range byVal {
		if len(group) > len(best) {
			bestVal, best = v, group
		}
	}
	f.impairedKey = key
	f.impairedValue = bestVal
	return capNodes(best, count)
}

// pickDiverseDomains returns `count` nodes chosen greedily to maximize distinct (zone, arch) domains, so the failures
// share no failure domain. Once every distinct domain is used, it falls back to filling from remaining nodes.
func pickDiverseDomains(nodes []*corev1.Node, count int) []*corev1.Node {
	domain := func(n *corev1.Node) string {
		return n.Labels[corev1.LabelTopologyZone] + "/" + n.Labels[corev1.LabelArchStable]
	}
	seen := sets.New[string]()
	var picked, rest []*corev1.Node
	for _, n := range nodes {
		if len(picked) >= count {
			break
		}
		if d := domain(n); !seen.Has(d) {
			seen.Insert(d)
			picked = append(picked, n)
		} else {
			rest = append(rest, n)
		}
	}
	for _, n := range rest {
		if len(picked) >= count {
			break
		}
		picked = append(picked, n)
	}
	return picked
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
		// SCHEDULE-time fault: flip the readiness gate False on the workload pods in the impaired domain so they go —
		// and stay — un-ready while their nodes remain Ready. The poll re-applies this to fresh in-domain replacements.
		setWorkloadReadyGate(f, corev1.ConditionFalse, true)
		return
	}
	if c.drain == drainNoKubeletDead {
		// "Eviction won't succeed": pin the faulted nodes' workload pods with the env's TestingFinalizer so an eviction
		// (a DELETE) is accepted but never completes — the pod sticks in Terminating, exactly as when a dead kubelet
		// never acks the graceful termination. The NodePool TGP then decides whether repair can force past it. Cleanup
		// strips TestingFinalizer, so this does not leak into teardown.
		blockEvictionOnFaultNodes(f)
	}
	sustainFault(c, f) // inject SimUnhealthy=True on the faulted nodes
}

// sustainFault (re-)asserts SimUnhealthy=True on every still-existing faulted node. It must be called repeatedly through
// the measurement window because KWOK's node-heartbeat-with-lease stage rewrites status.conditions from its managed set
// on each heartbeat and DROPS the injected custom condition — so without re-assertion the fault evaporates within a
// heartbeat interval and a slow (correct) repair, gated by the implementation's success dwell, never sees a durable
// candidate. LastTransitionTime is pinned to faultOnset so re-asserting never resets the repair toleration clock. A node
// that repair has already terminated (Get fails) is left alone, so the fault ends exactly when repair completes.
func sustainFault(c cell, f *fleet) {
	if c.impairment.workloadBroken() {
		return // workload-broken injects no node condition; there is nothing to sustain
	}
	ltt := metav1.NewTime(f.faultOnset)
	for i, n := range f.faultNodes {
		fresh := &corev1.Node{}
		if err := env.Client.Get(env.Context, client.ObjectKeyFromObject(n), fresh); err != nil {
			continue // node gone (repaired) — nothing to sustain
		}
		if hasTrueSimUnhealthy(fresh) {
			continue // still asserted (survived this heartbeat) — leave the pinned transition time intact
		}
		stored := fresh.DeepCopy()
		fresh.Status.Conditions = append(lo.Reject(fresh.Status.Conditions, func(cond corev1.NodeCondition, _ int) bool {
			return cond.Type == simUnhealthyCondition
		}), corev1.NodeCondition{
			Type: simUnhealthyCondition, Status: corev1.ConditionTrue, Reason: faultReason(c, i),
			LastHeartbeatTime: metav1.Now(), LastTransitionTime: ltt,
		})
		_ = env.Client.Status().Patch(env.Context, fresh, client.MergeFrom(stored))
	}
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

// measureRecovery is the ONE poll every cell runs: each tick re-asserts the base fault and applies the domain-scoped
// replacement mechanisms, then checks whether the workload is fully healthy on good capacity. Returns whether full
// recovery was observed within recoveryTimeout.
func measureRecovery(c cell, f *fleet) (recovered bool) {
	err := interruptibleEventually(recoveryTimeout, func(g Gomega) {
		// Keep the injected node fault alive against KWOK's heartbeat clobber until repair actually terminates the node
		// (no-op for workload-broken) — otherwise a slow (dwell-gated) repair never sees a durable candidate.
		sustainFault(c, f)
		// node-broken: any fresh replacement NODE that landed in the impaired domain is re-born unhealthy, so an
		// in-domain replacement never proves out (repair helps only by escaping the domain).
		reflagInDomain(c, f)
		// workload-broken: any fresh replacement POD that landed in the impaired domain keeps the unsatisfied readiness
		// gate, so it never becomes Ready even though its node is healthy.
		gateInDomain(c, f)

		// Fully recovered ⇒ at least `replicas` workload pods are Running, Ready, not terminating, and on GOOD capacity
		// (a node that is not currently flagged SimUnhealthy — which excludes both the original faulted nodes and any
		// node-broken in-domain replacements). Terminating pods are excluded so an eviction-pinned pod (kubelet-dead)
		// lingering in Terminating doesn't mask its healthy replacement.
		g.Expect(healthyOnGoodCapacity(f)).To(BeNumerically(">=", f.replicas))
	})
	return err == nil
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

// reflagInDomain realizes the node-broken LAUNCH mechanism: any fresh replacement NODE that landed in the impaired
// domain is re-born SimUnhealthy, so an in-domain replacement never proves out (repair helps only by ESCAPING the
// domain). Domain-scoped — it touches ONLY replacements whose zone/arch matches the impaired domain. No-op for every
// other impairment.
func reflagInDomain(c cell, f *fleet) {
	if !c.impairment.nodeBroken() {
		return
	}
	orig := f.originalNodeNames()
	for _, n := range env.Monitor.Nodes() {
		if orig.Has(n.Name) {
			continue // only fresh replacements, never the originally-faulted nodes
		}
		if n.Labels[f.impairedKey] != f.impairedValue {
			continue // out-of-domain replacement stays healthy (escaping the domain is how repair helps)
		}
		markNodeUnhealthy(n, "RepairPerfNodeBroken")
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

// countDisrupted returns how many of the originally-faulted workload pods no longer exist (were evicted/recreated).
func countDisrupted(f *fleet) int {
	if f.faultedPods.Len() == 0 {
		return 0
	}
	// A faulted pod is "disrupted" if its UID is no longer present anywhere for the workload (evicted/recreated).
	live := podUIDs(f.selector)
	disrupted := 0
	for uid := range f.faultedPods {
		if !live.Has(uid) {
			disrupted++
		}
	}
	return disrupted
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
// a clean voluntary-disruption candidate — so a Ready-based fault neither holds nor triggers repair. KWOK does not
// manage arbitrary conditions, so SimUnhealthy=True sticks while the node stays Ready=True (a valid candidate), exactly
// like a node-monitoring-agent condition. The deployed KWOK RepairPolicies() must include {SimUnhealthy, True}. Used to
// re-flag node-broken in-domain replacements (the initial faulted nodes are stamped by sustainFault instead).
func markNodeUnhealthy(n *corev1.Node, reason string) {
	GinkgoHelper()
	fresh := &corev1.Node{}
	if err := env.Client.Get(env.Context, client.ObjectKeyFromObject(n), fresh); err != nil {
		return
	}
	stored := fresh.DeepCopy()
	found := false
	for i := range fresh.Status.Conditions {
		if fresh.Status.Conditions[i].Type == simUnhealthyCondition {
			fresh.Status.Conditions[i].Status = corev1.ConditionTrue
			fresh.Status.Conditions[i].Reason = reason
			fresh.Status.Conditions[i].LastHeartbeatTime = metav1.Now()
			fresh.Status.Conditions[i].LastTransitionTime = metav1.Now()
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
		p.Status.Conditions = append(lo.Reject(p.Status.Conditions, func(cond corev1.PodCondition, _ int) bool {
			return cond.Type == simWorkloadReadyGate
		}), corev1.PodCondition{
			Type: simWorkloadReadyGate, Status: status, LastTransitionTime: metav1.Now(),
		})
		_ = env.Client.Status().Patch(env.Context, p, client.MergeFrom(stored))
	}
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

func podUIDs(selector labels.Selector) sets.Set[string] {
	s := sets.New[string]()
	list := &corev1.PodList{}
	if err := env.Client.List(env.Context, list, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return s
	}
	for i := range list.Items {
		s.Insert(string(list.Items[i].UID))
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
func scrapeCloudProviderCalls() int {
	pod, err := env.FindActiveKarpenterPod(env.Context)
	if err != nil || pod == nil {
		return 0
	}
	data, err := env.KubeClient.CoreV1().Pods(pod.Namespace).ProxyGet("http", pod.Name, "8080", "/metrics", nil).DoRaw(env.Context)
	if err != nil {
		return 0
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
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

func capNodes(nodes []*corev1.Node, n int) []*corev1.Node {
	if len(nodes) > n {
		return nodes[:n]
	}
	return nodes
}

func maxZero(v int) int {
	if v < 0 {
		return 0
	}
	return v
}
