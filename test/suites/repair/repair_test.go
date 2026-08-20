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

// The e2e runner: each cell is realized as (Deployment sized per correlation + PDB per drain axis + NodePool topology),
// provisioned against a DEPLOYED Karpenter/KWOK build; the fault is injected by patching KWOK node conditions; recovery
// is MEASURED with Eventually (never a fake-clock step, because this is a real control plane on a real clock).
//
// Cell → mechanism map:
//   help      → the replacement fault model, applied post-injection (healthy / launch-fail / re-flag / workload-broken).
//   scale     → fleet size (workload replicas, one pod per node) — makes the blast a meaningful FRACTION of the fleet.
//   blast     → how many nodes are faulted (single / ~10% / ~35% of the fleet).
//   structure → whether the faulted nodes share a failure domain (unrelated vs zone/ami/reason) — the correlation signal.
//   drain     → PDB presence/strictness, and kubelet-dead (eviction won't succeed ⇒ forceful past the NodePool TGP).
//   strat     → replace-first (elastic NodePool, headroom) vs terminate-first (static/reserved; currently un-runnable).
//   topo      → single NodePool vs per-AZ vs per-AMI (arch) NodePools; sets the domains a repair's budget lines up with.
//
// Metrics recorded per (cell, option), scale-agnostic: time-to-recovery, disrupted-pods, cloud-provider calls (report.go).

import (
	"bytes"
	"fmt"
	"os"
	"sigs.k8s.io/karpenter/test/pkg/debug"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck
	. "github.com/onsi/gomega"    //nolint:staticcheck
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	// reflagDwell approximates the "success dwell" boundary for the re-flag help models. In-dwell re-flags before it,
	// after-dwell re-flags past it. This must be shorter than recoveryTimeout so the run still converges.
	reflagDwell = 20 * time.Second

	// nodePoolTGP is the NodePool's TerminationGracePeriod — the customer-facing drain bound. Setting it on the NodePool
	// (NOT the RepairPolicy, which is an implementation detail) lets a correct implementation forcefully terminate a
	// node whose drain cannot complete (kubelet-dead / blocking-PDB), so those cells resolve within the window instead
	// of hanging forever. It must exceed the deployed toleration+dwell yet stay under recoveryTimeout.
	nodePoolTGP = 90 * time.Second

	// observeWindow bounds how long a NON-help cell waits to see whether repair takes a disrupting action before we
	// classify it declined. Acting cells return as soon as the action is observed (plus a short settle); only genuinely
	// declined cells wait the full window. Sized above the deployed toleration (~15s) with margin.
	observeWindow = 60 * time.Second

	// settleAfterAction is how long we let an observed disruption finish (terminate ⇒ churned, or stay stuck ⇒ wedged)
	// once repair has acted on a non-help cell. Covers the NodePool TGP so a forceful termination can complete.
	settleAfterAction = nodePoolTGP + 20*time.Second

	// The deployed KWOK build's RepairPolicies (kwok/cloudprovider/cloudprovider.go RepairPolicies()) must include the
	// custom {ConditionType: "SimUnhealthy", ConditionStatus: True} policy with a short (seconds) TolerationDuration, so
	// the injected fault (see patchNodeReady) is actionable in test time. This is a deployment-side change; the suite
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

// simWorkloadReadyGate is a pod readiness gate used only by the workload-broken help model. KWOK fakes container
// readiness, so a normal probe can't make a pod unready; a readiness gate makes Ready = AND(containers, gate), and
// nothing satisfies the gate unless we explicitly patch it. Provisioning patches it True to establish a healthy
// baseline; the workload-broken fault flips it False so the workload is genuinely down while the NODES stay healthy
// (the detection blind spot — repair must correctly decline).
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
	faultNodes  []*corev1.Node // the subset selected for the fault (count per blast, domain per structure)
	faultedPods sets.Set[string]
	faultOnset  time.Time // when the fault was injected (anchors the re-flag dwell)
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

// runCell provisions the cell, injects its fault, measures the outcome, and returns the observable metrics.
func runCell(c cell) repairMetrics {
	m := repairMetrics{}

	cpBefore := scrapeCloudProviderCalls()
	f := provisionCell(c)

	// Apply the replacement fault model that must be armed BEFORE the fault so it affects replacements as they appear.
	armHelpModel(c, &f)

	injectFault(c, &f)

	if c.help.helps() {
		// A genuine fault: the oracle is time-to-recovery. Wait for the workload to be healthy again on good capacity.
		recovered := measureRecovery(c, &f)
		m.outcome, m.timeToRecovery = classifyHelp(&f, recovered, f.faultOnset)
	} else {
		// No genuine fault: the oracle is "did repair correctly decline?" Observe whether it took a disrupting action
		// and whether that action completed (churned) or stuck mid-termination (wedged).
		acted, stuck := observeRepairAction(c, &f)
		m.outcome = classifyNonHelp(acted, stuck)
	}

	m.disruptedPods = countDisrupted(&f)
	m.cpCalls = maxZero(scrapeCloudProviderCalls() - cpBefore)
	m.mitigatedFraction = fractionRepaired(&f)
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

// classifyHelp scores a genuine-fault cell: recovered ⇒ mitigated (with time-to-recovery); otherwise wedged if a faulted
// node is stuck mid-termination, else unmitigated (a genuine miss — frozen breaker, launch-fail, endless re-flag).
func classifyHelp(f *fleet, recovered bool, onset time.Time) (outcome, *time.Duration) {
	if recovered {
		d := time.Since(onset)
		return outcomeMitigated, &d
	}
	if anyStuck(f.faultNodes) {
		return outcomeWedged, nil
	}
	return outcomeUnmitigated, nil
}

// classifyNonHelp scores a cell where repair cannot truly help: declining is correct; acting is waste (churned), and an
// action that can't finish is wedged.
func classifyNonHelp(acted, stuck bool) outcome {
	switch {
	case !acted:
		return outcomeDeclined
	case stuck:
		return outcomeWedged
	default:
		return outcomeChurned
	}
}

// observeRepairAction waits (bounded by observeWindow) for repair to take a disrupting action on the watched nodes —
// the fault domain, or (workload-broken, no fault domain) the whole workload fleet, which a correct implementation must
// leave alone. Once an action is seen, it lets it settle (up to settleAfterAction, covering the NodePool TGP) and
// reports whether it finished (churned) or is stuck mid-termination (wedged).
func observeRepairAction(c cell, f *fleet) (acted, stuck bool) {
	watch := f.faultNodes
	if len(watch) == 0 {
		watch = f.nodes
	}
	_ = interruptibleEventually(observeWindow, func(g Gomega) {
		sustainFault(c, f) // keep the fault alive against KWOK heartbeat clobber while waiting for repair to act
		g.Expect(anyDisrupted(watch) || countDisrupted(f) > 0).To(BeTrue())
	})
	acted = anyDisrupted(watch) || countDisrupted(f) > 0
	if !acted {
		return false, false
	}
	finished := interruptibleEventually(settleAfterAction, func(g Gomega) {
		g.Expect(allGone(watch)).To(BeTrue())
	}) == nil
	stuck = !finished && anyStuck(watch)
	return acted, stuck
}

// provisionCell builds the topology NodePools + workload Deployment (+ PDB), waits for healthy provisioning, and
// records the workload nodes and the fault-domain subset.
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
	f.faultNodes = selectFaultDomain(c, f)
	f.faultedPods = podUIDsOnNodes(f.selector, f.faultNodes)
	return f
}

// newWorkload builds a Deployment forced one-pod-per-node (hostname spread) so faulting N nodes disrupts N pods, sized
// so Karpenter must provision `replicas` nodes.
func newWorkload(c cell, replicas int) *appsv1.Deployment {
	name := "repair-" + test.RandomName()
	mods := []test.DeploymentOptionModifier{test.WithHostnameSpread()}
	// Requests large enough that each pod lands on its own node (with hostname spread as the belt-and-braces guard).
	opts := test.CreateDeploymentOptions(name, int32(replicas), "1", "1Gi", mods...)
	dep := test.Deployment(opts)
	if c.help == helpNoWorkloadBroken {
		// workload-broken blind spot: nodes stay Ready+healthy to Karpenter, but the workload itself is down. A readiness
		// gate makes each pod's readiness depend on a custom condition KWOK does not set; provisioning patches it True to
		// reach a healthy baseline, and the fault flips it False. No node condition is injected — so a correct repair
		// implementation sees only healthy nodes and declines.
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

// selectFaultDomain picks the nodes to fault per the blast (how many) and structure (how they relate) axes:
//   - workload-broken faults nothing (the fault is in the workload).
//   - single: exactly one node.
//   - zone / ami: `count` nodes confined to one shared domain (a correlated outage).
//   - reason: `count` arbitrary nodes (the shared "domain" is the injected reason; see injectFault), spread across zones/AMIs.
//   - unrelated: `count` nodes chosen to MAXIMIZE domain diversity (distinct zone+arch), so they share no domain.
func selectFaultDomain(c cell, f fleet) []*corev1.Node {
	if c.help == helpNoWorkloadBroken {
		return nil
	}
	count := c.blast.count(c.scale)
	if count > len(f.nodes) {
		count = len(f.nodes)
	}
	if c.blast == blastSingle || count <= 1 {
		return capNodes(f.nodes, 1)
	}
	switch c.structure {
	case structZone:
		return pickSharingDomain(f.nodes, corev1.LabelTopologyZone, count)
	case structAMI:
		return pickSharingDomain(f.nodes, corev1.LabelArchStable, count)
	case structReason:
		return capNodes(f.nodes, count) // shared reason is stamped in injectFault; domain spread is incidental
	default: // structUnrelated
		return pickDiverseDomains(f.nodes, count)
	}
}

// pickSharingDomain returns up to `count` nodes that all share the most-populated value of `key` (a correlated outage
// confined to one zone/arch). If the largest group is smaller than count, it returns that whole group.
func pickSharingDomain(nodes []*corev1.Node, key string, count int) []*corev1.Node {
	byVal := map[string][]*corev1.Node{}
	for _, n := range nodes {
		v := n.Labels[key]
		byVal[v] = append(byVal[v], n)
	}
	var best []*corev1.Node
	for _, group := range byVal {
		if len(group) > len(best) {
			best = group
		}
	}
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

// armHelpModel sets up implementation of the replacement fault model that must exist before/at fault injection.
func armHelpModel(c cell, f *fleet) {
	if c.help == helpNoLaunchFail {
		// launch-fail (ICE/capacity): replacements can never be created. We clamp every pool so no new node beyond the
		// current fleet can launch. The initial fleet is already provisioned, so this only blocks replacements.
		// TODO(validate-on-cluster): confirm this reliably yields ICE-style failed launches (call inflation) rather
		// than a silent no-op. An alternative is marking the KWOKNodeClass NotReady, which makes Create() return
		// NodeClassNotReadyError in the deployed kwok provider.
		for _, np := range f.nodePools {
			stored := np.DeepCopy()
			np.Spec.Limits = v1.Limits{corev1.ResourceCPU: resource.MustParse("0")}
			Expect(env.Client.Patch(env.Context, np, client.MergeFrom(stored))).To(Succeed())
		}
	}
}

// injectFault marks the fault-domain nodes with the repair-matching condition. For kubelet-dead it first blocks
// eviction on those nodes' workload pods (so a graceful drain can never complete). workload-broken has no fault domain,
// so this injects no node condition — the fault lives in the (readiness-gated) workload alone.
func injectFault(c cell, f *fleet) {
	if c.drain == drainNoKubeletDead {
		// "Eviction won't succeed": pin the faulted nodes' workload pods with the env's TestingFinalizer so an eviction
		// (a DELETE) is accepted but never completes — the pod sticks in Terminating, exactly as when a dead kubelet
		// never acks the graceful termination. The NodePool TGP then decides whether repair can force past it. Cleanup
		// strips TestingFinalizer, so this does not leak into teardown.
		blockEvictionOnFaultNodes(f)
	}
	f.faultOnset = time.Now()
	sustainFault(c, f)
}

// sustainFault (re-)asserts SimUnhealthy=True on every still-existing faulted node. It must be called repeatedly through
// the measurement window because KWOK's node-heartbeat-with-lease stage rewrites status.conditions from its managed set
// on each heartbeat and DROPS the injected custom condition — so without re-assertion the fault evaporates within a
// heartbeat interval and a slow (correct) repair, gated by the implementation's success dwell, never sees a durable
// candidate. LastTransitionTime is pinned to faultOnset so re-asserting never resets the repair toleration clock. A node
// that repair has already terminated (Get fails) is left alone, so the fault ends exactly when repair completes.
func sustainFault(c cell, f *fleet) {
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

// faultReason is the Reason stamped on the injected SimUnhealthy condition. For unrelated multi-node failures each node
// gets a DISTINCT reason (so they share no cause); every other structure shares one reason (a common cause — the
// correlation signal for the reason structure, and harmless for zone/ami which correlate on a label instead).
func faultReason(c cell, i int) string {
	if c.blast != blastSingle && c.structure == structUnrelated {
		return fmt.Sprintf("RepairPerfUnrelated-%d", i)
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

// measureRecovery waits (Eventually) for the workload to be healthy again on non-faulted capacity, re-flagging fresh
// replacements per the help model along the way. Returns whether recovery was observed within recoveryTimeout.
func measureRecovery(c cell, f *fleet) (recovered bool) {
	err := interruptibleEventually(recoveryTimeout, func(g Gomega) {
		// Keep the injected fault alive against KWOK's heartbeat clobber until repair actually terminates the node —
		// otherwise a slow (dwell-gated) repair never sees a durable candidate.
		sustainFault(c, f)
		// Re-flag fresh replacements for the re-flag help models: any workload node that is not one of the originally
		// faulted nodes and is Ready gets re-faulted (in-dwell vs after-dwell differ only in when we allow it).
		maybeReflag(c, f)

		// Recovered ⇒ at least `replicas` workload pods are Running on good capacity: not terminating and not on a
		// still-faulted node. Terminating pods are excluded so an eviction-pinned pod (kubelet-dead) lingering in
		// Terminating doesn't mask its healthy replacement.
		g.Expect(healthyOnGoodCapacity(f)).To(BeNumerically(">=", f.replicas))
	})
	return err == nil
}

// healthyOnGoodCapacity counts workload pods that are Running, not terminating, and not scheduled on a faulted node.
func healthyOnGoodCapacity(f *fleet) int {
	list := &corev1.PodList{}
	if err := env.Client.List(env.Context, list, client.MatchingLabelsSelector{Selector: f.selector}); err != nil {
		return 0
	}
	faulted := f.faultedNodeNames()
	n := 0
	for i := range list.Items {
		p := &list.Items[i]
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil && !faulted.Has(p.Spec.NodeName) {
			n++
		}
	}
	return n
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

// anyDisrupted reports whether repair has taken a disrupting action on any watched node: the node is gone, carries the
// disrupted taint, or is terminating.
func anyDisrupted(nodes []*corev1.Node) bool {
	for _, n := range nodes {
		exists, disrupted, terminating := nodeState(n.Name)
		if !exists || disrupted || terminating {
			return true
		}
	}
	return false
}

// anyStuck reports whether any watched node tried to terminate (disrupted taint or deletion timestamp) but is still
// present — i.e. an action that cannot finish (wedged).
func anyStuck(nodes []*corev1.Node) bool {
	for _, n := range nodes {
		exists, disrupted, terminating := nodeState(n.Name)
		if exists && (disrupted || terminating) {
			return true
		}
	}
	return false
}

// allGone reports whether every watched node has terminated (no longer exists).
func allGone(nodes []*corev1.Node) bool {
	for _, n := range nodes {
		if exists, _, _ := nodeState(n.Name); exists {
			return false
		}
	}
	return true
}

// maybeReflag re-injects the fault onto fresh replacement nodes for the re-flag help models, so a "healthy" replacement
// re-fails and the fault is never truly resolved.
func maybeReflag(c cell, f *fleet) {
	if c.help != helpNoUnhealthyInDwell && c.help != helpNoUnhealthyAfterDwell {
		return
	}
	// after-dwell: let the replacement be "proven" healthy past the success dwell before it re-fails. in-dwell:
	// re-fail immediately (before the dwell would credit it). reflagDwell is measured from fault onset.
	if c.help == helpNoUnhealthyAfterDwell && time.Since(f.faultOnset) < reflagDwell {
		return
	}
	orig := f.originalNodeNames()
	for _, n := range env.Monitor.Nodes() {
		if orig.Has(n.Name) {
			continue
		}
		// Only re-flag fresh replacement nodes that carry our workload.
		if podUIDsOnNodes(f.selector, []*corev1.Node{n}).Len() == 0 {
			continue
		}
		patchNodeReady(n, corev1.ConditionFalse, "RepairPerfReflag")
	}
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

// patchNodeReady injects the repair-matching fault as the CUSTOM SimUnhealthy=True condition (not Ready). Why not
// Ready: KWOK's node-initialize stage immediately reverts any Ready!=True back to Ready=True, AND a NotReady node isn't
// a clean voluntary-disruption candidate — so a Ready-based fault neither holds nor triggers repair. KWOK does not
// manage arbitrary conditions, so SimUnhealthy=True sticks while the node stays Ready=True (a valid candidate), exactly
// like a node-monitoring-agent condition. The deployed KWOK RepairPolicies() must include {SimUnhealthy, True}.
// The status/reason params are retained for call-site compatibility; any fault status maps to SimUnhealthy=True.
// TODO(kubelet-dead): the drainNoKubeletDead case still wants Ready=Unknown⇒forceful semantics; model that via a
// forceful (TGP=0) RepairPolicy for a distinct condition rather than the reverted Ready path.
func patchNodeReady(n *corev1.Node, status corev1.ConditionStatus, reason string) {
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
