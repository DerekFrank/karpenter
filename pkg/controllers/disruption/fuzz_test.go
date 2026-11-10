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

package disruption_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

// The validation fuzz harness asks whether a disruption command computed against one cluster state is still valid
// after the cluster churns, three ways:
//   - resim: today's validators, which re-simulate scheduling against current state;
//   - replay: a validator under test (fuzzReplay), which checks the command's own placements against current state;
//   - truth: the real kube-scheduler, which places the pods the command evicts once the harness carries it out.
//
// It runs only with FUZZ_CASES set, writing one JSON line per case to FUZZ_OUT:
//
//	FUZZ_CASES=200 FUZZ_SEED=1 FUZZ_OUT=/tmp/fuzz KUBE_SCHEDULER_BINARY=... go test ./pkg/controllers/disruption/ --ginkgo.focus="Validation Fuzz"

var fuzzCases, fuzzSeed = func() (int, int64) {
	cases, _ := strconv.Atoi(os.Getenv("FUZZ_CASES"))
	seed, _ := strconv.ParseInt(os.Getenv("FUZZ_SEED"), 10, 64)
	return cases, seed
}()

// fuzzReplay is the validator under test. It's nil until one is wired in, and the harness records only resim and
// truth.
// It returns the command it validated, narrowed and with its own placements in Results, if it accepts.
var fuzzReplay func(ctx context.Context, method disruption.Method, cmd disruption.Command) (*fuzzVerdict, *disruption.Command)

type fuzzVerdict struct {
	Accept   bool    `json:"accept"`
	Reason   string  `json:"reason,omitempty"`
	Path     string  `json:"path,omitempty"`
	Millis   float64 `json:"ms"`
	Narrowed int     `json:"narrowed,omitempty"`
}

type fuzzTruth struct {
	// Valid is whether the command is still safe and worthwhile: Policy, Launchable, Cheaper and Scheduled.
	Valid bool `json:"valid"`
	// Policy is false when churn made a candidate ineligible by policy, which kube-scheduler doesn't judge.
	Policy bool `json:"policy"`
	// Launchable is whether any of the replacement's instance types still has an available offering.
	Launchable bool `json:"launchable"`
	// Cheaper is whether the replacement launched costs less than the candidates.
	Cheaper bool `json:"cheaper"`
	// Scheduled is whether kube-scheduler bound every evicted pod.
	Scheduled      bool     `json:"scheduled"`
	Unschedulable  []string `json:"unschedulable,omitempty"`
	PendingBound   int      `json:"pendingBound"`
	PendingTotal   int      `json:"pendingTotal"`
	Evicted        int      `json:"evicted"`
	LaunchedType   string   `json:"launchedType,omitempty"`
	Millis         float64  `json:"ms"`
	TimedOut       bool     `json:"timedOut,omitempty"`
	SkippedReasons []string `json:"skipped,omitempty"`
	Diagnostics    []string `json:"diagnostics,omitempty"`
	// Transient are the pods a sequential drain's copies refused on their first attempt, in Order.
	Transient []string `json:"transient,omitempty"`
	Order     []string `json:"order,omitempty"`
	// Retried counts the pods given a fresh attempt once the rest had landed.
	Retried int `json:"retried,omitempty"`
	// Pinned is how many pods the plan pinned, for a truth run against a validator's plan.
	Pinned int `json:"pinned,omitempty"`
	// DeletingUnschedulable counts the pods of nodes churn marked for deletion that didn't schedule. They go first, so
	// they don't count against the command.
	DeletingUnschedulable int `json:"deletingUnschedulable,omitempty"`
}

type fuzzRecord struct {
	Seed         int64        `json:"seed"`
	Outcome      string       `json:"outcome"`
	Method       string       `json:"method,omitempty"`
	Nodes        int          `json:"nodes"`
	Pods         int          `json:"pods"`
	Pending      int          `json:"pending"`
	Candidates   int          `json:"candidates,omitempty"`
	Replacements int          `json:"replacements,omitempty"`
	Churn        []string     `json:"churn,omitempty"`
	Features     []string     `json:"features,omitempty"`
	Resim        *fuzzVerdict `json:"resim,omitempty"`
	Replay       *fuzzVerdict `json:"replay,omitempty"`
	Truth        *fuzzTruth   `json:"truth,omitempty"`
	// TruthReplayPlan carries out the command replay validated, each pod pinned where replay placed it.
	TruthReplayPlan *fuzzTruth `json:"truthReplayPlan,omitempty"`
	// TruthReplayFree carries out the command replay validated without pins, when replay narrowed it.
	TruthReplayFree *fuzzTruth `json:"truthReplayFree,omitempty"`
	// TruthResimPlan carries out the command, each pod pinned where resim's simulation placed it.
	TruthResimPlan *fuzzTruth `json:"truthResimPlan,omitempty"`
	// TruthResimSeq and TruthReplaySeq drain each validator's plan one pod at a time, in FUZZ_ORDERS random orders.
	TruthResimSeq  []*fuzzTruth `json:"truthResimSeq,omitempty"`
	TruthReplaySeq []*fuzzTruth `json:"truthReplaySeq,omitempty"`
	// ResimOrder and ReplayOrder predict whether each validator's plan drains safely in every order.
	ResimOrder  *fuzzOrderPrediction `json:"resimOrder,omitempty"`
	ReplayOrder *fuzzOrderPrediction `json:"replayOrder,omitempty"`
	Error       string               `json:"error,omitempty"`
}

var fuzzOutMu sync.Mutex

func writeFuzzRecord(r fuzzRecord) {
	dir := lo.CoalesceOrEmpty(os.Getenv("FUZZ_OUT"), os.TempDir())
	fuzzOutMu.Lock()
	defer fuzzOutMu.Unlock()
	f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("fuzz-%d.jsonl", GinkgoParallelProcess())), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	Expect(err).ToNot(HaveOccurred())
	defer f.Close()
	Expect(json.NewEncoder(f).Encode(r)).To(Succeed())
}

var _ = Describe("Validation Fuzz", func() {
	if fuzzCases == 0 {
		return
	}
	BeforeEach(func() {
		Expect(startKubeScheduler(lo.CoalesceOrEmpty(os.Getenv("FUZZ_OUT"), os.TempDir()))).To(Succeed())
	})
	seeds := lo.Times(fuzzCases, func(i int) int64 { return fuzzSeed + int64(i) })
	// FUZZ_SEEDS reruns chosen cases instead of a range.
	if list := os.Getenv("FUZZ_SEEDS"); list != "" {
		seeds = lo.Map(strings.Split(list, ","), func(s string, _ int) int64 { return lo.Must(strconv.ParseInt(s, 10, 64)) })
	}
	for _, seed := range seeds {
		It(fmt.Sprintf("case %d", seed), func() {
			w := &fuzzWorld{rng: rand.New(rand.NewSource(seed)), seed: seed}
			rec := &fuzzRecord{Seed: seed}
			DeferCleanup(func() {
				if report := CurrentSpecReport(); report.Failed() {
					rec.Outcome, rec.Error = "error", report.FailureMessage()+" at "+report.FailureLocation().String()+"\n"+report.Failure.Location.FullStackTrace
				}
				writeFuzzRecord(*rec)
			})
			w.run(rec)
		})
	}
})

// fuzzWorld is one case's cluster and the bookkeeping the churn and the ground truth need.
type fuzzWorld struct {
	rng  *rand.Rand
	seed int64

	nodePool      *v1.NodePool
	instanceTypes []*cloudprovider.InstanceType
	nodeClaims    map[string]*v1.NodeClaim // by node name
	nodes         map[string]*corev1.Node
	workloads     []fuzzWorkload
	rs            map[string]*metav1.OwnerReference
	features      sets.Set[string]

	// deleting are the non-candidate nodes churn marked for deletion, whose pods the ground truth evicts too.
	deleting sets.Set[string]
	// policyBroken records churn that makes a candidate ineligible by policy.
	policyBroken []string
	podSeq       int
}

type fuzzWorkload struct {
	name     string
	template corev1.Pod
}

const fuzzTaintKey = "fuzz.karpenter.sh/taint"

func (w *fuzzWorld) intRange(lo, hi int) int { return lo + w.rng.Intn(hi-lo+1) }
func (w *fuzzWorld) chance(p float64) bool   { return w.rng.Float64() < p }

func (w *fuzzWorld) run(rec *fuzzRecord) {
	w.build()
	rec.Nodes = len(w.nodes)
	pods := w.boundPods()
	rec.Pods = len(pods)
	rec.Pending = len(w.parkedPods())
	rec.Features = sets.List(w.features)

	method, cmd, ok := w.computeCommand()
	if !ok {
		rec.Outcome = "no-command"
		return
	}
	rec.Method = method.ConsolidationType()
	rec.Candidates, rec.Replacements = len(cmd.Candidates), len(cmd.Replacements)

	rec.Churn = w.churn(cmd)
	w.syncState()

	rec.Resim = w.resim(method, cmd)
	var replayed *disruption.Command
	if fuzzReplay != nil {
		rec.Replay, replayed = fuzzReplay(ctx, method, cmd)
	}
	resimPlan := w.resimPlan(method, cmd, rec.Resim)
	if resimPlan != nil {
		rec.ResimOrder = w.predictOrder(cmd, resimPlan)
	}
	if replayed != nil {
		rec.ReplayOrder = w.predictOrder(*replayed, &replayed.Results)
	}
	rec.Truth = w.truth(cmd, nil, nil)
	orders, _ := strconv.Atoi(os.Getenv("FUZZ_ORDERS"))
	if resimPlan != nil {
		rec.TruthResimPlan = w.truth(cmd, resimPlan, nil)
		// Both validators' plans drain in the same orders of the same pods.
		orderRNG := rand.New(rand.NewSource(w.seed))
		for range orders {
			rec.TruthResimSeq = append(rec.TruthResimSeq, w.truth(cmd, resimPlan, orderRNG))
		}
	}
	if replayed != nil {
		rec.TruthReplayPlan = w.truth(*replayed, &replayed.Results, nil)
		if rec.Replay.Narrowed > 0 {
			rec.TruthReplayFree = w.truth(*replayed, nil, nil)
		}
		orderRNG := rand.New(rand.NewSource(w.seed))
		for range orders {
			rec.TruthReplaySeq = append(rec.TruthReplaySeq, w.truth(*replayed, &replayed.Results, orderRNG))
		}
	}
	rec.Outcome = "ok"
}

// build draws the instance types, the NodePool, the nodes and the workloads, then has kube-scheduler place the pods.
func (w *fuzzWorld) build() {
	w.nodeClaims, w.nodes, w.rs, w.features, w.deleting = map[string]*v1.NodeClaim{}, map[string]*corev1.Node{}, map[string]*metav1.OwnerReference{}, sets.New[string](), sets.New[string]()
	for _, cpu := range []int{1, 2, 4, 8, 16} {
		for _, family := range []struct {
			name  string
			memGi int
		}{{"c", 2}, {"m", 4}} {
			res := corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewQuantity(int64(cpu), resource.DecimalSI),
				corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", cpu*family.memGi)),
				corev1.ResourcePods:   resource.MustParse("32"),
			}
			w.instanceTypes = append(w.instanceTypes, fake.NewInstanceType(fmt.Sprintf("%s-%dx", family.name, cpu), fake.WithResources(res)))
		}
	}
	cloudProvider.InstanceTypes = w.instanceTypes

	w.nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{
		Disruption: v1.Disruption{
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			Budgets:             []v1.Budget{{Nodes: "100%"}},
		},
		Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{Requirements: []v1.NodeSelectorRequirementWithMinValues{
			{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand}},
		}}},
	}})
	ExpectApplied(ctx, env.Client, w.nodePool)

	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	for range w.intRange(3, 9) {
		it := w.instanceTypes[w.intRange(2, len(w.instanceTypes)-1)]
		zone := fmt.Sprintf("test-zone-%d", w.intRange(1, 3))
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            w.nodePool.Name,
				corev1.LabelInstanceTypeStable: it.Name,
				v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone:       zone,
			}},
			Status: v1.NodeClaimStatus{Allocatable: it.Allocatable(), Capacity: it.Capacity},
		})
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		// The kubelet labels a node with its hostname, which kube-scheduler reads hostname topology from.
		node.Labels[corev1.LabelHostname] = node.Name
		if w.chance(0.15) {
			node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: fuzzTaintKey, Value: "initial", Effect: corev1.TaintEffectNoSchedule})
			w.features.Insert("tainted-node")
		}
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		nodeClaims, nodes = append(nodeClaims, nodeClaim), append(nodes, node)
	}
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
	for i := range nodes {
		w.nodes[nodes[i].Name], w.nodeClaims[nodes[i].Name] = nodes[i], nodeClaims[i]
	}

	for i := range w.intRange(2, 5) {
		w.workloads = append(w.workloads, w.drawWorkload(fmt.Sprintf("w%d", i)))
	}
	var toPlace []*corev1.Pod
	for _, wl := range w.workloads {
		for range w.intRange(2, 10) {
			toPlace = append(toPlace, w.newPod(wl, fuzzDefaultScheduler))
		}
	}
	placed := w.place(toPlace)
	// Some pods kube-scheduler couldn't place stay pending for Karpenter, parked so kube-scheduler leaves them.
	for _, pod := range toPlace {
		if !placed.Has(pod.Name) {
			ExpectDeletedForce(pod)
			if w.chance(0.5) {
				ExpectApplied(ctx, env.Client, w.newPod(w.workloadOf(pod), fuzzParkedScheduler))
			}
		}
	}
	w.syncState()
}

// drawWorkload draws a ReplicaSet's pod template: requests, and with some chance a hard zonal or hostname spread,
// hostname anti-affinity to itself, a zone selector and a toleration of the fuzz taint.
func (w *fuzzWorld) drawWorkload(name string) fuzzWorkload {
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, rs)
	w.rs[name] = &metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: new(true), BlockOwnerDeletion: new(true)}
	labels := map[string]string{"fuzz-workload": name}
	opts := test.PodOptions{
		ObjectMeta: metav1.ObjectMeta{Labels: labels},
		ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse([]string{"100m", "250m", "500m", "1", "1500m", "3"}[w.rng.Intn(6)]),
			corev1.ResourceMemory: resource.MustParse([]string{"128Mi", "512Mi", "1Gi", "2Gi"}[w.rng.Intn(4)]),
		}},
	}
	selector := &metav1.LabelSelector{MatchLabels: labels}
	if w.chance(0.25) {
		opts.TopologySpreadConstraints = append(opts.TopologySpreadConstraints, corev1.TopologySpreadConstraint{
			MaxSkew: int32(w.intRange(1, 2)), TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: selector,
		})
		w.features.Insert("zonal-spread")
	}
	if w.chance(0.15) {
		opts.TopologySpreadConstraints = append(opts.TopologySpreadConstraints, corev1.TopologySpreadConstraint{
			MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: selector,
		})
		w.features.Insert("hostname-spread")
	}
	if w.chance(0.15) {
		opts.PodAntiRequirements = []corev1.PodAffinityTerm{{LabelSelector: selector, TopologyKey: corev1.LabelHostname}}
		w.features.Insert("anti-affinity")
	}
	if w.chance(0.1) {
		opts.NodeSelector = map[string]string{corev1.LabelTopologyZone: fmt.Sprintf("test-zone-%d", w.intRange(1, 3))}
		w.features.Insert("zone-selector")
		// Karpenter only counts pods toward a spread that honors node affinity once their NodeClaim satisfies it (#3472).
		for i := range opts.TopologySpreadConstraints {
			opts.TopologySpreadConstraints[i].NodeAffinityPolicy = new(corev1.NodeInclusionPolicyIgnore)
		}
	}
	if w.chance(0.3) {
		opts.Tolerations = []corev1.Toleration{{Key: fuzzTaintKey, Operator: corev1.TolerationOpExists}}
	}
	return fuzzWorkload{name: name, template: *test.Pod(opts)}
}

func (w *fuzzWorld) newPod(wl fuzzWorkload, schedulerName string) *corev1.Pod {
	pod := wl.template.DeepCopy()
	pod.ObjectMeta = metav1.ObjectMeta{
		Name:            fmt.Sprintf("s%d-%s-%d", w.seed, wl.name, w.nextPod()),
		Namespace:       pod.Namespace,
		Labels:          lo.Assign(pod.Labels),
		OwnerReferences: []metav1.OwnerReference{*w.rs[wl.name]},
	}
	pod.Spec.NodeName = ""
	pod.Spec.SchedulerName = schedulerName
	pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	if schedulerName == fuzzParkedScheduler {
		// A pending pod is one kube-scheduler marked unschedulable, which is what Karpenter provisions for.
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	}
	return pod
}

func (w *fuzzWorld) nextPod() int { w.podSeq++; return w.podSeq }

func (w *fuzzWorld) workloadOf(pod *corev1.Pod) fuzzWorkload {
	return lo.Must(lo.Find(w.workloads, func(wl fuzzWorkload) bool { return wl.name == pod.Labels["fuzz-workload"] }))
}

// place creates pods for kube-scheduler and waits until each is bound or unschedulable, returning the names bound.
func (w *fuzzWorld) place(pods []*corev1.Pod) sets.Set[string] {
	for _, pod := range pods {
		Expect(env.Client.Create(ctx, pod)).To(Succeed())
	}
	bound, _ := waitScheduled(pods, 15*time.Second)
	for _, pod := range pods {
		if bound.Has(pod.Name) {
			stored := &corev1.Pod{}
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
			stored.Status.Phase = corev1.PodRunning
			Expect(env.Client.Status().Update(ctx, stored)).To(Succeed())
		}
	}
	return bound
}

// waitScheduled polls until every pod is bound or kube-scheduler has marked it unschedulable and nothing changed for
// a second, or until timeout. It returns the pods bound and whether it timed out.
func waitScheduled(pods []*corev1.Pod, timeout time.Duration) (sets.Set[string], bool) {
	deadline := time.Now().Add(timeout)
	var settledSince time.Time
	last := -1
	for {
		bound, decided := sets.New[string](), 0
		for _, pod := range pods {
			stored := &corev1.Pod{}
			if err := env.Client.Get(ctx, client.ObjectKeyFromObject(pod), stored); err != nil {
				decided++
				continue
			}
			if stored.Spec.NodeName != "" {
				bound.Insert(pod.Name)
				decided++
			} else if cond, ok := lo.Find(stored.Status.Conditions, func(c corev1.PodCondition) bool { return c.Type == corev1.PodScheduled }); ok && cond.Status == corev1.ConditionFalse {
				decided++
			}
		}
		if bound.Len() == len(pods) {
			return bound, false
		}
		if decided == len(pods) {
			if bound.Len() != last {
				settledSince, last = time.Now(), bound.Len()
			} else if time.Since(settledSince) > 3*time.Second {
				return bound, false
			}
		}
		if time.Now().After(deadline) {
			return bound, true
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func ExpectDeletedForce(pod *corev1.Pod) {
	GinkgoHelper()
	Expect(client.IgnoreNotFound(env.Client.Delete(ctx, pod, client.GracePeriodSeconds(0)))).To(Succeed())
}

// syncState shows cluster state every node, its NodeClaim and the pods bound to it, as the informers would.
func (w *fuzzWorld) syncState() {
	for name, node := range w.nodes {
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(w.nodeClaims[name]))
	}
}

func (w *fuzzWorld) listPods() []*corev1.Pod {
	var list corev1.PodList
	Expect(env.Client.List(ctx, &list)).To(Succeed())
	return lo.ToSlicePtr(list.Items)
}

func (w *fuzzWorld) boundPods() []*corev1.Pod {
	return lo.Filter(w.listPods(), func(p *corev1.Pod, _ int) bool { return p.Spec.NodeName != "" })
}

func (w *fuzzWorld) parkedPods() []*corev1.Pod {
	return lo.Filter(w.listPods(), func(p *corev1.Pod, _ int) bool {
		return p.Spec.NodeName == "" && p.Spec.SchedulerName == fuzzParkedScheduler && p.DeletionTimestamp.IsZero()
	})
}

func (w *fuzzWorld) podsOn(node string) []*corev1.Pod {
	return lo.Filter(w.listPods(), func(p *corev1.Pod, _ int) bool { return p.Spec.NodeName == node })
}

// computeCommand computes a command with emptiness, multi-node or single-node consolidation, without validation. It
// starts from a method drawn at random, weighted to consolidation, and falls back through the others in the disruption
// controller's order.
func (w *fuzzWorld) computeCommand() (disruption.Method, disruption.Command, bool) {
	c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)
	methods := []disruption.Method{
		disruption.NewEmptiness(c, disruption.WithValidator(NopValidator{})),
		disruption.NewMultiNodeConsolidation(c, disruption.WithValidator(NopValidator{})),
		disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{})),
	}
	first := lo.Ternary(w.chance(0.2), 0, w.intRange(1, 2))
	for _, method := range append(methods[first:], methods[:first]...) {
		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, method.ShouldDisrupt, method.Class(), queue)
		Expect(err).ToNot(HaveOccurred())
		if len(candidates) == 0 {
			continue
		}
		budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, env.Clock, env.Client, cloudProvider, recorder, method.Reason())
		Expect(err).ToNot(HaveOccurred())
		cmds, err := method.ComputeCommands(ctx, budgets, candidates...)
		Expect(err).ToNot(HaveOccurred())
		for _, cmd := range cmds {
			if cmd.Decision() != disruption.NoOpDecision {
				cmd.Method = method
				return method, cmd, true
			}
		}
	}
	return nil, disruption.Command{}, false
}

// resim validates cmd with today's validator for its method.
func (w *fuzzWorld) resim(method disruption.Method, cmd disruption.Command) *fuzzVerdict {
	c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)
	var validator disruption.Validator
	switch method.(type) {
	case *disruption.Emptiness:
		validator = disruption.NewEmptinessValidator(c)
	case *disruption.MultiNodeConsolidation:
		validator = disruption.NewMultiConsolidationValidator(c)
	default:
		validator = disruption.NewSingleConsolidationValidator(c)
	}
	start := time.Now()
	validated, err := validator.Validate(ctx, cmd, 0)
	v := &fuzzVerdict{Accept: err == nil, Millis: float64(time.Since(start).Microseconds()) / 1000}
	if err != nil {
		v.Reason = validationReason(err)
	} else if len(validated.Candidates) != len(cmd.Candidates) {
		// Emptiness validation keeps the candidates still valid; a partial command is a different decision.
		v.Accept, v.Reason = false, "partial"
	}
	return v
}

// resimPlan is the placement resim's simulation found for an accepted consolidation command, as validateCommand
// simulates it: against the candidates as they are now.
func (w *fuzzWorld) resimPlan(method disruption.Method, cmd disruption.Command, verdict *fuzzVerdict) *pscheduling.Results {
	if !verdict.Accept {
		return nil
	}
	if _, ok := method.(*disruption.Emptiness); ok {
		return nil
	}
	names := sets.New(candidateNames(cmd)...)
	candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, method.ShouldDisrupt, method.Class(), queue)
	Expect(err).ToNot(HaveOccurred())
	candidates = lo.Filter(candidates, func(c *disruption.Candidate, _ int) bool { return names.Has(c.Name()) })
	results, err := disruption.SimulateScheduling(ctx, env.Client, cluster, prov, env.Clock, recorder, []pscheduling.Options{pscheduling.IsConsolidationSimulation}, disruption.SimulationOptions{}, candidates...)
	Expect(err).ToNot(HaveOccurred())
	return &results
}

func validationReason(err error) string {
	var budget *disruption.BudgetValidationError
	var sched *disruption.SchedulingValidationError
	var churn *disruption.ChurnValidationError
	switch {
	case errors.As(err, &budget):
		return "budget"
	case errors.As(err, &churn):
		return "churn"
	case errors.As(err, &sched):
		if strings.Contains(err.Error(), "produced new results") {
			return "scheduling-new-results"
		}
		return "scheduling-unschedulable"
	default:
		return "other: " + err.Error()
	}
}
