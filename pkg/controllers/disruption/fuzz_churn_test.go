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
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"

	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

type fuzzChurnOp struct {
	name   string
	weight int
	// apply changes the cluster, returning a description, or false when the op doesn't apply to this case.
	apply func(w *fuzzWorld, cmd disruption.Command) (string, bool)
}

var fuzzChurnOps = []fuzzChurnOp{
	{"bind-anywhere", 4, func(w *fuzzWorld, _ disruption.Command) (string, bool) {
		return w.schedulePod(nil), true
	}},
	{"bind-destination", 5, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		dest, ok := w.pick(w.destinations(cmd))
		if !ok {
			return "", false
		}
		return w.schedulePod(map[string]string{corev1.LabelHostname: w.hostname(dest)}), true
	}},
	{"bind-candidate", 2, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		candidate, _ := w.pick(candidateNames(cmd))
		return w.schedulePod(map[string]string{corev1.LabelHostname: w.hostname(candidate)}), true
	}},
	{"pending", 3, func(w *fuzzWorld, _ disruption.Command) (string, bool) {
		wl := w.workloads[w.rng.Intn(len(w.workloads))]
		pod := w.newPod(wl, fuzzParkedScheduler)
		ExpectApplied(ctx, env.Client, pod)
		return "pending " + wl.name, true
	}},
	{"delete-pod", 3, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		pods := lo.Filter(w.boundPods(), func(p *corev1.Pod, _ int) bool { return !sets.New(candidateNames(cmd)...).Has(p.Spec.NodeName) })
		if len(pods) == 0 {
			return "", false
		}
		pod := pods[w.rng.Intn(len(pods))]
		ExpectDeletedForce(pod)
		return "delete-pod on " + pod.Spec.NodeName, true
	}},
	{"taint-node", 3, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		name, ok := w.pick(lo.Ternary(w.chance(0.7) && len(w.destinations(cmd)) > 0, w.destinations(cmd), w.others(cmd)))
		if !ok {
			return "", false
		}
		node := ExpectExists(ctx, env.Client, w.nodes[name])
		if lo.ContainsBy(node.Spec.Taints, func(t corev1.Taint) bool { return t.Key == fuzzTaintKey }) {
			return "", false
		}
		node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: fuzzTaintKey, Value: "churn", Effect: corev1.TaintEffectNoSchedule})
		ExpectApplied(ctx, env.Client, node)
		w.nodes[name] = node
		return "taint " + name, true
	}},
	{"delete-node", 2, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		name, ok := w.pick(lo.Ternary(w.chance(0.7) && len(w.destinations(cmd)) > 0, w.destinations(cmd), w.others(cmd)))
		if !ok {
			return "", false
		}
		for _, pod := range w.podsOn(name) {
			ExpectDeletedForce(pod)
			ExpectApplied(ctx, env.Client, w.newPod(w.workloadOf(pod), fuzzParkedScheduler))
		}
		ExpectDeleted(ctx, env.Client, w.nodes[name], w.nodeClaims[name])
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(w.nodes[name]))
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(w.nodeClaims[name]))
		delete(w.nodes, name)
		delete(w.nodeClaims, name)
		w.deleting.Delete(name)
		return "delete-node " + name, true
	}},
	{"mark-deleting", 2, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		name, ok := w.pick(lo.Ternary(w.chance(0.7) && len(w.destinations(cmd)) > 0, w.destinations(cmd), w.others(cmd)))
		if !ok {
			return "", false
		}
		cs.Deletion.Mark(w.nodeClaims[name].Name)
		w.deleting.Insert(name)
		return "mark-deleting " + name, true
	}},
	{"do-not-disrupt", 1, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		name, _ := w.pick(candidateNames(cmd))
		node := ExpectExists(ctx, env.Client, w.nodes[name])
		node.Annotations = lo.Assign(node.Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
		ExpectApplied(ctx, env.Client, node)
		w.nodes[name] = node
		w.policyBroken = append(w.policyBroken, "do-not-disrupt")
		return "do-not-disrupt " + name, true
	}},
	{"budget-zero", 1, func(w *fuzzWorld, _ disruption.Command) (string, bool) {
		w.nodePool = ExpectExists(ctx, env.Client, w.nodePool)
		w.nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
		ExpectApplied(ctx, env.Client, w.nodePool)
		w.policyBroken = append(w.policyBroken, "budget-zero")
		return "budget-zero", true
	}},
	{"nominate", 1, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		name, _ := w.pick(candidateNames(cmd))
		cs.Nomination.Nominate(ctx, w.nodeClaims[name].Name)
		w.policyBroken = append(w.policyBroken, "nominated")
		return "nominate " + name, true
	}},
	{"offering-unavailable", 2, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		if len(cmd.Replacements) == 0 {
			return "", false
		}
		it := w.cheapestOption(cmd)
		for _, o := range it.Offerings {
			o.Available = false
		}
		return "unavailable " + it.Name, true
	}},
	{"price-up", 2, func(w *fuzzWorld, cmd disruption.Command) (string, bool) {
		if len(cmd.Replacements) == 0 {
			return "", false
		}
		it := w.cheapestOption(cmd)
		for _, o := range it.Offerings {
			o.Price *= 4
		}
		return "price-up " + it.Name, true
	}},
	{"scale-spread", 2, func(w *fuzzWorld, _ disruption.Command) (string, bool) {
		spread := lo.Filter(w.workloads, func(wl fuzzWorkload, _ int) bool {
			return len(wl.template.Spec.TopologySpreadConstraints) > 0 || wl.template.Spec.Affinity != nil
		})
		if len(spread) == 0 {
			return "", false
		}
		wl := spread[w.rng.Intn(len(spread))]
		n := w.intRange(1, 3)
		pods := lo.Times(n, func(int) *corev1.Pod { return w.newPod(wl, fuzzDefaultScheduler) })
		bound := w.place(pods)
		w.parkUnplaced(pods, bound)
		return fmt.Sprintf("scale %s +%d (%d bound)", wl.name, n, bound.Len()), true
	}},
}

// churn applies up to three ops, or none for a sixth of cases.
func (w *fuzzWorld) churn(cmd disruption.Command) []string {
	if w.chance(1.0 / 6) {
		return []string{"none"}
	}
	total := lo.SumBy(fuzzChurnOps, func(op fuzzChurnOp) int { return op.weight })
	var applied []string
	for range w.intRange(1, 3) {
		for attempt := 0; attempt < 5; attempt++ {
			r := w.rng.Intn(total)
			op := lo.Must(lo.Find(fuzzChurnOps, func(op fuzzChurnOp) bool { r -= op.weight; return r < 0 }))
			if desc, ok := op.apply(w, cmd); ok {
				applied = append(applied, op.name+": "+desc)
				break
			}
		}
	}
	return applied
}

// schedulePod has kube-scheduler place one pod of a random workload, with nodeSelector added, parking it if it can't.
func (w *fuzzWorld) schedulePod(nodeSelector map[string]string) string {
	wl := w.workloads[w.rng.Intn(len(w.workloads))]
	pod := w.newPod(wl, fuzzDefaultScheduler)
	pod.Spec.NodeSelector = lo.Assign(pod.Spec.NodeSelector, nodeSelector)
	bound := w.place([]*corev1.Pod{pod})
	w.parkUnplaced([]*corev1.Pod{pod}, bound)
	stored := &corev1.Pod{}
	if bound.Has(pod.Name) && env.Client.Get(ctx, client.ObjectKeyFromObject(pod), stored) == nil {
		return fmt.Sprintf("%s -> %s", wl.name, stored.Spec.NodeName)
	}
	return fmt.Sprintf("%s -> pending (selector %v)", wl.name, nodeSelector)
}

func (w *fuzzWorld) parkUnplaced(pods []*corev1.Pod, bound sets.Set[string]) {
	for _, pod := range pods {
		if !bound.Has(pod.Name) {
			ExpectDeletedForce(pod)
			parked := w.newPod(w.workloadOf(pod), fuzzParkedScheduler)
			parked.Spec.NodeSelector = pod.Spec.NodeSelector
			ExpectApplied(ctx, env.Client, parked)
		}
	}
}

func (w *fuzzWorld) pick(names []string) (string, bool) {
	if len(names) == 0 {
		return "", false
	}
	sort.Strings(names)
	return names[w.rng.Intn(len(names))], true
}

// hostname is a node's hostname label, which isn't its name.
func (w *fuzzWorld) hostname(node string) string {
	return lo.Must(lo.Coalesce(w.nodes[node].Labels[corev1.LabelHostname], node))
}

func candidateNames(cmd disruption.Command) []string {
	return lo.Map(cmd.Candidates, func(c *disruption.Candidate, _ int) string { return c.Name() })
}

// destinations are the existing nodes the command's simulation placed the candidates' pods on.
func (w *fuzzWorld) destinations(cmd disruption.Command) []string {
	var names []string
	for _, en := range cmd.Results.ExistingNodes {
		if _, ok := w.nodes[en.Name()]; ok && len(en.Pods) > 0 {
			names = append(names, en.Name())
		}
	}
	return lo.Uniq(names)
}

func (w *fuzzWorld) others(cmd disruption.Command) []string {
	candidates := sets.New(candidateNames(cmd)...)
	return lo.Filter(lo.Keys(w.nodes), func(n string, _ int) bool { return !candidates.Has(n) })
}

func (w *fuzzWorld) instanceType(name string) *cloudprovider.InstanceType {
	return lo.Must(lo.Find(w.instanceTypes, func(it *cloudprovider.InstanceType) bool { return it.Name == name }))
}

// cheapestOption is the replacement's cheapest instance type option, by its cheapest offering at computation.
func (w *fuzzWorld) cheapestOption(cmd disruption.Command) *cloudprovider.InstanceType {
	options := cmd.Replacements[0].InstanceTypeOptions
	price := func(it *cloudprovider.InstanceType) float64 {
		if available := it.Offerings.Available(); len(available) > 0 {
			return available.Cheapest().Price
		}
		return math.Inf(1)
	}
	best := lo.MinBy(options, func(a, b *cloudprovider.InstanceType) bool { return price(a) < price(b) })
	return w.instanceType(best.Name)
}

// truth carries cmd out against the apiserver and has kube-scheduler place the pods it evicts: it launches the
// cheapest available offering of the replacement's instance types as a Node, taints the candidates and the nodes
// churn marked for deletion, and recreates their pods and the pending pods unbound under kube-scheduler. The pending
// pods and the deleting nodes' pods come first, so the command is judged by whether its own pods fit in what they
// leave. With plan set, each pod plan placed is pinned there by hostname, so kube-scheduler judges that placement
// rather than finding its own. truth restores the cluster before it returns.
func (w *fuzzWorld) truth(cmd disruption.Command, plan *pscheduling.Results, order *rand.Rand) *fuzzTruth {
	start := time.Now()
	t := &fuzzTruth{Policy: len(w.policyBroken) == 0, Launchable: true, Cheaper: true}
	defer func() { t.Millis = float64(time.Since(start).Microseconds()) / 1000 }()
	snapshot := w.snapshot()
	defer w.restore(snapshot)

	candidates := sets.New(candidateNames(cmd)...)
	candidatePrice := 0.0
	for name := range candidates {
		nc := w.nodeClaims[name]
		it := w.instanceType(nc.Labels[corev1.LabelInstanceTypeStable])
		candidatePrice += it.Offerings.Compatible(scheduling.NewLabelRequirements(map[string]string{
			corev1.LabelTopologyZone: nc.Labels[corev1.LabelTopologyZone], v1.CapacityTypeLabelKey: nc.Labels[v1.CapacityTypeLabelKey],
		})).Cheapest().Price
	}
	replacementName := fmt.Sprintf("fuzz-replacement-%d", w.seed)
	if len(cmd.Replacements) > 0 {
		replacement := cmd.Replacements[0]
		var launchIT *cloudprovider.InstanceType
		var launch *cloudprovider.Offering
		for _, option := range replacement.InstanceTypeOptions {
			it := w.instanceType(option.Name)
			for _, o := range it.Offerings.Available().Compatible(replacement.Requirements) {
				if launch == nil || o.Price < launch.Price {
					launchIT, launch = it, o
				}
			}
		}
		if launch == nil {
			t.Launchable, t.Cheaper = false, false
			t.SkippedReasons = append(t.SkippedReasons, "no available offering")
			return t
		}
		t.LaunchedType = launchIT.Name
		t.Cheaper = launch.Price < candidatePrice
		labels := map[string]string{v1.NodePoolLabelKey: w.nodePool.Name}
		for key := range replacement.Requirements.Keys() {
			if r := replacement.Requirements.Get(key); r.Operator() == corev1.NodeSelectorOpIn && r.Len() == 1 {
				labels[key] = r.Any()
			}
		}
		for key := range launchIT.Requirements.Keys() {
			if r := launchIT.Requirements.Get(key); r.Operator() == corev1.NodeSelectorOpIn && r.Len() == 1 {
				labels[key] = r.Any()
			}
		}
		labels[corev1.LabelTopologyZone] = launch.Requirements.Get(corev1.LabelTopologyZone).Any()
		labels[v1.CapacityTypeLabelKey] = launch.Requirements.Get(v1.CapacityTypeLabelKey).Any()
		labels[corev1.LabelHostname] = replacementName
		node := test.Node(test.NodeOptions{
			ObjectMeta:  metav1.ObjectMeta{Name: replacementName, Labels: labels},
			Allocatable: launchIT.Allocatable(),
			Capacity:    launchIT.Capacity,
			ReadyStatus: corev1.ConditionTrue,
		})
		ExpectApplied(ctx, env.Client, node)
		// A replacement registers and goes Ready before pods schedule to it. Admission tainted it not-ready on create,
		// and envtest runs no node lifecycle controller to lift that taint.
		node = ExpectExists(ctx, env.Client, node)
		node.Spec.Taints = w.nodePool.Spec.Template.Spec.Taints
		ExpectApplied(ctx, env.Client, node)
	}

	pins := map[string]string{}
	if plan != nil {
		for _, en := range plan.ExistingNodes {
			for _, pod := range en.Pods {
				pins[pod.Name] = w.hostname(en.Name())
			}
		}
		for i, nc := range plan.NewNodeClaims {
			if i > 0 {
				break
			}
			for _, pod := range nc.Pods {
				pins[pod.Name] = replacementName
			}
		}
		t.Pinned = len(pins)
	}

	draining := candidates.Union(w.deleting)
	for name := range draining {
		node := ExpectExists(ctx, env.Client, w.nodes[name])
		node.Spec.Taints = append(node.Spec.Taints, v1.DisruptedNoScheduleTaint)
		ExpectApplied(ctx, env.Client, node)
	}
	pending := w.parkedPods()
	deletingPods := lo.Filter(w.boundPods(), func(p *corev1.Pod, _ int) bool { return w.deleting.Has(p.Spec.NodeName) })
	evicted := lo.Filter(w.boundPods(), func(p *corev1.Pod, _ int) bool { return candidates.Has(p.Spec.NodeName) })
	t.PendingTotal, t.Evicted = len(pending), len(evicted)
	copies := func(pods []*corev1.Pod) []*corev1.Pod {
		return lo.Map(pods, func(pod *corev1.Pod, _ int) *corev1.Pod {
			ExpectDeletedForce(pod)
			c := w.newPod(w.workloadOf(pod), fuzzDefaultScheduler)
			c.Spec.NodeSelector = lo.Assign(pod.Spec.NodeSelector)
			if host, ok := pins[pod.Name]; ok {
				c.Spec.NodeSelector[corev1.LabelHostname] = host
				// Under nodeAffinityPolicy Honor, kube-scheduler counts only nodes matching the pod's selector as spread
				// domains, so the pin alone would leave the pod one domain and no skew. Ignore counts every node, as the
				// unpinned pod would: workloads without a selector count every node, and zone-pinned ones already Ignore.
				for i := range c.Spec.TopologySpreadConstraints {
					c.Spec.TopologySpreadConstraints[i].NodeAffinityPolicy = lo.ToPtr(corev1.NodeInclusionPolicyIgnore)
				}
			}
			return c
		})
	}
	firstCopies := copies(append(append([]*corev1.Pod{}, pending...), deletingPods...))
	var evictedCopies []*corev1.Pod
	gone := func(pods []*corev1.Pod) {
		// Let kube-scheduler see the originals go before their copies arrive, as a real eviction precedes the replica.
		Eventually(func() bool {
			return lo.EveryBy(pods, func(p *corev1.Pod) bool {
				return errors.IsNotFound(env.Client.Get(ctx, client.ObjectKeyFromObject(p), &corev1.Pod{}))
			})
		}).Should(BeTrue())
		time.Sleep(200 * time.Millisecond)
	}
	if order == nil {
		evictedCopies = copies(evicted)
		// FUZZ_SHUFFLE hands kube-scheduler the evicted pods in a random order, as evictions land in no particular order.
		if os.Getenv("FUZZ_SHUFFLE") != "" {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			r.Shuffle(len(evictedCopies), func(i, j int) { evictedCopies[i], evictedCopies[j] = evictedCopies[j], evictedCopies[i] })
		}
		gone(append(append(append([]*corev1.Pod{}, pending...), deletingPods...), evicted...))
		for _, pod := range append(append([]*corev1.Pod{}, firstCopies...), evictedCopies...) {
			Expect(env.Client.Create(ctx, pod)).To(Succeed())
		}
	} else {
		// A drain evicts the candidates' pods one at a time, in no order Karpenter controls, and the pods not yet evicted
		// still count toward topology. Each copy has to bind on its first attempt: a pod refused even for a moment is
		// pending, and provisioning launches capacity for it.
		gone(append(append([]*corev1.Pod{}, pending...), deletingPods...))
		for _, pod := range firstCopies {
			Expect(env.Client.Create(ctx, pod)).To(Succeed())
		}
		waitScheduled(firstCopies, 60*time.Second)
		shuffled := append([]*corev1.Pod{}, evicted...)
		order.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		// The termination controller evicts a drained node's pods together, so by default every candidate pod leaves
		// before its replicas land, one at a time in random order. FUZZ_DRAIN=serial evicts each just before its replica.
		burst := os.Getenv("FUZZ_DRAIN") != "serial"
		var replicas []*corev1.Pod
		if burst {
			replicas = copies(shuffled)
			gone(shuffled)
		}
		for i, pod := range shuffled {
			var c *corev1.Pod
			if burst {
				c = replicas[i]
			} else {
				c = copies([]*corev1.Pod{pod})[0]
				gone([]*corev1.Pod{pod})
			}
			Expect(env.Client.Create(ctx, c)).To(Succeed())
			evictedCopies = append(evictedCopies, c)
			if !firstAttemptBinds(c) {
				t.Transient = append(t.Transient, c.Name)
			}
		}
		t.Order = lo.Map(shuffled, func(p *corev1.Pod, _ int) string { return p.Name })
	}
	all := append(append([]*corev1.Pod{}, firstCopies...), evictedCopies...)
	bound, timedOut := waitScheduled(all, 60*time.Second)
	// kube-scheduler retries an unschedulable pod only on a cluster event, or after minutes. Give each pod still
	// unbound one fresh attempt, now that everything else has landed, as the eventual retry would.
	for i, pod := range all {
		if bound.Has(pod.Name) {
			continue
		}
		ExpectDeletedForce(pod)
		retry := pod.DeepCopy()
		retry.ObjectMeta = metav1.ObjectMeta{Name: pod.Name + "-retry", Namespace: pod.Namespace, Labels: pod.Labels, OwnerReferences: pod.OwnerReferences}
		retry.Status = corev1.PodStatus{}
		Expect(env.Client.Create(ctx, retry)).To(Succeed())
		all[i] = retry
		if i < len(firstCopies) {
			firstCopies[i] = retry
		} else {
			evictedCopies[i-len(firstCopies)] = retry
		}
		t.Retried++
	}
	if t.Retried > 0 {
		var retryTimedOut bool
		bound, retryTimedOut = waitScheduled(all, 60*time.Second)
		timedOut = timedOut || retryTimedOut
	}
	t.TimedOut = timedOut
	t.PendingBound = lo.CountBy(firstCopies[:len(pending)], func(p *corev1.Pod) bool { return bound.Has(p.Name) })
	t.DeletingUnschedulable = lo.CountBy(firstCopies[len(pending):], func(p *corev1.Pod) bool { return !bound.Has(p.Name) })
	t.Unschedulable = lo.FilterMap(evictedCopies, func(p *corev1.Pod, _ int) (string, bool) { return p.Name, !bound.Has(p.Name) })
	t.Scheduled = len(t.Unschedulable) == 0
	if !t.Scheduled {
		t.Diagnostics = w.diagnose(evictedCopies, t.Unschedulable)
	}
	t.Valid = t.Policy && t.Launchable && t.Cheaper && t.Scheduled && len(t.Transient) == 0
	return t
}

// firstAttemptBinds waits for kube-scheduler's first decision on pod: bound, or marked unschedulable.
func firstAttemptBinds(pod *corev1.Pod) bool {
	var bound bool
	Eventually(func() bool {
		stored := &corev1.Pod{}
		if err := env.Client.Get(ctx, client.ObjectKeyFromObject(pod), stored); err != nil {
			return false
		}
		if stored.Spec.NodeName != "" {
			bound = true
			return true
		}
		cond, ok := lo.Find(stored.Status.Conditions, func(c corev1.PodCondition) bool { return c.Type == corev1.PodScheduled })
		return ok && cond.Status == corev1.ConditionFalse
	}).WithTimeout(20 * time.Second).WithPolling(20 * time.Millisecond).Should(BeTrue())
	return bound
}

func (w *fuzzWorld) diagnose(copies []*corev1.Pod, unschedulable []string) []string {
	var out []string
	for _, name := range unschedulable {
		pod := lo.Must(lo.Find(copies, func(p *corev1.Pod) bool { return p.Name == name }))
		out = append(out, fmt.Sprintf("pod %s requests %v selector %v", name, pod.Spec.Containers[0].Resources.Requests, pod.Spec.NodeSelector))
	}
	var nodes corev1.NodeList
	Expect(env.Client.List(ctx, &nodes)).To(Succeed())
	zoneOf := lo.SliceToMap(nodes.Items, func(n corev1.Node) (string, string) { return n.Name, n.Labels[corev1.LabelTopologyZone] })
	for _, wl := range w.workloads {
		for _, tsc := range wl.template.Spec.TopologySpreadConstraints {
			counts := map[string]int{}
			for _, p := range w.boundPods() {
				if p.Labels["fuzz-workload"] == wl.name {
					counts[lo.Ternary(tsc.TopologyKey == corev1.LabelHostname, p.Spec.NodeName, zoneOf[p.Spec.NodeName])]++
				}
			}
			out = append(out, fmt.Sprintf("workload %s spread %s maxSkew %d affinityPolicy %v selector %v counts %v", wl.name, tsc.TopologyKey, tsc.MaxSkew, lo.FromPtr(tsc.NodeAffinityPolicy), wl.template.Spec.NodeSelector, counts))
		}
	}
	for _, n := range nodes.Items {
		used := corev1.ResourceList{}
		for _, p := range w.podsOn(n.Name) {
			for k, v := range p.Spec.Containers[0].Resources.Requests {
				q := used[k]
				q.Add(v)
				used[k] = q
			}
		}
		out = append(out, fmt.Sprintf("node %s zone %s alloc cpu %s used cpu %s taints %v", n.Name, n.Labels[corev1.LabelTopologyZone], n.Status.Allocatable.Cpu(), used.Cpu(), lo.Map(n.Spec.Taints, func(t corev1.Taint, _ int) string { return t.ToString() })))
	}
	return out
}

type fuzzSnapshot struct {
	pods  []*corev1.Pod
	nodes []*corev1.Node
}

func (w *fuzzWorld) snapshot() fuzzSnapshot {
	var nodes corev1.NodeList
	Expect(env.Client.List(ctx, &nodes)).To(Succeed())
	return fuzzSnapshot{
		pods:  lo.Map(w.listPods(), func(p *corev1.Pod, _ int) *corev1.Pod { return p.DeepCopy() }),
		nodes: lo.Map(lo.ToSlicePtr(nodes.Items), func(n *corev1.Node, _ int) *corev1.Node { return n.DeepCopy() }),
	}
}

// restore puts the pods and node taints back as snapshot found them, and deletes the nodes it didn't have. Bound pods
// are recreated bound, so kube-scheduler doesn't place them again.
func (w *fuzzWorld) restore(snapshot fuzzSnapshot) {
	for _, pod := range w.listPods() {
		ExpectDeletedForce(pod)
	}
	Eventually(func() int { return len(w.listPods()) }).Should(BeZero())
	var nodes corev1.NodeList
	Expect(env.Client.List(ctx, &nodes)).To(Succeed())
	known := lo.SliceToMap(snapshot.nodes, func(n *corev1.Node) (string, *corev1.Node) { return n.Name, n })
	for i := range nodes.Items {
		node := &nodes.Items[i]
		saved, ok := known[node.Name]
		if !ok {
			ExpectDeleted(ctx, env.Client, node)
			continue
		}
		node.Spec.Taints = saved.Spec.Taints
		Expect(env.Client.Update(ctx, node)).To(Succeed())
	}
	for _, saved := range snapshot.pods {
		pod := saved.DeepCopy()
		pod.ResourceVersion, pod.UID, pod.ManagedFields, pod.CreationTimestamp = "", "", nil, metav1.Time{}
		status := pod.Status
		Expect(env.Client.Create(ctx, pod)).To(Succeed())
		pod.Status = status
		Expect(env.Client.Status().Update(ctx, pod)).To(Succeed())
	}
}
