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

package disruption

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/singleton"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpentercluster "sigs.k8s.io/karpenter/pkg/cluster"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	karpenterstate "sigs.k8s.io/karpenter/pkg/state"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

type Controller struct {
	queue         *Queue
	kubeClient    client.Client
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
	recorder      events.Recorder
	clock         clock.Clock
	cloudProvider cloudprovider.CloudProvider
	clusterCost   *cost.ClusterCost
	methods       []Method
	mu            sync.Mutex
	lastRun       map[string]time.Time

	// changes counts the changes of what the controller wakes on, and the NodePools'.
	changes *changes
}

type ControllerOptions struct {
	methods []Method
}

func WithMethods(methods ...Method) option.Function[ControllerOptions] {
	return func(o *ControllerOptions) {
		o.methods = methods
	}
}

func NewController(ctx context.Context, clk clock.Clock, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cp cloudprovider.CloudProvider, recorder events.Recorder, cluster *state.Cluster, queue *Queue, clusterCost *cost.ClusterCost, opts ...option.Function[ControllerOptions]) *Controller {

	o := option.Resolve(append([]option.Function[ControllerOptions]{WithMethods(NewMethods(ctx, clk, cluster, kubeClient, provisioner, cp, recorder, queue)...)}, opts...)...)
	return &Controller{
		queue:         queue,
		clock:         clk,
		kubeClient:    kubeClient,
		cluster:       cluster,
		provisioner:   provisioner,
		recorder:      recorder,
		cloudProvider: cp,
		clusterCost:   clusterCost,
		lastRun:       map[string]time.Time{},
		changes:       &changes{},
		methods:       o.methods,
	}
}

func NewMethods(ctx context.Context, clk clock.Clock, cluster *state.Cluster, kubeClient client.Client, provisioner *provisioning.Provisioner, cp cloudprovider.CloudProvider, recorder events.Recorder, queue *Queue) []Method {
	c := MakeConsolidation(clk, cluster, kubeClient, provisioner, cp, recorder, queue)
	methods := []Method{}
	// Repair runs first: fixing a fault outranks any discretionary rebalance. Do not register the method while the
	// feature gate is disabled, since candidate construction performs cluster-wide API and scheduling work.
	// --legacy-node-repair runs the legacy node.health controller in place of this method.
	if options.FromContext(ctx).FeatureGates.NodeRepair && !options.FromContext(ctx).LegacyNodeRepair {
		methods = append(methods, NewRepair(c))
	}
	return append(methods, []Method{
		// Delete empty nodes across all consolidation policies (WhenEmpty, WhenEmptyOrUnderutilized, Balanced).
		NewEmptiness(c),
		// Terminate and create replacement for drifted NodeClaims in Static NodePool
		NewStaticDrift(cluster, provisioner, cp, recorder),
		// Terminate any NodeClaims that have drifted from provisioning specifications, allowing the pods to reschedule.
		NewDrift(kubeClient, cluster, provisioner, recorder, clk),
		// Attempt to identify multiple NodeClaims that we can consolidate simultaneously to reduce pod churn
		NewMultiNodeConsolidation(c),
		// And finally fall back our single NodeClaim consolidation to further reduce cluster cost.
		NewSingleNodeConsolidation(c),
	}...)
}

func (c *Controller) Name() string {
	return "disruption"
}

// Register wakes the controller on the changes that can open a disruption decision, and has the consolidation methods
// skip their work until one comes, in place of a poll:
//   - the NodeClaim store, narrowed to nodeClaimInputs: a NodeClaim coming, launching, linking its Node, starting to
//     delete or going; being marked for deletion; its Drifted or Consolidatable condition or its do-not-disrupt
//     annotation changing; or its buffer pods coming or going. Its nomination, status heartbeats and other conditions
//     don't wake it.
//   - the Node store, narrowed to nodeInputs: a Node initializing, its do-not-disrupt annotation or its repair policy
//     matches changing in number; and its pods' usage, as a pod binds to it or leaves it, starts to terminate, or gains
//     or loses the do-not-disrupt annotation. A pod's status, its other metadata, pods waiting to schedule, which have
//     no Node, and the Node's heartbeat don't wake it.
//   - a managed NodePool's spec changing, through its generation.
//   - the NodeClaim store's UnlaunchedSource: a NodeClaim launching, which can end the wait on NodeClaimsLaunched. It
//     doesn't count as a change for consolidation, since a launch also changes the NodeClaim's inputs.
//
// The controller still runs every revalidationPeriod for what shows only with time.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	for _, method := range c.methods {
		if t, ok := method.(changeTracker); ok {
			t.trackChanges(c.changes)
		}
	}
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		WatchesRawSource(karpenterstate.Requires(c.cluster)).
		// Every source enqueues the same empty request, so the controller runs once for each burst of changes.
		WatchesRawSource(c.wakes()).
		Watches(&v1.NodePool{}, handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
			c.changes.add()
			return []reconcile.Request{{}}
		}), builder.WithPredicates(predicate.GenerationChangedPredicate{}, nodepoolutils.IsManagedPredicateFuncs(c.cloudProvider))).
		Complete(singleton.AsReconciler(c))
}

//nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	// this won't catch if the reconciler loop hangs forever, but it will catch other issues
	c.logAbnormalRuns(ctx)
	defer c.logAbnormalRuns(ctx)
	c.recordRun("disruption-loop")

	// Log if there are any budgets that are misconfigured that weren't caught by validation.
	// Only validate the first reason, since CEL validation will catch invalid disruption reasons
	c.logInvalidBudgets(ctx)

	// A NodeClaim that's launching isn't a node in cluster state yet, so a decision made now would be made off a
	// smaller subset of nodes than actually exist. The unlaunched NodeClaims wake the controller once they launch.
	if !c.cluster.NodeClaimsLaunched(ctx) {
		return reconciler.Result{}, nil
	}

	// Karpenter taints nodes with a karpenter.sh/disruption taint as part of the disruption process while it progresses in memory.
	// If Karpenter restarts or fails with an error during a disruption action, some nodes can be left tainted.
	// Idempotently remove this taint from candidates that are not in the orchestration queue before continuing.
	// Reboot bypasses the queue, so preserve its DisruptionReason during drain.
	outdatedNodes := lo.Reject(c.cluster.DeepCopyNodes(), func(s *karpentercluster.StateNode, _ int) bool {
		return c.queue.HasNode(s) || s.MarkedForDeletion() || s.RebootInProgress()
	})
	if err := RequireNoScheduleTaint(ctx, c.kubeClient, false, outdatedNodes...); err != nil {
		if errors.IsConflict(err) {
			return reconciler.Result{Requeue: true}, nil
		}
		return reconciler.Result{}, serrors.Wrap(fmt.Errorf("removing taint from nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
	}
	if err := ClearNodeClaimsCondition(ctx, c.kubeClient, c.clock, v1.ConditionTypeDisruptionReason, outdatedNodes...); err != nil {
		if errors.IsConflict(err) {
			return reconciler.Result{Requeue: true}, nil
		}
		return reconciler.Result{}, serrors.Wrap(fmt.Errorf("removing condition from nodeclaims, %w", err), "condition", v1.ConditionTypeDisruptionReason)
	}
	if options.FromContext(ctx).FeatureGates.PredictionEnabled && !c.provisioner.PredictionStoreHydrated(ctx) {
		return reconciler.Result{RequeueAfter: time.Second}, nil //storelint:deadline poll: the prediction store hydrates without a source
	}

	// Attempt different disruption methods. We'll only let one method perform an action
	for _, m := range c.methods {
		c.recordRun(fmt.Sprintf("%T", m))
		success, err := c.disrupt(ctx, m)
		if err != nil {
			if errors.IsConflict(err) {
				return reconciler.Result{Requeue: true}, nil
			}
			return reconciler.Result{}, serrors.Wrap(fmt.Errorf("disrupting, %w", err), strings.ToLower(string(m.Reason())), "reason")
		}
		if success {
			return reconciler.Result{RequeueAfter: singleton.RequeueImmediately}, nil //storelint:deadline the singleton runs again at once after a command
		}
	}

	// All methods did nothing, so wait for a change, or for the revalidation period
	return reconciler.Result{RequeueAfter: revalidationPeriod}, nil //storelint:deadline revalidation period, for what shows only with time
}

func (c *Controller) disrupt(ctx context.Context, disruption Method) (bool, error) {
	defer metrics.Measure(EvaluationDurationSeconds, map[string]string{
		metrics.ReasonLabel:    strings.ToLower(string(disruption.Reason())),
		ConsolidationTypeLabel: disruption.ConsolidationType(),
	})()
	candidates, nodePoolTotals, err := GetCandidatesWithTotals(ctx, c.cluster, c.kubeClient, c.recorder, c.clock, c.cloudProvider, disruption.ShouldDisrupt, disruption.Class(), c.queue, c.clusterCost)
	if err != nil {
		return false, fmt.Errorf("determining candidates, %w", err)
	}
	EligibleNodes.Set(float64(len(candidates)), map[string]string{
		metrics.ReasonLabel: strings.ToLower(string(disruption.Reason())),
	})

	// If there are no candidates, move to the next disruption
	if len(candidates) == 0 {
		return false, nil
	}
	// Pass precomputed NodePool totals to consolidation methods for balanced scoring
	if setter, ok := disruption.(NodePoolTotalsSetter); ok {
		setter.SetNodePoolTotals(nodePoolTotals)
	}
	disruptionBudgetMapping, err := BuildDisruptionBudgetMapping(ctx, c.cluster, c.clock, c.kubeClient, c.cloudProvider, c.recorder, disruption.Reason())
	if err != nil {
		return false, fmt.Errorf("building disruption budgets, %w", err)
	}
	// Determine the disruption action
	cmds, err := disruption.ComputeCommands(ctx, disruptionBudgetMapping, candidates...)
	if err != nil {
		// A method may have reserved for commands it returns with its error; nothing will start them.
		for i := range cmds {
			releaseReservations(&cmds[i])
		}
		return false, fmt.Errorf("computing disruption decision, %w", err)
	}
	cmds = lo.Filter(cmds, func(c Command, _ int) bool {
		if c.Decision() == NoOpDecision {
			releaseReservations(&c)
			return false
		}
		return true
	})
	if len(cmds) == 0 {
		return false, nil
	}

	errs := make([]error, len(cmds))
	attempted := make([]bool, len(cmds))
	// ParallelizeUntil skips the commands it hadn't started once ctx is done, so nothing will start them.
	defer func() {
		for i := range cmds {
			if !attempted[i] {
				releaseReservations(&cmds[i])
			}
		}
	}()
	workqueue.ParallelizeUntil(ctx, len(cmds), len(cmds), func(i int) {
		attempted[i] = true
		cmd := cmds[i]

		// Assign common fields
		cmd.CreationTimestamp = c.clock.Now()
		cmd.ID = uuid.New()
		cmd.Method = disruption

		// Attempt to disrupt
		if err := c.queue.StartCommand(ctx, &cmd); err != nil {
			errs[i] = fmt.Errorf("disrupting candidates, %w", err)
		}
	})
	if err = multierr.Combine(errs...); err != nil {
		return false, fmt.Errorf("disrupting candidates, %w", err)
	}
	return true, nil
}

func (c *Controller) recordRun(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastRun[s] = c.clock.Now()
}

func (c *Controller) logAbnormalRuns(ctx context.Context) {
	const AbnormalTimeLimit = 15 * time.Minute
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, runTime := range c.lastRun {
		if timeSince := c.clock.Since(runTime); timeSince > AbnormalTimeLimit {
			log.FromContext(ctx).V(1).Info("abnormal time between runs", "name", name, "time_since", timeSince)
		}
	}
}

// logInvalidBudgets will log if there are any invalid schedules detected
func (c *Controller) logInvalidBudgets(ctx context.Context) {
	nps, err := nodepoolutils.ListManaged(ctx, c.kubeClient, c.cloudProvider)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed listing nodepools")
		return
	}
	var buf bytes.Buffer
	for _, np := range nps {
		// Use a dummy value of 100 since we only care if this errors.
		for _, method := range c.methods {
			if _, err := np.GetAllowedDisruptionsByReason(c.clock, 100, method.Reason()); err != nil {
				fmt.Fprintf(&buf, "invalid disruption budgets in nodepool %s, %s", np.Name, err)
				break // Prevent duplicate error message
			}
		}
	}
	if buf.Len() > 0 {
		log.FromContext(ctx).Error(stderrors.New(buf.String()), "detected disruption budget errors")
	}
}
