//go:build test_performance

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

package scheduling_test

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"sync"
	"testing"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/prediction"
	"sigs.k8s.io/karpenter/pkg/state/workloads"
	"sigs.k8s.io/karpenter/pkg/test"
)

const predictionPodsPerReplicaSet = 10

// predictionFixture is a consolidation simulation's input: pods migrating off their nodes, owned by ReplicaSets of
// Deployments that each have a prediction. Owners are read through a real controller-runtime informer cache, the same
// read path as workloads.Reader on the manager's client in production.
type predictionFixture struct {
	ctx           context.Context
	client        client.Client
	cluster       *state.Cluster
	nodePool      *v1.NodePool
	instanceTypes map[string][]*cloudprovider.InstanceType
	pods          []*corev1.Pod
	deletingUIDs  sets.Set[types.UID]
	store         *prediction.Store
}

var (
	predictionEnvOnce sync.Once
	predictionCfg     *rest.Config
	predictionCtx     context.Context
	predictionByPods  = map[int]*predictionFixture{}
)

// predictionClient returns a client reading through a started informer cache with ReplicaSets registered, as
// workloads.Informers registers them in production.
func predictionClient(tb testing.TB) client.Client {
	predictionEnvOnce.Do(func() {
		predictionCfg = lo.Must((&envtest.Environment{}).Start())
		predictionCtx = benchCtx()
	})
	c := lo.Must(cache.New(predictionCfg, cache.Options{}))
	lo.Must(c.GetInformer(predictionCtx, &appsv1.ReplicaSet{}))
	go func() { lo.Must0(c.Start(predictionCtx)) }()
	if !c.WaitForCacheSync(predictionCtx) {
		tb.Fatal("cache failed to sync")
	}
	return lo.Must(client.New(predictionCfg, client.Options{Cache: &client.CacheOptions{Reader: c}}))
}

func buildPredictionFixture(tb testing.TB, podCount int) *predictionFixture {
	tb.Helper()
	if f, ok := predictionByPods[podCount]; ok {
		return f
	}
	ctx := options.ToContext(benchCtx(), test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{PredictionEnabled: lo.ToPtr(true)}}))
	c := predictionClient(tb)
	cp := fake.NewCloudProvider()
	its := fake.InstanceTypes(400)
	cp.InstanceTypes = its
	cl := state.NewCluster(&clock.RealClock{}, c, cp)
	store := prediction.NewStore(workloads.NewReader(c))
	lo.Must0(store.RegisterTargetKinds(prediction.SupportedTargets.UnsortedList()...))

	ns := fmt.Sprintf("bench-%d", podCount)
	lo.Must0(c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	var pods []*corev1.Pod
	for i := 0; i < podCount/predictionPodsPerReplicaSet; i++ {
		deployment := fmt.Sprintf("app-%d", i)
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: deployment + "-5d8f7c", Namespace: ns,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment, UID: uuid.NewUUID(), Controller: lo.ToPtr(true)}},
			},
			Spec: appsv1.ReplicaSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": deployment}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": deployment}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app"}}},
				},
			},
		}
		lo.Must0(c.Create(ctx, rs))
		store.Set(types.NamespacedName{Namespace: ns, Name: "vpa-" + deployment},
			prediction.WorkloadReference{GroupKind: schema.GroupKind{Group: "apps", Kind: "Deployment"}, NamespacedName: types.NamespacedName{Namespace: ns, Name: deployment}},
			&prediction.Prediction{Containers: map[string]corev1.ResourceList{"app": {corev1.ResourceCPU: resource.MustParse("750m"), corev1.ResourceMemory: resource.MustParse("512Mi")}}},
			rs.CreationTimestamp.Time,
		)
		for j := 0; j < predictionPodsPerReplicaSet; j++ {
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("%s-%d", rs.Name, j), Namespace: ns, UID: uuid.NewUUID(),
					Labels:          map[string]string{"app": deployment},
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: lo.ToPtr(true)}},
				},
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("256Mi")}},
			})
			pod.Spec.Containers[0].Name = "app"
			pods = append(pods, pod)
		}
	}
	// Wait until the cache holds every ReplicaSet, so every pod resolves to its Deployment's prediction
	want := resource.MustParse("750m")
	want.Mul(int64(len(pods)))
	for {
		if requests := store.PredictedRequests(ctx, pods...); requests.Cpu().Cmp(want) == 0 {
			break
		}
	}
	nodePool := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("10000000"), corev1.ResourceMemory: resource.MustParse("10000000Gi")}}})
	f := &predictionFixture{
		ctx:           ctx,
		client:        c,
		cluster:       cl,
		nodePool:      nodePool,
		instanceTypes: map[string][]*cloudprovider.InstanceType{nodePool.Name: its},
		pods:          pods,
		deletingUIDs:  sets.New(lo.Map(pods, func(p *corev1.Pod, _ int) types.UID { return p.UID })...),
		store:         store,
	}
	predictionByPods[podCount] = f
	return f
}

// simulate runs one consolidation-style scheduling simulation: a fresh scheduler, as each disruption simulation builds,
// solving the migrating pods.
func (f *predictionFixture) simulate(tb testing.TB, store *prediction.Store) int {
	topology := lo.Must(scheduling.NewTopology(f.ctx, f.client, f.cluster, nil, []*v1.NodePool{f.nodePool}, f.instanceTypes, f.pods))
	s := scheduling.NewScheduler(f.ctx, f.client, []*v1.NodePool{f.nodePool}, f.cluster, nil, topology, f.instanceTypes, nil,
		events.NewRecorder(&record.FakeRecorder{}), &clock.RealClock{}, nil, nil, f.deletingUIDs, store, scheduling.NumConcurrentReconciles(5))
	results, err := s.Solve(f.ctx, f.pods)
	if err != nil {
		tb.Fatalf("solving, %s", err)
	}
	if len(results.PodErrors) > 0 {
		tb.Fatalf("expected all pods to schedule, got %d errors", len(results.PodErrors))
	}
	return len(results.NewNodeClaims)
}

func BenchmarkPredictionSimulation(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		f := buildPredictionFixture(b, n)
		b.Run(fmt.Sprintf("predictions=off/pods=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var nodeClaims int
			for i := 0; i < b.N; i++ {
				nodeClaims = f.simulate(b, nil)
			}
			b.ReportMetric(float64(nodeClaims), "nodeclaims")
		})
		b.Run(fmt.Sprintf("predictions=on/pods=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var nodeClaims int
			for i := 0; i < b.N; i++ {
				nodeClaims = f.simulate(b, f.store)
			}
			b.ReportMetric(float64(nodeClaims), "nodeclaims")
		})
	}
}

// BenchmarkPredictedRequests isolates the per-simulation cost of applying predictions.
func BenchmarkPredictedRequests(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		f := buildPredictionFixture(b, n)
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				predictedRequests(f)
			}
		})
	}
}

// TestPredictionProfile writes CPU profiles of simulations with and without predictions.
// go test -tags=test_performance -run=PredictionProfile ./pkg/controllers/provisioning/scheduling/
func TestPredictionProfile(t *testing.T) {
	f := buildPredictionFixture(t, 5000)
	dir := lo.Ternary(os.Getenv("PROFILE_DIR") != "", os.Getenv("PROFILE_DIR"), ".")
	for _, tc := range []struct {
		name  string
		store *prediction.Store
	}{{"off", nil}, {"on", f.store}} {
		out := lo.Must(os.Create(fmt.Sprintf("%s/prediction-%s.cpuprofile", dir, tc.name)))
		lo.Must0(pprof.StartCPUProfile(out))
		for i := 0; i < 20; i++ {
			f.simulate(t, tc.store)
		}
		pprof.StopCPUProfile()
		lo.Must0(out.Close())
	}
	out := lo.Must(os.Create(fmt.Sprintf("%s/prediction-requests.cpuprofile", dir)))
	lo.Must0(pprof.StartCPUProfile(out))
	for i := 0; i < 200; i++ {
		predictedRequests(f)
	}
	pprof.StopCPUProfile()
	lo.Must0(out.Close())
}

// predictedRequests applies predictions to the fixture's pods as one scheduling simulation does.
func predictedRequests(f *predictionFixture) {
	f.store.PredictedRequests(f.ctx, f.pods...)
}
