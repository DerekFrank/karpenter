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

package memo

import (
	"context"
	"math"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type podInputs struct {
	Phase    corev1.PodPhase
	Requests corev1.ResourceList
	Cost     float64
}

// podTracker tracks pods by node name, counting the computes of each node's value.
func podTracker(t *testing.T) (*Tracker[*corev1.Pod, string, int], func(string) int) {
	t.Helper()
	computes := map[string]int{}
	m := New(func(_ context.Context, node string) (int, error) {
		computes[node]++
		return computes[node], nil
	})
	tracker := Track(m, func(_ context.Context, pod *corev1.Pod) (string, podInputs) {
		cost := 0.0
		if pod.Annotations["cost"] == "nan" {
			cost = math.NaN()
		}
		return pod.Spec.NodeName, podInputs{Phase: pod.Status.Phase, Requests: pod.Spec.Containers[0].Resources.Requests, Cost: cost}
	})
	// recomputes reads the node's value and returns how many times it was computed since the last call.
	seen := map[string]int{}
	return tracker, func(node string) int {
		get(t, m, node)
		n := computes[node] - seen[node]
		seen[node] = computes[node]
		return n
	}
}

func pod(node, cpu string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
		}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestTrackerInvalidatesOnlyWhenInputsChange(t *testing.T) {
	tracker, recomputes := podTracker(t)
	p := pod("a", "100m")
	tracker.Observe(ctx, p)
	if n := recomputes("a"); n != 1 {
		t.Fatalf("computed a %d times, want 1", n)
	}

	ready := p.DeepCopy()
	ready.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	ready.ResourceVersion = "2"
	tracker.Observe(ctx, ready)
	tracker.Observe(ctx, ready)
	if n := recomputes("a"); n != 0 {
		t.Fatalf("recomputed a %d times after updates that don't change its inputs, want 0", n)
	}

	// An equal quantity in another form isn't a change; another quantity is.
	tracker.Observe(ctx, pod("a", "0.1"))
	if n := recomputes("a"); n != 0 {
		t.Fatalf("recomputed a %d times after 100m became 0.1, want 0", n)
	}
	tracker.Observe(ctx, pod("a", "300m"))
	if n := recomputes("a"); n != 1 {
		t.Fatalf("recomputed a %d times after a resize, want 1", n)
	}
}

func TestTrackerInvalidatesBothKeysOfAMove(t *testing.T) {
	tracker, recomputes := podTracker(t)
	tracker.Observe(ctx, pod("a", "100m"))
	recomputes("a")
	recomputes("b")
	if previous, ok := tracker.Observe(ctx, pod("b", "100m")); !ok || previous != "a" {
		t.Fatalf("Observe returned %q, %t, want the previous key a", previous, ok)
	}
	if recomputes("a") != 1 || recomputes("b") != 1 {
		t.Fatal("a move didn't recompute both keys")
	}
	tracker.Forget(client.ObjectKeyFromObject(pod("b", "100m")))
	if recomputes("b") != 1 {
		t.Fatal("Forget didn't recompute the key the pod counted toward")
	}
	if _, ok := tracker.Key(client.ObjectKeyFromObject(pod("b", "100m"))); ok {
		t.Fatal("still tracking a forgotten pod")
	}
}

func TestTrackerClaim(t *testing.T) {
	tracker, recomputes := podTracker(t)
	p := pod("a", "100m")
	if !tracker.Claim("a", p) {
		t.Fatal("claiming an untracked pod returned false, want true so the caller checks it exists")
	}
	if tracker.Claim("a", p) {
		t.Fatal("claiming a pod already tracked under the key returned true")
	}
	recomputes("a")
	// A claimed pod's inputs are unknown, so its first observation invalidates.
	tracker.Observe(ctx, p)
	if recomputes("a") != 1 {
		t.Fatal("observing a claimed pod didn't recompute")
	}
}

func TestTrackerInvalidatesInputsJSONCantEncode(t *testing.T) {
	tracker, recomputes := podTracker(t)
	p := pod("a", "100m")
	p.Annotations = map[string]string{"cost": "nan"}
	tracker.Observe(ctx, p)
	recomputes("a")
	tracker.Observe(ctx, p)
	if recomputes("a") != 1 {
		t.Fatal("unencodable inputs didn't invalidate")
	}
}

func TestTrackRejectsInputsJSONHashesWrong(t *testing.T) {
	for name, track := range map[string]func(){
		"unexported": func() {
			Track(New[string, int](nil), func(context.Context, *corev1.Pod) (string, struct{ phase string }) {
				return "", struct{ phase string }{}
			})
		},
		"time": func() {
			Track(New[string, int](nil), func(context.Context, *corev1.Pod) (string, struct{ Created metav1.Time }) {
				return "", struct{ Created metav1.Time }{}
			})
		},
		"interface": func() {
			Track(New[string, int](nil), func(context.Context, *corev1.Pod) (string, struct{ Any any }) {
				return "", struct{ Any any }{}
			})
		},
		"skipped": func() {
			type skipped struct {
				Phase string `json:"-"`
			}
			Track(New[string, int](nil), func(context.Context, *corev1.Pod) (string, skipped) { return "", skipped{} })
		},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil || !strings.Contains(r.(string), "can't be hashed") {
					t.Fatalf("Track recovered %v, want a panic saying the inputs can't be hashed", r)
				}
			}()
			track()
		})
	}
}
