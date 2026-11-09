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

package overlay

import (
	"fmt"
	"math/rand/v2"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// The overlay's suite runs its own API server: pkg/test's environment builds an overlay, so the suite can't use it.

type testEnv struct {
	envtest.Environment
	// Client reads and writes the API server directly.
	Client client.Client
}

func newTestEnv() *testEnv {
	e := &testEnv{Environment: envtest.Environment{Scheme: scheme.Scheme, CRDs: apis.CRDs}}
	lo.Must(e.Start())
	e.Client = lo.Must(client.New(e.Config, client.Options{Scheme: scheme.Scheme}))
	return e
}

func (e *testEnv) Stop() error { return e.Environment.Stop() }

func randomName() string { return fmt.Sprintf("overlay-%d", rand.Uint64()) } //nolint:gosec

func randomProviderID() string { return fmt.Sprintf("fake:///%d", rand.Uint64()) } //nolint:gosec

// newNodeClaim returns a NodeClaim with the labels, as the API server accepts it.
func newNodeClaim(labels map[string]string) *v1.NodeClaim {
	return &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: randomName(), Labels: labels},
		Spec: v1.NodeClaimSpec{
			NodeClassRef: &v1.NodeClassReference{Group: "karpenter.test.sh", Kind: "TestNodeClass", Name: "default"},
			Requirements: []v1.NodeSelectorRequirementWithMinValues{},
		},
		Status: v1.NodeClaimStatus{ProviderID: randomProviderID()},
	}
}

// newPod returns a pod in the default namespace bound to nodeName, if any, as the API server accepts it.
func newPod(nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: randomName(), Namespace: "default", UID: types.UID("")},
		Spec:       corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Name: "c", Image: "public.ecr.aws/eks-distro/kubernetes/pause:3.2"}}},
	}
}
