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

	"github.com/awslabs/operatorpkg/option"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

// testFinalizer holds an object a spec deletes as being deleted.
const testFinalizer = "karpenter.sh/test-finalizer"

// env is the suite's environment, which the package's own specs write through, as the suite's do.
var env *test.Environment

// UseEnvironment gives the package's own specs the suite's environment.
func UseEnvironment(e *test.Environment) { env = e }

// testContext is the context the package's own specs build cluster state with: the test options, with fields.
func testContext(fields ...test.OptionsFields) context.Context {
	return options.ToContext(context.Background(), test.Options(fields...))
}

// newSuiteState builds cluster state over the suite's overlay and closes it once the spec ends.
func newSuiteState(ctx context.Context, opts ...option.Function[Options]) State {
	GinkgoHelper()
	s := New(ctx, env.Clock, env.Overlay, fake.NewCloudProvider(), opts...)
	DeferCleanup(s.Close)
	return s
}

// apply creates or updates each object through env.Client, spec then status, as the object gives them, and reads it
// back. An object being deleted keeps the finalizers that hold it. env.Client returns once every store following the overlay holds each write.
func apply(objs ...client.Object) {
	GinkgoHelper()
	for _, obj := range objs {
		Expect(upsert(obj)).To(Succeed())
	}
}

// upsert is apply of one object, returning its error, for a spec's goroutine.
func upsert(obj client.Object) error {
	ctx := context.Background()
	want := obj.DeepCopyObject().(client.Object)
	if err := retry.OnError(retry.DefaultBackoff, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) || apierrors.IsNotFound(err)
	},
		func() error {
			current := want.DeepCopyObject().(client.Object)
			next := want.DeepCopyObject().(client.Object)
			if err := env.Client.Get(ctx, client.ObjectKeyFromObject(want), current); err != nil {
				if !apierrors.IsNotFound(err) {
					return err
				}
				next.SetResourceVersion("")
				next.SetUID("")
				if err := env.Client.Create(ctx, next); err != nil {
					return err
				}
				current = next
			} else {
				next.SetResourceVersion(current.GetResourceVersion())
				next.SetUID(current.GetUID())
				next.SetDeletionTimestamp(current.GetDeletionTimestamp())
				if !current.GetDeletionTimestamp().IsZero() {
					// An object being deleted keeps the finalizers that hold it.
					next.SetFinalizers(current.GetFinalizers())
				}
				if err := env.Client.Update(ctx, next); err != nil {
					return err
				}
				current = next
			}
			status := want.DeepCopyObject().(client.Object)
			status.SetResourceVersion(current.GetResourceVersion())
			status.SetUID(current.GetUID())
			status.SetDeletionTimestamp(current.GetDeletionTimestamp())
			status.SetFinalizers(current.GetFinalizers())
			if err := env.Client.Status().Update(ctx, status); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			return nil
		}); err != nil {
		return err
	}
	return env.Client.Get(ctx, client.ObjectKeyFromObject(obj), obj)
}

// remove deletes each object through env.Client, dropping its finalizers first, and returns once it's gone.
func remove(objs ...client.Object) {
	GinkgoHelper()
	for _, obj := range objs {
		Expect(removeNow(obj)).To(Succeed())
	}
}

// removeNow is remove of one object, returning its error, for a spec's goroutine.
func removeNow(obj client.Object) error {
	ctx := context.Background()
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := obj.DeepCopyObject().(client.Object)
		if err := env.Client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
			return err
		}
		if len(current.GetFinalizers()) == 0 {
			return nil
		}
		current.SetFinalizers(nil)
		return env.Client.Update(ctx, current)
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	zero := int64(0)
	return client.IgnoreNotFound(env.Client.Delete(ctx, obj.DeepCopyObject().(client.Object), &client.DeleteOptions{GracePeriodSeconds: &zero}))
}

// markDeleting deletes the object through env.Client with a finalizer that holds it, so that it's being deleted, and
// reads it back.
func markDeleting(obj client.Object) {
	GinkgoHelper()
	Expect(markDeletingNow(obj)).To(Succeed())
}

// markDeletingNow is markDeleting, returning its error, for a spec's goroutine.
func markDeletingNow(obj client.Object) error {
	ctx := context.Background()
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current := obj.DeepCopyObject().(client.Object)
		if err := env.Client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
			return err
		}
		if !current.GetDeletionTimestamp().IsZero() {
			return nil
		}
		current.SetFinalizers(append(current.GetFinalizers(), testFinalizer))
		return env.Client.Update(ctx, current)
	}); err != nil {
		return err
	}
	if err := env.Client.Delete(ctx, obj.DeepCopyObject().(client.Object)); client.IgnoreNotFound(err) != nil {
		return err
	}
	return env.Client.Get(ctx, client.ObjectKeyFromObject(obj), obj)
}
