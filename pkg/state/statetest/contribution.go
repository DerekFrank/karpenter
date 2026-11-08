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

// Package statetest checks the state primitives' contracts in tests.
package statetest

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
)

// Contribution is state.Contribution, restated so that pkg/state's own tests can use ExpectContribution.
type Contribution[F, C any] interface {
	Update(fields F, prev C) F
	Delete(fields F) F
}

// ExpectContribution checks that prev and next keep the laws state.Contribution states, on fields that start as base:
// Delete undoes Update from the zero contribution, and Update(fields, prev) equals prev.Delete followed by
// Update(fields, zero). Fields compare semantically, so a nil and an empty map are equal, as are equal quantities.
func ExpectContribution[F any, C Contribution[F, C]](t testing.TB, base F, prev, next C) {
	t.Helper()
	ExpectContributionWith(t, func(a, b F) bool { return equality.Semantic.DeepEqual(a, b) }, base, prev, next)
}

// ExpectContributionWith checks the laws as ExpectContribution does, comparing fields with equal, for fields that
// equality.Semantic can't compare, such as ones with unexported fields.
func ExpectContributionWith[F any, C Contribution[F, C]](t testing.TB, equal func(a, b F) bool, base F, prev, next C) {
	t.Helper()
	var zero C
	for _, c := range []C{prev, next} {
		if got := c.Delete(c.Update(base, zero)); !equal(got, base) {
			t.Errorf("%+v: Delete after Update from zero = %+v, want the fields it started from, %+v", c, got, base)
		}
	}
	withPrev := prev.Update(base, zero)
	if got, want := next.Update(withPrev, prev), next.Update(prev.Delete(withPrev), zero); !equal(got, want) {
		t.Errorf("%+v replacing %+v: Update = %+v, want what Delete then Update from zero gives, %+v", next, prev, got, want)
	}
}
