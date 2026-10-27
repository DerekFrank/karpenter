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
	"context"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

var _ = Describe("Metric Seeding", func() {
	It("should seed the series of every disruption method", func() {
		// Repair needs cluster state built with a repair policy matcher, so it's covered by seededMethods directly.
		ctx := options.ToContext(context.Background(), test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{NodeRepair: new(false)}}))
		methods := NewMethods(ctx, clocktesting.NewFakeClock(time.Now()), nil, nil, nil, nil, nil, nil)
		seeded := lo.Map(seededMethods, func(m Method, _ int) reflect.Type { return reflect.TypeOf(m) })
		Expect(seeded).To(ContainElement(reflect.TypeFor[*Repair]()))
		for _, m := range methods {
			Expect(seeded).To(ContainElement(reflect.TypeOf(m)))
		}
	})
})
