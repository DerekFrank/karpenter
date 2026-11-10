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

	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
)

// The replay validator is the validator under test, for the consolidation methods it covers.
func init() {
	fuzzReplay = func(ctx context.Context, method disruption.Method, cmd disruption.Command) (*fuzzVerdict, *disruption.Command) {
		c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue)
		var validator *disruption.ReplayValidator
		switch method.(type) {
		case *disruption.MultiNodeConsolidation:
			validator = disruption.NewMultiReplayValidator(c)
		case *disruption.SingleNodeConsolidation:
			validator = disruption.NewSingleReplayValidator(c)
		default:
			return nil, nil
		}
		result := validator.Check(ctx, cmd)
		v := &fuzzVerdict{Accept: result.Accepted, Reason: result.Reason, Path: result.Path, Millis: float64(result.Duration.Microseconds()) / 1000}
		if result.Accepted && len(cmd.Replacements) > 0 {
			v.Narrowed = len(cmd.Replacements[0].InstanceTypeOptions) - len(result.Command.Replacements[0].InstanceTypeOptions)
		}
		if !result.Accepted {
			return v, nil
		}
		return v, &result.Command
	}
}
