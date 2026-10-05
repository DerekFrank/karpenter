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

package operator_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	prometheusmodel "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	clientmetrics "k8s.io/client-go/tools/metrics"

	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

func TestOperator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Operator")
}

var _ = Describe("Operator", func() {
	It("should fire a metric with the build_info", func() {
		m, found := FindMetricWithLabelValues("karpenter_build_info", map[string]string{})
		Expect(found).To(BeTrue())

		for _, label := range []string{"version", "goversion", "goarch", "commit"} {
			_, ok := lo.Find(m.GetLabel(), func(l *prometheusmodel.LabelPair) bool { return lo.FromPtr(l.Name) == label })
			Expect(ok).To(BeTrue())
		}
	})
	It("should fire a metric for client-go rate limiter latency", func() {
		clientmetrics.RateLimiterLatency.Observe(context.Background(), "PUT", url.URL{Path: "/apis/karpenter.sh/v1/nodeclaims/{name}/status"}, 2*time.Second)
		m, found := FindMetricWithLabelValues("client_go_rate_limiter_duration_seconds", map[string]string{
			"verb":        "UPDATE",
			"group":       "karpenter.sh",
			"version":     "v1",
			"kind":        "nodeclaims",
			"subresource": "status",
		})
		Expect(found).To(BeTrue())
		Expect(m.GetHistogram().GetSampleCount()).To(BeNumerically("==", 1))
		Expect(m.GetHistogram().GetSampleSum()).To(BeNumerically("==", 2))
	})
})
