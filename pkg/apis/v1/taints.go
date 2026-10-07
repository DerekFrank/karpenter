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

package v1

import (
	"github.com/awslabs/operatorpkg/docs"
	"github.com/awslabs/operatorpkg/wellknown"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/karpenter/pkg/apis"
)

// Karpenter specific taints
const (
	DisruptedTaintKey    = apis.Group + "/disrupted"
	UnregisteredTaintKey = apis.Group + "/unregistered"
	RebootingTaintKey    = apis.Group + "/rebooting"
)

var (
	DisruptedNoScheduleTaint = v1.Taint{
		Key:    DisruptedTaintKey,
		Effect: v1.TaintEffectNoSchedule,
	}
	UnregisteredNoExecuteTaint = v1.Taint{
		Key:    UnregisteredTaintKey,
		Effect: v1.TaintEffectNoExecute,
	}
	RebootingNoScheduleTaint = v1.Taint{
		Key:    RebootingTaintKey,
		Effect: v1.TaintEffectNoSchedule,
	}
)

var (
	DisruptedTaint = wellknown.Taint{
		Taint:  DisruptedNoScheduleTaint,
		UsedOn: []runtime.Object{&v1.Node{}},
		Help: "Karpenter adds this to a node it is disrupting or terminating, so no new pods schedule to it while " +
			"it drains, and removes it if the disruption is abandoned. Karpenter doesn't evict pods that tolerate " +
			"it, so tolerate it only on pods that should run until the node is gone, e.g. DaemonSet pods.",
		Stage: docs.GA,
	}
	UnregisteredTaint = wellknown.Taint{
		Taint:  UnregisteredNoExecuteTaint,
		UsedOn: []runtime.Object{&v1.Node{}},
		Help: "Nodes launched by Karpenter register with this taint. Karpenter removes it once it has synced the " +
			"NodeClaim's labels, annotations, and taints onto the node, so pods can't start before then.",
		Stage:        docs.Alpha,
		InternalOnly: true,
	}
	RebootingTaint = wellknown.Taint{
		Taint:  RebootingNoScheduleTaint,
		UsedOn: []runtime.Object{&v1.Node{}},
		Help: "Karpenter adds this to a node it is rebooting, so no new pods schedule to it, and removes it once " +
			"the node boots with a new boot ID.",
		Stage:        docs.Alpha,
		InternalOnly: true,
	}
)

// KarpenterTaints are the well known taints Karpenter adds or removes.
var KarpenterTaints = []wellknown.Taint{
	DisruptedTaint,
	UnregisteredTaint,
	RebootingTaint,
}
