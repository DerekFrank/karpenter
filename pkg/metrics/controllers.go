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

package metrics

// Controller names are pure metric/log identifiers (the `controller` dimension and
// the logger name) — they are never used in reconcile logic — so they are
// centralized here as first-class metrics.Value vars. Each controller's Name()
// returns its value's .Name, giving one source of truth for the string and its help.
//
// ControllerValues is the `controller` dimension's value set. The metrics docs
// generator reads it (unioned with the provider's ControllerValues and the
// operatorpkg status/events controllers synthesized from their registration
// sites). Add a controller here and reference it from the controller's Name().
var (
	DisruptionController         = Value{Name: "disruption", Help: "Disrupts nodes via consolidation, drift, and expiration."}
	ProvisionerController        = Value{Name: "provisioner", Help: "Provisions nodes for unschedulable pods."}
	NodeClaimLifecycleController = Value{Name: "nodeclaim.lifecycle", Help: "Launches, registers, and initializes the instance backing a NodeClaim."}
)

// ControllerValues enumerates the core controllers for the `controller` dimension.
var ControllerValues = []Value{
	DisruptionController,
	ProvisionerController,
	NodeClaimLifecycleController,
}
