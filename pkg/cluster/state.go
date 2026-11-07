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

// Package cluster holds the cluster's state: an accessor for each kind Karpenter reads, returning the objects with the
// fields Karpenter derives about them, built from the primitives in pkg/state. One file per kind. See
// pkg/state/AGENTS.md.
package cluster

import (
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// State holds every accessor and the stores behind them. Build it once, with New, where controllers are wired, and
// give each controller the accessors it reads, never State itself, so that each constructor says what it reads.
type State struct {
	NodePools *NodePools
	// NodePoolUsage keeps the NodePools' usage. Its NodeClaims Contributor must run as a manager runnable.
	NodePoolUsage *NodePoolUsage
}

// New builds every store and accessor, in dependency order, reading objects through kubeClient and following them
// through informers.
func New(kubeClient client.Client, informers cache.Informers) State {
	usage := NewNodePoolUsage(kubeClient, informers)
	return State{
		NodePools:     NewNodePools(kubeClient, usage),
		NodePoolUsage: usage,
	}
}
