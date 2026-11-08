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
	"context"
	"fmt"

	"github.com/awslabs/operatorpkg/option"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cluster/repair"
)

// State holds every accessor and the stores behind them. Build it once, with New, where the operator is wired, start
// its stores with Register, and give each controller the accessors it reads, never State itself, so that each
// constructor says what it reads.
type State struct {
	NodePools     *NodePools
	NodePoolUsage *NodePoolUsage
	// Nodes holds the stores behind the nodes Karpenter schedules onto and disrupts.
	Nodes *NodeState
}

// Options configure the state New builds.
type Options struct {
	repairPolicyMatcher *repair.RepairPolicyMatcher
}

// WithRepairPolicyMatcher makes the node state match each Node against the repair policies as it changes. Node repair
// reads the same matcher back through NodeState.RepairPolicyMatcher.
func WithRepairPolicyMatcher(matcher *repair.RepairPolicyMatcher) option.Function[Options] {
	return func(o *Options) { o.repairPolicyMatcher = matcher }
}

// New builds every store and accessor, in dependency order, reading objects through kubeClient and following them
// through informers once Register starts them. ctx carries the options derived fields read, and cloudProvider's node
// classes decide which NodeClaims the node state holds.
func New(ctx context.Context, clk clock.Clock, kubeClient client.Client, informers cache.Informers, cloudProvider cloudprovider.CloudProvider, opts ...option.Function[Options]) State {
	o := option.Resolve(opts...)
	usage := NewNodePoolUsage(kubeClient, informers)
	return State{
		NodePools:     NewNodePools(kubeClient, usage),
		NodePoolUsage: usage,
		Nodes:         NewNodeState(ctx, clk, kubeClient, informers, cloudProvider, o.repairPolicyMatcher),
	}
}

// Register adds every shared object and join store as a manager runnable, once, so that each lists its kinds and
// follows them after the manager's caches sync. A controller reading them declares it with state.Requires, which holds
// its workers until they're synced. A controller adds only the stores it builds itself.
func (s State) Register(m manager.Manager) error {
	for _, store := range []manager.Runnable{s.NodePoolUsage.NodeClaims, s.Nodes.NodeClaimStore, s.Nodes.NodeStore, s.Nodes.PodStore} {
		if err := m.Add(store); err != nil {
			return fmt.Errorf("adding cluster state store, %w", err)
		}
	}
	return nil
}
