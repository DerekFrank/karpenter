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

package node

import "github.com/prometheus/client_golang/prometheus"

// The node count's contributions, for their law checks.
type (
	NodeCounts        = nodeCounts
	LaunchedNodeClaim = launchedNodeClaim
	UnmanagedNode     = unmanagedNode
)

// SyncedCollector is the collector of whether cluster state is synced, for a test to register on its own registry.
func (c *Controller) SyncedCollector() prometheus.Collector { return c.syncedCollector() }
