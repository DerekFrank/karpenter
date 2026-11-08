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

package state

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Synced is a store, or anything else that reports whether it holds what its sources held when it started.
type Synced interface {
	Synced() bool
}

// Requires returns a source that enqueues nothing and holds the controller's workers until every store is synced. A
// controller that reads stores without watching them declares it in Register, with WatchesRawSource(Requires(...)), so
// that it never acts on a store that is still filling. The wait counts against the controller's CacheSyncTimeout.
func Requires(stores ...Synced) source.SyncingSource {
	return requires{stores: stores, poll: 10 * time.Millisecond}
}

type requires struct {
	stores []Synced
	poll   time.Duration
}

func (r requires) Start(context.Context, workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	return nil
}

func (r requires) WaitForSync(ctx context.Context) error {
	ticker := time.NewTicker(r.poll)
	defer ticker.Stop()
	for {
		if r.synced() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for stores to sync, %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (r requires) synced() bool {
	for _, s := range r.stores {
		if !s.Synced() {
			return false
		}
	}
	return true
}

func (r requires) String() string { return fmt.Sprintf("requires %d stores", len(r.stores)) }
