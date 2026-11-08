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
	"sync/atomic"
	"testing"
	"time"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

type syncFlag struct{ synced atomic.Bool }

func (f *syncFlag) Synced() bool { return f.synced.Load() }

func TestRequiresHoldsUntilEveryStoreIsSyncedAndEnqueuesNothing(t *testing.T) {
	a, b := &syncFlag{}, &syncFlag{}
	a.synced.Store(true)
	src := Requires(a, b)
	if err := src.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := src.WaitForSync(short); err == nil {
		t.Fatal("WaitForSync returned before every store synced")
	}
	b.synced.Store(true)
	if err := src.WaitForSync(ctx); err != nil {
		t.Fatalf("WaitForSync = %v once every store synced", err)
	}
	nodeClaims := NewObjectStore[*v1.NodeClaim](nil, nil)
	s := NewStore[string, poolFields]()
	Derive(nodeClaims, s, poolOf, usageOf)
	if err := Requires(nodeClaims, s).WaitForSync(ctx); err != nil {
		t.Fatalf("WaitForSync = %v for stores fed by a caller, which are synced from the start", err)
	}
}
