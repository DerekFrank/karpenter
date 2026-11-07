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
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestWatchWhenEnqueuesOnlyTheChangesItSelects(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	counts := NewStore[string, int]()
	queue := newQueue(t)
	must(t, From(counts, ByName).When(func(old, new int) bool { return (old == 0) != (new == 0) }).Start(ctx, queue))
	counts.Update("a", func(int) int { return 1 })
	counts.Update("b", func(int) int { return 1 })
	if got := drain(queue); len(got) != 2 {
		t.Fatalf("queued %v, want a and b, which each gained fields", got)
	}
	counts.Update("a", func(int) int { return 2 })
	counts.Update("b", func(int) int { return 0 })
	if got := drain(queue); len(got) != 1 || got[0] != "b" {
		t.Fatalf("queued %v, want only b, whose fields went away; a's change keeps it nonzero", got)
	}
}

func TestFromEachEnqueuesEveryObjectAKeyAffects(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	counts := NewStore[string, int]()
	members := map[string][]client.ObjectKey{"pool": {{Name: "x"}, {Name: "y"}}}
	queue := newQueue(t)
	must(t, FromEach(counts, func(key string, _ int) []client.ObjectKey { return members[key] }).Start(ctx, queue))
	counts.Update("pool", func(int) int { return 1 })
	if got := drain(queue); len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("queued %v, want x and y", got)
	}
}

func TestFromEachEnqueuesTheObjectsOfTheFieldsBeforeAndAfter(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	owners := NewStore[string, string]() // providerID -> the Node that holds it
	queue := newQueue(t)
	must(t, FromEach(owners, func(_ string, node string) []client.ObjectKey {
		if node == "" {
			return nil
		}
		return []client.ObjectKey{{Name: node}}
	}).Start(ctx, queue))
	owners.Update("id", func(string) string { return "a" })
	owners.Update("id", func(string) string { return "b" })
	owners.Update("id", func(string) string { return "" })
	if got := drain(queue); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("queued %v, want a, which gained and lost the key, and b, which gained it and lost it as it went away", got)
	}
}

func TestWakeFoldsEveryChangeIntoOneRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	a, b := NewStore[string, int](), NewStore[string, string]()
	a.Update("x", func(int) int { return 1 })
	queue := newQueue(t)
	must(t, Wake(From(a, ByName), From(b, ByName)).Start(ctx, queue))
	a.Update("y", func(int) int { return 1 })
	b.Update("z", func(string) string { return "set" })
	if got := drain(queue); len(got) != 1 || got[0] != "" {
		t.Fatalf("queued %q, want one empty request for the existing key and both changes", got)
	}
}

func TestAfterDelaysAndFoldsChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	counts := NewStore[string, int]()
	queue := newQueue(t)
	must(t, After(100*time.Millisecond, From(counts, ByName)).Start(ctx, queue))
	for i := 1; i <= 5; i++ {
		counts.Update("a", func(int) int { return i })
	}
	if queue.Len() != 0 {
		t.Fatal("queued a change before its delay")
	}
	time.Sleep(300 * time.Millisecond)
	if got := drain(queue); len(got) != 1 || got[0] != "a" {
		t.Fatalf("queued %v, want a once for its five changes", got)
	}
}

func TestWatchWaitsForWhatFeedsItToSync(t *testing.T) {
	s := &synced{}
	w := Wake(From(NewStore[string, int](), ByName, s))
	waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := w.WaitForSync(waitCtx); err == nil {
		t.Fatal("WaitForSync returned before the store synced")
	}
	s.Store(true)
	must(t, w.WaitForSync(ctx))
}
