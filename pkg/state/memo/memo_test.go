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

package memo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

var ctx = context.Background()

// source is the cache a memo computes from: a value per key and the number of computations.
type source struct {
	mu       sync.Mutex
	values   map[string]int
	computes atomic.Int64
}

func (s *source) compute(_ context.Context, key string) (int, error) {
	s.computes.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key], nil
}

func (s *source) set(key string, value int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
}

func get(t *testing.T, m *Memo[string, int], key string) int {
	t.Helper()
	v, err := m.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestMemoCachesUntilInvalidated(t *testing.T) {
	s := &source{values: map[string]int{"a": 1, "b": 2}}
	m := New(s.compute)
	for _, key := range []string{"a", "a", "b"} {
		if got, want := get(t, m, key), s.values[key]; got != want {
			t.Fatalf("%s = %d, want %d", key, got, want)
		}
	}
	if n := s.computes.Load(); n != 2 {
		t.Fatalf("computed %d times, want once per key", n)
	}
	s.set("a", 3)
	if get(t, m, "a") != 1 {
		t.Fatal("recomputed without an invalidation")
	}
	m.Invalidate("a")
	if get(t, m, "a") != 3 || get(t, m, "b") != 2 {
		t.Fatal("invalidating a didn't recompute a alone")
	}
	s.set("b", 4)
	m.Reset()
	if get(t, m, "b") != 4 {
		t.Fatal("Reset didn't drop b")
	}
}

func TestMemoDoesNotCacheAValueComputedDuringAnInvalidation(t *testing.T) {
	s := &source{values: map[string]int{"a": 1}}
	started, release := make(chan struct{}), make(chan struct{})
	m := New(func(ctx context.Context, key string) (int, error) {
		v, _ := s.compute(ctx, key)
		if s.computes.Load() == 1 {
			close(started)
			<-release
		}
		return v, nil
	})
	done := make(chan int)
	go func() { done <- get(t, m, "a") }()
	<-started
	s.set("a", 2)
	m.Invalidate("a")
	close(release)
	if v := <-done; v != 1 {
		t.Fatalf("in-flight Get = %d, want what it read, 1", v)
	}
	if v := get(t, m, "a"); v != 2 {
		t.Fatalf("Get after the invalidation = %d, want 2", v)
	}
}

func TestMemoDoesNotCacheErrors(t *testing.T) {
	fail := true
	m := New(func(context.Context, string) (int, error) {
		if fail {
			return 0, errors.New("unavailable")
		}
		return 1, nil
	})
	if _, err := m.Get(ctx, "a"); err == nil {
		t.Fatal("want the error")
	}
	fail = false
	if get(t, m, "a") != 1 {
		t.Fatal("cached the error")
	}
}

func TestMemoDropsInvalidatedEntries(t *testing.T) {
	s := &source{values: map[string]int{}}
	m := New(s.compute)
	for _, key := range []string{"a", "b", "c"} {
		get(t, m, key)
	}
	m.Invalidate("a", "b")
	m.Reset()
	if n := len(m.entries); n != 0 {
		t.Fatalf("%d entries left, want 0", n)
	}
}

// TestMemoConverges changes values and invalidates them from many goroutines, as events do, while others read; once
// the changes stop, every key reads its latest value.
func TestMemoConverges(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	s := &source{values: map[string]int{}}
	m := New(s.compute)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					for _, key := range keys {
						_, _ = m.Get(ctx, key)
					}
				}
			}
		})
	}
	var writers sync.WaitGroup
	for w := range 4 {
		writers.Go(func() {
			for i := range 2000 {
				key := keys[(w+i)%len(keys)]
				s.set(key, w*10000+i)
				m.Invalidate(key)
			}
		})
	}
	writers.Wait()
	close(stop)
	wg.Wait()
	for _, key := range keys {
		if got, want := get(t, m, key), s.values[key]; got != want {
			t.Fatalf("%s = %d, want the latest value %d", key, got, want)
		}
	}
}
