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

package aggregate

import (
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// The contention study runs only with AGGREGATE_CONTENTION=1.

type variant interface {
	apply(obj *v1.NodeClaim)
	get(key string) (corev1.ResourceList, int)
}

type usage = Aggregate[*v1.NodeClaim, corev1.ResourceList]

type baseline struct{ *usage }

func (b baseline) apply(obj *v1.NodeClaim)                   { b.usage.apply(obj, false, false) }
func (b baseline) get(key string) (corev1.ResourceList, int) { return b.Get(key) }

// combining stages writes and has whichever writer holds the lock apply them. Each lock holder applies one batch, so
// a reader waiting on the lock gets it between batches. A writer may return before its write is applied, so a read
// applies the staged writes first.
type combining struct {
	*usage
	stageMu sync.Mutex
	stage   []*v1.NodeClaim
}

func (c *combining) drainOnce() {
	c.stageMu.Lock()
	batch := c.stage
	c.stage = nil
	c.stageMu.Unlock()
	for _, obj := range batch {
		c.applyLocked(obj, false, false)
	}
}

func (c *combining) apply(obj *v1.NodeClaim) {
	c.stageMu.Lock()
	c.stage = append(c.stage, obj)
	c.stageMu.Unlock()
	if c.mu.TryLock() {
		c.drainOnce()
		c.mu.Unlock()
	}
}

func (c *combining) get(key string) (corev1.ResourceList, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drainOnce()
	g, ok := c.groups[key]
	if !ok {
		return nil, 0
	}
	return g.value.Result(), g.count
}

type noop struct{}

func (noop) apply(*v1.NodeClaim)                   {}
func (noop) get(string) (corev1.ResourceList, int) { return nil, 0 }

type sharded struct {
	shards []*usage
	seed   maphash.Seed
}

func (s sharded) apply(obj *v1.NodeClaim) {
	s.shards[maphash.String(s.seed, string(obj.UID))%uint64(len(s.shards))].apply(obj, false, false)
}

func (s sharded) get(key string) (corev1.ResourceList, int) {
	sum := corev1.ResourceList{}
	total := 0
	for _, shard := range s.shards {
		v, n := shard.Get(key)
		resources.MergeInto(sum, v)
		total += n
	}
	return sum, total
}

func newUsage() *usage {
	a := New(nil, nil, &v1.NodeClaimList{}, func(nc *v1.NodeClaim) string { return nc.Labels[v1.NodePoolLabelKey] },
		Resources(func(nc *v1.NodeClaim) corev1.ResourceList {
			return lo.Assign(nc.Status.Capacity, corev1.ResourceList{resources.Node: resource.MustParse("1")})
		}))
	a.synced = true
	return a
}

func newVariant(name string) variant {
	switch name {
	case "baseline":
		return baseline{newUsage()}
	case "combining":
		return &combining{usage: newUsage()}
	case "noop":
		return noop{}
	case "sharded":
		s := sharded{seed: maphash.MakeSeed()}
		for range 64 {
			s.shards = append(s.shards, newUsage())
		}
		return s
	}
	panic(name)
}

type fixture struct {
	v        variant
	rv       atomic.Uint64
	versions [][]*v1.NodeClaim
	keys     int
}

func newFixture(name string, objects, keys, goroutines int) *fixture {
	f := &fixture{v: newVariant(name), keys: keys}
	cpu := []string{"1", "2", "4", "8", "16"}
	base := make([]*v1.NodeClaim, objects)
	for i := range base {
		base[i] = &v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   fmt.Sprintf("nc-%d", i),
				UID:    types.UID(fmt.Sprintf("uid-%d", i)),
				Labels: map[string]string{v1.NodePoolLabelKey: fmt.Sprintf("np-%d", i%keys)},
			},
			Status: v1.NodeClaimStatus{Capacity: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse(cpu[i%len(cpu)]),
				corev1.ResourceMemory:           resource.MustParse(fmt.Sprintf("%dGi", 4*(i%8+1))),
				corev1.ResourcePods:             resource.MustParse("110"),
				corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
			}},
		}
		nc := base[i].DeepCopy()
		nc.ResourceVersion = strconv.FormatUint(f.rv.Add(1), 10)
		f.v.apply(nc)
	}
	// Each goroutine reuses its own copies, changing only the resourceVersion before each write; the aggregate keeps
	// no reference to the objects.
	f.versions = make([][]*v1.NodeClaim, goroutines)
	for g := range f.versions {
		r := rand.New(rand.NewPCG(uint64(g), 1)) //nolint:gosec
		for range 1024 {
			nc := base[r.IntN(objects)].DeepCopy()
			nc.Labels[v1.NodePoolLabelKey] = fmt.Sprintf("np-%d", r.IntN(keys))
			f.versions[g] = append(f.versions[g], nc)
		}
	}
	return f
}

func (f *fixture) write(g, i int) {
	nc := f.versions[g][i%len(f.versions[g])]
	nc.ResourceVersion = strconv.FormatUint(f.rv.Add(1), 10)
	f.v.apply(nc)
}

type latencies struct {
	mu sync.Mutex
	d  []time.Duration
}

func (l *latencies) add(d []time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.d = append(l.d, d...)
}

func (l *latencies) pct(p float64) time.Duration {
	if len(l.d) == 0 {
		return 0
	}
	slices.Sort(l.d)
	return l.d[min(len(l.d)-1, int(float64(len(l.d))*p))]
}

func (l *latencies) max() time.Duration {
	if len(l.d) == 0 {
		return 0
	}
	return slices.Max(l.d)
}

func fmtD(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1e3)
	default:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
}

// openLoop schedules writes at rate across goroutines and measures each write from its scheduled start, so a write
// that waited behind others counts its wait. Readers read at readRate.
//
//nolint:gocyclo
func openLoop(f *fixture, goroutines, rate, readers, readRate int, d time.Duration) (writes, service, reads *latencies, achieved float64) {
	writes, service, reads = &latencies{}, &latencies{}, &latencies{}
	var wg sync.WaitGroup
	var done atomic.Int64
	start := time.Now().Add(10 * time.Millisecond)
	end := start.Add(d)
	perGoroutine := time.Duration(float64(time.Second) * float64(goroutines) / float64(rate))
	for g := range goroutines {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(g), 3)) //nolint:gosec
			var lat, svc []time.Duration
			next := start.Add(time.Duration(r.Int64N(int64(perGoroutine))))
			for i := 0; next.Before(end); i++ {
				if wait := time.Until(next); wait > 0 {
					time.Sleep(wait)
				}
				called := time.Now()
				f.write(g, i)
				now := time.Now()
				lat = append(lat, now.Sub(next))
				svc = append(svc, now.Sub(called))
				done.Add(1)
				next = next.Add(time.Duration(r.ExpFloat64() * float64(perGoroutine)))
			}
			writes.add(lat)
			service.add(svc)
		})
	}
	readInterval := time.Duration(float64(time.Second) * float64(readers) / float64(max(readRate, 1)))
	for g := range readers {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(1000+g), 4)) //nolint:gosec
			var lat []time.Duration
			next := start
			for next.Before(end) {
				if wait := time.Until(next); wait > 0 {
					time.Sleep(wait)
				}
				f.v.get(fmt.Sprintf("np-%d", r.IntN(f.keys)))
				lat = append(lat, time.Since(next))
				next = next.Add(readInterval)
			}
			reads.add(lat)
		})
	}
	wg.Wait()
	return writes, service, reads, float64(done.Load()) / d.Seconds()
}

// closedLoop runs writers back to back and readers back to back, which measures the ceiling.
func closedLoop(f *fixture, writers, readers int, d time.Duration) (float64, float64, *latencies, *latencies) {
	var stop atomic.Bool
	var wg sync.WaitGroup
	var w, r atomic.Int64
	wl, rl := &latencies{}, &latencies{}
	for g := range writers {
		wg.Go(func() {
			var lat []time.Duration
			for i := 0; !stop.Load(); i++ {
				if i%32 == 0 {
					s := time.Now()
					f.write(g, i)
					lat = append(lat, time.Since(s))
				} else {
					f.write(g, i)
				}
				w.Add(1)
			}
			wl.add(lat)
		})
	}
	for g := range readers {
		wg.Go(func() {
			rr := rand.New(rand.NewPCG(uint64(2000+g), 5)) //nolint:gosec
			var lat []time.Duration
			for i := 0; !stop.Load(); i++ {
				key := fmt.Sprintf("np-%d", rr.IntN(f.keys))
				if i%32 == 0 {
					s := time.Now()
					f.v.get(key)
					lat = append(lat, time.Since(s))
				} else {
					f.v.get(key)
				}
				r.Add(1)
			}
			rl.add(lat)
		})
	}
	time.Sleep(d)
	stop.Store(true)
	wg.Wait()
	return float64(w.Load()) / d.Seconds(), float64(r.Load()) / d.Seconds(), wl, rl
}

// burst releases n goroutines at once, each doing one write, like a static NodePool creating n NodeClaims, and
// measures how long the last one waited.
func burst(f *fixture, n int) *latencies {
	l := &latencies{}
	var ready, wg sync.WaitGroup
	gate := make(chan struct{})
	ready.Add(n)
	for g := range n {
		wg.Go(func() {
			ready.Done()
			<-gate
			s := time.Now()
			f.write(g%len(f.versions), g)
			l.add([]time.Duration{time.Since(s)})
		})
	}
	ready.Wait()
	close(gate)
	wg.Wait()
	return l
}

func env(name string, def []string) []string {
	if v := os.Getenv(name); v != "" {
		return strings.Split(v, ",")
	}
	return def
}

func atoi(s string) int { return lo.Must(strconv.Atoi(s)) }

//nolint:gocyclo
func TestContention(t *testing.T) {
	if os.Getenv("AGGREGATE_CONTENTION") == "" {
		t.Skip("set AGGREGATE_CONTENTION=1 to run the contention study")
	}
	variants := env("AGGREGATE_VARIANTS", []string{"noop", "baseline", "combining", "sharded"})
	procs := lo.Map(env("AGGREGATE_PROCS", []string{"12"}), func(s string, _ int) int { return atoi(s) })
	d := lo.Must(time.ParseDuration(env("AGGREGATE_DURATION", []string{"3s"})[0]))
	modes := env("AGGREGATE_MODES", []string{"closed", "open", "burst"})
	objects, keys := atoi(env("AGGREGATE_OBJECTS", []string{"20000"})[0]), atoi(env("AGGREGATE_KEYS", []string{"10"})[0])
	for _, p := range procs {
		runtime.GOMAXPROCS(p)
		if slices.Contains(modes, "closed") {
			fmt.Printf("\n## closed loop, GOMAXPROCS=%d, %d objects, %d keys, 4 readers\n", p, objects, keys)
			fmt.Printf("%-10s %7s | %10s %9s %9s %9s | %10s %9s %9s\n", "variant", "writers", "writes/s", "w p50", "w p99", "w p999", "reads/s", "r p50", "r p99")
			for _, writers := range []int{1, 4, 16, 64, 256, 1024} {
				for _, name := range variants {
					runtime.GC()
					f := newFixture(name, objects, keys, writers)
					w, r, wl, rl := closedLoop(f, writers, 4, d)
					fmt.Printf("%-10s %7d | %10.0f %9s %9s %9s | %10.0f %9s %9s\n", name, writers, w,
						fmtD(wl.pct(.5)), fmtD(wl.pct(.99)), fmtD(wl.pct(.999)), r, fmtD(rl.pct(.5)), fmtD(rl.pct(.99)))
				}
			}
		}
		if slices.Contains(modes, "open") {
			fmt.Printf("\n## open loop, GOMAXPROCS=%d, %d objects, %d keys, 256 writer goroutines, 16 readers at 2000 reads/s\n", p, objects, keys)
			fmt.Printf("%-10s %9s | %10s %9s %9s %9s %9s | %9s %9s %9s %9s | %9s %9s %9s\n", "variant", "target/s", "achieved/s", "w p50", "w p99", "w p999", "w max", "svc p50", "svc p99", "svc p999", "svc max", "r p50", "r p99", "r max")
			for _, rate := range []int{1000, 10000, 50000, 100000, 200000, 400000} {
				for _, name := range variants {
					runtime.GC()
					f := newFixture(name, objects, keys, 256)
					wl, sl, rl, achieved := openLoop(f, 256, rate, 16, 2000, d)
					fmt.Printf("%-10s %9d | %10.0f %9s %9s %9s %9s | %9s %9s %9s %9s | %9s %9s %9s\n", name, rate, achieved,
						fmtD(wl.pct(.5)), fmtD(wl.pct(.99)), fmtD(wl.pct(.999)), fmtD(wl.max()),
						fmtD(sl.pct(.5)), fmtD(sl.pct(.99)), fmtD(sl.pct(.999)), fmtD(sl.max()),
						fmtD(rl.pct(.5)), fmtD(rl.pct(.99)), fmtD(rl.max()))
				}
			}
		}
		if slices.Contains(modes, "burst") {
			fmt.Printf("\n## burst, GOMAXPROCS=%d, %d objects, %d keys: n goroutines released at once, one write each\n", p, objects, keys)
			fmt.Printf("%-10s %6s | %9s %9s %9s\n", "variant", "n", "p50", "p99", "max")
			for _, n := range []int{100, 1000, 5000} {
				for _, name := range variants {
					runtime.GC()
					f := newFixture(name, objects, keys, 64)
					all := &latencies{}
					for range 20 {
						all.add(burst(f, n).d)
					}
					fmt.Printf("%-10s %6d | %9s %9s %9s\n", name, n, fmtD(all.pct(.5)), fmtD(all.pct(.99)), fmtD(all.max()))
				}
			}
		}
	}
}
