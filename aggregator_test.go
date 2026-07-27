package quelon

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// incr is a counter-increment payload: it carries its own Key (identity) so the
// downstream ProcessFunc knows which counter a coalesced delta belongs to, since
// only the payload — not Task.Key — reaches ProcessFunc.
type incr struct {
	Key   string `json:"key"`
	Delta int64  `json:"delta"`
}

// incrMerge sums deltas, keeping the identity from the seeded (first) payload.
func incrMerge(acc, in any) any {
	a := acc.(incr)
	a.Delta += in.(incr).Delta
	return a
}

func incrTask(key string, delta int64) Task {
	return Task{Key: key, Payload: incr{Key: key, Delta: delta}}
}

func drain(p *Pool) {
	go func() {
		for range p.Results() {
		}
	}()
}

// mustEventually fails the test if cond is not true within timeout. It builds on
// the shared waitFor helper (pool_test.go).
func mustEventually(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	if !waitFor(t, timeout, cond) {
		t.Fatalf("condition not met within %s: %s", timeout, msg)
	}
}

// TestAggregatorCoalescesByKey checks that many same-key increments collapse into
// far fewer processed tasks while preserving each key's exact total.
func TestAggregatorCoalescesByKey(t *testing.T) {
	var (
		mu        sync.Mutex
		got       = map[string]int64{}
		processed int
	)
	pool := NewPool(func(_ context.Context, payload any) error {
		v := payload.(incr)
		mu.Lock()
		got[v.Key] += v.Delta
		processed++
		mu.Unlock()
		return nil
	}, WithAggregator(incrMerge, 20*time.Millisecond, 1024), WithWorkerCount(4))
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	drain(pool)

	const perKey = 1000
	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		for range perKey {
			if err := pool.Submit(t.Context(), incrTask(k, 1)); err != nil {
				t.Fatalf("submit: %v", err)
			}
		}
	}
	pool.CloseAndWait()

	for _, k := range keys {
		if got[k] != perKey {
			t.Errorf("key %s: sum = %d, want %d", k, got[k], perKey)
		}
	}
	submits := len(keys) * perKey
	if processed >= submits {
		t.Errorf("no coalescing: %d submits produced %d processed tasks", submits, processed)
	}
	t.Logf("coalesced %d submits into %d processed tasks (%.1fx)", submits, processed, float64(submits)/float64(processed))
}

// TestAggregatorConcurrentSubmitPreservesTotal stresses concurrent folds on a
// small key set; the grand total must be exact (run with -race).
func TestAggregatorConcurrentSubmitPreservesTotal(t *testing.T) {
	var total atomic.Int64
	pool := NewPool(func(_ context.Context, payload any) error {
		total.Add(payload.(incr).Delta)
		return nil
	}, WithAggregator(incrMerge, 10*time.Millisecond, 512), WithWorkerCount(8))
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	drain(pool)

	const goroutines, each = 16, 500
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			key := fmt.Sprintf("k%d", g%4)
			for range each {
				if err := pool.Submit(t.Context(), incrTask(key, 1)); err != nil {
					t.Errorf("submit: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
	pool.CloseAndWait()

	if got, want := total.Load(), int64(goroutines*each); got != want {
		t.Errorf("total = %d, want %d", got, want)
	}
}

// TestAggregatorFlushesOnInterval checks the time-based flush fires without any
// explicit shutdown, and that folds within the window are combined.
func TestAggregatorFlushesOnInterval(t *testing.T) {
	got := make(chan incr, 1)
	pool := NewPool(func(_ context.Context, payload any) error {
		select {
		case got <- payload.(incr):
		default:
		}
		return nil
	}, WithAggregator(incrMerge, 15*time.Millisecond, 1024))
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	drain(pool)
	defer pool.CloseAndWait()

	if err := pool.Submit(t.Context(), incrTask("x", 5)); err != nil {
		t.Fatal(err)
	}
	if err := pool.Submit(t.Context(), incrTask("x", 7)); err != nil {
		t.Fatal(err)
	}

	select {
	case v := <-got:
		if v.Delta != 12 {
			t.Errorf("delta = %d, want 12 (5+7 coalesced)", v.Delta)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aggregator did not flush on its interval")
	}
}

// TestAggregatorCapTriggersFlush uses a very long interval so the only thing that
// can cause a flush is the maxKeys size cap.
func TestAggregatorCapTriggersFlush(t *testing.T) {
	var processed atomic.Int64
	pool := NewPool(func(_ context.Context, _ any) error {
		processed.Add(1)
		return nil
	}, WithAggregator(incrMerge, time.Hour, 50), WithWorkerCount(4))
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	drain(pool)
	defer pool.CloseAndWait()

	for i := range 200 {
		if err := pool.Submit(t.Context(), incrTask(fmt.Sprintf("k%d", i), 1)); err != nil {
			t.Fatal(err)
		}
	}
	// With a 1h interval, only the cap can have flushed anything.
	mustEventually(t, 2*time.Second, "cap did not trigger a flush", func() bool {
		return processed.Load() >= 50
	})
}

// TestAggregatorFlushesOnClose is the no-loss guarantee: with the interval and
// cap effectively disabled, nothing flushes until CloseAndWait drains it.
func TestAggregatorFlushesOnClose(t *testing.T) {
	var total atomic.Int64
	pool := NewPool(func(_ context.Context, payload any) error {
		total.Add(payload.(incr).Delta)
		return nil
	}, WithAggregator(incrMerge, time.Hour, 0))
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	drain(pool)

	for range 100 {
		if err := pool.Submit(t.Context(), incrTask("z", 1)); err != nil {
			t.Fatal(err)
		}
	}
	pool.CloseAndWait()

	if got := total.Load(); got != 100 {
		t.Errorf("close drain lost data: total = %d, want 100", got)
	}
}

// TestAggregatorWithPartitions checks aggregation composes with ordered lanes:
// each key still totals correctly when routed through partitions.
func TestAggregatorWithPartitions(t *testing.T) {
	var (
		mu  sync.Mutex
		got = map[string]int64{}
	)
	pool := NewPool(func(_ context.Context, payload any) error {
		v := payload.(incr)
		mu.Lock()
		got[v.Key] += v.Delta
		mu.Unlock()
		return nil
	}, WithAggregator(incrMerge, 10*time.Millisecond, 1024), WithPartitions(8))
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	drain(pool)

	keys := []string{"a", "b", "c", "d", "e"}
	const perKey = 200
	for _, k := range keys {
		for range perKey {
			if err := pool.Submit(t.Context(), incrTask(k, 1)); err != nil {
				t.Fatal(err)
			}
		}
	}
	pool.CloseAndWait()

	for _, k := range keys {
		if got[k] != perKey {
			t.Errorf("key %s: sum = %d, want %d", k, got[k], perKey)
		}
	}
}

// memStore is a minimal in-memory Store with pending-task replay, for the
// durability tests.
type memStore struct {
	mu      sync.Mutex
	pending map[string]RawTask
}

func newMemStore() *memStore { return &memStore{pending: map[string]RawTask{}} }

func (s *memStore) SaveTask(t RawTask) error {
	s.mu.Lock()
	s.pending[t.ID] = t
	s.mu.Unlock()
	return nil
}

func (s *memStore) DeleteTask(id string) error {
	s.mu.Lock()
	delete(s.pending, id)
	s.mu.Unlock()
	return nil
}

func (s *memStore) LoadPendingTasks() ([]RawTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RawTask, 0, len(s.pending))
	for _, t := range s.pending {
		out = append(out, t)
	}
	return out, nil
}

func (s *memStore) SaveDeadLetter(RawDeadLetter) error        { return nil }
func (s *memStore) LoadDeadLetters() ([]RawDeadLetter, error) { return nil, nil }
func (s *memStore) DeleteDeadLetter(string) error             { return nil }

func (s *memStore) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// sumDeltas decodes every pending record's payload and sums its delta.
func (s *memStore) sumDeltas(t *testing.T) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sum int64
	for _, raw := range s.pending {
		var v incr
		if err := json.Unmarshal(raw.Payload, &v); err != nil {
			t.Fatalf("decode pending %s: %v", raw.ID, err)
		}
		sum += v.Delta
	}
	return sum
}

// incrCodec round-trips an incr payload so replayed pending records decode back
// into a concrete incr (the default codec would yield a map).
func incrCodec() Option {
	return WithCodec(
		func(v any) ([]byte, error) { return json.Marshal(v) },
		func(b []byte) (any, error) {
			var v incr
			err := json.Unmarshal(b, &v)
			return v, err
		},
	)
}

// TestAggregatorPersistAllCleansUp checks that with PersistAll the coalesced
// tasks are write-ahead-logged and then deleted once processed, leaving no
// orphaned pending records after a graceful shutdown.
func TestAggregatorPersistAllCleansUp(t *testing.T) {
	store := newMemStore()
	var total atomic.Int64
	pool := NewPool(func(_ context.Context, payload any) error {
		total.Add(payload.(incr).Delta)
		return nil
	}, WithAggregator(incrMerge, 10*time.Millisecond, 1024), WithStore(store), incrCodec(),
		WithWorkerCount(2), WithRetryPolicy(fastPolicy(1)))
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	drain(pool)

	for range 500 {
		if err := pool.Submit(t.Context(), incrTask("p", 1)); err != nil {
			t.Fatal(err)
		}
	}
	pool.CloseAndWait()

	if got := total.Load(); got != 500 {
		t.Errorf("total = %d, want 500", got)
	}
	if n := store.len(); n != 0 {
		t.Errorf("store left %d pending records after clean shutdown; want 0", n)
	}
}

// TestAggregatorPersistReplaysCoalescedValue proves durability of coalesced
// work: a crash (Start ctx cancelled) before the flushed tasks are processed
// leaves them pending, and a fresh pool over the same store replays them with
// their accumulated deltas intact.
func TestAggregatorPersistReplaysCoalescedValue(t *testing.T) {
	store := newMemStore()

	// Phase 1: a single worker blocks on the first task while the rest of the
	// flushed, persisted tasks pile up (saved but unprocessed) in the buffer.
	// Many distinct keys ensure the flush produces many such tasks; cancelling
	// Start's ctx then simulates a crash before they are processed.
	ctx1, cancel1 := context.WithCancel(context.Background())
	block := make(chan struct{})
	poolA := NewPool(func(context.Context, any) error {
		<-block
		return nil
	}, WithAggregator(incrMerge, 10*time.Millisecond, 4096), WithStore(store), incrCodec(),
		WithBufferSize(1024), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)))
	if err := poolA.Start(ctx1); err != nil {
		t.Fatal(err)
	}
	drain(poolA)

	const keys = 200
	for i := range keys {
		if err := poolA.Submit(ctx1, incrTask(fmt.Sprintf("p%d", i), 1)); err != nil {
			t.Fatal(err)
		}
	}
	// Wait until the flush has persisted a solid batch of pending records.
	mustEventually(t, 2*time.Second, "pending records were not persisted", func() bool {
		return store.len() >= keys/2
	})
	cancel1()    // crash: stop workers without draining
	close(block) // release the blocked worker so CloseAndWait can return
	poolA.CloseAndWait()

	persisted := store.sumDeltas(t)
	if persisted == 0 {
		t.Fatal("nothing persisted before the simulated crash")
	}

	// Phase 2: a fresh pool over the same store replays the coalesced records.
	var total atomic.Int64
	poolB := NewPool(func(_ context.Context, payload any) error {
		total.Add(payload.(incr).Delta)
		return nil
	}, WithStore(store), incrCodec(), WithWorkerCount(2), WithRetryPolicy(fastPolicy(1)))
	if err := poolB.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	drain(poolB)
	mustEventually(t, 2*time.Second, "replayed tasks did not all process", func() bool {
		return total.Load() == persisted
	})
	poolB.CloseAndWait()

	if got := total.Load(); got != persisted {
		t.Errorf("replayed total = %d, want %d", got, persisted)
	}
	if n := store.len(); n != 0 {
		t.Errorf("store still has %d pending after replay+process; want 0", n)
	}
}

// BenchmarkAggregatorFold measures the per-Submit cost of folding into a hot
// key (interval and cap disabled, so every Submit is a pure in-memory fold).
func BenchmarkAggregatorFold(b *testing.B) {
	pool := NewPool(func(context.Context, any) error { return nil },
		WithAggregator(incrMerge, time.Hour, 0), WithWorkerCount(1))
	if err := pool.Start(b.Context()); err != nil {
		b.Fatal(err)
	}
	drain(pool)

	ctx := b.Context()
	task := incrTask("hot", 1)
	b.ReportAllocs()
	for b.Loop() {
		_ = pool.Submit(ctx, task)
	}
	b.StopTimer()
	pool.CloseAndWait()
}

// BenchmarkAggregatorSubmitParallel measures concurrent Submit throughput
// against a small, contended key set — the counter hot-path shape.
func BenchmarkAggregatorSubmitParallel(b *testing.B) {
	pool := NewPool(func(context.Context, any) error { return nil },
		WithAggregator(incrMerge, 50*time.Millisecond, 100_000), WithWorkerCount(4))
	if err := pool.Start(b.Context()); err != nil {
		b.Fatal(err)
	}
	drain(pool)

	ctx := b.Context()
	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"}
	tasks := make([]Task, len(keys))
	for i, k := range keys {
		tasks[i] = incrTask(k, 1)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = pool.Submit(ctx, tasks[i&7])
			i++
		}
	})
	b.StopTimer()
	pool.CloseAndWait()
}
