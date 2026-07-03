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

// keyedTasks builds n tasks that all share the same ordering key.
func keyedTasks(key string, n int) []Task {
	tasks := make([]Task, n)
	for i := range tasks {
		tasks[i] = Task{ID: fmt.Sprintf("%s-%d", key, i), Key: key, Payload: i}
	}
	return tasks
}

// --- ordering: same key is serialized ---

// Tasks sharing a key must never be in-process on two workers at once.
func TestPartitions_SameKeyProcessedSerially(t *testing.T) {
	const n = 40
	var inFlight, peak atomic.Int32

	pool := NewPool(func(_ context.Context, _ any) error {
		c := inFlight.Add(1)
		for {
			old := peak.Load()
			if c <= old || peak.CompareAndSwap(old, c) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
		return nil
	}, WithPartitions(8), WithBufferSize(n), WithRetryPolicy(fastPolicy(1)))

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	submitAndClose(t, pool, keyedTasks("acct", n))

	if got := peak.Load(); got != 1 {
		t.Errorf("peak concurrency for one key = %d, want 1", got)
	}
}

// Tasks sharing a key must be processed in submission (FIFO) order.
func TestPartitions_SameKeyProcessedInOrder(t *testing.T) {
	const n = 64
	var (
		mu    sync.Mutex
		order []int
	)

	pool := NewPool(func(_ context.Context, payload any) error {
		mu.Lock()
		order = append(order, payload.(int))
		mu.Unlock()
		return nil
	}, WithPartitions(4), WithBufferSize(n), WithRetryPolicy(fastPolicy(1)))

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	submitAndClose(t, pool, keyedTasks("acct", n))

	if len(order) != n {
		t.Fatalf("processed %d tasks, want %d", len(order), n)
	}
	for i, v := range order {
		if v != i {
			t.Fatalf("order[%d] = %d, want %d (per-key FIFO violated): %v", i, v, i, order)
		}
	}
}

// --- parallelism: distinct keys on distinct shards run concurrently ---

func TestPartitions_DifferentKeysRunInParallel(t *testing.T) {
	pool := NewPool(nil, WithPartitions(8), WithBufferSize(8))

	// Two keys that provably land on different shards, so they can only both be
	// in-process at once if the partitions run in parallel.
	ka := "k-0"
	var kb string
	for i := 1; ; i++ {
		k := fmt.Sprintf("k-%d", i)
		if pool.partitionFor(k) != pool.partitionFor(ka) {
			kb = k
			break
		}
	}

	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	pool.process = func(_ context.Context, _ any) error {
		arrived <- struct{}{}
		<-release
		return nil
	}

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	var results []Result
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); results = collectResults(pool) }()

	ctx := t.Context()
	if err := pool.Submit(ctx, Task{ID: "a", Key: ka, Payload: 1}); err != nil {
		t.Fatal(err)
	}
	if err := pool.Submit(ctx, Task{ID: "b", Key: kb, Payload: 2}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d/2 keys ran concurrently; partitions are not parallel", i)
		}
	}
	close(release)
	pool.CloseAndWait()
	wg.Wait()

	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}
}

// --- backward compatibility: unkeyed tasks still all get processed ---

func TestPartitions_UnkeyedTasksAllProcessed(t *testing.T) {
	const n = 50
	var count atomic.Int64

	pool := NewPool(func(_ context.Context, _ any) error {
		count.Add(1)
		return nil
	}, WithPartitions(4), WithBufferSize(n), WithRetryPolicy(fastPolicy(1)))

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	results := submitAndClose(t, pool, makeTasks(n)) // makeTasks leaves Key empty

	if int(count.Load()) != n {
		t.Errorf("processed %d unkeyed tasks, want %d", count.Load(), n)
	}
	if len(results) != n {
		t.Errorf("got %d results, want %d", len(results), n)
	}
}

// --- structure: one lane (worker) per partition ---

func TestPartitions_OneWorkerPerPartition(t *testing.T) {
	const parts = 6
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithPartitions(parts), WithBufferSize(parts))

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer pool.CloseAndWait()

	if got := pool.ActiveWorkers(); got != parts {
		t.Errorf("ActiveWorkers() = %d, want %d (one per partition)", got, parts)
	}
}

// --- persistence: a reloaded task keeps its Key and routes to the same lane ---

// Pending tasks reloaded on Start must honor their persisted Key, so same-key
// tasks that survived a crash still land on one serial lane rather than being
// spread across shards (which would let them run concurrently and out of order).
func TestPartitions_ReloadedKeyedTasksStaySerial(t *testing.T) {
	const n = 24

	pending := make([]RawTask, n)
	for i := range pending {
		payload, err := json.Marshal(i)
		if err != nil {
			t.Fatal(err)
		}
		pending[i] = RawTask{ID: fmt.Sprintf("acct-%d", i), Key: "acct", Payload: payload}
	}
	store := &preloadedStore{pending: pending}

	var (
		inFlight, peak atomic.Int32
		mu             sync.Mutex
		order          []int
	)
	pool := NewPool(func(_ context.Context, payload any) error {
		c := inFlight.Add(1)
		for {
			old := peak.Load()
			if c <= old || peak.CompareAndSwap(old, c) {
				break
			}
		}
		mu.Lock()
		order = append(order, int(payload.(float64))) // JSON numbers decode to float64
		mu.Unlock()
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
		return nil
	}, WithPartitions(8), WithBufferSize(n), WithStore(store), WithRetryPolicy(fastPolicy(1)))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); collectResults(pool) }()

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	pool.CloseAndWait()
	wg.Wait()

	if got := peak.Load(); got != 1 {
		t.Errorf("peak concurrency for reloaded key = %d, want 1 (Key not honored on reload?)", got)
	}
	if len(order) != n {
		t.Fatalf("processed %d reloaded tasks, want %d", len(order), n)
	}
	for i, v := range order {
		if v != i {
			t.Fatalf("reloaded order[%d] = %d, want %d (per-key FIFO violated on reload)", i, v, i)
		}
	}
}

// --- mutual exclusion with dynamic scaling ---

func TestPartitions_DisablesDynamicWorkers(t *testing.T) {
	h, logger := newTestLogger()
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithPartitions(4),
		WithDynamicWorkers(2, 16, 0.5),
		WithLogger(logger),
	)

	if pool.dynamic {
		t.Error("dynamic scaling should be disabled when partitioning is enabled")
	}
	if !h.hasMessage("partitioning disables dynamic worker scaling") {
		t.Errorf("expected a warning about disabled dynamic scaling; got %v", h.messages())
	}
}
