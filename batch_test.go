package quelon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- happy path ---

func TestBatchAllTasksProcessed(t *testing.T) {
	const n = 50
	var (
		mu        sync.Mutex
		seen      = map[string]bool{}
		batchSeen []int
	)

	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		mu.Lock()
		batchSeen = append(batchSeen, len(tasks))
		for _, task := range tasks {
			seen[task.ID] = true
		}
		mu.Unlock()
		return make([]error, len(tasks))
	},
		WithBufferSize(n),
		WithMaxBatchSize(8),
		WithMaxBatchWait(20*time.Millisecond),
		WithWorkerCount(4),
		WithRetryPolicy(fastPolicy(1)),
	)

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	results := submitAndClose(t, pool, makeTasks(n))

	if len(results) != n {
		t.Errorf("got %d results, want %d", len(results), n)
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("task %s: unexpected error: %v", r.Task.ID, r.Err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Errorf("processed %d distinct tasks, want %d", len(seen), n)
	}
	for _, size := range batchSeen {
		if size > 8 {
			t.Errorf("batch of size %d exceeded maxSize 8", size)
		}
	}
}

func TestBatchRespectsMaxSize(t *testing.T) {
	const n = 30
	var maxObserved atomic.Int64

	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		for {
			cur := maxObserved.Load()
			if int64(len(tasks)) <= cur || maxObserved.CompareAndSwap(cur, int64(len(tasks))) {
				break
			}
		}
		return make([]error, len(tasks))
	},
		WithBufferSize(n),
		WithMaxBatchSize(5),
		WithMaxBatchWait(50*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
	)
	// Single worker so the buffer is full when the worker grabs its batch.
	pool.Start(t.Context())

	// Pre-fill the buffer before the worker drains it.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = collectResults(pool)
	}()
	for _, task := range makeTasks(n) {
		if err := pool.Submit(t.Context(), task); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	pool.CloseAndWait()
	wg.Wait()

	if maxObserved.Load() > 5 {
		t.Errorf("observed batch of %d, want <= 5", maxObserved.Load())
	}
	if maxObserved.Load() < 2 {
		t.Errorf("never batched more than %d tasks; batching not engaging", maxObserved.Load())
	}
}

// --- time-based flush ---

func TestBatchFlushesOnMaxWait(t *testing.T) {
	got := make(chan int, 4)

	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		got <- len(tasks)
		return make([]error, len(tasks))
	},
		WithBufferSize(16),
		WithMaxBatchSize(100),
		WithMaxBatchWait(30*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())
	go func() {
		for range pool.Results() {
		}
	}()

	// Submit fewer tasks than maxSize; the batch must flush on the timer.
	for i := range 3 {
		if err := pool.Submit(t.Context(), Task{ID: fmt.Sprintf("t-%d", i), Payload: i}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	select {
	case size := <-got:
		if size != 3 {
			t.Errorf("flushed batch of %d, want 3", size)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch never flushed on maxWait")
	}

	pool.CloseAndWait()
}

// --- partial failure & per-item retry ---

func TestBatchPartialFailureDeadLettersOnlyFailures(t *testing.T) {
	const n = 10
	var (
		mu          sync.Mutex
		deadLetters []DeadLetter
	)

	// Odd payloads always fail; evens succeed.
	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		errs := make([]error, len(tasks))
		for i, task := range tasks {
			if task.Payload.(int)%2 != 0 {
				errs[i] = errors.New("odd fails")
			}
		}
		return errs
	},
		WithBufferSize(n),
		WithMaxBatchSize(n),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(2),
		WithRetryPolicy(fastPolicy(1)),
		WithHooks(Hooks{
			OnDeadLetter: func(dl DeadLetter) {
				mu.Lock()
				deadLetters = append(deadLetters, dl)
				mu.Unlock()
			},
		}),
	)
	pool.Start(t.Context())

	results := submitAndClose(t, pool, makeTasks(n))

	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
			if r.Task.Payload.(int)%2 == 0 {
				t.Errorf("even task %s failed: %v", r.Task.ID, r.Err)
			}
		}
	}
	if failed != 5 {
		t.Errorf("got %d failed results, want 5 (odds)", failed)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(deadLetters) != 5 {
		t.Errorf("got %d dead letters, want 5", len(deadLetters))
	}
}

func TestBatchRetriesOnlyFailingItems(t *testing.T) {
	// One task fails twice then succeeds; the rest always succeed. The retried
	// attempts must carry only the failing task, never the successful ones.
	var (
		mu       sync.Mutex
		attempts = map[string]int{}
		subSizes []int
	)

	process := func(_ context.Context, tasks []Task) []error {
		mu.Lock()
		subSizes = append(subSizes, len(tasks))
		errs := make([]error, len(tasks))
		for i, task := range tasks {
			attempts[task.ID]++
			if task.ID == "task-2" && attempts[task.ID] < 3 {
				errs[i] = errors.New("transient")
			}
		}
		mu.Unlock()
		return errs
	}

	pool := NewPoolWithBatch(process,
		WithBufferSize(5),
		WithMaxBatchSize(5),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}),
	)
	pool.Start(t.Context())

	results := submitAndClose(t, pool, makeTasks(5))

	for _, r := range results {
		if r.Err != nil {
			t.Errorf("task %s failed after retries: %v", r.Task.ID, r.Err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts["task-2"] != 3 {
		t.Errorf("task-2 ran %d times, want 3", attempts["task-2"])
	}
	for id, a := range attempts {
		if id != "task-2" && a != 1 {
			t.Errorf("%s ran %d times, want 1 (must not be re-run on retry)", id, a)
		}
	}
	// First call is the full batch (5); retries must be size 1.
	if len(subSizes) < 2 {
		t.Fatalf("expected at least 2 processor calls, got %d", len(subSizes))
	}
	if subSizes[0] != 5 {
		t.Errorf("first batch size = %d, want 5", subSizes[0])
	}
	for _, s := range subSizes[1:] {
		if s != 1 {
			t.Errorf("retry batch size = %d, want 1 (only the failing item)", s)
		}
	}
}

func TestBatchPermanentErrorSkipsRetries(t *testing.T) {
	var calls atomic.Int64

	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		calls.Add(1)
		errs := make([]error, len(tasks))
		for i := range tasks {
			errs[i] = NewPermanentError(errors.New("fatal"))
		}
		return errs
	},
		WithBufferSize(3),
		WithMaxBatchSize(3),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 5, BaseDelay: 0, MaxDelay: 0}),
	)
	pool.Start(t.Context())

	results := submitAndClose(t, pool, makeTasks(3))

	for _, r := range results {
		if r.Err == nil {
			t.Errorf("task %s: expected permanent error", r.Task.ID)
		}
	}
	if calls.Load() != 1 {
		t.Errorf("processor called %d times, want 1 (permanent must not retry)", calls.Load())
	}
}

func TestBatchExhaustedRetriesDeadLetters(t *testing.T) {
	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		errs := make([]error, len(tasks))
		for i := range tasks {
			errs[i] = errors.New("always fails")
		}
		return errs
	},
		WithBufferSize(3),
		WithMaxBatchSize(3),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}),
	)
	pool.Start(t.Context())

	results := submitAndClose(t, pool, makeTasks(3))

	for _, r := range results {
		if r.Err == nil {
			t.Errorf("task %s: expected error after exhausted retries", r.Task.ID)
		}
	}
}

// --- panic recovery ---

func TestBatchPanicRecovered(t *testing.T) {
	pool := NewPoolWithBatch(func(_ context.Context, _ []Task) []error {
		panic("boom")
	},
		WithBufferSize(3),
		WithMaxBatchSize(3),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())

	results := submitAndClose(t, pool, makeTasks(3))
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	for _, r := range results {
		if r.Err == nil {
			t.Errorf("task %s: expected error from recovered panic", r.Task.ID)
		}
	}
}

// --- nil/short return treated as success ---

func TestBatchNilReturnAllSucceed(t *testing.T) {
	pool := NewPoolWithBatch(func(_ context.Context, _ []Task) []error {
		return nil // nil = every task succeeded
	},
		WithBufferSize(5),
		WithMaxBatchSize(5),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())

	results := submitAndClose(t, pool, makeTasks(5))
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("task %s: nil return should mean success, got %v", r.Task.ID, r.Err)
		}
	}
}

// --- clean drain on shutdown ---

func TestBatchCleanDrainOnClose(t *testing.T) {
	const n = 100
	var processed atomic.Int64

	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		processed.Add(int64(len(tasks)))
		return make([]error, len(tasks))
	},
		WithBufferSize(n),
		WithMaxBatchSize(16),
		WithMaxBatchWait(5*time.Millisecond),
		WithWorkerCount(4),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())

	results := submitAndClose(t, pool, makeTasks(n))

	if int(processed.Load()) != n {
		t.Errorf("processed %d tasks, want %d", processed.Load(), n)
	}
	if len(results) != n {
		t.Errorf("got %d results, want %d", len(results), n)
	}
}

// --- best-effort (maxWait <= 0) ---

func TestBatchBestEffortNoLinger(t *testing.T) {
	const n = 20
	var processed atomic.Int64

	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		processed.Add(int64(len(tasks)))
		return make([]error, len(tasks))
	},
		WithBufferSize(n),
		WithMaxBatchSize(8),
		WithMaxBatchWait(0),
		WithWorkerCount(2),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())

	results := submitAndClose(t, pool, makeTasks(n))
	if int(processed.Load()) != n {
		t.Errorf("processed %d tasks, want %d", processed.Load(), n)
	}
	if len(results) != n {
		t.Errorf("got %d results, want %d", len(results), n)
	}
}

// --- BatchStore is used when available ---

type recordingBatchStore struct {
	NoopStore
	mu          sync.Mutex
	deleteCalls int
	deletedIDs  int
	savedDLs    int
}

func (s *recordingBatchStore) DeleteTasks(ids []string) error {
	s.mu.Lock()
	s.deleteCalls++
	s.deletedIDs += len(ids)
	s.mu.Unlock()
	return nil
}

func (s *recordingBatchStore) SaveDeadLetters(dls []RawDeadLetter) error {
	s.mu.Lock()
	s.savedDLs += len(dls)
	s.mu.Unlock()
	return nil
}

// orderingStore records the sequence of settle operations so tests can assert
// that a dead letter is persisted before the task is removed from pending.
type orderingStore struct {
	NoopStore
	mu  sync.Mutex
	ops []string
}

func (s *orderingStore) record(op string) {
	s.mu.Lock()
	s.ops = append(s.ops, op)
	s.mu.Unlock()
}

func (s *orderingStore) DeleteTask(string) error               { s.record("delete"); return nil }
func (s *orderingStore) SaveDeadLetter(RawDeadLetter) error    { s.record("save"); return nil }
func (s *orderingStore) DeleteTasks([]string) error            { s.record("delete"); return nil }
func (s *orderingStore) SaveDeadLetters([]RawDeadLetter) error { s.record("save"); return nil }

func (s *orderingStore) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ops...)
}

func TestSingleDeadLetterSavedBeforeDelete(t *testing.T) {
	store := &orderingStore{}
	pool := NewPool(func(_ context.Context, _ any) error {
		return NewPermanentError(errors.New("fail"))
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)), WithStore(store))

	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if got := store.snapshot(); len(got) != 2 || got[0] != "save" || got[1] != "delete" {
		t.Errorf("want [save delete], got %v", got)
	}
}

func TestBatchDeadLettersSavedBeforeDelete(t *testing.T) {
	store := &orderingStore{}
	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		errs := make([]error, len(tasks))
		for i := range errs {
			errs[i] = NewPermanentError(errors.New("fail"))
		}
		return errs
	},
		WithBufferSize(4),
		WithMaxBatchSize(4),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
		WithStore(store),
	)

	pool.Start(t.Context())
	submitAndClose(t, pool, makeTasks(4))

	if got := store.snapshot(); len(got) != 2 || got[0] != "save" || got[1] != "delete" {
		t.Errorf("want [save delete], got %v", got)
	}
}

func TestBatchRetryResumesFromLoadedAttempt(t *testing.T) {
	var calls atomic.Int64
	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		calls.Add(int64(len(tasks)))
		errs := make([]error, len(tasks))
		for i := range errs {
			errs[i] = errors.New("always fails")
		}
		return errs
	},
		WithBufferSize(3),
		WithMaxBatchSize(3),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}),
	)

	pool.Start(t.Context())
	// Each task is reloaded with Attempt 2, so each has a single try left: three
	// tasks => three processor calls total, then all are dead-lettered.
	tasks := []Task{
		{ID: "a", Payload: 0, Attempt: 2},
		{ID: "b", Payload: 1, Attempt: 2},
		{ID: "c", Payload: 2, Attempt: 2},
	}
	results := submitAndClose(t, pool, tasks)

	if calls.Load() != 3 {
		t.Errorf("resumed batch made %d task-calls, want 3 (one each)", calls.Load())
	}
	for _, r := range results {
		if r.Err == nil {
			t.Errorf("task %s: expected dead-letter error after resumed budget", r.Task.ID)
		}
	}
}

func TestBatchUsesBatchStore(t *testing.T) {
	const n = 12
	store := &recordingBatchStore{}

	pool := NewPoolWithBatch(func(_ context.Context, tasks []Task) []error {
		errs := make([]error, len(tasks))
		for i, task := range tasks {
			if task.Payload.(int) < 3 {
				errs[i] = NewPermanentError(errors.New("fail"))
			}
		}
		return errs
	},
		WithBufferSize(n),
		WithMaxBatchSize(n),
		WithMaxBatchWait(10*time.Millisecond),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
		WithStore(store),
	)
	// One worker + a buffer pre-fill so the whole set lands in one batch.
	pool.Start(t.Context())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = collectResults(pool)
	}()
	for _, task := range makeTasks(n) {
		if err := pool.Submit(t.Context(), task); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	pool.CloseAndWait()
	wg.Wait()

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.deletedIDs != n {
		t.Errorf("deleted %d task ids, want %d", store.deletedIDs, n)
	}
	if store.savedDLs != 3 {
		t.Errorf("saved %d dead letters, want 3 (payloads 0,1,2)", store.savedDLs)
	}
}
