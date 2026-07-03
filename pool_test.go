package quelon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// collectingHandler captures slog records for test assertions.
type collectingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *collectingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *collectingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *collectingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *collectingHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *collectingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	msgs := make([]string, len(h.records))
	for i, r := range h.records {
		msgs[i] = r.Message
	}
	return msgs
}

func (h *collectingHandler) hasMessage(msg string) bool {
	for _, m := range h.messages() {
		if m == msg {
			return true
		}
	}
	return false
}

func (h *collectingHandler) attrFor(msg, key string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message != msg {
			continue
		}
		var found string
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				found = a.Value.String()
				return false
			}
			return true
		})
		if found != "" {
			return found, true
		}
	}
	return "", false
}

func newTestLogger() (*collectingHandler, *slog.Logger) {
	h := &collectingHandler{}
	return h, slog.New(h)
}

func fastPolicy(attempts int) RetryPolicy {
	return RetryPolicy{MaxAttempts: attempts, BaseDelay: 0, MaxDelay: 0}
}

func collectResults(p *Pool) []Result {
	var out []Result
	for r := range p.Results() {
		out = append(out, r)
	}
	return out
}

// submitAndClose feeds tasks then shuts the pool down; results are collected
// concurrently so the jobs channel never deadlocks.
func submitAndClose(t *testing.T, p *Pool, tasks []Task) []Result {
	t.Helper()
	var (
		wg      sync.WaitGroup
		results []Result
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		results = collectResults(p)
	}()

	for _, task := range tasks {
		if err := p.Submit(t.Context(), task); err != nil {
			t.Fatalf("Submit(%s): %v", task.ID, err)
		}
	}
	p.CloseAndWait()
	wg.Wait()
	return results
}

func makeTasks(n int) []Task {
	tasks := make([]Task, n)
	for i := range tasks {
		tasks[i] = Task{ID: fmt.Sprintf("task-%d", i), Payload: i}
	}
	return tasks
}

// --- happy path ---

func TestAllTasksProcessed(t *testing.T) {
	const n = 20
	var count atomic.Int64

	pool := NewPool(func(_ context.Context, _ any) error {
		count.Add(1)
		return nil
	}, WithBufferSize(n), WithWorkerCount(4), WithRetryPolicy(fastPolicy(1)))

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	results := submitAndClose(t, pool, makeTasks(n))

	if int(count.Load()) != n {
		t.Errorf("processed %d tasks, want %d", count.Load(), n)
	}
	if len(results) != n {
		t.Errorf("got %d results, want %d", len(results), n)
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("task %s: unexpected error: %v", r.Task.ID, r.Err)
		}
	}
}

func TestSingleWorker(t *testing.T) {
	const n = 10
	var mu sync.Mutex
	order := make([]int, 0, n)

	pool := NewPool(func(_ context.Context, p any) error {
		mu.Lock()
		order = append(order, p.(int))
		mu.Unlock()
		return nil
	}, WithBufferSize(n), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)))

	pool.Start(t.Context())
	submitAndClose(t, pool, makeTasks(n))

	if len(order) != n {
		t.Errorf("got %d processed, want %d", len(order), n)
	}
}

// --- retries ---

func TestRetryOnTransientError(t *testing.T) {
	var attempts atomic.Int64

	pool := NewPool(func(_ context.Context, _ any) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(t.Context())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "hello"}})

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("expected success after retries, got: %v", results[0].Err)
	}
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestZeroMaxAttemptsStillRunsTask(t *testing.T) {
	var attempts atomic.Int64

	// A policy with MaxAttempts: 0 must not skip the task and report a phantom
	// success; the pool clamps it to a single attempt.
	pool := NewPool(func(_ context.Context, _ any) error {
		attempts.Add(1)
		return nil
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(RetryPolicy{MaxAttempts: 0, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(t.Context())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if attempts.Load() != 1 {
		t.Errorf("expected task to run exactly once, got %d attempts", attempts.Load())
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Errorf("expected one successful result, got %+v", results)
	}
}

func TestExhaustedRetriesProducesError(t *testing.T) {
	pool := NewPool(func(_ context.Context, _ any) error {
		return errors.New("always fails")
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(t.Context())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if results[0].Err == nil {
		t.Error("expected error after exhausted retries, got nil")
	}
}

// attemptRecordingStore tracks the highest Attempt value ever persisted, so a
// test can assert the retry loop advances the durable attempt counter.
type attemptRecordingStore struct {
	NoopStore
	mu  sync.Mutex
	max int
}

func (s *attemptRecordingStore) SaveTask(t RawTask) error {
	s.mu.Lock()
	if t.Attempt > s.max {
		s.max = t.Attempt
	}
	s.mu.Unlock()
	return nil
}

func (s *attemptRecordingStore) maxAttempt() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

func TestRetryPersistsAdvancingAttempt(t *testing.T) {
	store := &attemptRecordingStore{}
	pool := NewPool(func(_ context.Context, _ any) error {
		return errors.New("always fails")
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}), WithStore(store))

	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	// Attempts 0,1,2 run; the durable counter is advanced to 2 before the final
	// try so a crash would resume there rather than at 0.
	if got := store.maxAttempt(); got != 2 {
		t.Errorf("max persisted attempt = %d, want 2", got)
	}
}

func TestRetryResumesFromLoadedAttempt(t *testing.T) {
	var calls atomic.Int64
	pool := NewPool(func(_ context.Context, _ any) error {
		calls.Add(1)
		return errors.New("always fails")
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(t.Context())
	// A task reloaded with Attempt 2 (it already used attempts 0 and 1) has a
	// single try left, not a fresh budget of 3.
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x", Attempt: 2}})

	if calls.Load() != 1 {
		t.Errorf("resumed task ran %d times, want 1 (remaining budget)", calls.Load())
	}
	if results[0].Err == nil {
		t.Error("expected dead-letter error after the resumed budget was exhausted")
	}
}

// --- permanent errors ---

func TestPermanentErrorSkipsRetries(t *testing.T) {
	var attempts atomic.Int64

	pool := NewPool(func(_ context.Context, _ any) error {
		attempts.Add(1)
		return NewPermanentError(errors.New("fatal"))
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(RetryPolicy{MaxAttempts: 5, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(t.Context())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if results[0].Err == nil {
		t.Error("expected error for permanent failure")
	}
	if attempts.Load() != 1 {
		t.Errorf("permanent error should not retry: got %d attempts", attempts.Load())
	}
}

func TestPermanentErrorMarkedInDeadLetter(t *testing.T) {
	var dl DeadLetter
	pool := NewPool(
		func(_ context.Context, _ any) error {
			return NewPermanentError(errors.New("fatal"))
		},
		WithBufferSize(1),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
		WithHooks(Hooks{
			OnDeadLetter: func(d DeadLetter) { dl = d },
		}),
	)

	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if !dl.Permanent {
		t.Error("expected DeadLetter.Permanent = true")
	}
}

// --- dead letters ---

func TestDeadLetterHookFired(t *testing.T) {
	var mu sync.Mutex
	var deadLetters []DeadLetter

	pool := NewPool(
		func(_ context.Context, p any) error {
			if p.(int)%2 == 0 {
				return errors.New("even always fails")
			}
			return nil
		},
		WithBufferSize(10),
		WithWorkerCount(4),
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
	submitAndClose(t, pool, makeTasks(10))

	mu.Lock()
	got := len(deadLetters)
	mu.Unlock()

	if got != 5 {
		t.Errorf("expected 5 dead letters (evens 0,2,4,6,8), got %d", got)
	}
}

func TestOnSuccessHookFired(t *testing.T) {
	var count atomic.Int64

	pool := NewPool(
		func(_ context.Context, _ any) error { return nil },
		WithBufferSize(5),
		WithWorkerCount(2),
		WithHooks(Hooks{
			OnSuccess: func(_ Task) { count.Add(1) },
		}),
	)

	pool.Start(t.Context())
	submitAndClose(t, pool, makeTasks(5))

	if count.Load() != 5 {
		t.Errorf("OnSuccess fired %d times, want 5", count.Load())
	}
}

func TestOnRetryHookFired(t *testing.T) {
	var retries atomic.Int64

	pool := NewPool(
		func(_ context.Context, _ any) error {
			if retries.Load() < 2 {
				return errors.New("not yet")
			}
			return nil
		},
		WithBufferSize(1),
		WithWorkerCount(1),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}),
		WithHooks(Hooks{
			OnRetry: func(_ Task, _ error, _ int) { retries.Add(1) },
		}),
	)

	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if retries.Load() != 2 {
		t.Errorf("expected 2 OnRetry calls, got %d", retries.Load())
	}
}

// --- context cancellation ---

func TestContextCancellationStopsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	var started atomic.Int64
	pool := NewPool(func(ctx context.Context, _ any) error {
		started.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}, WithBufferSize(100), WithWorkerCount(2), WithRetryPolicy(fastPolicy(1)))

	pool.Start(ctx)

	for _, task := range makeTasks(10) {
		pool.Submit(ctx, task)
	}

	deadline := time.Now().Add(2 * time.Second)
	for started.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	pool.CloseAndWait()
	// No assertion needed — test passes if it doesn't deadlock
}

// --- task timeout ---

func TestTaskTimeout(t *testing.T) {
	pool := NewPool(
		func(ctx context.Context, _ any) error {
			select {
			case <-time.After(10 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		WithBufferSize(1),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
		WithTaskTimeout(50*time.Millisecond),
	)

	pool.Start(t.Context())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "slow"}})

	if results[0].Err == nil {
		t.Error("expected timeout error, got nil")
	}
}

// --- panic recovery ---

func TestPanicInProcessFuncRecovered(t *testing.T) {
	pool := NewPool(func(_ context.Context, _ any) error {
		panic("unexpected condition")
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)))

	pool.Start(t.Context())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "boom"}})

	if results[0].Err == nil {
		t.Error("expected error from recovered panic, got nil")
	}
}

// --- concurrent stream ---

func TestConcurrentStreamSubmit(t *testing.T) {
	const n = 100
	var count atomic.Int64

	pool := NewPool(func(_ context.Context, _ any) error {
		count.Add(1)
		return nil
	}, WithBufferSize(n), WithWorkerCount(8), WithRetryPolicy(fastPolicy(1)))

	pool.Start(t.Context())

	var wg sync.WaitGroup
	results := make([]Result, 0, n)
	var mu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range pool.Results() {
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}
	}()

	var producers sync.WaitGroup
	for i := range 4 {
		producers.Add(1)
		go func(offset int) {
			defer producers.Done()
			for j := range n / 4 {
				id := offset*25 + j
				pool.Submit(t.Context(), Task{ID: fmt.Sprintf("task-%d", id), Payload: id})
			}
		}(i)
	}

	producers.Wait()
	pool.CloseAndWait()
	wg.Wait()

	if int(count.Load()) != n {
		t.Errorf("processed %d tasks, want %d", count.Load(), n)
	}
	if len(results) != n {
		t.Errorf("got %d results, want %d", len(results), n)
	}
}

// --- logger ---

func TestLoggerStartup(t *testing.T) {
	h, logger := newTestLogger()

	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithWorkerCount(2),
		WithLogger(logger),
		WithRetryPolicy(fastPolicy(1)),
	)
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	pool.CloseAndWait()
	// drain results
	for range pool.Results() {
	}

	if !h.hasMessage("quelon started") {
		t.Fatalf("expected 'quelon started' log; got %v", h.messages())
	}
	if v, ok := h.attrFor("quelon started", "workers"); !ok || v != "2" {
		t.Errorf("expected workers=2, got %q (ok=%v)", v, ok)
	}
}

func TestLoggerTaskLifecycle(t *testing.T) {
	h, logger := newTestLogger()

	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(1),
		WithWorkerCount(1),
		WithLogger(logger),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	for _, want := range []string{"task submitted", "task started", "task succeeded"} {
		if !h.hasMessage(want) {
			t.Errorf("missing log message %q; got %v", want, h.messages())
		}
	}
	if v, ok := h.attrFor("task submitted", "task_id"); !ok || v != "t1" {
		t.Errorf("task submitted: expected task_id=t1, got %q", v)
	}
	if v, ok := h.attrFor("task succeeded", "task_id"); !ok || v != "t1" {
		t.Errorf("task succeeded: expected task_id=t1, got %q", v)
	}
}

func TestLoggerRetry(t *testing.T) {
	h, logger := newTestLogger()
	var attempts atomic.Int64

	pool := NewPool(func(_ context.Context, _ any) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	},
		WithBufferSize(1),
		WithWorkerCount(1),
		WithLogger(logger),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}),
	)
	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if !h.hasMessage("task retrying") {
		t.Fatalf("expected 'task retrying' log; got %v", h.messages())
	}
	// two retries before success
	h.mu.Lock()
	var retryCount int
	for _, r := range h.records {
		if r.Message == "task retrying" {
			retryCount++
		}
	}
	h.mu.Unlock()
	if retryCount != 2 {
		t.Errorf("expected 2 'task retrying' records, got %d", retryCount)
	}
}

func TestLoggerDeadLetter(t *testing.T) {
	h, logger := newTestLogger()

	pool := NewPool(func(_ context.Context, _ any) error {
		return errors.New("always fails")
	},
		WithBufferSize(1),
		WithWorkerCount(1),
		WithLogger(logger),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if !h.hasMessage("task dead-lettered") {
		t.Fatalf("expected 'task dead-lettered' log; got %v", h.messages())
	}
	if v, ok := h.attrFor("task dead-lettered", "task_id"); !ok || v != "t1" {
		t.Errorf("dead-letter: expected task_id=t1, got %q", v)
	}
}

func TestLoggerDefaultOff(t *testing.T) {
	// nil logger (default) must not panic and produce no output
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(1),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})
	// reaching here without panic is the assertion
}

// --- buffer full ---

func TestSubmitReturnsErrBufferFull(t *testing.T) {
	// buffer of 1, slow worker so the slot stays occupied
	pool := NewPool(func(_ context.Context, _ any) error {
		time.Sleep(10 * time.Second)
		return nil
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)))

	pool.Start(t.Context())

	// first submit fills the buffer
	pool.Submit(t.Context(), Task{ID: "t1", Payload: "x"})

	// second submit should get ErrBufferFull immediately
	err := pool.Submit(t.Context(), Task{ID: "t2", Payload: "x"})
	if !errors.Is(err, ErrBufferFull) {
		t.Errorf("expected ErrBufferFull, got %v", err)
	}
}

// --- close without draining results ---

func TestCloseAndWaitWithoutResultDrain(t *testing.T) {
	// A consumer that never drains Results() must not be able to wedge the pool:
	// CloseAndWait has to return rather than block forever on workers stuck
	// emitting results into a full, unread channel.
	pool := NewPool(func(_ context.Context, _ any) error {
		return nil
	}, WithBufferSize(2), WithWorkerCount(2), WithRetryPolicy(fastPolicy(1)))
	pool.Start(t.Context())

	for i := range 50 {
		_ = pool.Submit(t.Context(), Task{ID: fmt.Sprintf("t-%d", i), Payload: i})
	}

	done := make(chan struct{})
	go func() {
		pool.CloseAndWait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("CloseAndWait deadlocked with an undrained Results() channel")
	}
}

// trackingStore keeps the live set of persisted-but-not-deleted task ids so a
// test can assert a rejected Submit leaves nothing orphaned.
type trackingStore struct {
	NoopStore
	mu      sync.Mutex
	pending map[string]bool
}

func newTrackingStore() *trackingStore { return &trackingStore{pending: map[string]bool{}} }

func (s *trackingStore) SaveTask(t RawTask) error {
	s.mu.Lock()
	s.pending[t.ID] = true
	s.mu.Unlock()
	return nil
}

func (s *trackingStore) DeleteTask(id string) error {
	s.mu.Lock()
	delete(s.pending, id)
	s.mu.Unlock()
	return nil
}

func (s *trackingStore) isPending(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending[id]
}

func TestSubmitRollsBackPersistOnReject(t *testing.T) {
	store := newTrackingStore()
	block := make(chan struct{})
	pool := NewPool(func(_ context.Context, _ any) error {
		<-block
		return nil
	}, WithBufferSize(1), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)), WithStore(store))
	pool.Start(t.Context())

	go func() {
		for range pool.Results() {
		}
	}()

	var rejected string
	for i := range 100 {
		id := fmt.Sprintf("t-%d", i)
		if err := pool.Submit(t.Context(), Task{ID: id, Payload: i}); errors.Is(err, ErrBufferFull) {
			rejected = id
			break
		}
	}
	if rejected == "" {
		t.Fatal("expected a Submit to be rejected with ErrBufferFull")
	}
	if store.isPending(rejected) {
		t.Errorf("rejected task %s was left orphaned in the store", rejected)
	}

	close(block)
	pool.CloseAndWait()
}

func TestSubmitRejectsCancelledContext(t *testing.T) {
	store := newTrackingStore()
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(4), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)), WithStore(store))
	pool.Start(t.Context())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := pool.Submit(ctx, Task{ID: "t1", Payload: "x"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if store.isPending("t1") {
		t.Errorf("task was persisted despite a cancelled context")
	}

	pool.CloseAndWait()
}

// --- store encoding skip ---

func TestNoStoreSkipsEncoding(t *testing.T) {
	encodeCallCount := 0

	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(1),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
		WithCodec(
			func(v any) ([]byte, error) {
				encodeCallCount++
				return []byte(`"x"`), nil
			},
			func(b []byte) (any, error) { return "x", nil },
		),
	)
	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if encodeCallCount != 0 {
		t.Errorf("encode called %d times with no store configured, want 0", encodeCallCount)
	}
}

func TestWithStoreCallsEncoding(t *testing.T) {
	encodeCallCount := 0

	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(1),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
		WithStore(NoopStore{}),
		WithCodec(
			func(v any) ([]byte, error) {
				encodeCallCount++
				return []byte(`"x"`), nil
			},
			func(b []byte) (any, error) { return "x", nil },
		),
	)
	pool.Start(t.Context())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if encodeCallCount == 0 {
		t.Error("encode never called with store configured")
	}
}

// --- EnqueuedAt auto-set ---

func TestEnqueuedAtAutoSet(t *testing.T) {
	before := time.Now()

	pool := NewPool(
		func(_ context.Context, _ any) error { return nil },
		WithBufferSize(1),
		WithWorkerCount(1),
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(t.Context())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	after := time.Now()
	ts := results[0].Task.EnqueuedAt
	if ts.Before(before) || ts.After(after) {
		t.Errorf("EnqueuedAt %v not between %v and %v", ts, before, after)
	}
}

// --- dynamic worker scaling ---

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestDynamicWorkersScaleUpAndDown(t *testing.T) {
	pool := NewPool(func(_ context.Context, _ any) error {
		time.Sleep(15 * time.Millisecond)
		return nil
	},
		WithBufferSize(64),
		WithRetryPolicy(fastPolicy(1)),
		WithDynamicWorkers(2, 16, 0.25),
	)
	// Tighten timings so the test doesn't depend on the production defaults.
	pool.scaleInterval = 5 * time.Millisecond
	pool.idleTimeout = 60 * time.Millisecond

	pool.Start(t.Context())
	go func() {
		for range pool.Results() {
		}
	}()

	if got := pool.ActiveWorkers(); got != 2 {
		t.Fatalf("initial workers = %d, want 2 (min)", got)
	}

	// Flood to build a backlog; ErrBufferFull on a full buffer is fine here.
	floodDone := make(chan struct{})
	go func() {
		defer close(floodDone)
		for i := range 600 {
			pool.Submit(t.Context(), Task{ID: fmt.Sprintf("t-%d", i), Payload: i})
		}
	}()

	if !waitFor(t, 2*time.Second, func() bool { return pool.ActiveWorkers() > 2 }) {
		t.Fatalf("workers never scaled up beyond min; got %d", pool.ActiveWorkers())
	}
	if got := pool.ActiveWorkers(); got > 16 {
		t.Fatalf("workers exceeded max: %d", got)
	}

	// Stop producing before closing: CloseAndWait closes the jobs channel and
	// must not race a concurrent Submit.
	<-floodDone

	// Once the backlog drains and burst workers idle out, return to min.
	if !waitFor(t, 3*time.Second, func() bool { return pool.ActiveWorkers() == 2 }) {
		t.Fatalf("workers never scaled back to min; got %d", pool.ActiveWorkers())
	}

	pool.CloseAndWait()
}

func TestDynamicWorkersRespectMax(t *testing.T) {
	const max = 8
	pool := NewPool(func(_ context.Context, _ any) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	},
		WithBufferSize(32),
		WithRetryPolicy(fastPolicy(1)),
		WithDynamicWorkers(2, max, 0.1),
	)
	pool.scaleInterval = 3 * time.Millisecond
	pool.idleTimeout = 50 * time.Millisecond

	pool.Start(t.Context())
	go func() {
		for range pool.Results() {
		}
	}()

	stop := make(chan struct{})
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		for {
			select {
			case <-stop:
				return
			default:
				pool.Submit(t.Context(), Task{ID: "x", Payload: 1})
			}
		}
	}()

	peak := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if a := pool.ActiveWorkers(); a > peak {
			peak = a
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	<-prodDone // ensure no Submit races the CloseAndWait below

	if peak > max {
		t.Fatalf("workers exceeded max %d: peaked at %d", max, peak)
	}
	if peak <= 2 {
		t.Fatalf("workers never scaled up under sustained load; peaked at %d", peak)
	}

	pool.CloseAndWait()
}

func TestDynamicWorkersCleanShutdown(t *testing.T) {
	const n = 50
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(n),
		WithRetryPolicy(fastPolicy(1)),
		WithDynamicWorkers(2, 8, 0.5),
	)
	pool.scaleInterval = 5 * time.Millisecond

	pool.Start(t.Context())

	done := make(chan struct{})
	go func() {
		submitAndClose(t, pool, makeTasks(n))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("CloseAndWait did not return — supervisor or worker leak")
	}

	if a := pool.ActiveWorkers(); a != 0 {
		t.Errorf("active workers after shutdown = %d, want 0", a)
	}
}

func TestMachineAwareLimitClampsOverProvisionedMax(t *testing.T) {
	capacity := runtime.GOMAXPROCS(0)
	handler, logger := newTestLogger()

	pool := newPool(
		WithLogger(logger),
		WithDynamicWorkers(2, capacity+10, 0.5),
		WithMachineAwareLimit(),
	)

	if pool.maxWorkers != capacity {
		t.Fatalf("maxWorkers = %d, want clamped to capacity %d", pool.maxWorkers, capacity)
	}
	if !handler.hasMessage("clamping max_workers to machine capacity") {
		t.Fatal("expected a clamp warning to be logged")
	}
	if got, ok := handler.attrFor("clamping max_workers to machine capacity", "configured_max_workers"); !ok || got != fmt.Sprint(capacity+10) {
		t.Fatalf("configured_max_workers attr = %q, want %d", got, capacity+10)
	}
	if got, ok := handler.attrFor("clamping max_workers to machine capacity", "machine_capacity"); !ok || got != fmt.Sprint(capacity) {
		t.Fatalf("machine_capacity attr = %q, want %d", got, capacity)
	}
}

func TestMachineAwareLimitClampsMinWhenAboveNewMax(t *testing.T) {
	capacity := runtime.GOMAXPROCS(0)
	_, logger := newTestLogger()

	// min itself exceeds the machine's capacity, so after max is clamped down
	// to capacity, min must be pulled down too or min > max would leave the
	// pool in an inconsistent state.
	pool := newPool(
		WithLogger(logger),
		WithDynamicWorkers(capacity+5, capacity+10, 0.5),
		WithMachineAwareLimit(),
	)

	if pool.maxWorkers != capacity {
		t.Fatalf("maxWorkers = %d, want %d", pool.maxWorkers, capacity)
	}
	if pool.minWorkers != capacity {
		t.Fatalf("minWorkers = %d, want pulled down to %d", pool.minWorkers, capacity)
	}
}

func TestMachineAwareLimitNoopWhenNotOptedIn(t *testing.T) {
	capacity := runtime.GOMAXPROCS(0)
	_, logger := newTestLogger()

	// Same over-provisioned max as the clamp test above, but without
	// WithMachineAwareLimit: existing I/O-bound callers must see no change.
	pool := newPool(
		WithLogger(logger),
		WithDynamicWorkers(2, capacity+10, 0.5),
	)

	if pool.maxWorkers != capacity+10 {
		t.Fatalf("maxWorkers = %d, want left untouched at %d", pool.maxWorkers, capacity+10)
	}
}

func TestMachineAwareLimitNoopWhenWithinCapacity(t *testing.T) {
	capacity := runtime.GOMAXPROCS(0)
	handler, logger := newTestLogger()

	// max exactly equals capacity: the boundary must NOT trigger a clamp or a
	// warning log (condition is strictly greater-than).
	pool := newPool(
		WithLogger(logger),
		WithDynamicWorkers(1, capacity, 0.5),
		WithMachineAwareLimit(),
	)

	if pool.maxWorkers != capacity {
		t.Fatalf("maxWorkers = %d, want unchanged at %d", pool.maxWorkers, capacity)
	}
	if handler.hasMessage("clamping max_workers to machine capacity") {
		t.Fatal("did not expect a clamp warning when max == capacity")
	}
}

func TestMachineAwareLimitNoopWithoutDynamicWorkers(t *testing.T) {
	// WithMachineAwareLimit with no WithDynamicWorkers call: dynamic mode is
	// off, so there's no maxWorkers to clamp — must not panic or set dynamic.
	pool := newPool(WithMachineAwareLimit())

	if pool.dynamic {
		t.Fatal("expected dynamic to remain false without WithDynamicWorkers")
	}
	if pool.maxWorkers != 0 {
		t.Fatalf("maxWorkers = %d, want 0 (untouched)", pool.maxWorkers)
	}
}

func TestMachineAwareLimitNoLoggerConfigured(t *testing.T) {
	capacity := runtime.GOMAXPROCS(0)

	// No WithLogger: p.log must no-op safely rather than panic on a nil logger.
	pool := newPool(
		WithDynamicWorkers(2, capacity+10, 0.5),
		WithMachineAwareLimit(),
	)

	if pool.maxWorkers != capacity {
		t.Fatalf("maxWorkers = %d, want clamped to %d even without a logger", pool.maxWorkers, capacity)
	}
}
