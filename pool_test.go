package ember

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
		if err := p.Submit(context.Background(), task); err != nil {
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

	pool := NewPool(n, func(_ context.Context, _ any) error {
		count.Add(1)
		return nil
	}, WithRetryPolicy(fastPolicy(1)))

	if err := pool.Start(context.Background(), 4); err != nil {
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

	pool := NewPool(n, func(_ context.Context, p any) error {
		mu.Lock()
		order = append(order, p.(int))
		mu.Unlock()
		return nil
	}, WithRetryPolicy(fastPolicy(1)))

	pool.Start(context.Background(), 1)
	submitAndClose(t, pool, makeTasks(n))

	if len(order) != n {
		t.Errorf("got %d processed, want %d", len(order), n)
	}
}

// --- retries ---

func TestRetryOnTransientError(t *testing.T) {
	var attempts atomic.Int64

	pool := NewPool(1, func(_ context.Context, _ any) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	}, WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(context.Background(), 1)
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

func TestExhaustedRetriesProducesError(t *testing.T) {
	pool := NewPool(1, func(_ context.Context, _ any) error {
		return errors.New("always fails")
	}, WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(context.Background(), 1)
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if results[0].Err == nil {
		t.Error("expected error after exhausted retries, got nil")
	}
}

// --- permanent errors ---

func TestPermanentErrorSkipsRetries(t *testing.T) {
	var attempts atomic.Int64

	pool := NewPool(1, func(_ context.Context, _ any) error {
		attempts.Add(1)
		return NewPermanentError(errors.New("fatal"))
	}, WithRetryPolicy(RetryPolicy{MaxAttempts: 5, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(context.Background(), 1)
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
		1, func(_ context.Context, _ any) error {
			return NewPermanentError(errors.New("fatal"))
		},
		WithRetryPolicy(fastPolicy(1)),
		WithHooks(Hooks{
			OnDeadLetter: func(d DeadLetter) { dl = d },
		}),
	)

	pool.Start(context.Background(), 1)
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
		10, func(_ context.Context, p any) error {
			if p.(int)%2 == 0 {
				return errors.New("even always fails")
			}
			return nil
		},
		WithRetryPolicy(fastPolicy(1)),
		WithHooks(Hooks{
			OnDeadLetter: func(dl DeadLetter) {
				mu.Lock()
				deadLetters = append(deadLetters, dl)
				mu.Unlock()
			},
		}),
	)

	pool.Start(context.Background(), 4)
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
		5, func(_ context.Context, _ any) error { return nil },
		WithHooks(Hooks{
			OnSuccess: func(_ Task) { count.Add(1) },
		}),
	)

	pool.Start(context.Background(), 2)
	submitAndClose(t, pool, makeTasks(5))

	if count.Load() != 5 {
		t.Errorf("OnSuccess fired %d times, want 5", count.Load())
	}
}

func TestOnRetryHookFired(t *testing.T) {
	var retries atomic.Int64

	pool := NewPool(
		1, func(_ context.Context, _ any) error {
			if retries.Load() < 2 {
				return errors.New("not yet")
			}
			return nil
		},
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}),
		WithHooks(Hooks{
			OnRetry: func(_ Task, _ error, _ int) { retries.Add(1) },
		}),
	)

	pool.Start(context.Background(), 1)
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if retries.Load() != 2 {
		t.Errorf("expected 2 OnRetry calls, got %d", retries.Load())
	}
}

// --- context cancellation ---

func TestContextCancellationStopsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var started atomic.Int64
	pool := NewPool(100, func(ctx context.Context, _ any) error {
		started.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}, WithRetryPolicy(fastPolicy(1)))

	pool.Start(ctx, 2)

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
		1, func(ctx context.Context, _ any) error {
			select {
			case <-time.After(10 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		WithRetryPolicy(fastPolicy(1)),
		WithTaskTimeout(50*time.Millisecond),
	)

	pool.Start(context.Background(), 1)
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "slow"}})

	if results[0].Err == nil {
		t.Error("expected timeout error, got nil")
	}
}

// --- panic recovery ---

func TestPanicInProcessFuncRecovered(t *testing.T) {
	pool := NewPool(1, func(_ context.Context, _ any) error {
		panic("unexpected condition")
	}, WithRetryPolicy(fastPolicy(1)))

	pool.Start(context.Background(), 1)
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "boom"}})

	if results[0].Err == nil {
		t.Error("expected error from recovered panic, got nil")
	}
}

// --- concurrent stream ---

func TestConcurrentStreamSubmit(t *testing.T) {
	const n = 100
	var count atomic.Int64

	pool := NewPool(n, func(_ context.Context, _ any) error {
		count.Add(1)
		return nil
	}, WithRetryPolicy(fastPolicy(1)))

	pool.Start(context.Background(), 8)

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
				pool.Submit(context.Background(), Task{ID: fmt.Sprintf("task-%d", id), Payload: id})
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

// --- EnqueuedAt auto-set ---

func TestEnqueuedAtAutoSet(t *testing.T) {
	before := time.Now()

	pool := NewPool(
		1, func(_ context.Context, _ any) error { return nil },
		WithRetryPolicy(fastPolicy(1)),
	)
	pool.Start(context.Background(), 1)
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	after := time.Now()
	ts := results[0].Task.EnqueuedAt
	if ts.Before(before) || ts.After(after) {
		t.Errorf("EnqueuedAt %v not between %v and %v", ts, before, after)
	}
}
