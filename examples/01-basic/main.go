// Command basic is the smallest useful quelon program. It runs a worker pool
// that processes tasks concurrently, retries transient failures with backoff,
// and dead-letters the ones that never succeed — the core of the library in
// under a screenful.
//
// Run it with:
//
//	go run ./examples/01-basic
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shantanu200/quelon"
)

func main() {
	ctx := context.Background()

	// tries counts attempts per task id so the example can simulate a transient
	// failure that recovers on retry.
	var (
		mu    sync.Mutex
		tries = make(map[int]int)
	)

	pool := quelon.NewPool(
		func(_ context.Context, payload any) error {
			id := payload.(int)

			// Task 3 is malformed: a PermanentError is dead-lettered on the first
			// attempt, skipping retries entirely.
			if id == 3 {
				return quelon.NewPermanentError(fmt.Errorf("task %d is malformed", id))
			}

			// Task 7 fails once, then succeeds — a transient error the RetryPolicy
			// recovers from.
			if id == 7 {
				mu.Lock()
				tries[id]++
				n := tries[id]
				mu.Unlock()
				if n < 2 {
					return fmt.Errorf("task %d hit a transient error", id)
				}
			}

			return nil // success
		},
		quelon.WithWorkerCount(4),
		quelon.WithRetryPolicy(quelon.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   5 * time.Millisecond,
			MaxDelay:    50 * time.Millisecond,
		}),
		quelon.WithHooks(quelon.Hooks{
			OnRetry: func(t quelon.Task, err error, attempt int) {
				fmt.Printf("retry:       %s after attempt %d (%v)\n", t.ID, attempt+1, err)
			},
			OnDeadLetter: func(dl quelon.DeadLetter) {
				fmt.Printf("dead-letter: %s (%s)\n", dl.Task.ID, dl.Err)
			},
		}),
	)

	if err := pool.Start(ctx); err != nil {
		panic(err)
	}

	// Drain outcomes on Results() concurrently, tallying successes vs failures.
	var (
		ok, failed int
		done       = make(chan struct{})
	)
	go func() {
		for r := range pool.Results() {
			if r.Err != nil {
				failed++
			} else {
				ok++
			}
		}
		close(done)
	}()

	// Submit 10 tasks. Submit is non-blocking; it returns ErrBufferFull if the
	// buffer is full rather than waiting.
	for i := range 10 {
		if err := pool.Submit(ctx, quelon.Task{ID: fmt.Sprintf("t-%d", i), Payload: i}); err != nil {
			fmt.Printf("submit %d rejected: %v\n", i, err)
		}
	}

	// CloseAndWait stops accepting work, waits for in-flight tasks, and closes
	// Results() — so the drain goroutine above finishes.
	pool.CloseAndWait()
	<-done

	fmt.Printf("\nsucceeded=%d dead-lettered=%d\n", ok, failed)
}
