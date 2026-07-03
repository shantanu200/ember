package quelon

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkGatherBatchTimer isolates the timer-handling mechanism gatherBatch
// uses for its maxBatchWait deadline: allocating a fresh time.Timer on every
// call (the old behavior) vs. reusing one timer across calls via resetTimer
// (see batch.go). This is what changes per batch in partitioned/batch-mode
// pools; the surrounding channel/select machinery is identical either way.
func BenchmarkGatherBatchTimer(b *testing.B) {
	const wait = time.Millisecond

	b.Run("NewPerCall", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			t := time.NewTimer(wait)
			t.Stop()
		}
	})

	b.Run("Reused", func(b *testing.B) {
		b.ReportAllocs()
		t := time.NewTimer(wait)
		if !t.Stop() {
			<-t.C
		}
		defer t.Stop()
		for range b.N {
			resetTimer(t, wait)
		}
	})
}

func BenchmarkPool_Throughput(b *testing.B) {
	pool := NewPool(func(_ context.Context, _ any) error {
		return nil
	}, WithBufferSize(b.N), WithWorkerCount(4), WithRetryPolicy(fastPolicy(1)))

	pool.Start(context.Background())

	go func() {
		for range pool.Results() {
		}
	}()

	b.ResetTimer()
	for i := range b.N {
		pool.Submit(context.Background(), Task{ID: fmt.Sprintf("%d", i), Payload: i})
	}
	pool.CloseAndWait()
}

// BenchmarkPool_Throughput_NoTaskTimeout is BenchmarkPool_Throughput with the
// per-task deadline disabled (WithTaskTimeout(0)), isolating the cost of the
// context.WithTimeout wrapper runOnce applies on every attempt.
func BenchmarkPool_Throughput_NoTaskTimeout(b *testing.B) {
	pool := NewPool(func(_ context.Context, _ any) error {
		return nil
	}, WithBufferSize(b.N), WithWorkerCount(4), WithRetryPolicy(fastPolicy(1)), WithTaskTimeout(0))

	pool.Start(context.Background())

	go func() {
		for range pool.Results() {
		}
	}()

	b.ResetTimer()
	for i := range b.N {
		pool.Submit(context.Background(), Task{ID: fmt.Sprintf("%d", i), Payload: i})
	}
	pool.CloseAndWait()
}

func BenchmarkPool_WorkerScaling(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			pool := NewPool(func(_ context.Context, _ any) error {
				return nil
			}, WithBufferSize(b.N), WithWorkerCount(workers), WithRetryPolicy(fastPolicy(1)))

			pool.Start(context.Background())

			go func() {
				for range pool.Results() {
				}
			}()

			b.ResetTimer()
			for i := range b.N {
				pool.Submit(context.Background(), Task{ID: fmt.Sprintf("%d", i), Payload: i})
			}
			pool.CloseAndWait()
		})
	}
}

func BenchmarkPool_WithRetry(b *testing.B) {
	var attempt int
	pool := NewPool(func(_ context.Context, _ any) error {
		attempt++
		if attempt%3 != 0 {
			return fmt.Errorf("transient")
		}
		return nil
	}, WithBufferSize(b.N), WithWorkerCount(4), WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(context.Background())

	go func() {
		for range pool.Results() {
		}
	}()

	b.ResetTimer()
	for i := range b.N {
		pool.Submit(context.Background(), Task{ID: fmt.Sprintf("%d", i), Payload: i})
	}
	pool.CloseAndWait()
}

func BenchmarkPool_Submit(b *testing.B) {
	pool := NewPool(func(_ context.Context, _ any) error {
		return nil
	}, WithBufferSize(b.N+1), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)))

	pool.Start(context.Background())

	go func() {
		for range pool.Results() {
		}
	}()

	ctx := context.Background()
	task := Task{ID: "bench", Payload: 42}

	b.ResetTimer()
	for range b.N {
		pool.Submit(ctx, task)
	}
	pool.CloseAndWait()
}
