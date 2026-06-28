package ember

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkPool_Throughput(b *testing.B) {
	pool := NewPool(b.N, func(_ context.Context, _ any) error {
		return nil
	}, WithRetryPolicy(fastPolicy(1)))

	pool.Start(context.Background(), 4)

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
			pool := NewPool(b.N, func(_ context.Context, _ any) error {
				return nil
			}, WithRetryPolicy(fastPolicy(1)))

			pool.Start(context.Background(), workers)

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
	pool := NewPool(b.N, func(_ context.Context, _ any) error {
		attempt++
		if attempt%3 != 0 {
			return fmt.Errorf("transient")
		}
		return nil
	}, WithRetryPolicy(RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}))

	pool.Start(context.Background(), 4)

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
	pool := NewPool(b.N+1, func(_ context.Context, _ any) error {
		return nil
	}, WithRetryPolicy(fastPolicy(1)))

	pool.Start(context.Background(), 1)

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
