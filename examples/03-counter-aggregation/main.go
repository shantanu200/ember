// Command counter-aggregation is the advanced example: it uses
// quelon.WithAggregator to absorb a storm of per-event counter increments
// (likes, views) and flush them to a datastore as a handful of coalesced
// updates instead of one write per event.
//
// WithAggregator folds same-key payloads in memory as they arrive and flushes
// one accumulated task per key on an interval, turning buffer and write cost
// from O(events) into O(distinct keys). WithPartitions keeps each counter on a
// single lane, so there is exactly one writer per key.
//
// Run it with:
//
//	go run ./examples/03-counter-aggregation
package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shantanu200/quelon"
)

// incr is one counter increment. The payload carries its own Key because only
// the payload — not Task.Key — reaches the ProcessFunc, so it must know which
// counter a coalesced delta belongs to.
type incr struct {
	Key   string
	Delta int64
}

// store stands in for the database of record. writes counts how many times we
// actually touched it, so the example can show the coalescing win.
type store struct {
	mu     sync.Mutex
	counts map[string]int64
	writes atomic.Int64
}

func (s *store) apply(key string, delta int64) {
	s.mu.Lock()
	s.counts[key] += delta
	s.mu.Unlock()
	s.writes.Add(1)
}

func main() {
	db := &store{counts: make(map[string]int64)}
	ctx := context.Background()

	pool := quelon.NewPool(
		func(_ context.Context, payload any) error {
			v := payload.(incr)
			db.apply(v.Key, v.Delta) // one UPSERT ... SET val = val + delta
			return nil
		},
		// Fold same-key increments in memory; flush the coalesced total every
		// 100ms (or once 10k distinct keys are live).
		quelon.WithAggregator(
			func(acc, in any) any {
				a := acc.(incr)
				a.Delta += in.(incr).Delta
				return a
			},
			100*time.Millisecond,
			10_000,
		),
		quelon.WithPartitions(16),
	)

	if err := pool.Start(ctx); err != nil {
		panic(err)
	}
	go func() {
		for range pool.Results() {
		}
	}()

	// Simulate 100k like events across 5 posts, produced by 8 goroutines.
	posts := []string{"post:1", "post:2", "post:3", "post:4", "post:5"}
	const (
		producers   = 8
		perProducer = 12_500 // 8 * 12_500 = 100_000 total
	)
	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for i := range perProducer {
				key := posts[i%len(posts)] + ":likes"
				// Submit never blocks in aggregating mode — it just folds.
				_ = pool.Submit(ctx, quelon.Task{Key: key, Payload: incr{Key: key, Delta: 1}})
			}
		})
	}
	wg.Wait()

	// CloseAndWait drains every accumulator before returning, so no increment is
	// lost on a graceful shutdown.
	pool.CloseAndWait()

	const total = producers * perProducer
	writes := db.writes.Load()
	fmt.Printf("submitted %d like events\n", total)
	fmt.Printf("database writes: %d (%.0fx fewer than one-write-per-event)\n", writes, float64(total)/float64(writes))

	keys := make([]string, 0, len(db.counts))
	for k := range db.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %s = %d\n", k, db.counts[k])
	}
}
