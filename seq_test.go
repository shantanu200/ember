package quelon

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// Submit stamps each task with a distinct, monotonically increasing sequence
// number, surfaced on the emitted Result. Seq is the tie-break the store uses to
// replay work in submission order after a crash.
func TestSubmit_AssignsMonotonicSeq(t *testing.T) {
	const n = 32

	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(n), WithRetryPolicy(fastPolicy(1)))
	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	results := submitAndClose(t, pool, makeTasks(n))
	if len(results) != n {
		t.Fatalf("got %d results, want %d", len(results), n)
	}

	seen := make(map[uint64]bool, n)
	for _, r := range results {
		if r.Task.Seq == 0 {
			t.Errorf("task %s has zero Seq; Submit did not stamp it", r.Task.ID)
		}
		if seen[r.Task.Seq] {
			t.Errorf("duplicate Seq %d", r.Task.Seq)
		}
		seen[r.Task.Seq] = true
	}
	if len(seen) != n {
		t.Errorf("got %d distinct Seq values, want %d", len(seen), n)
	}
}

// On reload, same-key pending tasks must be replayed in Seq order — i.e. their
// original submission order — even when the store returns them out of order.
func TestPartitions_ReloadReplaysInSeqOrder(t *testing.T) {
	const n = 20

	// Present pending tasks in reverse submission order; Seq encodes the true
	// order (want+1) so a correct reload sorts them back to 0..n-1.
	pending := make([]RawTask, n)
	for i := range pending {
		want := n - 1 - i // reversed
		payload, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		pending[i] = RawTask{
			ID:      fmt.Sprintf("acct-%d", want),
			Key:     "acct",
			Seq:     uint64(want + 1),
			Payload: payload,
		}
	}
	store := &preloadedStore{pending: pending}

	var (
		mu    sync.Mutex
		order []int
	)
	pool := NewPool(func(_ context.Context, payload any) error {
		mu.Lock()
		order = append(order, int(payload.(float64)))
		mu.Unlock()
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

	if len(order) != n {
		t.Fatalf("processed %d reloaded tasks, want %d", len(order), n)
	}
	for i, v := range order {
		if v != i {
			t.Fatalf("reload order[%d] = %d, want %d (not replayed in Seq order): %v", i, v, i, order)
		}
	}
}

// After reload the submission counter must resume above the highest reloaded
// Seq, so tasks submitted post-restart sort after the recovered ones rather than
// colliding with or preceding them.
func TestSubmit_SeqResumesAboveReloaded(t *testing.T) {
	payload, err := json.Marshal(0)
	if err != nil {
		t.Fatal(err)
	}
	const reloadedMax = 500
	store := &preloadedStore{pending: []RawTask{
		{ID: "old", Key: "k", Seq: reloadedMax, Payload: payload},
	}}

	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithPartitions(4), WithBufferSize(8), WithStore(store), WithRetryPolicy(fastPolicy(1)))

	var (
		mu   sync.Mutex
		seqs []uint64
	)
	// Capture Seq of the freshly submitted task via a hook-free path: read it off
	// Results.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range pool.Results() {
			if r.Task.ID == "new" {
				mu.Lock()
				seqs = append(seqs, r.Task.Seq)
				mu.Unlock()
			}
		}
	}()

	if err := pool.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := pool.Submit(t.Context(), Task{ID: "new", Key: "k", Payload: 1}); err != nil {
		t.Fatal(err)
	}
	pool.CloseAndWait()
	wg.Wait()

	if len(seqs) != 1 {
		t.Fatalf("expected 1 seq for the new task, got %v", seqs)
	}
	if seqs[0] <= reloadedMax {
		t.Errorf("new task Seq = %d, want > %d (counter did not resume above reloaded max)", seqs[0], reloadedMax)
	}
}
