package quelon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// commitRecordingStore implements CommitStore and records every group-commit
// call so tests can assert what the writer batched.
type commitRecordingStore struct {
	NoopStore
	mu          sync.Mutex
	commits     int
	saves       map[string]int // id -> times saved
	deletes     map[string]int // id -> times deleted
	deadLetters map[string]int // id -> times dead-lettered
}

func newCommitRecordingStore() *commitRecordingStore {
	return &commitRecordingStore{
		saves:       map[string]int{},
		deletes:     map[string]int{},
		deadLetters: map[string]int{},
	}
}

func (s *commitRecordingStore) Commit(saves []RawTask, deletes []string, dls []RawDeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
	for _, t := range saves {
		s.saves[t.ID]++
	}
	for _, id := range deletes {
		s.deletes[id]++
	}
	for _, dl := range dls {
		s.deadLetters[dl.Task.ID]++
	}
	return nil
}

func (s *commitRecordingStore) snapshot() (commits int, saves, deletes, dls map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := func(m map[string]int) map[string]int {
		out := make(map[string]int, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	return s.commits, cp(s.saves), cp(s.deletes), cp(s.deadLetters)
}

// In PersistAll, a successfully processed task is saved on submit and deleted on
// completion — both flow through the group-commit writer's CommitStore path.
func TestGroupCommitPersistsAndDeletesOnSuccess(t *testing.T) {
	store := newCommitRecordingStore()
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(8), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStore(store))

	pool.Start(context.Background())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	_, saves, deletes, dls := store.snapshot()
	if saves["t1"] == 0 {
		t.Error("t1 was never saved")
	}
	if deletes["t1"] == 0 {
		t.Error("t1 was never deleted after completion")
	}
	if dls["t1"] != 0 {
		t.Errorf("t1 should not be dead-lettered, got %d", dls["t1"])
	}
}

// Many tasks submitted in a burst must be settled in far fewer commits than
// tasks — that is the whole point of group commit (one fsync per window, not
// per task).
func TestGroupCommitBatchesManyTasksIntoFewCommits(t *testing.T) {
	const n = 200
	store := newCommitRecordingStore()
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(n), WithWorkerCount(4), WithRetryPolicy(fastPolicy(1)),
		WithStore(store), WithGroupCommit(64, 5*time.Millisecond))

	pool.Start(context.Background())
	submitAndClose(t, pool, makeTasks(n))

	commits, saves, deletes, _ := store.snapshot()
	if len(saves) != n {
		t.Errorf("saved %d distinct tasks, want %d", len(saves), n)
	}
	if len(deletes) != n {
		t.Errorf("deleted %d distinct tasks, want %d", len(deletes), n)
	}
	// 2n logical ops (a save + a delete each) settled in << 2n commits.
	if commits >= 2*n {
		t.Errorf("group commit made %d commits for %d tasks; expected far fewer", commits, n)
	}
}

// PersistDeadLettersOnly must never touch the store for pending tasks: no saves,
// no deletes — only dead letters for failures.
func TestPersistDeadLettersOnlySkipsPending(t *testing.T) {
	store := newCommitRecordingStore()
	pool := NewPool(func(_ context.Context, p any) error {
		if p == "bad" {
			return NewPermanentError(errors.New("poison"))
		}
		return nil
	}, WithBufferSize(8), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStoreMode(store, PersistDeadLettersOnly))

	pool.Start(context.Background())
	submitAndClose(t, pool, []Task{{ID: "ok", Payload: "good"}, {ID: "bad", Payload: "bad"}})

	_, saves, deletes, dls := store.snapshot()
	if len(saves) != 0 {
		t.Errorf("PersistDeadLettersOnly saved pending tasks: %v", saves)
	}
	if len(deletes) != 0 {
		t.Errorf("PersistDeadLettersOnly deleted pending tasks: %v", deletes)
	}
	if dls["bad"] == 0 {
		t.Error("failed task was not dead-lettered")
	}
	if dls["ok"] != 0 {
		t.Error("successful task should not be dead-lettered")
	}
}

// A dead-lettered task must be recorded as a dead letter in the same window it is
// deleted from pending (atomic settle), and never saved-without-settle.
func TestGroupCommitDeadLetterAndDeleteTogether(t *testing.T) {
	store := newCommitRecordingStore()
	pool := NewPool(func(_ context.Context, _ any) error {
		return NewPermanentError(errors.New("fatal"))
	}, WithBufferSize(4), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStore(store))

	pool.Start(context.Background())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	_, _, deletes, dls := store.snapshot()
	if dls["t1"] == 0 {
		t.Error("t1 was not dead-lettered")
	}
	if deletes["t1"] == 0 {
		t.Error("t1 was not removed from pending")
	}
}
