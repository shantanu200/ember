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

// PersistNone (the default PersistMode) must never touch an attached store, even
// though one is configured — WithStoreMode(s, PersistNone) is a way to hold a
// store reference without opting into any persistence behavior.
func TestPersistModeNoneNeverTouchesStore(t *testing.T) {
	store := newCommitRecordingStore()
	pool := NewPool(func(_ context.Context, p any) error {
		if p == "bad" {
			return NewPermanentError(errors.New("poison"))
		}
		return nil
	}, WithBufferSize(8), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStoreMode(store, PersistNone))

	pool.Start(context.Background())
	submitAndClose(t, pool, []Task{{ID: "ok", Payload: "good"}, {ID: "bad", Payload: "bad"}})

	commits, saves, deletes, dls := store.snapshot()
	if commits != 0 || len(saves) != 0 || len(deletes) != 0 || len(dls) != 0 {
		t.Errorf("PersistNone touched the store: commits=%d saves=%v deletes=%v dls=%v",
			commits, saves, deletes, dls)
	}
}

// plainStore implements only the required Store methods — no CommitStore, no
// BatchStore (NoopStore is deliberately not embedded here, since it implements
// BatchStore too and would mask the per-item fallback path under test) — so the
// group-commit writer must fall back to one call per item, still ordered save ->
// dead letter -> delete within a window.
type plainStore struct {
	mu           sync.Mutex
	saveErr      error
	saved        []string
	deletedOrder []string
	dlOrder      []string
}

func (s *plainStore) LoadPendingTasks() ([]RawTask, error)      { return nil, nil }
func (s *plainStore) LoadDeadLetters() ([]RawDeadLetter, error) { return nil, nil }
func (s *plainStore) DeleteDeadLetter(string) error             { return nil }

func (s *plainStore) SaveTask(t RawTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved = append(s.saved, t.ID)
	return nil
}

func (s *plainStore) DeleteTask(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedOrder = append(s.deletedOrder, id)
	return nil
}

func (s *plainStore) SaveDeadLetter(dl RawDeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dlOrder = append(s.dlOrder, dl.Task.ID)
	return nil
}

func (s *plainStore) snapshot() (saved, deleted, dls []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.saved...), append([]string(nil), s.deletedOrder...), append([]string(nil), s.dlOrder...)
}

// A store that implements neither CommitStore nor BatchStore must still be
// driven correctly by the group-commit writer, one item at a time, preserving
// the save -> dead-letter -> delete ordering within a window.
func TestGroupCommitFallsBackToPerItemStoreCalls(t *testing.T) {
	store := &plainStore{}
	pool := NewPool(func(_ context.Context, p any) error {
		if p == "bad" {
			return NewPermanentError(errors.New("poison"))
		}
		return nil
	}, WithBufferSize(8), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStore(store))

	pool.Start(context.Background())
	submitAndClose(t, pool, []Task{{ID: "ok", Payload: "good"}, {ID: "bad", Payload: "bad"}})

	saved, deleted, dls := store.snapshot()
	if len(saved) != 2 {
		t.Errorf("want 2 SaveTask calls, got %v", saved)
	}
	if len(dls) != 1 || dls[0] != "bad" {
		t.Errorf("want bad dead-lettered via per-item SaveDeadLetter, got %v", dls)
	}
	if len(deleted) != 2 {
		t.Errorf("want both tasks deleted from pending, got %v", deleted)
	}
}

// batchStoreOnly implements Store + BatchStore but not CommitStore, so the
// writer's fallback path should prefer the batch calls over per-item ones.
type batchStoreOnly struct {
	NoopStore
	mu            sync.Mutex
	deleteBatches int
	dlBatches     int
	perItemDelete int
	perItemDL     int
}

func (s *batchStoreOnly) DeleteTasks(ids []string) error {
	s.mu.Lock()
	s.deleteBatches++
	s.mu.Unlock()
	return nil
}

func (s *batchStoreOnly) SaveDeadLetters(dls []RawDeadLetter) error {
	s.mu.Lock()
	s.dlBatches++
	s.mu.Unlock()
	return nil
}

func (s *batchStoreOnly) DeleteTask(string) error {
	s.mu.Lock()
	s.perItemDelete++
	s.mu.Unlock()
	return nil
}

func (s *batchStoreOnly) SaveDeadLetter(RawDeadLetter) error {
	s.mu.Lock()
	s.perItemDL++
	s.mu.Unlock()
	return nil
}

func TestGroupCommitPrefersBatchStoreOverPerItem(t *testing.T) {
	store := &batchStoreOnly{}
	pool := NewPool(func(_ context.Context, _ any) error {
		return NewPermanentError(errors.New("fatal"))
	}, WithBufferSize(8), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStore(store))

	pool.Start(context.Background())
	submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.deleteBatches == 0 || store.dlBatches == 0 {
		t.Errorf("expected batch calls to be used, got deleteBatches=%d dlBatches=%d",
			store.deleteBatches, store.dlBatches)
	}
	if store.perItemDelete != 0 || store.perItemDL != 0 {
		t.Errorf("expected no per-item fallback calls when BatchStore is available, got delete=%d dl=%d",
			store.perItemDelete, store.perItemDL)
	}
}

// When CommitStore.Commit fails, the writer must surface the error via
// OnStoreError but keep the pool healthy — the task outcome is still emitted and
// CloseAndWait still completes; a store outage doesn't wedge processing.
func TestStoreErrorHookFiredOnCommitFailure(t *testing.T) {
	store := &commitFailingStore{err: errors.New("disk full")}
	var storeErrs []error
	var mu sync.Mutex
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(8), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStore(store),
		WithHooks(Hooks{OnStoreError: func(err error) {
			mu.Lock()
			storeErrs = append(storeErrs, err)
			mu.Unlock()
		}}))

	pool.Start(context.Background())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("expected the task to still succeed despite the store outage, got %+v", results)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(storeErrs) == 0 {
		t.Error("expected OnStoreError to fire when Commit failed")
	}
}

type commitFailingStore struct {
	NoopStore
	err error
}

func (s *commitFailingStore) Commit([]RawTask, []string, []RawDeadLetter) error {
	return s.err
}

// A payload that fails to encode can't be given a durable pending record, but the
// task must still be processed in memory and the failure surfaced via
// OnStoreError rather than silently dropped or blocking Submit.
func TestSubmitEncodeFailureSkipsPersistButStillProcesses(t *testing.T) {
	store := newCommitRecordingStore()
	var storeErrs []error
	var mu sync.Mutex
	encodeErr := errors.New("unsupported type")

	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(8), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStore(store),
		WithCodec(
			func(any) ([]byte, error) { return nil, encodeErr },
			func(b []byte) (any, error) { return b, nil },
		),
		WithHooks(Hooks{OnStoreError: func(err error) {
			mu.Lock()
			storeErrs = append(storeErrs, err)
			mu.Unlock()
		}}))

	pool.Start(context.Background())
	results := submitAndClose(t, pool, []Task{{ID: "t1", Payload: "x"}})

	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("task should still process despite the encode failure, got %+v", results)
	}
	_, saves, _, _ := store.snapshot()
	if saves["t1"] != 0 {
		t.Errorf("task should not have a durable pending record, got %v", saves)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(storeErrs) == 0 {
		t.Error("expected OnStoreError to fire on encode failure")
	}
}

// The group-commit writer must flush a window on its own timer, not only when
// the pool shuts down — otherwise a low-traffic pool would leave work undurable
// indefinitely between bursts.
func TestGroupCommitFlushesOnTimerWithoutClose(t *testing.T) {
	store := newCommitRecordingStore()
	block := make(chan struct{})
	pool := NewPool(func(_ context.Context, _ any) error {
		<-block
		return nil
	}, WithBufferSize(4), WithWorkerCount(1), WithRetryPolicy(fastPolicy(1)),
		WithStore(store), WithGroupCommit(1000, 20*time.Millisecond))

	pool.Start(context.Background())
	if err := pool.Submit(context.Background(), Task{ID: "t1", Payload: "x"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, saves, _, _ := store.snapshot()
		if saves["t1"] != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("save was never flushed by the group-commit timer")
		}
		time.Sleep(5 * time.Millisecond)
	}

	close(block)
	pool.CloseAndWait()
}

// Start must replay pending tasks recorded by a prior process (crash recovery)
// and resume each one from the Attempt it had already reached, not from zero.
func TestStartResumesPendingTasksFromStore(t *testing.T) {
	store := &preloadedStore{
		pending: []RawTask{{ID: "t1", Payload: []byte(`"x"`), Attempt: 1}},
	}
	var gotAttempt int
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(4), WithWorkerCount(1), WithRetryPolicy(fastPolicy(2)),
		WithStore(store),
		WithHooks(Hooks{OnSuccess: func(task Task) { gotAttempt = task.Attempt }}))

	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	go func() {
		for range pool.Results() {
		}
	}()
	pool.CloseAndWait()

	if gotAttempt != 1 {
		t.Errorf("resumed task ran with Attempt=%d, want 1 (resumed from the loaded attempt)", gotAttempt)
	}
}

// A failure loading pending tasks at startup must surface as an error from
// Start rather than silently starting the pool with missing recovered work.
func TestStartReturnsErrorWhenLoadPendingTasksFails(t *testing.T) {
	loadErr := errors.New("corrupt wal")
	store := &preloadedStore{loadErr: loadErr}
	pool := NewPool(func(_ context.Context, _ any) error { return nil },
		WithBufferSize(4), WithWorkerCount(1), WithStore(store))

	err := pool.Start(context.Background())
	if !errors.Is(err, loadErr) {
		t.Fatalf("Start error = %v, want wrapping %v", err, loadErr)
	}
}

type preloadedStore struct {
	NoopStore
	pending []RawTask
	loadErr error
}

func (s *preloadedStore) LoadPendingTasks() ([]RawTask, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.pending, nil
}
