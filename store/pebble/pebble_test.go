package pebblestore_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/shantanu200/quelon"
	pebblestore "github.com/shantanu200/quelon/store/pebble"
)

var errStub = errors.New("stub failure")

func id(i int) string { return "t-" + strconv.Itoa(i) }

func openStore(t *testing.T) *pebblestore.Store {
	t.Helper()
	s, err := pebblestore.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func rawTask(id string) quelon.RawTask {
	return quelon.RawTask{
		ID:         id,
		Payload:    []byte(`"hello"`),
		EnqueuedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Attempt:    0,
	}
}

// --- SaveTask / LoadPendingTasks ---

func TestSaveAndLoadPendingTasks(t *testing.T) {
	s := openStore(t)

	tasks := []quelon.RawTask{rawTask("a"), rawTask("b"), rawTask("c")}
	for _, task := range tasks {
		if err := s.SaveTask(task); err != nil {
			t.Fatalf("SaveTask(%s): %v", task.ID, err)
		}
	}

	got, err := s.LoadPendingTasks()
	if err != nil {
		t.Fatalf("LoadPendingTasks: %v", err)
	}
	if len(got) != len(tasks) {
		t.Fatalf("got %d tasks, want %d", len(got), len(tasks))
	}

	byID := make(map[string]quelon.RawTask, len(got))
	for _, g := range got {
		byID[g.ID] = g
	}
	for _, want := range tasks {
		g, ok := byID[want.ID]
		if !ok {
			t.Errorf("task %s missing from LoadPendingTasks", want.ID)
			continue
		}
		if string(g.Payload) != string(want.Payload) {
			t.Errorf("task %s: payload %q, want %q", want.ID, g.Payload, want.Payload)
		}
		if !g.EnqueuedAt.Equal(want.EnqueuedAt) {
			t.Errorf("task %s: EnqueuedAt %v, want %v", want.ID, g.EnqueuedAt, want.EnqueuedAt)
		}
	}
}

func TestLoadPendingTasksEmpty(t *testing.T) {
	s := openStore(t)
	got, err := s.LoadPendingTasks()
	if err != nil {
		t.Fatalf("LoadPendingTasks on empty store: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(got))
	}
}

// --- DeleteTask ---

func TestDeleteTask(t *testing.T) {
	s := openStore(t)

	if err := s.SaveTask(rawTask("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTask("x"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	got, err := s.LoadPendingTasks()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 tasks after delete, got %d", len(got))
	}
}

func TestDeleteTaskIdempotent(t *testing.T) {
	s := openStore(t)
	// deleting a non-existent key must not error
	if err := s.DeleteTask("nonexistent"); err != nil {
		t.Errorf("DeleteTask on missing key: %v", err)
	}
}

// --- SaveDeadLetter / LoadDeadLetters ---

func TestSaveAndLoadDeadLetters(t *testing.T) {
	s := openStore(t)

	// Fixed timestamps (not time.Now()) so FailedAt round-trip is asserted
	// deterministically. This is the field that a JSON-tag/field-name mismatch
	// on RawDeadLetter would silently drop to the zero time — the exact class
	// of bug this test now guards against.
	failed1 := time.Date(2026, 7, 1, 13, 28, 47, 0, time.UTC)
	failed2 := time.Date(2026, 7, 1, 13, 29, 12, 0, time.UTC)
	dls := []quelon.RawDeadLetter{
		{Task: rawTask("d1"), Err: "boom", Permanent: false, FailedAt: failed1},
		{Task: rawTask("d2"), Err: "fatal", Permanent: true, FailedAt: failed2},
	}
	for _, dl := range dls {
		if err := s.SaveDeadLetter(dl); err != nil {
			t.Fatalf("SaveDeadLetter(%s): %v", dl.Task.ID, err)
		}
	}

	got, err := s.LoadDeadLetters()
	if err != nil {
		t.Fatalf("LoadDeadLetters: %v", err)
	}
	if len(got) != len(dls) {
		t.Fatalf("got %d dead letters, want %d", len(got), len(dls))
	}

	byID := make(map[string]quelon.RawDeadLetter, len(got))
	for _, g := range got {
		byID[g.Task.ID] = g
	}
	for _, want := range dls {
		g, ok := byID[want.Task.ID]
		if !ok {
			t.Errorf("dead letter %s missing", want.Task.ID)
			continue
		}
		if g.Err != want.Err {
			t.Errorf("dl %s: Err %q, want %q", want.Task.ID, g.Err, want.Err)
		}
		if g.Permanent != want.Permanent {
			t.Errorf("dl %s: Permanent %v, want %v", want.Task.ID, g.Permanent, want.Permanent)
		}
		if !g.FailedAt.Equal(want.FailedAt) {
			t.Errorf("dl %s: FailedAt %v, want %v", want.Task.ID, g.FailedAt, want.FailedAt)
		}
		// The nested RawTask carries its own timestamp; assert it survives the
		// round trip too, since RawTask.EnqueuedAt has the same tag/field risk.
		if !g.Task.EnqueuedAt.Equal(want.Task.EnqueuedAt) {
			t.Errorf("dl %s: Task.EnqueuedAt %v, want %v", want.Task.ID, g.Task.EnqueuedAt, want.Task.EnqueuedAt)
		}
	}
}

func TestLoadDeadLettersEmpty(t *testing.T) {
	s := openStore(t)
	got, err := s.LoadDeadLetters()
	if err != nil {
		t.Fatalf("LoadDeadLetters on empty store: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 dead letters, got %d", len(got))
	}
}

// --- DeleteDeadLetter ---

func TestDeleteDeadLetter(t *testing.T) {
	s := openStore(t)

	dl := quelon.RawDeadLetter{Task: rawTask("d1"), Err: "oops", FailedAt: time.Now()}
	if err := s.SaveDeadLetter(dl); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteDeadLetter("d1"); err != nil {
		t.Fatalf("DeleteDeadLetter: %v", err)
	}

	got, err := s.LoadDeadLetters()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 dead letters after delete, got %d", len(got))
	}
}

func TestDeleteDeadLetterIdempotent(t *testing.T) {
	s := openStore(t)
	if err := s.DeleteDeadLetter("nonexistent"); err != nil {
		t.Errorf("DeleteDeadLetter on missing key: %v", err)
	}
}

// --- key isolation: pending and dead keys must not collide ---

func TestPendingAndDeadKeysDontCollide(t *testing.T) {
	s := openStore(t)

	if err := s.SaveTask(rawTask("shared-id")); err != nil {
		t.Fatal(err)
	}
	dl := quelon.RawDeadLetter{Task: rawTask("shared-id"), Err: "fail", FailedAt: time.Now()}
	if err := s.SaveDeadLetter(dl); err != nil {
		t.Fatal(err)
	}

	pending, err := s.LoadPendingTasks()
	if err != nil {
		t.Fatal(err)
	}
	dead, err := s.LoadDeadLetters()
	if err != nil {
		t.Fatal(err)
	}

	if len(pending) != 1 || pending[0].ID != "shared-id" {
		t.Errorf("pending: %v", pending)
	}
	if len(dead) != 1 || dead[0].Task.ID != "shared-id" {
		t.Errorf("dead: %v", dead)
	}
}

// --- Commit (group commit) ---

func TestCommitAppliesSavesDeadLettersAndDeletes(t *testing.T) {
	s := openStore(t)

	// First window: persist three pending tasks in one atomic commit.
	if err := s.Commit([]quelon.RawTask{rawTask("a"), rawTask("b"), rawTask("c")}, nil, nil); err != nil {
		t.Fatalf("Commit (saves): %v", err)
	}
	if got, _ := s.LoadPendingTasks(); len(got) != 3 {
		t.Fatalf("after save commit: %d pending, want 3", len(got))
	}

	// Second window: "a" succeeds (delete), "b" fails (dead letter + delete),
	// "c" stays pending. One atomic commit settles all of it.
	dl := quelon.RawDeadLetter{Task: rawTask("b"), Err: "boom", FailedAt: time.Now()}
	if err := s.Commit(nil, []string{"a", "b"}, []quelon.RawDeadLetter{dl}); err != nil {
		t.Fatalf("Commit (settle): %v", err)
	}

	pending, err := s.LoadPendingTasks()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != "c" {
		t.Errorf("pending after settle = %v, want [c]", pending)
	}
	dead, err := s.LoadDeadLetters()
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].Task.ID != "b" {
		t.Errorf("dead letters after settle = %v, want [b]", dead)
	}
}

func TestCommitEmptyIsNoOp(t *testing.T) {
	s := openStore(t)
	if err := s.Commit(nil, nil, nil); err != nil {
		t.Errorf("empty Commit: %v", err)
	}
}

// --- end-to-end through the pool's group-commit writer ---

// After a burst of successful tasks settles, the store must hold no pending
// records — the completion deletes are never lost or reordered behind the saves.
func TestPoolGroupCommitLeavesNoPendingOnSuccess(t *testing.T) {
	s := openStore(t)

	pool := quelon.NewPool(func(context.Context, any) error { return nil },
		quelon.WithBufferSize(256),
		quelon.WithWorkerCount(4),
		quelon.WithRetryPolicy(quelon.RetryPolicy{MaxAttempts: 1}),
		quelon.WithStore(s),
	)
	if err := pool.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	go func() {
		for range pool.Results() {
		}
	}()
	for i := 0; i < 500; i++ {
		if err := pool.Submit(t.Context(), quelon.Task{ID: id(i), Payload: i}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	pool.CloseAndWait()

	pending, err := s.LoadPendingTasks()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Errorf("after all tasks succeeded, %d pending records remain (want 0)", len(pending))
	}
}

// A permanently failing task must end up durable as a dead letter and gone from
// pending — the dead-letter write committing before the pending delete.
func TestPoolGroupCommitPersistsDeadLetter(t *testing.T) {
	s := openStore(t)

	pool := quelon.NewPool(func(context.Context, any) error {
		return quelon.NewPermanentError(errStub)
	},
		quelon.WithWorkerCount(1),
		quelon.WithRetryPolicy(quelon.RetryPolicy{MaxAttempts: 1}),
		quelon.WithStore(s),
	)
	if err := pool.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	go func() {
		for range pool.Results() {
		}
	}()
	if err := pool.Submit(t.Context(), quelon.Task{ID: "poison", Payload: 1}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	pool.CloseAndWait()

	pending, err := s.LoadPendingTasks()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Errorf("dead-lettered task still pending: %d", len(pending))
	}
	dead, err := s.LoadDeadLetters()
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].Task.ID != "poison" {
		t.Errorf("dead letters = %v, want [poison]", dead)
	}
}

// --- Store satisfies the quelon interfaces at compile time ---

var (
	_ quelon.Store       = (*pebblestore.Store)(nil)
	_ quelon.CommitStore = (*pebblestore.Store)(nil)
)
