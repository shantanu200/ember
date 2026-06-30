package pebblestore_test

import (
	"testing"
	"time"

	"github.com/shantanu200/quelon"
	pebblestore "github.com/shantanu200/quelon/store/pebble"
)

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

	dls := []quelon.RawDeadLetter{
		{Task: rawTask("d1"), Err: "boom", Permanent: false, FailedAt: time.Now()},
		{Task: rawTask("d2"), Err: "fatal", Permanent: true, FailedAt: time.Now()},
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

// --- Store satisfies the quelon.Store interface at compile time ---

var _ quelon.Store = (*pebblestore.Store)(nil)
