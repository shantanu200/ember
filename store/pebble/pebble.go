package pebblestore

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/shantanu200/quelon"
)

type Store struct {
	db *pebble.DB
}

func Open(path string) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening pebble db at %s: %w", path, err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func pendingKey(id string) []byte { return []byte("pending:" + id) }
func deadKey(id string) []byte    { return []byte("dead:" + id) }

func (s *Store) SaveTask(t quelon.RawTask) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshaling task %s: %w", t.ID, err)
	}
	return s.db.Set(pendingKey(t.ID), data, pebble.Sync)
}

func (s *Store) DeleteTask(id string) error {
	return s.db.Delete(pendingKey(id), pebble.Sync)
}

func (s *Store) LoadPendingTasks() ([]quelon.RawTask, error) {
	lower := []byte("pending:")
	upper := []byte("pending;")

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("creating pending tasks iterator: %w", err)
	}
	defer iter.Close()

	var out []quelon.RawTask
	for iter.First(); iter.Valid(); iter.Next() {
		var t quelon.RawTask
		if err := json.Unmarshal(iter.Value(), &t); err != nil {
			return nil, fmt.Errorf("unmarshaling pending task: %w", err)
		}
		out = append(out, t)
	}
	return out, iter.Error()
}

func (s *Store) SaveDeadLetter(dl quelon.RawDeadLetter) error {
	data, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("marshaling dead letter for task %s: %w", dl.Task.ID, err)
	}
	return s.db.Set(deadKey(dl.Task.ID), data, pebble.Sync)
}

func (s *Store) LoadDeadLetters() ([]quelon.RawDeadLetter, error) {
	lower := []byte("dead:")
	upper := []byte("dead;")

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("creating dead letters iterator: %w", err)
	}
	defer iter.Close()

	var out []quelon.RawDeadLetter
	for iter.First(); iter.Valid(); iter.Next() {
		var dl quelon.RawDeadLetter
		if err := json.Unmarshal(iter.Value(), &dl); err != nil {
			return nil, fmt.Errorf("unmarshaling dead letter: %w", err)
		}
		out = append(out, dl)
	}
	return out, iter.Error()
}

func (s *Store) DeleteDeadLetter(id string) error {
	return s.db.Delete(deadKey(id), pebble.Sync)
}

// Commit applies a whole group-commit window atomically with a single fsync.
// The staged Set/Delete calls take no per-op sync option; the durability cost
// is paid once, in Apply(pebble.Sync), amortised across the entire batch — the
// difference between one fsync per task and one per flush window. Saves are
// applied before dead letters before deletes, so a failed task is durable as a
// dead letter before its pending record is removed, and the batch is atomic so
// the window commits in full or not at all.
func (s *Store) Commit(saves []quelon.RawTask, deletes []string, deadLetters []quelon.RawDeadLetter) error {
	b := s.db.NewBatch()
	defer b.Close()

	for _, t := range saves {
		data, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("marshaling task %s: %w", t.ID, err)
		}
		if err := b.Set(pendingKey(t.ID), data, nil); err != nil {
			return fmt.Errorf("staging task %s: %w", t.ID, err)
		}
	}
	for _, dl := range deadLetters {
		data, err := json.Marshal(dl)
		if err != nil {
			return fmt.Errorf("marshaling dead letter for task %s: %w", dl.Task.ID, err)
		}
		if err := b.Set(deadKey(dl.Task.ID), data, nil); err != nil {
			return fmt.Errorf("staging dead letter %s: %w", dl.Task.ID, err)
		}
	}
	for _, id := range deletes {
		if err := b.Delete(pendingKey(id), nil); err != nil {
			return fmt.Errorf("staging delete %s: %w", id, err)
		}
	}

	return s.db.Apply(b, pebble.Sync)
}
