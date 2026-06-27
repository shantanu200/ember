package pebblestore

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/shantanu200/ember"
)

type Store[T any] struct {
	db *pebble.DB
}

func Open[T any](path string) (*Store[T], error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening pebble db at %s: %w", path, err)
	}
	return &Store[T]{db: db}, nil
}

func (s *Store[T]) Close() error {
	return s.db.Close()
}

func pendingKey(id string) []byte {
	return []byte("pending:" + id)
}

func deadKey(id string) []byte {
	return []byte("dead:" + id)
}

func (s *Store[T]) LoadPendingTasks() ([]taskqueue.Task[T], error) {
	lower := []byte("pending:")
	upper := []byte("pending;")

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("creating pending tasks iterator: %w", err)
	}
	defer iter.Close()

	var out []taskqueue.Task[T]
	for iter.First(); iter.Valid(); iter.Next() {
		var t taskqueue.Task[T]
		if err := json.Unmarshal(iter.Value(), &t); err != nil {
			continue
		}
		out = append(out, t)
	}
	return out, iter.Error()
}

func (s *Store[T]) SaveDeadLetter(dl taskqueue.DeadLetter[T]) error {
	data, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("marshaling dead letter for task %s: %w", dl.Task.ID, err)
	}
	return s.db.Set(deadKey(dl.Task.ID), data, pebble.Sync)
}

func (s *Store[T]) LoadDeadLetters() ([]taskqueue.DeadLetter[T], error) {
	lower := []byte("dead:")
	upper := []byte("dead;")

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("creating dead letters iterator: %w", err)
	}
	defer iter.Close()

	var out []taskqueue.DeadLetter[T]
	for iter.First(); iter.Valid(); iter.Next() {
		var dl taskqueue.DeadLetter[T]
		if err := json.Unmarshal(iter.Value(), &dl); err != nil {
			continue
		}
		out = append(out, dl)
	}

	return out, iter.Error()
}

func (s *Store[T]) DeleteDeadLetter(id string) error {
	return s.db.Delete(deadKey(id), pebble.Sync)
}
