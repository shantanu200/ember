package ember

import "time"

type RawTask struct {
	ID         string
	Payload    []byte
	EnqueuedAt time.Time
	Attempt    int
}

type RawDeadLetter struct {
	Task      RawTask
	Err       string
	Permanent bool
	FailedAt  time.Time
}

type Store interface {
	SaveTask(RawTask) error
	DeleteTask(id string) error
	LoadPendingTasks() ([]RawTask, error)

	SaveDeadLetter(RawDeadLetter) error
	LoadDeadLetters() ([]RawDeadLetter, error)
	DeleteDeadLetter(id string) error
}

// BatchStore is an optional interface a Store may implement to settle a whole
// batch in one round-trip instead of one call per task. When batching is
// enabled (see WithBatching), the pool type-asserts the configured Store for
// BatchStore and uses it if present, falling back to the per-item Store methods
// otherwise. Implementing it is purely a performance optimisation.
type BatchStore interface {
	DeleteTasks(ids []string) error
	SaveDeadLetters([]RawDeadLetter) error
}

type NoopStore struct{}

func (NoopStore) SaveTask(RawTask) error                    { return nil }
func (NoopStore) DeleteTask(string) error                   { return nil }
func (NoopStore) LoadPendingTasks() ([]RawTask, error)      { return nil, nil }
func (NoopStore) SaveDeadLetter(RawDeadLetter) error        { return nil }
func (NoopStore) LoadDeadLetters() ([]RawDeadLetter, error) { return nil, nil }
func (NoopStore) DeleteDeadLetter(string) error             { return nil }
func (NoopStore) DeleteTasks([]string) error                { return nil }
func (NoopStore) SaveDeadLetters([]RawDeadLetter) error     { return nil }
