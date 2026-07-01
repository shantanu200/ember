package quelon

import "time"

type RawTask struct {
	ID         string    `json:"id"`
	Payload    []byte    `json:"payload"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	Attempt    int       `json:"attempt"`
}

type RawDeadLetter struct {
	Task      RawTask   `json:"task"`
	Err       string    `json:"err"`
	Permanent bool      `json:"permanent"`
	FailedAt  time.Time `json:"failed_at"`
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
// enabled (see NewPoolWithBatch), the pool type-asserts the configured Store for
// BatchStore and uses it if present, falling back to the per-item Store methods
// otherwise. Implementing it is purely a performance optimisation.
type BatchStore interface {
	DeleteTasks(ids []string) error
	SaveDeadLetters([]RawDeadLetter) error
}

// CommitStore is an optional interface a Store may implement to apply a whole
// group of pending-store mutations atomically with a single durable commit (one
// fsync), instead of one synced write per task. The pool's group-commit writer
// coalesces many submit/complete/retry operations into one Commit call; stores
// that don't implement it fall back to the per-item Store methods, which is
// correct but pays a sync per operation. The implementation must apply the
// saves, then the dead letters, then the deletes — so a failed task is durable
// as a dead letter before its pending record is removed — ideally in one atomic
// batch so the whole window commits or none of it does.
type CommitStore interface {
	Commit(saves []RawTask, deletes []string, deadLetters []RawDeadLetter) error
}

// PersistMode selects what a configured Store is used for. It maps to the
// durability/availability trade-off the same way a message broker's acks
// setting does: who owns durability of in-flight work.
type PersistMode int

const (
	// PersistNone disables persistence (the default; equivalent to NoopStore).
	PersistNone PersistMode = iota
	// PersistDeadLettersOnly uses the store purely as a dead-letter archive.
	// Ingestion never touches the store, so it is never blocked by it; in
	// exchange there is no crash recovery of pending work. This is the natural
	// fit when an upstream broker (Kafka, SQS, Pub/Sub) already owns durability
	// and you only need somewhere to park poison messages.
	PersistDeadLettersOnly
	// PersistAll uses the store as a write-ahead log plus dead-letter archive:
	// pending tasks are persisted (via the group-commit writer) so they survive
	// a crash and replay on restart. This is the mode for a front-door queue
	// where Submit is the only durable record of accepted work.
	PersistAll
)

type NoopStore struct{}

func (NoopStore) SaveTask(RawTask) error                    { return nil }
func (NoopStore) DeleteTask(string) error                   { return nil }
func (NoopStore) LoadPendingTasks() ([]RawTask, error)      { return nil, nil }
func (NoopStore) SaveDeadLetter(RawDeadLetter) error        { return nil }
func (NoopStore) LoadDeadLetters() ([]RawDeadLetter, error) { return nil, nil }
func (NoopStore) DeleteDeadLetter(string) error             { return nil }
func (NoopStore) DeleteTasks([]string) error                { return nil }
func (NoopStore) SaveDeadLetters([]RawDeadLetter) error     { return nil }
