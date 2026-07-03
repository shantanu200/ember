package quelon

import "time"

// RawTask is the on-disk counterpart of Task: identical except Payload has
// already been serialized to bytes by the pool's codec (see WithCodec), so a
// Store implementation never needs to know the payload's concrete type.
type RawTask struct {
	ID         string    `json:"id"`
	Key        string    `json:"key,omitempty"`
	Seq        uint64    `json:"seq,omitempty"`
	Payload    []byte    `json:"payload"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	Attempt    int       `json:"attempt"`
}

// RawDeadLetter is the on-disk counterpart of DeadLetter, carrying a RawTask
// (serialized payload) instead of a Task.
type RawDeadLetter struct {
	Task      RawTask   `json:"task"`
	Err       string    `json:"err"`
	Permanent bool      `json:"permanent"`
	FailedAt  time.Time `json:"failed_at"`
}

// Store is the durability interface a Pool writes to and reads from when
// configured via WithStore/WithStoreMode. Implementations must make each
// method safe for concurrent use, since the pool's group-commit writer and
// (for LoadPendingTasks/LoadDeadLetters) Start may call into it. See
// BatchStore and CommitStore for optional interfaces that let a Store batch
// or atomically commit multiple operations for better throughput; a Store
// that implements neither still works correctly, just with a sync per op.
type Store interface {
	// SaveTask durably records a task as pending (write-ahead), so it can be
	// replayed by LoadPendingTasks after a crash. Called on Submit and again
	// on each retry to advance the persisted Attempt.
	SaveTask(RawTask) error
	// DeleteTask removes a task's pending record once its outcome (success or
	// dead-letter) has been recorded, so it is not replayed on restart.
	DeleteTask(id string) error
	// LoadPendingTasks returns every task with a pending record, in any
	// order — the pool sorts by Seq itself before replaying them.
	LoadPendingTasks() ([]RawTask, error)

	// SaveDeadLetter durably archives a permanently failed task.
	SaveDeadLetter(RawDeadLetter) error
	// LoadDeadLetters returns every archived dead letter. The pool itself
	// never calls this; it exists for callers building tooling on top of a
	// Store to inspect or reprocess dead letters.
	LoadDeadLetters() ([]RawDeadLetter, error)
	// DeleteDeadLetter removes an archived dead letter, e.g. after a caller
	// has manually reprocessed or discarded it. The pool itself never calls
	// this.
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

// NoopStore is a Store that discards everything and loads nothing. It is the
// default Store when no WithStore/WithStoreMode option is given, giving the
// pool zero persistence overhead (PersistNone) until the caller opts in.
type NoopStore struct{}

func (NoopStore) SaveTask(RawTask) error                    { return nil }
func (NoopStore) DeleteTask(string) error                   { return nil }
func (NoopStore) LoadPendingTasks() ([]RawTask, error)      { return nil, nil }
func (NoopStore) SaveDeadLetter(RawDeadLetter) error        { return nil }
func (NoopStore) LoadDeadLetters() ([]RawDeadLetter, error) { return nil, nil }
func (NoopStore) DeleteDeadLetter(string) error             { return nil }
func (NoopStore) DeleteTasks([]string) error                { return nil }
func (NoopStore) SaveDeadLetters([]RawDeadLetter) error     { return nil }
