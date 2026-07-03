package quelon

import "time"

// Task is a unit of work submitted to a Pool via Submit. Callers set ID,
// Payload, and optionally Key; the pool owns Seq, EnqueuedAt (if left zero),
// and Attempt, updating them as the task moves through submission, retries,
// and (if persistence is enabled) crash recovery.
type Task struct {
	// ID uniquely identifies the task. It is used as the key for durable
	// storage (pending records and dead letters) and should be stable and
	// unique per logical unit of work — reusing an ID for a different task
	// will overwrite the earlier task's stored record.
	ID string `json:"id"`
	// Key is the optional ordering/partition key. Tasks sharing a non-empty Key
	// are processed one at a time, in submission order, when the pool runs in
	// partitioned mode (see WithPartitions). An empty Key means the task carries
	// no ordering constraint. Ignored unless WithPartitions is set.
	Key string `json:"key,omitempty"`
	// Seq is a monotonic submission sequence number stamped by Submit. It is the
	// tie-break used to replay persisted tasks in submission order after a crash,
	// so per-key FIFO ordering survives a restart regardless of the order the
	// store returns pending tasks in. Assigned by the pool; a value set by the
	// caller is overwritten on Submit.
	Seq uint64 `json:"seq,omitempty"`
	// Payload is the caller-defined data passed to ProcessFunc (or
	// BatchProcessFunc). When persistence is enabled it is serialized with
	// the pool's codec (see WithCodec), so its type must round-trip through
	// that codec — the default (encoding/json) requires exported fields and
	// concrete types decode-able from JSON, not interfaces.
	Payload any `json:"payload"`
	// EnqueuedAt records when the task was accepted by Submit. If left zero
	// by the caller, Submit fills it in with time.Now().
	EnqueuedAt time.Time `json:"enqueued_at"`
	// Attempt is the zero-based index of the current (or, after a failed
	// run, the next) processing attempt. The pool advances it on each retry
	// and, when persistence is enabled, persists it durably so a crash
	// mid-retry resumes with the remaining budget instead of a fresh one.
	Attempt int `json:"attempt"`
}

// Result reports the outcome of one processed Task, delivered on
// Pool.Results(). Err is nil on success; on failure it is the final error
// after retries were exhausted (or the *PermanentError that skipped retries).
type Result struct {
	Task Task
	Err  error
}

// DeadLetter records a task that failed permanently: either it exhausted its
// RetryPolicy's MaxAttempts, or its error was a *PermanentError. It is passed
// to Hooks.OnDeadLetter and, when persistence is enabled, durably archived via
// the configured Store.
type DeadLetter struct {
	Task Task `json:"task"`
	// Err is the final failure's message (Task.Payload's processing error),
	// captured as a string so DeadLetter remains serializable regardless of
	// the underlying error type.
	Err string `json:"err"`
	// Permanent is true when the task failed via a *PermanentError (skipped
	// retries) rather than by exhausting its retry budget.
	Permanent bool `json:"permanent"`
	// FailedAt records when the task was dead-lettered.
	FailedAt time.Time `json:"failed_at"`
}
