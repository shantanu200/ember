package quelon

import "time"

type Task struct {
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
	Seq        uint64    `json:"seq,omitempty"`
	Payload    any       `json:"payload"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	Attempt    int       `json:"attempt"`
}

type Result struct {
	Task Task
	Err  error
}

type DeadLetter struct {
	Task      Task      `json:"task"`
	Err       string    `json:"err"`
	Permanent bool      `json:"permanent"`
	FailedAt  time.Time `json:"failed_at"`
}
