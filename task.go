package quelon

import "time"

type Task struct {
	ID string `json:"id"`
	// Key is the optional ordering/partition key. Tasks sharing a non-empty Key
	// are processed one at a time, in submission order, when the pool runs in
	// partitioned mode (see WithPartitions). An empty Key means the task carries
	// no ordering constraint. Ignored unless WithPartitions is set.
	Key        string    `json:"key,omitempty"`
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
