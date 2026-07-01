package quelon

import "time"

type Task struct {
	ID         string    `json:"id"`
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
