package quelon

import "time"

type Task struct {
	ID         string
	Payload    any
	EnqueuedAt time.Time
	Attempt    int
}

type Result struct {
	Task Task
	Err  error
}

type DeadLetter struct {
	Task      Task
	Err       string
	Permanent bool
	FailedAt  time.Time
}
