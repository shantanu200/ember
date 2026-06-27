package ember

import "time"

type Task[T any] struct {
	ID         string
	Payload    T
	EnqueuedAt time.Time
	Attempt    int
}

type Result[T any] struct {
	Task Task[T]
	Err  error
}

type DeadLetter[T any] struct {
	Task        Task[T]
	Err         string
	Permanent   bool
	FailedAt    time.Time
	ReplayCount int
	LastReplay  time.Time
}
