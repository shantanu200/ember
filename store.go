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

type NoopStore struct{}

func (NoopStore) SaveTask(RawTask) error                    { return nil }
func (NoopStore) DeleteTask(string) error                   { return nil }
func (NoopStore) LoadPendingTasks() ([]RawTask, error)      { return nil, nil }
func (NoopStore) SaveDeadLetter(RawDeadLetter) error        { return nil }
func (NoopStore) LoadDeadLetters() ([]RawDeadLetter, error) { return nil, nil }
func (NoopStore) DeleteDeadLetter(string) error             { return nil }
