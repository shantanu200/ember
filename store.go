package ember

type Store[T any] interface {
	SaveTask(Task[T]) error
	DeleteTask(id string) error
	LoadPendingTasks() ([]Task[T], error)

	SaveDeadLetter(DeadLetter[T]) error
	LoadDeadLetters() ([]DeadLetter[T], error)
	DeleteDeadLetter(id string) error
}

type NoopStore[T any] struct{}

func (NoopStore[T]) SaveTask(Task[T]) error {
	return nil
}

func (NoopStore[T]) DeleteTask(string) error {
	return nil
}

func (NoopStore[T]) LoadPendingTasks() ([]Task[T], error) {
	return nil, nil
}

func (NoopStore[T]) SaveDeadLetter(DeadLetter[T]) error {
	return nil
}

func (NoopStore[T]) LoadDeadLetters() ([]DeadLetter[T], error) {
	return nil, nil
}

func (NoopStore[T]) DeleteDeadLetter(string) error {
	return nil
}
