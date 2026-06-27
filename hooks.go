package ember

type Hooks[T any] struct {
	OnSuccess    func(Task[T])
	OnRetry      func(task Task[T], err error, attempt int)
	OnDeadLetter func(DeadLetter[T])
	OnStoreError func(err error)
}
