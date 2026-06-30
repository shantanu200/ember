package quelon

type Hooks struct {
	OnSuccess    func(Task)
	OnRetry      func(task Task, err error, attempt int)
	OnDeadLetter func(DeadLetter)
	OnStoreError func(err error)
}
