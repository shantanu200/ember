package quelon

// Hooks are optional callbacks invoked synchronously as tasks move through
// the pool. A nil field is simply skipped. Hooks run on the worker goroutine
// handling the task, so a slow hook delays that worker from picking up its
// next task; keep hooks fast or hand off work asynchronously yourself.
type Hooks struct {
	// OnSuccess fires once a task's ProcessFunc (or BatchProcessFunc entry)
	// returns nil, after the task has been removed from the pending store (if
	// persistence is enabled) but before its Result is emitted.
	OnSuccess func(Task)
	// OnRetry fires after a transient failure, once the pool has decided to
	// retry rather than dead-letter, before the backoff delay is applied.
	// attempt is the zero-based attempt index that just failed.
	OnRetry func(task Task, err error, attempt int)
	// OnDeadLetter fires when a task exhausts its retry budget or fails with
	// a *PermanentError (see NewPermanentError), after the dead letter has
	// been queued for durable storage (if persistence is enabled).
	OnDeadLetter func(DeadLetter)
	// OnStoreError fires whenever a durable-store operation (save, delete,
	// dead-letter, or group commit) fails. The task itself still completes
	// in memory — a store failure never blocks processing — so this is the
	// only signal that durability may have been lost for that operation.
	OnStoreError func(err error)
}
