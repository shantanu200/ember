// Package quelon is an in-process concurrent worker pool for Go: submit tasks,
// process them across a pool of workers with retries and dead-lettering, and
// optionally persist work so it survives a restart.
//
// # Mental model
//
// A [Pool] is a queue plus a processing function plus failure handling. You
// hand it a [ProcessFunc] (one task per call) or a [BatchProcessFunc] (many per
// call), feed it [Task] values with Submit, and read outcomes as [Result]
// values from Results. Work that keeps failing is retried per a [RetryPolicy]
// and finally recorded as a [DeadLetter]. Attach a [Store] and the pool becomes
// durable: pending tasks are written ahead and reloaded on the next Start.
//
// The pool is a local executor, not a message broker. It has no topics or
// subscribers; a single pool runs a single processing function. To route a
// variety of event types through one pool, and to fan one event out to several
// handlers, wrap that dispatch in a [Mux] (see "Event routing" below).
//
// # Quick start
//
//	pool := quelon.NewPool(func(ctx context.Context, payload any) error {
//		fmt.Println("processing", payload)
//		return nil
//	}, quelon.WithWorkerCount(8))
//
//	if err := pool.Start(ctx); err != nil {
//		log.Fatal(err)
//	}
//	pool.Submit(ctx, quelon.Task{ID: "t1", Payload: "hello"})
//	pool.CloseAndWait() // drain in-flight work and release goroutines
//
// # Lifecycle
//
// Construct with [NewPool] or [NewPoolWithBatch], configure with Options, then:
//
//	Start(ctx)      // spin up workers; on a durable pool, reload pending tasks
//	Submit(ctx, t)  // enqueue a task (non-blocking; returns ErrBufferFull when full)
//	Results()       // <-chan Result: one Result per task, success or final failure
//	CloseAndWait()  // stop accepting work, finish in-flight tasks, flush the store
//
// A zero Pool is not usable; always construct via a constructor. Read Results in
// its own goroutine if you care about outcomes — a pool that no one drains will
// block workers once the results buffer fills.
//
// # Retries and failure
//
// A returned error retries the task per the pool's [RetryPolicy] (attempts,
// backoff, optional jitter). Two ways to skip retries and dead-letter
// immediately: return a [PermanentError] (via [NewPermanentError]) for a task
// that can never succeed, such as a malformed payload. A task that exhausts its
// retry budget is dead-lettered too. Test any error with [IsPermanent]. A panic
// inside a processing function is recovered and treated as a returned error.
//
// # Ordered processing
//
// By default any worker may pick up any task, so tasks run concurrently and can
// complete out of order. When correctness depends on per-key order (event
// sourcing, CDC, per-account state machines), [WithPartitions] routes each task
// by its [Task] Key to one of n serial lanes: tasks sharing a Key are processed
// one at a time in submission order, while different keys still run in parallel.
// Ordering is inherently at odds with elastic scaling, so [WithPartitions] is
// mutually exclusive with [WithDynamicWorkers]. Use it only for the keys that
// need order; leave everything else on the default work-stealing pool.
//
// # Persistence
//
// By default the pool is memory-only. [WithStore] (or [WithStoreMode] for finer
// control via [PersistMode]) attaches a durable [Store]. Persistence is
// asynchronous and group-committed (tune with [WithGroupCommit]): ingestion
// never pays a per-task fsync, and in exchange a crash can lose work accepted
// within the current flush window. The in-tree Pebble store lives in its own
// module (store/pebble) so the core stays dependency-free; implement [Store]
// (optionally [BatchStore] or [CommitStore]) to back the pool with any engine.
//
// # Event routing
//
// [Mux] turns a pool's single [ProcessFunc] into event-type routing with
// fan-out. Payloads implement [Event] (an EventType tag); handlers register per
// event; mux.Process is the func you pass to [NewPool]. Registering two handlers
// for one event type fans out. Because mux.Process is stable the moment
// [NewMux] returns, you pass it to the pool up front and register handlers
// afterward — which also breaks the construction cycle that arises when a
// handler's dependencies need the pool while the pool needs the handler. Pass
// [Mux.Codec] alongside a store so persisted events replay into their concrete
// type rather than a generic map.
//
// # API map
//
// Construction:
//   - [NewPool], [NewPoolWithBatch] — build a single- or batch-processing pool
//   - [NewMux] — an event-routing dispatcher used as the pool's ProcessFunc
//
// Options (passed to the constructors):
//   - Concurrency: [WithWorkerCount], [WithBufferSize], [WithDynamicWorkers],
//     [WithMachineAwareLimit]
//   - Ordering: [WithPartitions] (routes by [Task] Key to serial lanes)
//   - Processing: [WithTaskTimeout], [WithRetryPolicy], [WithMaxBatchSize],
//     [WithMaxBatchWait]
//   - Durability: [WithStore], [WithStoreMode], [WithGroupCommit], [WithCodec]
//   - Observability: [WithHooks], [WithLogger]
//
// Submitting and results: [Task], [Result], [DeadLetter], [Pool.Submit],
// [Pool.Results].
//
// Processing: [ProcessFunc], [BatchProcessFunc].
//
// Failure: [RetryPolicy], [DefaultRetryPolicy], [PermanentError],
// [NewPermanentError], [IsPermanent], [ErrBufferFull].
//
// Persistence contracts: [Store], [BatchStore], [CommitStore], [PersistMode],
// [RawTask], [RawDeadLetter], [NoopStore].
//
// Event routing: [Mux], [Event].
//
// Observability: [Hooks].
package quelon
