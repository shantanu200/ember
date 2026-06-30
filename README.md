# quelon

A lightweight, concurrent worker pool for Go with retries, dead-lettering, optional
auto-scaling, and pluggable durable storage.

The core library depends only on the standard library — installing it pulls in **zero
external dependencies**. Durability (persisting tasks across restarts) is opt-in through a
separate module so you never pay for what you don't use.

## Install

Core pool (stdlib only):

```bash
go get github.com/shantanu200/quelon
```

Optional Pebble-backed durable store (separate module, pulls in `cockroachdb/pebble`):

```bash
go get github.com/shantanu200/quelon/store/pebble
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/shantanu200/quelon"
)

func main() {
	ctx := context.Background()

	pool := quelon.NewPool(func(ctx context.Context, payload any) error {
		fmt.Printf("processing %v\n", payload)
		return nil
	},
		quelon.WithBufferSize(128), // defaults to runtime.NumCPU()*10
		quelon.WithWorkerCount(4),  // defaults to runtime.NumCPU()
	)

	if err := pool.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Drain results in the background.
	go func() {
		for r := range pool.Results() {
			if r.Err != nil {
				log.Printf("task %s failed: %v", r.Task.ID, r.Err)
			}
		}
	}()

	for i := 0; i < 10; i++ {
		if err := pool.Submit(ctx, quelon.Task{ID: fmt.Sprintf("t-%d", i), Payload: i}); err != nil {
			log.Printf("submit failed: %v", err) // e.g. quelon.ErrBufferFull
		}
	}

	pool.CloseAndWait() // stops workers, drains in flight, closes Results()
}
```

## Core concepts

| Type | Purpose |
|------|---------|
| `Pool` | The worker pool. Create with `NewPool`, run with `Start`, feed with `Submit`. |
| `ProcessFunc` | `func(ctx context.Context, payload any) error` — your per-task work. |
| `Task` | `{ ID, Payload, EnqueuedAt, Attempt }` — unit of work. |
| `Result` | `{ Task, Err }` — emitted on `Results()` after each task settles. |
| `RetryPolicy` | Backoff config: `MaxAttempts`, `BaseDelay`, `MaxDelay`, `JitterFactor`. |
| `Hooks` | Lifecycle callbacks: `OnSuccess`, `OnRetry`, `OnDeadLetter`, `OnStoreError`. |
| `Store` | Interface for durable task persistence. Defaults to `NoopStore`. |

### Lifecycle

- `Submit` returns `ErrBufferFull` immediately when the job buffer is full (non-blocking),
  or the context error if `ctx` is cancelled.
- `CloseAndWait` stops accepting work, waits for in-flight tasks to finish, then closes the
  `Results()` channel. Cancelling the `ctx` passed to `Start` stops workers without draining.

## Options

```go
pool := quelon.NewPool(process,
	quelon.WithBufferSize(256),
	quelon.WithWorkerCount(8),
	quelon.WithRetryPolicy(quelon.RetryPolicy{
		MaxAttempts:  5,
		BaseDelay:    200 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		JitterFactor: 0.5, // 0 = no jitter, 1.0 = full jitter
	}),
	quelon.WithTaskTimeout(15*time.Second), // per-attempt timeout (default 30s)
	quelon.WithHooks(quelon.Hooks{
		OnDeadLetter: func(dl quelon.DeadLetter) { /* alert / metric */ },
	}),
	quelon.WithLogger(slog.Default()), // off by default
)
```

| Option | Effect |
|--------|--------|
| `WithBufferSize(n)` | Job buffer capacity. Default: `runtime.NumCPU()*10`. |
| `WithWorkerCount(n)` | Number of workers. Default: `runtime.NumCPU()`. |
| `WithRetryPolicy(r)` | Exponential backoff config. Default: 3 attempts, 200ms base, 10s cap. |
| `WithTaskTimeout(d)` | Per-attempt context timeout. Default 30s. |
| `WithHooks(h)` | Success / retry / dead-letter / store-error callbacks. |
| `WithLogger(l)` | Attach an `*slog.Logger`. Logs nothing by default. |
| `WithCodec(enc, dec)` | Custom payload encode/decode for the store. Defaults to JSON. |
| `WithStore(s)` | Durable task persistence (see below). |
| `WithDynamicWorkers(min, max, threshold)` | Auto-scale workers (see below). |
| `WithMaxBatchSize(n)` | Max tasks per batch (`NewPoolWithBatch` only). Default: 10. |
| `WithMaxBatchWait(d)` | Max wait to fill a batch. 0 = best-effort (`NewPoolWithBatch` only). |

## Retries and dead-lettering

Each task runs up to `MaxAttempts` times with exponential backoff (`BaseDelay << attempt`,
capped at `MaxDelay`, optionally jittered). When all attempts are exhausted, the task is
**dead-lettered** — surfaced via `Hooks.OnDeadLetter` and (if a store is attached) persisted.

To fail fast without retrying, wrap the error as permanent:

```go
func process(ctx context.Context, payload any) error {
	if invalid(payload) {
		return quelon.NewPermanentError(errors.New("bad payload"))
	}
	return nil
}
```

`quelon.IsPermanent(err)` reports whether an error is permanent.

## Auto-scaling workers

`WithDynamicWorkers` lets the pool grow under load and shrink back when idle:

```go
pool := quelon.NewPool(process,
	quelon.WithBufferSize(1024),
	quelon.WithDynamicWorkers(4, 64, 0.5), // min, max, scale threshold
)
pool.Start(ctx) // starts at min workers; WithWorkerCount is ignored in dynamic mode
```

A supervisor samples the job-buffer fill level; when it crosses `threshold` (a fraction in
`(0,1]`), the worker count grows multiplicatively up to `max`. Burst workers retire after an
idle period, returning the pool to `min`. Inspect the live count with `pool.ActiveWorkers()`.

## Batching

For message consumers (Kafka, SQS, Pub/Sub) it's often far cheaper to handle many
messages per call — one bulk DB write, one batched downstream request, one batch ack —
than one message at a time. `NewPoolWithBatch` makes each worker pull up to `maxSize`
tasks and hand them to a batch processor, while multiple workers still run in parallel:

```go
pool := quelon.NewPoolWithBatch(
	func(ctx context.Context, tasks []quelon.Task) []error {
		errs := make([]error, len(tasks)) // errs[i] belongs to tasks[i]; nil = success
		rows := make([]Row, len(tasks))
		for i, t := range tasks {
			rows[i] = t.Payload.(Row)
		}
		if err := bulkInsert(ctx, rows); err != nil {
			for i := range errs {
				errs[i] = err // whole batch failed
			}
		}
		return errs
	},
	quelon.WithBufferSize(1024),
	quelon.WithMaxBatchSize(100),
	quelon.WithMaxBatchWait(50*time.Millisecond),
	quelon.WithWorkerCount(8), // 8 workers, each forming its own batch
)
pool.Start(ctx)

for _, row := range incoming {
	pool.Submit(ctx, quelon.Task{ID: row.ID, Payload: row})
}
```

A worker blocks for the first task, then collects more until the batch hits `maxSize` or
`maxWait` elapses since the first task — whichever comes first — so a trickle of work is
never left unflushed. `maxWait <= 0` is best-effort: take whatever is already buffered
without lingering.

**Per-item outcomes.** The processor returns one error per task, indexed to match the
input slice (`nil` = that task succeeded). quelon settles each task individually:

- successes are acked immediately and fire `OnSuccess`;
- transient failures are retried **per item** — only the still-failing tasks are
  re-submitted to the processor on the next attempt, so successful messages are never
  reprocessed;
- permanent errors (`NewPermanentError`) and retry-exhausted tasks are dead-lettered
  individually via `OnDeadLetter`.

Returning a `nil` (or shorter) slice treats the unaddressed tasks as successful. One
`Result` is still emitted per task on `Results()`, so the rest of the API is unchanged.
Batching composes with `WithDynamicWorkers` and `WithStore`; producers keep calling
`Submit` with individual tasks.

A `Store` may optionally implement `BatchStore` (`DeleteTasks`, `SaveDeadLetters`) to
settle a whole batch in one round-trip; quelon falls back to the per-item `Store` methods
when it doesn't.

## Durable storage

By default the pool keeps tasks in memory only (`NoopStore`). Attach a `Store` to persist
pending tasks and dead letters so they survive restarts — on `Start`, pending tasks are
reloaded and re-enqueued automatically.

```go
import (
	"github.com/shantanu200/quelon"
	pebblestore "github.com/shantanu200/quelon/store/pebble"
)

store, err := pebblestore.Open("./data/queue")
if err != nil {
	log.Fatal(err)
}
defer store.Close()

pool := quelon.NewPool(process, quelon.WithBufferSize(256), quelon.WithStore(store))
```

Implement the `quelon.Store` interface to back the pool with any other engine (Redis, SQL,
etc.); keeping the interface in the core is what lets heavy implementations live in their own
submodule without bloating the core's dependency graph.

## License

See repository.
