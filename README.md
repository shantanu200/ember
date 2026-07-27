# quelon

A lightweight, concurrent worker pool for Go with retries, dead-lettering, optional
auto-scaling, and pluggable durable storage.

The core library depends only on the standard library — installing it pulls in **zero
external dependencies**. Durability (persisting tasks across restarts) is opt-in through a
separate module so you never pay for what you don't use.

Requires Go 1.25+ (uses `sync.WaitGroup.Go`, `b.Loop`, `t.Context`/`b.Context`).

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

## Motivation

Most Go queues force a topology choice up front. Broker-backed queues (Postgres,
Redis, a message broker) give you durability and retries, but every task is a
network hop and an operational dependency, and you persist *everything*.
In-process goroutine pools give you speed and zero dependencies, but a crash
loses in-flight work and there's no retry or dead-letter story.

quelon collapses that choice: **an async queue that lives inside the server
process** — same address space, no broker hop — while still keeping a
write-ahead log and a retry/dead-letter mechanism so accepted work survives
transient failures and restarts.

```
             ┌──────────────── your server process ────────────────┐
  request ──►│ Submit ─► [ in-memory buffer ] ─► workers ─► ProcessFunc
  handler    │                  │                   │              │
             │                  ▼                   ▼              ▼
             │         group-commit WAL         retry w/       dead-letter
             │         (async, batched)         backoff        archive
             └──────────────────┬──────────────────────────────────┘
                                ▼
                        reload pending on Start
```

- **Produce and consume in one component.** The server *produces* by calling
  `Submit` on the request hot path — non-blocking, returns `ErrBufferFull`
  instead of stalling the handler — and the same pool *consumes* via its worker
  goroutines. No separate producer/consumer deployment.
- **The log.** `WithStore` write-ahead-logs each `Submit` through a group-commit
  writer (one durable commit per flush window, tuned with `WithGroupCommit`), so
  ingestion never blocks on a per-task fsync; pending tasks reload on `Start`.
  It's *group-committed durability, not persist-before-ack* — a crash can lose
  the current flush window (see [Persistence modes](#persistence-modes--who-owns-durability)).
- **The retry mechanism.** Exponential backoff with jitter, fail-fast permanent
  errors, and dead-lettering of exhausted tasks — see below.

The core stays zero-dependency, so you get all of this by embedding a package,
not by standing up another system.

### When not to use quelon

Being in-process is quelon's strength and also its boundary. Reach for something
else when:

- **You need cross-process or cross-node work distribution.** quelon runs inside
  one server; it does no routing, sharding, or rebalancing across instances. For
  a shared queue many services drain, use a broker (Redis/Kafka/SQS) or a
  DB-backed queue. quelon can sit *behind* one as a node-local consumer.
- **You need global exactly-once or transactional enqueue.** Durability is
  group-committed, not persist-before-ack, and retries are at-least-once — so
  handlers must be idempotent. If a task must commit atomically with your DB
  transaction, a transactional queue (e.g. a Postgres-backed one) fits better.
- **You cannot lose a single task on crash.** A crash can drop the current
  flush window. Shrink it with `WithGroupCommit`, or let an upstream log own
  durability and run quelon in `PersistDeadLettersOnly`.
- **Work must outlive the process regardless of restarts, or scale workers
  independently of the producer.** A separate queue + worker fleet decouples
  producer and consumer lifecycles in a way an embedded pool cannot.

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
| `WithStore(s)` | Durable task persistence in `PersistAll` mode (see below). |
| `WithStoreMode(s, mode)` | Durable persistence with an explicit `PersistMode` (see below). |
| `WithGroupCommit(size, every)` | Tune the store writer: flush every `size` ops or `every` interval. Default: 256 / 5ms. |
| `WithDynamicWorkers(min, max, threshold)` | Auto-scale workers (see below). Mutually exclusive with `WithPartitions`. |
| `WithMachineAwareLimit()` | Clamp `WithDynamicWorkers`' `max` to `GOMAXPROCS(0)` if it exceeds it (see below). |
| `WithPartitions(n)` | Ordered mode: route by `Task.Key` to `n` serial lanes (see below). |
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

`WithMachineAwareLimit()` is an opt-in guard against over-provisioning: if `max` exceeds
`runtime.GOMAXPROCS(0)`, it's clamped down to it (with a logged warning). `GOMAXPROCS` honors
container/cgroup CPU quotas when paired with a library that sets it accordingly (e.g.
`uber-go/automaxprocs`), so this is useful for CPU-bound workloads sized to fit a container.
It's off by default because I/O-bound workloads legitimately run more goroutine-workers than
CPU cores.

## Ordered processing (partitions)

By default any worker can pick up any task, so tasks are processed concurrently
and can complete out of order — ideal when work is independent, wrong when
correctness depends on per-key order (event sourcing, CDC, per-account state
machines). `WithPartitions` gives each task an ordering key and guarantees that
**tasks sharing a `Key` are processed one at a time, in submission order**, while
different keys still run in parallel:

```go
pool := quelon.NewPool(process, quelon.WithPartitions(16))

pool.Submit(ctx, quelon.Task{ID: "e1", Key: acct, Payload: evt}) // same Key → same lane,
pool.Submit(ctx, quelon.Task{ID: "e2", Key: acct, Payload: evt}) // serial and in order
```

A task is routed to lane `fnv1a(Key) % n`; each lane is a buffered channel
(sized to `WithBufferSize`) drained by a single dedicated worker. Tasks with an
empty `Key` carry no ordering constraint and are spread across lanes by their
`ID`. The persisted `Key` is honored on reload, so same-key tasks that survive a
crash return to the same lane — and each task's monotonic submission sequence
(`Task.Seq`, stamped by `Submit`) is replayed in order, so per-key FIFO is
restored even if the store returns pending tasks out of order.

The trade-offs are inherent to ordering: a slow or **retrying key blocks other
keys hashed to its lane** (head-of-line blocking), a hot key cannot spread beyond
its lane, and lanes are fixed — so `WithPartitions` is **mutually exclusive with
`WithDynamicWorkers`** (dynamic scaling is disabled, with a warning, if both are
set). Reach for it only on the keys that need order; leave everything else on the
default work-stealing pool.

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

## Event routing with `Mux`

A pool has one `ProcessFunc`. When one pool must handle a *variety* of event types — and
fan a single event out to several independent reactions — wrap that dispatch in a `Mux`
instead of hand-writing a type switch. Payloads that implement `Event` (an `EventType()
string` tag) are routed to the handlers registered for them; registering more than one
handler for the same event type fans out.

```go
type BatchCreated struct {
	BatchID  string   `json:"batchID"`
	Subjects []string `json:"subjects"`
	Faculty  []string `json:"faculty"`
}

func (BatchCreated) EventType() string { return "batch.created" }

mux := quelon.NewMux()
mux.Handle(BatchCreated{}, facultySvc.OnBatchCreated) // one event…
mux.Handle(BatchCreated{}, subjectSvc.OnBatchCreated) // …two reactions

pool := quelon.NewPool(mux.Process, mux.Codec(), quelon.WithStore(store))
pool.Start(ctx)

// Publishers submit the typed event; use Key for per-entity ordering.
pool.Submit(ctx, quelon.Task{ID: "batch-created-" + id, Key: id, Payload: BatchCreated{...}})
```

`Mux` also dissolves the construction cycle you hit when a handler's dependencies need the
pool (to `Submit`) while the pool needs the handler (to process): `mux.Process` is a stable
method the moment `NewMux()` returns, so you pass it to `NewPool` up front and register
handlers afterward, once services are wired.

Pass `mux.Codec()` alongside any `WithStore`/`WithStoreMode`: it writes a `{type, data}`
envelope so a persisted task **replays back into its concrete event type** after a crash.
Without it the default codec decodes payloads into a generic map, which `Process` cannot
route. An unroutable task (not an `Event`, or no handler for its type) is dead-lettered, not
retried. On fan-out the first handler error stops the rest and the whole task retries, so
handlers must be **idempotent**; if that is not acceptable, submit one task per reaction
instead so each carries its own retry budget and dead-letter record.

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

### Persistence modes — who owns durability

Ingestion never blocks on a per-task disk sync. All store mutations flow through a single
**group-commit writer** that batches them into one durable commit per flush window (tune with
`WithGroupCommit`) — the same trade-off a broker's producer batching (`linger.ms` / `acks`)
makes. `WithStoreMode` picks what the store is for:

| Mode | Store is… | Ingestion | Crash recovery of pending work |
|------|-----------|-----------|--------------------------------|
| `PersistNone` (default) | unused | never touches store | none |
| `PersistDeadLettersOnly` | a dead-letter archive | never touches store | none — the upstream source (Kafka/SQS/…) owns durability |
| `PersistAll` (`WithStore`) | write-ahead log + dead-letter archive | persisted via the writer | pending tasks reload on `Start` |

```go
// Broker-backed consumer: re-consume on restart, only archive poison messages.
pool := quelon.NewPoolWithBatch(process, quelon.WithStoreMode(store, quelon.PersistDeadLettersOnly))

// Front-door queue: Submit is the only durable record of accepted work.
pool := quelon.NewPool(process, quelon.WithStore(store)) // == PersistAll
```

Because persistence is asynchronous, `PersistAll` is **group-committed durability, not
persist-before-ack**: a crash can lose work accepted within the current flush window (bounded
by `WithGroupCommit`). A `Store` may implement the optional `CommitStore` interface
(`Commit(saves, deletes, deadLetters)`) to apply a whole window atomically with one fsync — the
Pebble store does. Stores without it fall back to per-item `Store`/`BatchStore` calls.

## License

See repository.
