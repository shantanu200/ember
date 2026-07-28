# quelon

An in-process worker pool for Go with retries, dead-lettering, optional auto-scaling,
and pluggable durable storage.

The core has **zero external dependencies** — stdlib only. Durability is opt-in through a
separate module.

Requires Go 1.25+.

## Install

```bash
go get github.com/shantanu200/quelon              # core, stdlib only
go get github.com/shantanu200/quelon/store/pebble # optional Pebble-backed store
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

	go func() {
		for r := range pool.Results() {
			if r.Err != nil {
				log.Printf("task %s failed: %v", r.Task.ID, r.Err)
			}
		}
	}()

	for i := range 10 {
		if err := pool.Submit(ctx, quelon.Task{ID: fmt.Sprintf("t-%d", i), Payload: i}); err != nil {
			log.Printf("submit failed: %v", err) // e.g. quelon.ErrBufferFull
		}
	}

	pool.CloseAndWait() // stops workers, drains in flight, closes Results()
}
```

## Why

Broker-backed queues (Redis, Postgres, Kafka) give durability and retries at the cost of a
network hop and an operational dependency. Plain goroutine pools give speed and no
dependencies, but lose in-flight work on crash and have no retry story.

quelon sits in between: an async queue inside your server process, with a write-ahead log
and retry/dead-letter handling.

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

`Submit` is non-blocking on the request hot path (returns `ErrBufferFull` rather than
stalling); the same pool's workers consume. With `WithStore`, each Submit is logged through
a group-commit writer and pending tasks reload on `Start`.

### When not to use it

- **Cross-process or cross-node distribution.** quelon does no routing, sharding, or
  rebalancing. Use a broker; quelon can sit behind one as a node-local consumer.
- **Exactly-once or transactional enqueue.** Durability is group-committed, not
  persist-before-ack, and retries are at-least-once — handlers must be idempotent.
- **Zero task loss on crash.** A crash can drop the current flush window.
- **Scaling workers independently of producers.** An embedded pool ties the two lifecycles
  together.

## Core concepts

| Type | Purpose |
|------|---------|
| `Pool` | The worker pool. Create with `NewPool`, run with `Start`, feed with `Submit`. |
| `ProcessFunc` | `func(ctx context.Context, payload any) error` — your per-task work. |
| `Task` | `{ ID, Payload, EnqueuedAt, Attempt }` — unit of work. |
| `Result` | `{ Task, Err }` — emitted on `Results()` after each task settles. |
| `RetryPolicy` | `MaxAttempts`, `BaseDelay`, `MaxDelay`, `JitterFactor`. |
| `Hooks` | `OnSuccess`, `OnRetry`, `OnDeadLetter`, `OnStoreError`. |
| `Store` | Durable task persistence. Defaults to `NoopStore`. |

`Submit` returns `ErrBufferFull` immediately when the buffer is full, or the context error
if `ctx` is cancelled. `CloseAndWait` stops accepting work, waits for in-flight tasks, then
closes `Results()`. Cancelling the `ctx` passed to `Start` stops workers without draining.

## Options

```go
pool := quelon.NewPool(process,
	quelon.WithBufferSize(256),
	quelon.WithWorkerCount(8),
	quelon.WithRetryPolicy(quelon.RetryPolicy{
		MaxAttempts:  5,
		BaseDelay:    200 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		JitterFactor: 0.5, // 0 = none, 1.0 = full jitter
	}),
	quelon.WithTaskTimeout(15*time.Second),
	quelon.WithHooks(quelon.Hooks{
		OnDeadLetter: func(dl quelon.DeadLetter) { /* alert / metric */ },
	}),
	quelon.WithLogger(slog.Default()),
)
```

| Option | Effect |
|--------|--------|
| `WithBufferSize(n)` | Job buffer capacity. Default `runtime.NumCPU()*10`. |
| `WithWorkerCount(n)` | Number of workers. Default `runtime.NumCPU()`. |
| `WithRetryPolicy(r)` | Backoff config. Default 3 attempts, 200ms base, 10s cap. |
| `WithTaskTimeout(d)` | Per-attempt context timeout. Default 30s. |
| `WithHooks(h)` | Lifecycle callbacks. |
| `WithLogger(l)` | Attach an `*slog.Logger`. Silent by default. |
| `WithCodec(enc, dec)` | Custom payload encode/decode for the store. Default JSON. |
| `WithStore(s)` | Durable persistence in `PersistAll` mode. |
| `WithStoreMode(s, mode)` | Durable persistence with an explicit `PersistMode`. |
| `WithGroupCommit(size, every)` | Store writer flush triggers. Default 256 / 5ms. |
| `WithDynamicWorkers(min, max, threshold)` | Auto-scale workers. Excludes `WithPartitions`. |
| `WithMachineAwareLimit()` | Clamp dynamic `max` to `GOMAXPROCS(0)`. |
| `WithPartitions(n)` | Ordered mode: route by `Task.Key` to `n` serial lanes. |
| `WithMaxBatchSize(n)` | Max tasks per batch (`NewPoolWithBatch` only). Default 10. |
| `WithMaxBatchWait(d)` | Max wait to fill a batch. 0 = best-effort. |
| `WithAggregator(merge, every, maxKeys)` | Coalesce same-key Submits in memory. |

## Retries and dead-lettering

Each task runs up to `MaxAttempts` times with exponential backoff (`BaseDelay << attempt`,
capped at `MaxDelay`, optionally jittered). Exhausted tasks are dead-lettered — surfaced via
`Hooks.OnDeadLetter` and persisted if a store is attached.

To fail fast, wrap the error as permanent:

```go
if invalid(payload) {
	return quelon.NewPermanentError(errors.New("bad payload"))
}
```

`quelon.IsPermanent(err)` reports whether an error is permanent.

## Auto-scaling workers

```go
pool := quelon.NewPool(process,
	quelon.WithBufferSize(1024),
	quelon.WithDynamicWorkers(4, 64, 0.5), // min, max, threshold
)
pool.Start(ctx) // starts at min; WithWorkerCount is ignored in dynamic mode
```

A supervisor samples buffer fill; crossing `threshold` (a fraction in `(0,1]`) grows the
worker count multiplicatively up to `max`. Burst workers retire after an idle period.
`pool.ActiveWorkers()` reports the live count.

`WithMachineAwareLimit()` clamps `max` to `runtime.GOMAXPROCS(0)` when it exceeds it. Useful
for CPU-bound work in containers (pair with something like `uber-go/automaxprocs`). Off by
default, since I/O-bound workloads legitimately run more workers than cores.

## Ordered processing (partitions)

By default any worker can take any task, so tasks complete out of order. `WithPartitions`
guarantees that **tasks sharing a `Key` are processed one at a time, in submission order**,
while different keys run in parallel:

```go
pool := quelon.NewPool(process, quelon.WithPartitions(16))

pool.Submit(ctx, quelon.Task{ID: "e1", Key: acct, Payload: evt}) // same Key → same lane,
pool.Submit(ctx, quelon.Task{ID: "e2", Key: acct, Payload: evt}) // serial and in order
```

Tasks route to lane `fnv1a(Key) % n`; each lane is a buffered channel drained by one
dedicated worker. Empty-`Key` tasks carry no ordering constraint and spread by `ID`. The
persisted `Key` and the monotonic `Task.Seq` stamped by `Submit` are honored on reload, so
per-key FIFO survives a crash even if the store returns tasks out of order.

Trade-offs: a slow or retrying key blocks other keys in its lane (head-of-line blocking), a
hot key can't spread beyond its lane, and lanes are fixed — so `WithPartitions` is
**mutually exclusive with `WithDynamicWorkers`** (dynamic scaling is disabled, with a
warning, if both are set).

## Batching

For message consumers it's often cheaper to handle many messages per call — one bulk write,
one batched request, one batch ack. `NewPoolWithBatch` has each worker pull up to `maxSize`
tasks and hand them to a batch processor:

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
				errs[i] = err
			}
		}
		return errs
	},
	quelon.WithBufferSize(1024),
	quelon.WithMaxBatchSize(100),
	quelon.WithMaxBatchWait(50*time.Millisecond),
	quelon.WithWorkerCount(8), // each worker forms its own batch
)
```

A worker blocks for the first task, then collects more until the batch hits `maxSize` or
`maxWait` elapses since that first task. `maxWait <= 0` is best-effort: take whatever is
already buffered.

**Per-item outcomes.** The processor returns one error per task, indexed to the input slice.
Successes are acked immediately; transient failures are retried **per item**, so only
still-failing tasks are re-submitted on the next attempt; permanent and retry-exhausted
tasks are dead-lettered individually. A `nil` or short slice treats unaddressed tasks as
successful. One `Result` is still emitted per task.

A `Store` may implement `BatchStore` (`DeleteTasks`, `SaveDeadLetters`) to settle a batch in
one round-trip; quelon falls back to per-item calls otherwise.

## Aggregation

Batching still buffers — and, under `WithStore`, logs — one task per `Submit`. For counter
workloads that's wasteful: 50k `+1`s to one key become 50k buffered tasks summed into a
single row update. `WithAggregator` folds same-key payloads in memory at ingest and flushes
one accumulated task per key on an interval, turning buffer and log cost from `O(events)`
into `O(distinct keys)`.

```go
type incr struct {
	Key   string // identity: the payload, not Task.Key, reaches ProcessFunc
	Delta int64
}

pool := quelon.NewPool(
	func(ctx context.Context, payload any) error {
		v := payload.(incr)
		return bulkAdd(ctx, v.Key, v.Delta) // one UPSERT ... SET val = val + delta
	},
	quelon.WithAggregator(
		func(acc, in any) any {
			a := acc.(incr)
			a.Delta += in.(incr).Delta
			return a
		},
		200*time.Millisecond, // flush window
		50_000,               // maxKeys: flush early at this many live keys
	),
	quelon.WithPartitions(64), // same Key → same lane → one writer per counter
)

pool.Submit(ctx, quelon.Task{Key: "post:123:likes", Payload: incr{"post:123:likes", 1}})
```

The first payload for a key seeds the accumulator; later ones combine via `merge`, which
must be **associative, ideally commutative** so partial flushes combine correctly at the
sink. The flushed task carries the shared `Key` plus a unique per-flush `ID`.

Trade-offs:

- `Submit` never returns `ErrBufferFull` in aggregating mode — folding is a map update.
  Backpressure surfaces as the background flusher waiting for lane room.
- `CloseAndWait` drains every accumulator, so a graceful stop loses nothing. Cancelling the
  `Start` ctx can lose the current unflushed window.
- `merge` runs under a shard lock: keep it fast and side-effect-free.
- `maxKeys <= 0` disables the size trigger, risking unbounded memory under high key
  cardinality.

Aggregation composes with `WithStore` (the coalesced task is what's persisted and replayed),
`WithPartitions`, and batching. Only worth it when per-key fan-in is high.

## Event routing with `Mux`

A pool has one `ProcessFunc`. When it must handle a variety of event types — and fan one
event out to several reactions — use a `Mux` instead of a hand-written type switch. Payloads
implementing `Event` (an `EventType() string` tag) route to their registered handlers.

```go
type BatchCreated struct {
	BatchID  string   `json:"batchID"`
	Subjects []string `json:"subjects"`
}

func (BatchCreated) EventType() string { return "batch.created" }

mux := quelon.NewMux()
mux.Handle(BatchCreated{}, facultySvc.OnBatchCreated) // one event…
mux.Handle(BatchCreated{}, subjectSvc.OnBatchCreated) // …two reactions

pool := quelon.NewPool(mux.Process, mux.Codec(), quelon.WithStore(store))
pool.Submit(ctx, quelon.Task{ID: "batch-created-" + id, Key: id, Payload: BatchCreated{...}})
```

`mux.Process` is usable the moment `NewMux()` returns, so you can pass it to `NewPool` up
front and register handlers later — which breaks the construction cycle where a handler's
dependencies need the pool while the pool needs the handler.

Pass `mux.Codec()` alongside any `WithStore`/`WithStoreMode`: it writes a `{type, data}`
envelope so persisted tasks replay back into their concrete event type. Without it the
default codec decodes into a generic map, which `Process` can't route. Unroutable tasks are
dead-lettered, not retried. On fan-out the first handler error stops the rest and the whole
task retries, so handlers must be idempotent; if that's unacceptable, submit one task per
reaction.

## Durable storage

By default the pool is memory-only (`NoopStore`). Attach a `Store` to persist pending tasks
and dead letters; pending tasks reload and re-enqueue on `Start`.

```go
import pebblestore "github.com/shantanu200/quelon/store/pebble"

store, err := pebblestore.Open("./data/queue")
if err != nil {
	log.Fatal(err)
}
defer store.Close()

pool := quelon.NewPool(process, quelon.WithBufferSize(256), quelon.WithStore(store))
```

Implement `quelon.Store` to back the pool with any other engine (Redis, SQL, …). The
interface lives in the core so heavy implementations can live in their own submodule.

### Persistence modes

Ingestion never blocks on a per-task disk sync. All store mutations flow through a single
group-commit writer that batches them into one durable commit per flush window (tune with
`WithGroupCommit`). `WithStoreMode` picks what the store is for:

| Mode | Store is… | Ingestion | Crash recovery |
|------|-----------|-----------|----------------|
| `PersistNone` (default) | unused | never touches store | none |
| `PersistDeadLettersOnly` | dead-letter archive | never touches store | none — upstream owns durability |
| `PersistAll` (`WithStore`) | WAL + dead-letter archive | persisted via the writer | pending tasks reload on `Start` |

```go
// Broker-backed consumer: re-consume on restart, only archive poison messages.
pool := quelon.NewPoolWithBatch(process, quelon.WithStoreMode(store, quelon.PersistDeadLettersOnly))

// Front-door queue: Submit is the only durable record of accepted work.
pool := quelon.NewPool(process, quelon.WithStore(store)) // == PersistAll
```

Because persistence is asynchronous, `PersistAll` is group-committed durability, **not**
persist-before-ack: a crash can lose work accepted within the current flush window. A
`Store` may implement `CommitStore` (`Commit(saves, deletes, deadLetters)`) to apply a whole
window atomically in one fsync — the Pebble store does. Others fall back to per-item calls.

## Examples

Runnable programs in [`examples/`](examples), basic to advanced. Run with
`go run ./examples/<dir>`.

- [`01-basic`](examples/01-basic) — `NewPool`, `Submit`, `Results`, retries, dead-lettering.
- [`02-batch-consumer`](examples/02-batch-consumer) — `NewPoolWithBatch` with a durable dead-letter archive.
- [`03-counter-aggregation`](examples/03-counter-aggregation) — `WithAggregator` + `WithPartitions` over 100k events.

## License

See repository.
