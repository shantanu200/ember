# ember

A lightweight, concurrent worker pool for Go with retries, dead-lettering, optional
auto-scaling, and pluggable durable storage.

The core library depends only on the standard library — installing it pulls in **zero
external dependencies**. Durability (persisting tasks across restarts) is opt-in through a
separate module so you never pay for what you don't use.

## Install

Core pool (stdlib only):

```bash
go get github.com/shantanu200/ember
```

Optional Pebble-backed durable store (separate module, pulls in `cockroachdb/pebble`):

```bash
go get github.com/shantanu200/ember/store/pebble
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/shantanu200/ember"
)

func main() {
	ctx := context.Background()

	// bufferSize 0 defaults to runtime.NumCPU() * 10.
	pool := ember.NewPool(128, func(ctx context.Context, payload any) error {
		fmt.Printf("processing %v\n", payload)
		return nil
	})

	// workerCount 0 defaults to runtime.NumCPU().
	if err := pool.Start(ctx, 4); err != nil {
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
		if err := pool.Submit(ctx, ember.Task{ID: fmt.Sprintf("t-%d", i), Payload: i}); err != nil {
			log.Printf("submit failed: %v", err) // e.g. ember.ErrBufferFull
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
pool := ember.NewPool(256, process,
	ember.WithRetryPolicy(ember.RetryPolicy{
		MaxAttempts:  5,
		BaseDelay:    200 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		JitterFactor: 0.5, // 0 = no jitter, 1.0 = full jitter
	}),
	ember.WithTaskTimeout(15*time.Second), // per-attempt timeout (default 30s)
	ember.WithHooks(ember.Hooks{
		OnDeadLetter: func(dl ember.DeadLetter) { /* alert / metric */ },
	}),
	ember.WithLogger(slog.Default()), // off by default
)
```

| Option | Effect |
|--------|--------|
| `WithRetryPolicy(r)` | Exponential backoff config. Default: 3 attempts, 200ms base, 10s cap. |
| `WithTaskTimeout(d)` | Per-attempt context timeout. Default 30s. |
| `WithHooks(h)` | Success / retry / dead-letter / store-error callbacks. |
| `WithLogger(l)` | Attach an `*slog.Logger`. Logs nothing by default. |
| `WithCodec(enc, dec)` | Custom payload encode/decode for the store. Defaults to JSON. |
| `WithStore(s)` | Durable task persistence (see below). |
| `WithDynamicWorkers(min, max, threshold)` | Auto-scale workers (see below). |

## Retries and dead-lettering

Each task runs up to `MaxAttempts` times with exponential backoff (`BaseDelay << attempt`,
capped at `MaxDelay`, optionally jittered). When all attempts are exhausted, the task is
**dead-lettered** — surfaced via `Hooks.OnDeadLetter` and (if a store is attached) persisted.

To fail fast without retrying, wrap the error as permanent:

```go
func process(ctx context.Context, payload any) error {
	if invalid(payload) {
		return ember.NewPermanentError(errors.New("bad payload"))
	}
	return nil
}
```

`ember.IsPermanent(err)` reports whether an error is permanent.

## Auto-scaling workers

`WithDynamicWorkers` lets the pool grow under load and shrink back when idle:

```go
pool := ember.NewPool(1024, process,
	ember.WithDynamicWorkers(4, 64, 0.5), // min, max, scale threshold
)
pool.Start(ctx, 0) // workerCount is ignored in dynamic mode; starts at min
```

A supervisor samples the job-buffer fill level; when it crosses `threshold` (a fraction in
`(0,1]`), the worker count grows multiplicatively up to `max`. Burst workers retire after an
idle period, returning the pool to `min`. Inspect the live count with `pool.ActiveWorkers()`.

## Durable storage

By default the pool keeps tasks in memory only (`NoopStore`). Attach a `Store` to persist
pending tasks and dead letters so they survive restarts — on `Start`, pending tasks are
reloaded and re-enqueued automatically.

```go
import (
	"github.com/shantanu200/ember"
	pebblestore "github.com/shantanu200/ember/store/pebble"
)

store, err := pebblestore.Open("./data/queue")
if err != nil {
	log.Fatal(err)
}
defer store.Close()

pool := ember.NewPool(256, process, ember.WithStore(store))
```

Implement the `ember.Store` interface to back the pool with any other engine (Redis, SQL,
etc.); keeping the interface in the core is what lets heavy implementations live in their own
submodule without bloating the core's dependency graph.

## License

See repository.
