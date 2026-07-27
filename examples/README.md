# quelon examples

Runnable programs, ordered from basic to advanced. Each is self-contained
(`package main`, standard library + quelon only) and prints what it did before
exiting. Run any of them from the repo root:

```bash
go run ./examples/01-basic
go run ./examples/02-batch-consumer
go run ./examples/03-counter-aggregation
```

| # | Example | Level | Shows |
|---|---------|-------|-------|
| 01 | [basic](01-basic) | Basic | `NewPool`, `Submit`, `Results`, retry with backoff, permanent vs transient failures, dead-lettering via `Hooks` |
| 02 | [batch-consumer](02-batch-consumer) | Intermediate | `NewPoolWithBatch` bulk processing, per-item error slices, and a durable dead-letter archive with `WithStoreMode(..., PersistDeadLettersOnly)` |
| 03 | [counter-aggregation](03-counter-aggregation) | Advanced | `WithAggregator` + `WithPartitions` coalescing a high-fan-in write storm (100k like events → a handful of counter writes) |

Start at 01 to learn the core loop, then move up as you need batching,
durability, or ingest-time aggregation. For event-type routing and fan-out, see
the `Mux` section in the top-level [README](../README.md#event-routing-with-mux).
