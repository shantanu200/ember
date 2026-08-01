package quelon

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// MergeFunc folds an incoming payload into the accumulator already held for a
// key. The first payload seen for a key seeds the accumulator directly (MergeFunc
// is not called for it); each subsequent payload for that key is combined via
// MergeFunc(acc, incoming), and the return value becomes the new accumulator.
//
// It runs under the aggregator's per-shard lock, so it MUST be fast and MUST NOT
// block or perform I/O. For partial flushes to combine correctly at the sink it
// should be associative — ideally commutative — e.g. integer addition for
// counters or a last-write-wins pick for state. The payload itself must carry
// whatever identity the downstream ProcessFunc needs (the Key is used only for
// routing/coalescing and is not passed to ProcessFunc).
type MergeFunc func(acc, incoming any) any

// aggShardCount stripes the accumulator map so concurrent Submits for different
// keys rarely contend on the same lock. A key always hashes to one shard.
const aggShardCount = 16

// WithAggregator turns the pool into a write-behind aggregator: instead of
// enqueuing every Submit, same-key payloads are folded in memory via merge and
// one coalesced task per key is flushed into the normal pipeline every
// flushEvery (or sooner, once the number of live keys reaches maxKeys). This
// collapses a high-fan-in write storm — thousands of increments to one counter —
// into a single task (and, with WithStore, a single write-ahead record) per key
// per window, turning buffer and log cost from O(events) into O(distinct keys).
//
// The flushed task carries the shared Key (so it still routes to one lane under
// WithPartitions, coalescing maximally) plus a unique per-flush ID. In this mode
// Submit never returns ErrBufferFull: folding is an in-memory map update, so
// backpressure surfaces instead as the background flusher waiting for lane room.
// CloseAndWait drains every accumulator into the pipeline before shutting down,
// so a graceful stop loses nothing; an ungraceful stop (cancelling Start's ctx)
// can lose the current unflushed window — the same trade-off the group-commit
// writer makes for durability.
//
// flushEvery <= 0 defaults to 100ms. maxKeys <= 0 disables the size trigger
// (time-based flush only), which risks unbounded memory under high key
// cardinality — set a cap unless the key space is known to be small.
func WithAggregator(merge MergeFunc, flushEvery time.Duration, maxKeys int) Option {
	return func(p *Pool) {
		p.aggMerge = merge
		p.aggFlushEvery = flushEvery
		p.aggMaxKeys = maxKeys
	}
}

// aggEntry is one key's in-flight accumulator between flushes.
type aggEntry struct {
	key        string
	payload    any
	enqueuedAt time.Time
}

// aggShard is one striped partition of the accumulator map, guarded by its own
// mutex. Folds for a given key are serialized by its shard lock, so no update is
// ever lost under concurrent Submit.
type aggShard struct {
	mu      sync.Mutex
	entries map[string]*aggEntry
}

// aggregator holds the sharded in-memory accumulators and the single background
// goroutine that periodically flushes them into the pool's normal ingest path.
type aggregator struct {
	p          *Pool
	merge      MergeFunc
	flushEvery time.Duration
	maxKeys    int
	shards     [aggShardCount]*aggShard
	liveKeys   atomic.Int64  // approximate count of accumulated keys, for the cap
	flushNow   chan struct{} // coalesced early-flush signal (cap trigger)
	quit       chan struct{} // closed by stopAndDrain to stop run
	done       chan struct{} // closed when run returns
	ctx        context.Context
}

func newAggregator(p *Pool, merge MergeFunc, flushEvery time.Duration, maxKeys int) *aggregator {
	if flushEvery <= 0 {
		flushEvery = 100 * time.Millisecond
	}
	a := &aggregator{
		p:          p,
		merge:      merge,
		flushEvery: flushEvery,
		maxKeys:    maxKeys,
		flushNow:   make(chan struct{}, 1),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	for i := range a.shards {
		a.shards[i] = &aggShard{entries: make(map[string]*aggEntry)}
	}
	return a
}

// aggShardIndex is an inlined FNV-1a over the key that allocates nothing — it
// runs on every Submit, so it avoids the per-call hasher allocation fnv.New32a
// would incur.
func aggShardIndex(key string) uint32 {
	const (
		offset = uint32(2166136261)
		prime  = uint32(16777619)
	)
	h := offset
	for i := range len(key) {
		h ^= uint32(key[i])
		h *= prime
	}
	return h % aggShardCount
}

// fold merges t into the per-key accumulator. It is the Submit path in
// aggregating mode: an in-memory map update under a shard lock, never a channel
// send, so it does not block and cannot report ErrBufferFull.
func (a *aggregator) fold(t Task) {
	key := t.Key
	if key == "" {
		key = t.ID
	}
	sh := a.shards[aggShardIndex(key)]

	sh.mu.Lock()
	if e, ok := sh.entries[key]; ok {
		e.payload = a.merge(e.payload, t.Payload)
		sh.mu.Unlock()
		return
	}
	sh.entries[key] = &aggEntry{key: key, payload: t.Payload, enqueuedAt: t.EnqueuedAt}
	sh.mu.Unlock()

	// A newly-tracked key: account for it and, if we've hit the cap, nudge the
	// flusher to run early. The signal is coalesced (buffered size 1) so a burst
	// of new keys wakes the flusher at most once before it drains.
	if n := a.liveKeys.Add(1); a.maxKeys > 0 && n >= int64(a.maxKeys) {
		select {
		case a.flushNow <- struct{}{}:
		default:
		}
	}
}

func (a *aggregator) start(ctx context.Context) {
	a.ctx = ctx
	go a.run(ctx)
}

// run is the single flush goroutine. It flushes on the interval timer or on an
// early cap signal, and exits when ctx is cancelled (ungraceful stop) or quit is
// closed (graceful stop, where stopAndDrain does the final flush itself).
func (a *aggregator) run(ctx context.Context) {
	defer close(a.done)

	timer := time.NewTimer(a.flushEvery)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.quit:
			return
		case <-a.flushNow:
			a.flushAll(ctx)
			resetTimer(timer, a.flushEvery)
		case <-timer.C:
			a.flushAll(ctx)
			timer.Reset(a.flushEvery)
		}
	}
}

// flushAll swaps each shard's accumulator map out under its lock (a short
// critical section with no I/O) and then enqueues one coalesced task per key
// outside the lock, so folds for other keys keep making progress while the
// flush's channel sends drain.
func (a *aggregator) flushAll(ctx context.Context) {
	for _, sh := range a.shards {
		sh.mu.Lock()
		if len(sh.entries) == 0 {
			sh.mu.Unlock()
			continue
		}
		entries := sh.entries
		sh.entries = make(map[string]*aggEntry)
		sh.mu.Unlock()

		a.liveKeys.Add(-int64(len(entries)))
		for _, e := range entries {
			seq := a.p.seq.Add(1)
			task := Task{
				ID:         e.key + "#" + strconv.FormatUint(seq, 10),
				Key:        e.key,
				Seq:        seq,
				Payload:    e.payload,
				EnqueuedAt: e.enqueuedAt,
			}
			// A coalesced value stands in for many Submits, so wait for lane room
			// rather than dropping it on a transient full buffer. dispatch releases
			// on ctx cancellation or pool shutdown. No release channel: the final
			// flush runs after CloseAndWait has shut submitters out but before the
			// lanes close, and must still deliver onto them.
			_ = a.p.dispatch(ctx, task, true, nil)
		}
	}
}

// stopAndDrain stops the flush goroutine and then flushes every remaining
// accumulator into the pipeline. Called by CloseAndWait before the jobs lanes
// are closed and while the workers are still draining them, so no coalesced
// value is lost on a graceful shutdown. After <-a.done the run goroutine has
// returned, making this the sole sender on the lanes.
func (a *aggregator) stopAndDrain() {
	close(a.quit)
	<-a.done
	a.flushAll(a.ctx)
}
