package quelon

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Pool struct {
	jobs               chan Task
	results            chan Result
	store              Store
	persistPending     bool // persist pending tasks (write-ahead log) — PersistAll
	persistDeadLetters bool // persist dead letters — PersistDeadLettersOnly or PersistAll
	process            ProcessFunc
	policy             RetryPolicy
	hooks              Hooks
	timeout            time.Duration
	encode             func(any) ([]byte, error)
	decode             func([]byte) (any, error)
	logger             *slog.Logger
	wg                 sync.WaitGroup

	// Group-commit writer: a single goroutine owns all store mutations,
	// batching them into one durable commit per flush window so ingestion never
	// pays a per-task fsync. See commit.go.
	ops           chan storeOp
	writerDone    chan struct{}
	writerRunning bool
	flushSize     int           // flush when this many ops accumulate
	flushEvery    time.Duration // ...or this long since the window opened

	batchEnabled bool
	maxBatchSize int
	maxBatchWait time.Duration
	batchProcess BatchProcessFunc

	dynamic        bool
	machineAware   bool // set via WithMachineAwareLimit; clamps maxWorkers to GOMAXPROCS
	minWorkers     int
	maxWorkers     int
	scaleThreshold float64       // jobs-buffer fill fraction that triggers scale-up
	scaleInterval  time.Duration // how often the supervisor samples queue depth
	idleTimeout    time.Duration // a burst worker retires after this long without a task
	activeWorkers  atomic.Int32
	quit           chan struct{} // closed by CloseAndWait to stop the supervisor
	superDone      chan struct{} // closed when the supervisor goroutine exits
	closing        chan struct{} // closed by CloseAndWait to release workers blocked emitting results

	bufferSize  int // set via WithBufferSize; defaults to runtime.NumCPU()*10
	workerCount int // set via WithWorkerCount; defaults to runtime.NumCPU()

	// Partitioned (ordered) mode: set via WithPartitions. When partitions > 0 the
	// single shared jobs channel is replaced by one buffered shard channel per
	// partition, each drained by exactly one worker, so tasks that hash to the
	// same shard are processed serially and in submission order. Zero (the
	// default) keeps the shared work-stealing channel.
	partitions int
	shards     []chan Task
}

type ProcessFunc func(ctx context.Context, payload any) error

type Option func(*Pool)

// WithStore attaches a durable Store in PersistAll mode: pending tasks are
// persisted (write-ahead) and dead letters are archived. Use WithStoreMode to
// pick a different PersistMode.
func WithStore(s Store) Option {
	return WithStoreMode(s, PersistAll)
}

// WithStoreMode attaches a Store and selects what it is used for. See
// PersistMode for the trade-offs: PersistDeadLettersOnly keeps ingestion fully
// decoupled from the store (no crash recovery of pending work), while
// PersistAll persists pending tasks through the group-commit writer.
func WithStoreMode(s Store, mode PersistMode) Option {
	return func(p *Pool) {
		p.store = s
		switch mode {
		case PersistAll:
			p.persistPending = true
			p.persistDeadLetters = true
		case PersistDeadLettersOnly:
			p.persistDeadLetters = true
		}
	}
}

// WithGroupCommit tunes the durability/throughput trade-off of the store
// writer. The writer flushes a durable commit when `size` operations accumulate
// or `every` elapses since the window opened, whichever comes first — one fsync
// per flush. Smaller values tighten the crash-loss window; larger values raise
// throughput. size < 1 and every <= 0 are ignored. Defaults: 256 ops / 5ms.
func WithGroupCommit(size int, every time.Duration) Option {
	return func(p *Pool) {
		if size >= 1 {
			p.flushSize = size
		}
		if every > 0 {
			p.flushEvery = every
		}
	}
}

func WithRetryPolicy(r RetryPolicy) Option {
	return func(p *Pool) { p.policy = r }
}

func WithHooks(h Hooks) Option {
	return func(p *Pool) { p.hooks = h }
}

func WithTaskTimeout(d time.Duration) Option {
	return func(p *Pool) { p.timeout = d }
}

func WithCodec(encode func(any) ([]byte, error), decode func([]byte) (any, error)) Option {
	return func(p *Pool) {
		p.encode = encode
		p.decode = decode
	}
}

// WithLogger attaches a *slog.Logger for execution tracing.
// By default the pool logs nothing; pass slog.Default() or a custom logger to enable.
func WithLogger(l *slog.Logger) Option {
	return func(p *Pool) { p.logger = l }
}

// WithDynamicWorkers enables auto-scaling of the worker pool.
//
// The pool starts with min workers. A supervisor samples the jobs-buffer fill
// level; when it reaches scaleThreshold (a fraction in (0,1]) the worker count
// is grown — multiplicatively — up to max. Burst workers retire after an idle
// period, returning the pool to min. This absorbs load spikes without forcing
// the caller to permanently over-provision workers.
//
// min < 1 is clamped to 1; max < min is clamped to min; a scaleThreshold
// outside (0,1] defaults to 0.5.
func WithBufferSize(n int) Option {
	return func(p *Pool) { p.bufferSize = n }
}

func WithWorkerCount(n int) Option {
	return func(p *Pool) { p.workerCount = n }
}

// WithPartitions enables ordered (partitioned) processing. Instead of one shared
// job buffer drained by interchangeable workers, the pool runs n partitions,
// each a buffered lane drained by a single dedicated worker. A task is routed to
// partition fnv1a(Task.Key) % n, so all tasks sharing a Key land on the same
// lane and are processed one at a time, in submission order. Tasks with an empty
// Key carry no ordering constraint and are spread across lanes by their ID.
//
// This is the model to reach for when correctness depends on per-key order
// (event sourcing, CDC, per-account state machines) — the guarantee the default
// work-stealing pool cannot make. The trade-offs are inherent: a slow or
// retrying key blocks other keys hashed to its lane (head-of-line blocking), a
// hot key cannot spread beyond its lane, and the lanes are fixed — so
// partitioning is mutually exclusive with WithDynamicWorkers (dynamic scaling is
// disabled, with a warning, when both are set). Each lane is buffered to
// bufferSize. n < 1 is clamped to 1 (a single global-order lane).
func WithPartitions(n int) Option {
	return func(p *Pool) {
		if n < 1 {
			n = 1
		}
		p.partitions = n
	}
}

func WithMaxBatchSize(n int) Option {
	return func(p *Pool) {
		if n >= 1 {
			p.maxBatchSize = n
		}
	}
}

func WithMaxBatchWait(d time.Duration) Option {
	return func(p *Pool) { p.maxBatchWait = d }
}

func WithDynamicWorkers(min, max int, scaleThreshold float64) Option {
	return func(p *Pool) {
		if min < 1 {
			min = 1
		}
		if max < min {
			max = min
		}
		if scaleThreshold <= 0 || scaleThreshold > 1 {
			scaleThreshold = 0.5
		}
		p.dynamic = true
		p.minWorkers = min
		p.maxWorkers = max
		p.scaleThreshold = scaleThreshold
	}
}

// WithMachineAwareLimit clamps maxWorkers (set via WithDynamicWorkers) to
// runtime.GOMAXPROCS(0) if the configured max exceeds it, logging a warning.
// GOMAXPROCS reflects the GOMAXPROCS env var when set, so it also honors
// container/cgroup CPU quotas when paired with a library that sets GOMAXPROCS
// from the cgroup limit (e.g. uber-go/automaxprocs).
//
// This is opt-in: callers with I/O-bound workloads commonly and legitimately
// run more goroutine-workers than CPU cores, so no clamping happens unless
// this option is set.
func WithMachineAwareLimit() Option {
	return func(p *Pool) { p.machineAware = true }
}

// ActiveWorkers returns the current number of running workers, including any
// burst workers spawned by the dynamic scaler.
func (p *Pool) ActiveWorkers() int {
	return int(p.activeWorkers.Load())
}

func (p *Pool) log(level slog.Level, msg string, args ...any) {
	if p.logger == nil {
		return
	}
	p.logger.Log(context.Background(), level, msg, args...)
}

// NewPool creates a pool that processes one task per call to process. Run it
// with Start and feed it with Submit. For batch processing (message consumers
// that handle many messages per call), use NewPoolWithBatch instead.
func NewPool(process ProcessFunc, opts ...Option) *Pool {
	p := newPool(opts...)
	p.process = process
	return p
}

// NewPoolWithBatch creates a pool that hands each worker a batch of up to
// maxSize tasks per call to process, instead of one task at a time. This is the
// natural fit for message consumers (Kafka, SQS, Pub/Sub) where bulk DB writes,
// batched downstream calls, or batch acks amortise per-message overhead across
// the whole batch while multiple workers still run in parallel.
//
// A worker blocks for the first task, then keeps collecting until the batch
// reaches maxSize or maxWait elapses since the first task — whichever comes
// first — so a trickle of work never sits unflushed. maxWait <= 0 means
// best-effort: take whatever is already buffered without lingering.
//
// process returns one error per task, indexed to match the input slice (nil =
// that task succeeded). Successful tasks are acked immediately; transient
// failures are retried per-item on the next attempt (only the still-failing
// tasks are re-submitted to process), and permanent or retry-exhausted tasks
// are dead-lettered individually. Returning a nil or shorter slice treats the
// unaddressed tasks as successful.
//
// maxSize < 1 is clamped to 1. Batching composes with the same options as
// NewPool (WithStore, WithDynamicWorkers, WithRetryPolicy, …); producers keep
// calling Submit with individual tasks.
func NewPoolWithBatch(process BatchProcessFunc, opts ...Option) *Pool {
	p := newPool(opts...)
	p.batchEnabled = true
	if p.maxBatchSize < 1 {
		p.maxBatchSize = 10
	}
	p.batchProcess = process
	return p
}

// newPool builds a pool with default configuration and applies opts. The
// mode-defining process function is set by the exported constructors.
func newPool(opts ...Option) *Pool {
	p := &Pool{
		store:   NoopStore{},
		policy:  DefaultRetryPolicy,
		timeout: 30 * time.Second,
		encode:  func(v any) ([]byte, error) { return json.Marshal(v) },
		decode: func(b []byte) (any, error) {
			var v any
			return v, json.Unmarshal(b, &v)
		},
		logger:        nil,
		scaleInterval: 100 * time.Millisecond,
		idleTimeout:   1 * time.Second,
		flushSize:     256,
		flushEvery:    5 * time.Millisecond,
		quit:          make(chan struct{}),
		superDone:     make(chan struct{}),
		closing:       make(chan struct{}),
		writerDone:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	// Partitioning pins each lane to a single dedicated worker, which is
	// fundamentally incompatible with a scaler that grows and retires
	// interchangeable workers on a shared queue. Ordering wins; scaling is off.
	if p.partitions > 0 && p.dynamic {
		p.log(
			slog.LevelWarn, "partitioning disables dynamic worker scaling",
			"partitions", p.partitions,
		)
		p.dynamic = false
	}

	if p.dynamic && p.machineAware {
		if capacity := runtime.GOMAXPROCS(0); p.maxWorkers > capacity {
			p.log(
				slog.LevelWarn, "clamping max_workers to machine capacity",
				"configured_max_workers", p.maxWorkers,
				"machine_capacity", capacity,
			)
			p.maxWorkers = capacity
			if p.minWorkers > p.maxWorkers {
				p.minWorkers = p.maxWorkers
			}
		}
	}

	if p.bufferSize == 0 {
		p.bufferSize = runtime.NumCPU() * 10
	}
	p.jobs = make(chan Task, p.bufferSize)
	p.results = make(chan Result, p.bufferSize)
	p.ops = make(chan storeOp, p.bufferSize)

	// One buffered lane per partition, each drained by a single worker so tasks
	// on the same lane stay serial and in order.
	if p.partitions > 0 {
		p.shards = make([]chan Task, p.partitions)
		for i := range p.shards {
			p.shards[i] = make(chan Task, p.bufferSize)
		}
	}

	// A policy with MaxAttempts < 1 would skip the retry loop entirely and
	// report every task as succeeded without ever invoking process, silently
	// dropping work. At least one attempt is always required.
	if p.policy.MaxAttempts < 1 {
		p.policy.MaxAttempts = 1
	}

	return p
}

func (p *Pool) Start(ctx context.Context) error {
	workerCount := p.workerCount
	if p.dynamic {
		// In dynamic mode the worker count is governed by min/max; the
		// workerCount option is ignored in favour of the configured floor.
		workerCount = p.minWorkers
	} else if workerCount == 0 {
		workerCount = runtime.NumCPU()
	}

	p.log(
		slog.LevelInfo, "quelon started",
		"workers", workerCount,
		"dynamic", p.dynamic,
		"min_workers", p.minWorkers,
		"max_workers", p.maxWorkers,
		"batch", p.batchEnabled,
		"max_batch_size", p.maxBatchSize,
		"max_batch_wait", p.maxBatchWait,
		"buffer", cap(p.jobs),
		"timeout", p.timeout,
		"max_attempts", p.policy.MaxAttempts,
		"base_delay", p.policy.BaseDelay,
		"max_delay", p.policy.MaxDelay,
	)

	if p.partitions > 0 {
		// One dedicated worker per lane: the lane's single consumer is what makes
		// same-key tasks serial and in order.
		for i := range p.shards {
			p.activeWorkers.Add(1)
			p.wg.Add(1)
			go p.partitionWorker(ctx, p.shards[i])
		}
	} else {
		for i := 0; i < workerCount; i++ {
			p.activeWorkers.Add(1)
			p.wg.Add(1)
			go p.worker(ctx, false)
		}
	}

	if p.dynamic {
		go p.supervise(ctx)
	}

	if p.persistPending || p.persistDeadLetters {
		p.writerRunning = true
		go p.commitLoop()
	}

	if p.persistPending {
		rawTasks, err := p.store.LoadPendingTasks()
		if err != nil {
			return fmt.Errorf("loading pending tasks: %w", err)
		}
		for _, r := range rawTasks {
			payload, err := p.decode(r.Payload)
			if err != nil {
				return fmt.Errorf("decoding pending task %s: %w", r.ID, err)
			}
			// Route reloaded tasks through the same lane function as Submit so a
			// persisted Key keeps replayed same-key tasks on one serial lane.
			t := Task{ID: r.ID, Key: r.Key, Payload: payload, EnqueuedAt: r.EnqueuedAt, Attempt: r.Attempt}
			p.jobsFor(t) <- t
		}
	}

	return nil
}

func (p *Pool) Submit(ctx context.Context, t Task) error {
	// Submit is non-blocking, so a cancelled context can only be observed here,
	// up front — there is no blocking send for a ctx.Done() select case to win.
	if err := ctx.Err(); err != nil {
		return err
	}

	if t.EnqueuedAt.IsZero() {
		t.EnqueuedAt = time.Now()
	}

	// Queue the save before enqueuing the task. The task can't be picked up — and
	// so can't have its completion delete queued — until it is in p.jobs, so the
	// writer always observes the save ahead of the delete. The reverse order
	// would let a fast worker's delete land in an earlier flush window than the
	// save, leaving a durable pending record for an already-finished task that
	// would wrongly replay on restart.
	persisted := false
	if p.persistPending {
		encoded, err := p.encode(t.Payload)
		if err != nil {
			// The task can still be processed in memory; it just won't have a
			// durable pending record. Surface the failure without rejecting it.
			p.storeErr(fmt.Errorf("encoding payload for task %s: %w", t.ID, err))
		} else {
			p.queueOp(storeOp{
				kind: opSave,
				task: RawTask{ID: t.ID, Key: t.Key, Payload: encoded, EnqueuedAt: t.EnqueuedAt, Attempt: t.Attempt},
			})
			persisted = true
		}
	}

	select {
	case p.jobsFor(t) <- t:
		if p.logger != nil {
			p.log(slog.LevelDebug, "task submitted", "task_id", t.ID)
		}
		return nil
	default:
		// Rejected after the save was queued: queue a compensating delete so the
		// task the caller was told was not accepted isn't left pending. It
		// coalesces with the save in the writer's window.
		if persisted {
			p.queueOp(storeOp{kind: opDelete, id: t.ID})
		}
		return ErrBufferFull
	}
}

// partitionFor maps an ordering key to a partition index in [0, p.partitions).
// It is only meaningful in partitioned mode (p.partitions > 0).
func (p *Pool) partitionFor(key string) int {
	h := fnv.New32a()
	// hash.Hash never returns an error on Write.
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(p.partitions))
}

// jobsFor returns the channel a task should be enqueued on. In the default
// (unpartitioned) mode that is always the shared jobs channel; in partitioned
// mode it is the lane for the task's Key. An empty Key carries no ordering
// constraint, so it is spread across lanes by the task ID rather than piling
// every unkeyed task onto a single lane.
func (p *Pool) jobsFor(t Task) chan Task {
	if p.partitions == 0 {
		return p.jobs
	}
	routeKey := t.Key
	if routeKey == "" {
		routeKey = t.ID
	}
	return p.shards[p.partitionFor(routeKey)]
}

func (p *Pool) Results() <-chan Result {
	return p.results
}

func (p *Pool) CloseAndWait() {
	if p.dynamic {
		// Stop the supervisor before closing jobs so it can't call wg.Add
		// concurrently with the wg.Wait below.
		close(p.quit)
		<-p.superDone
	}
	close(p.jobs)
	// In partitioned mode the workers drain the per-lane shard channels, not the
	// shared jobs channel, so close each lane to let its worker finish and exit.
	for _, shard := range p.shards {
		close(shard)
	}
	// Release any worker blocked delivering a result to a consumer that has
	// stopped draining Results(); without this, wg.Wait below would hang
	// forever. Task outcomes are already committed (hooks + store) before a
	// result is emitted, so a dropped result loses no authoritative state.
	close(p.closing)
	p.wg.Wait()
	// Workers have stopped, so no more store ops will be queued. Close the op
	// channel and let the writer commit its final window before we return —
	// every task outcome is durably recorded before CloseAndWait completes.
	if p.writerRunning {
		close(p.ops)
		<-p.writerDone
	}
	close(p.results)
}

// emit delivers a result on Results() without blocking forever. It first tries
// a non-blocking send so a keeping-up consumer always receives every result;
// only if the buffer is full does it wait, and then a shutdown (p.closing) or a
// cancelled ctx lets the worker drop the result and exit instead of deadlocking.
func (p *Pool) emit(ctx context.Context, r Result) {
	select {
	case p.results <- r:
		return
	default:
	}
	select {
	case p.results <- r:
	case <-p.closing:
	case <-ctx.Done():
	}
}

// worker runs the job loop. Permanent workers (ephemeral == false) live until
// ctx is cancelled or the jobs channel is closed. Ephemeral burst workers,
// spawned by the dynamic scaler, additionally retire after idleTimeout with no
// work so the pool can shrink back to its floor.
func (p *Pool) worker(ctx context.Context, ephemeral bool) {
	defer p.wg.Done()
	defer p.activeWorkers.Add(-1)

	if p.batchEnabled {
		p.batchWorker(ctx, ephemeral)
		return
	}

	if !ephemeral {
		for {
			select {
			case <-ctx.Done():
				return
			case task, ok := <-p.jobs:
				if !ok {
					return
				}
				p.handle(ctx, task)
			}
		}
	}

	idle := time.NewTimer(p.idleTimeout)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-p.jobs:
			if !ok {
				return
			}
			p.handle(ctx, task)
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(p.idleTimeout)
		case <-idle.C:
			return
		}
	}
}

// partitionWorker is the sole consumer of one partition lane. Because a lane has
// exactly one worker, tasks on it are handled one at a time, in the order they
// were enqueued — the ordering guarantee of partitioned mode. It handles both
// single-task and batch pools (batches are gathered from this lane only, so a
// batch never mixes keys across partitions). It exits when its lane is closed
// (CloseAndWait) or ctx is cancelled.
func (p *Pool) partitionWorker(ctx context.Context, shard chan Task) {
	defer p.wg.Done()
	defer p.activeWorkers.Add(-1)

	if p.batchEnabled {
		for {
			batch, ok := p.gatherBatch(ctx, shard, false)
			if len(batch) > 0 {
				p.handleBatch(ctx, batch)
			}
			if !ok {
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-shard:
			if !ok {
				return
			}
			p.handle(ctx, task)
		}
	}
}

// supervise samples the jobs-buffer depth and grows the worker pool toward
// maxWorkers when it crosses scaleThreshold. Growth is multiplicative for fast
// reaction to spikes; shrink-back is handled by idle burst workers retiring.
func (p *Pool) supervise(ctx context.Context) {
	defer close(p.superDone)

	ticker := time.NewTicker(p.scaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.quit:
			return
		case <-ticker.C:
			capJobs := cap(p.jobs)
			if capJobs == 0 {
				continue
			}
			depth := float64(len(p.jobs)) / float64(capJobs)
			cur := int(p.activeWorkers.Load())
			if depth < p.scaleThreshold || cur >= p.maxWorkers {
				continue
			}

			target := min(cur*2, p.maxWorkers)
			for i := cur; i < target; i++ {
				p.activeWorkers.Add(1)
				p.wg.Add(1)
				go p.worker(ctx, true)
			}
			p.log(
				slog.LevelDebug, "scaled up workers",
				"from", cur, "to", target, "queue_depth", depth,
			)
		}
	}
}

func (p *Pool) handle(ctx context.Context, task Task) {
	start := time.Now()
	if p.logger != nil {
		p.log(slog.LevelDebug, "task started", "task_id", task.ID)
	}
	err := p.runWithRetry(ctx, &task)

	if err != nil {
		dl := DeadLetter{
			Task:      task,
			Err:       err.Error(),
			Permanent: IsPermanent(err),
			FailedAt:  time.Now(),
		}

		if p.persistDeadLetters {
			encoded, _ := p.encode(task.Payload)
			p.queueOp(storeOp{
				kind: opDeadLetter,
				dl: RawDeadLetter{
					Task:      RawTask{ID: task.ID, Key: task.Key, Payload: encoded, EnqueuedAt: task.EnqueuedAt, Attempt: task.Attempt},
					Err:       dl.Err,
					Permanent: dl.Permanent,
					FailedAt:  dl.FailedAt,
				},
			})
		}

		p.log(
			slog.LevelWarn, "task dead-lettered",
			"task_id", task.ID,
			"attempts", task.Attempt+1,
			"permanent", dl.Permanent,
			"error", dl.Err,
			"elapsed", time.Since(start),
		)
		if p.hooks.OnDeadLetter != nil {
			p.hooks.OnDeadLetter(dl)
		}
	} else {
		if p.logger != nil {
			p.log(
				slog.LevelDebug, "task succeeded",
				"task_id", task.ID,
				"attempts", task.Attempt+1,
				"elapsed", time.Since(start),
			)
		}
		if p.hooks.OnSuccess != nil {
			p.hooks.OnSuccess(task)
		}
	}

	// Remove the task from the pending store once its outcome has been recorded.
	// The dead-letter op above is queued before this delete op, and the writer
	// applies dead letters before deletes within a window, so a failed task is
	// never deleted from pending before it is durable as a dead letter.
	if p.persistPending {
		p.queueOp(storeOp{kind: opDelete, id: task.ID})
	}

	p.emit(ctx, Result{Task: task, Err: err})
}

func (p *Pool) runWithRetry(ctx context.Context, task *Task) error {
	var err error

	// Resume from the task's current attempt rather than always 0: a task
	// reloaded from the store after a crash carries the attempt it had reached,
	// so it continues with its remaining budget instead of a fresh one.
	start := max(task.Attempt, 0)
	if start >= p.policy.MaxAttempts {
		start = p.policy.MaxAttempts - 1
	}

	for attempt := start; attempt < p.policy.MaxAttempts; attempt++ {
		task.Attempt = attempt
		err = p.runOnce(ctx, task.Payload)
		if err == nil {
			return nil
		}

		if IsPermanent(err) {
			return err
		}

		p.log(
			slog.LevelWarn, "task retrying",
			"task_id", task.ID,
			"attempt", attempt+1,
			"max_attempts", p.policy.MaxAttempts,
			"error", err,
		)
		if p.hooks.OnRetry != nil {
			p.hooks.OnRetry(*task, err, attempt)
		}

		// Durably advance the attempt counter so a crash before the next try
		// resumes with the remaining budget rather than starting over.
		if attempt+1 < p.policy.MaxAttempts {
			task.Attempt = attempt + 1
			p.persistAttempt(task)
		}

		delay := p.policy.Delay(attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("after %d attempts: %w", p.policy.MaxAttempts, err)
}

// persistAttempt durably records a task's advanced Attempt so that, if the
// process crashes between retries, the reloaded task resumes with its remaining
// budget instead of a fresh one. No-op without a store.
func (p *Pool) persistAttempt(task *Task) {
	if !p.persistPending {
		return
	}
	encoded, err := p.encode(task.Payload)
	if err != nil {
		p.storeErr(fmt.Errorf("encoding task %s to persist attempt: %w", task.ID, err))
		return
	}
	p.queueOp(storeOp{
		kind: opSave,
		task: RawTask{ID: task.ID, Key: task.Key, Payload: encoded, EnqueuedAt: task.EnqueuedAt, Attempt: task.Attempt},
	})
}

func (p *Pool) runOnce(parent context.Context, payload any) (err error) {
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	return p.process(ctx, payload)
}
