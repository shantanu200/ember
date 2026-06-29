package ember

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Pool struct {
	jobs         chan Task
	results      chan Result
	store        Store
	storeEnabled bool
	process      ProcessFunc
	policy       RetryPolicy
	hooks        Hooks
	timeout      time.Duration
	encode       func(any) ([]byte, error)
	decode       func([]byte) (any, error)
	logger       *slog.Logger
	wg           sync.WaitGroup

	batchEnabled bool
	maxBatchSize int
	maxBatchWait time.Duration
	batchProcess BatchProcessFunc

	dynamic        bool
	minWorkers     int
	maxWorkers     int
	scaleThreshold float64       // jobs-buffer fill fraction that triggers scale-up
	scaleInterval  time.Duration // how often the supervisor samples queue depth
	idleTimeout    time.Duration // a burst worker retires after this long without a task
	activeWorkers  atomic.Int32
	quit           chan struct{} // closed by CloseAndWait to stop the supervisor
	superDone      chan struct{} // closed when the supervisor goroutine exits
}

type ProcessFunc func(ctx context.Context, payload any) error

type Option func(*Pool)

func WithStore(s Store) Option {
	return func(p *Pool) {
		p.store = s
		p.storeEnabled = true
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

// WithBatching makes each worker pull up to maxSize tasks at once and hand them
// to fn as a single batch, instead of processing one task per call. This is the
// natural fit for message consumers (Kafka, SQS, Pub/Sub) where bulk DB writes,
// batched downstream calls, or batch acks amortise per-message overhead across
// the whole batch while multiple workers still run in parallel.
//
// A worker blocks for the first task, then keeps collecting until the batch
// reaches maxSize or maxWait elapses since the first task — whichever comes
// first — so a trickle of work never sits unflushed. maxWait <= 0 means
// best-effort: take whatever is already buffered without lingering.
//
// fn returns one error per task, indexed to match the input slice (nil = that
// task succeeded). Successful tasks are acked immediately; transient failures
// are retried per-item on the next attempt (only the still-failing tasks are
// re-submitted to fn), and permanent or retry-exhausted tasks are dead-lettered
// individually. Returning a nil or shorter slice treats the unaddressed tasks
// as successful.
//
// maxSize < 1 is clamped to 1. Batching composes with WithDynamicWorkers and
// WithStore; producers keep calling Submit with individual tasks.
func WithBatching(maxSize int, maxWait time.Duration, fn BatchProcessFunc) Option {
	return func(p *Pool) {
		if maxSize < 1 {
			maxSize = 1
		}
		p.batchEnabled = true
		p.maxBatchSize = maxSize
		p.maxBatchWait = maxWait
		p.batchProcess = fn
	}
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

func NewPool(bufferSize int, process ProcessFunc, opts ...Option) *Pool {
	cpus := runtime.NumCPU()
	if bufferSize == 0 {
		bufferSize = cpus * 10
	}

	p := &Pool{
		jobs:    make(chan Task, bufferSize),
		results: make(chan Result, bufferSize),
		store:   NoopStore{},
		process: process,
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
		quit:          make(chan struct{}),
		superDone:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *Pool) Start(ctx context.Context, workerCount int) error {
	if p.dynamic {
		// In dynamic mode the worker count is governed by min/max; the
		// workerCount argument is ignored in favour of the configured floor.
		workerCount = p.minWorkers
	} else if workerCount == 0 {
		workerCount = runtime.NumCPU()
	}

	p.log(
		slog.LevelInfo, "ember started",
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

	for i := 0; i < workerCount; i++ {
		p.activeWorkers.Add(1)
		p.wg.Add(1)
		go p.worker(ctx, false)
	}

	if p.dynamic {
		go p.supervise(ctx)
	}

	if p.storeEnabled {
		rawTasks, err := p.store.LoadPendingTasks()
		if err != nil {
			return fmt.Errorf("loading pending tasks: %w", err)
		}
		for _, r := range rawTasks {
			payload, err := p.decode(r.Payload)
			if err != nil {
				return fmt.Errorf("decoding pending task %s: %w", r.ID, err)
			}
			p.jobs <- Task{ID: r.ID, Payload: payload, EnqueuedAt: r.EnqueuedAt, Attempt: r.Attempt}
		}
	}

	return nil
}

func (p *Pool) Submit(ctx context.Context, t Task) error {
	if t.EnqueuedAt.IsZero() {
		t.EnqueuedAt = time.Now()
	}

	if p.storeEnabled {
		encoded, err := p.encode(t.Payload)
		if err != nil {
			return fmt.Errorf("encoding payload for task %s: %w", t.ID, err)
		}
		raw := RawTask{ID: t.ID, Payload: encoded, EnqueuedAt: t.EnqueuedAt, Attempt: t.Attempt}
		if err := p.store.SaveTask(raw); err != nil {
			return fmt.Errorf("persisting task %s: %w", t.ID, err)
		}
	}

	select {
	case p.jobs <- t:
		if p.logger != nil {
			p.log(slog.LevelDebug, "task submitted", "task_id", t.ID)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrBufferFull
	}
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
	p.wg.Wait()
	close(p.results)
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

	if p.storeEnabled {
		if delErr := p.store.DeleteTask(task.ID); delErr != nil && p.hooks.OnStoreError != nil {
			p.hooks.OnStoreError(fmt.Errorf("deleting completed task %s: %w", task.ID, delErr))
		}
	}

	if err != nil {
		dl := DeadLetter{
			Task:      task,
			Err:       err.Error(),
			Permanent: IsPermanent(err),
			FailedAt:  time.Now(),
		}

		if p.storeEnabled {
			encoded, _ := p.encode(task.Payload)
			raw := RawDeadLetter{
				Task:      RawTask{ID: task.ID, Payload: encoded, EnqueuedAt: task.EnqueuedAt, Attempt: task.Attempt},
				Err:       dl.Err,
				Permanent: dl.Permanent,
				FailedAt:  dl.FailedAt,
			}
			if saveErr := p.store.SaveDeadLetter(raw); saveErr != nil && p.hooks.OnStoreError != nil {
				p.hooks.OnStoreError(fmt.Errorf("saving dead letter for task %s: %w", task.ID, saveErr))
			}
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

	p.results <- Result{Task: task, Err: err}
}

func (p *Pool) runWithRetry(ctx context.Context, task *Task) error {
	var err error

	for attempt := 0; attempt < p.policy.MaxAttempts; attempt++ {
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

		delay := p.policy.Delay(attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("after %d attempts: %w", p.policy.MaxAttempts, err)
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
