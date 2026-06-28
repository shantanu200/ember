package ember

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

type Pool struct {
	jobs    chan Task
	results chan Result
	store   Store
	process ProcessFunc
	policy  RetryPolicy
	hooks   Hooks
	timeout time.Duration
	encode  func(any) ([]byte, error)
	decode  func([]byte) (any, error)
	logger  func(string)
	wg      sync.WaitGroup
	rwg     sync.WaitGroup
}

type ProcessFunc func(ctx context.Context, payload any) error

type Option func(*Pool)

func WithStore(s Store) Option {
	return func(p *Pool) { p.store = s }
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

// WithLogger sets a function to receive the startup info line.
// Pass nil to silence it.
func WithLogger(fn func(string)) Option {
	return func(p *Pool) { p.logger = fn }
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
		logger: func(msg string) { fmt.Fprintln(os.Stderr, msg) },
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *Pool) Start(ctx context.Context, workerCount int) error {
	if workerCount == 0 {
		workerCount = runtime.NumCPU()
	}

	if p.logger != nil {
		p.logger(fmt.Sprintf(
			"[ember] workers=%d buffer=%d timeout=%s retries=%d delay=%s..%s",
			workerCount,
			cap(p.jobs),
			p.timeout,
			p.policy.MaxAttempts,
			p.policy.BaseDelay,
			p.policy.MaxDelay,
		))
	}

	rawTasks, err := p.store.LoadPendingTasks()
	if err != nil {
		return fmt.Errorf("loading pending tasks: %w", err)
	}

	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}

	for _, r := range rawTasks {
		payload, err := p.decode(r.Payload)
		if err != nil {
			return fmt.Errorf("decoding pending task %s: %w", r.ID, err)
		}
		p.jobs <- Task{ID: r.ID, Payload: payload, EnqueuedAt: r.EnqueuedAt, Attempt: r.Attempt}
	}

	return nil
}

func (p *Pool) Submit(ctx context.Context, t Task) error {
	if t.EnqueuedAt.IsZero() {
		t.EnqueuedAt = time.Now()
	}

	encoded, err := p.encode(t.Payload)
	if err != nil {
		return fmt.Errorf("encoding payload for task %s: %w", t.ID, err)
	}

	raw := RawTask{ID: t.ID, Payload: encoded, EnqueuedAt: t.EnqueuedAt, Attempt: t.Attempt}
	if err := p.store.SaveTask(raw); err != nil {
		return fmt.Errorf("persisting task %s: %w", t.ID, err)
	}

	select {
	case p.jobs <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) Results() <-chan Result {
	return p.results
}

func (p *Pool) CloseAndWait() {
	close(p.jobs)
	p.wg.Wait()
	p.rwg.Wait()
	close(p.results)
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()
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

func (p *Pool) handle(ctx context.Context, task Task) {
	err := p.runWithRetry(ctx, &task)

	if delErr := p.store.DeleteTask(task.ID); delErr != nil && p.hooks.OnStoreError != nil {
		p.hooks.OnStoreError(fmt.Errorf("deleting completed task %s: %w", task.ID, delErr))
	}

	if err != nil {
		dl := DeadLetter{
			Task:      task,
			Err:       err.Error(),
			Permanent: IsPermanent(err),
			FailedAt:  time.Now(),
		}

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

		if p.hooks.OnDeadLetter != nil {
			p.hooks.OnDeadLetter(dl)
		}
	} else if p.hooks.OnSuccess != nil {
		p.hooks.OnSuccess(task)
	}

	p.rwg.Add(1)
	go func() {
		defer p.rwg.Done()
		p.results <- Result{Task: task, Err: err}
	}()
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

		if p.hooks.OnRetry != nil {
			p.hooks.OnRetry(*task, err, attempt)
		}

		timer := time.NewTimer(p.policy.Delay(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return fmt.Errorf("after %d attempts: %w", p.policy.MaxAttempts, err)
}

func (p *Pool) runOnce(parent context.Context, payload any) error {
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic: %v", r)
			}
		}()
		done <- p.process(ctx, payload)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("task timed out: %w", ctx.Err())
	}
}
