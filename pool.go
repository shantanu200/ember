package ember

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Pool[T any] struct {
	jobs    chan Task[T]
	results chan Result[T]
	store   Store[T]
	process ProcessFunc[T]
	policy  RetryPolicy
	hooks   Hooks[T]
	timeout time.Duration
	wg      sync.WaitGroup
	rwg     sync.WaitGroup // tracks in-flight result sends
}

type ProcessFunc[T any] func(ctx context.Context, payload T) error

type Option[T any] func(*Pool[T])

func WithStore[T any](s Store[T]) Option[T] {
	return func(p *Pool[T]) { p.store = s }
}

func WithRetryPolicy[T any](r RetryPolicy) Option[T] {
	return func(p *Pool[T]) { p.policy = r }
}

func WithHooks[T any](h Hooks[T]) Option[T] {
	return func(p *Pool[T]) { p.hooks = h }
}

func WithTaskTimeout[T any](d time.Duration) Option[T] {
	return func(p *Pool[T]) { p.timeout = d }
}

func NewPool[T any](bufferSize int, process ProcessFunc[T], opts ...Option[T]) *Pool[T] {
	p := &Pool[T]{
		jobs:    make(chan Task[T], bufferSize),
		results: make(chan Result[T], bufferSize),
		store:   NoopStore[T]{},
		process: process,
		policy:  DefaultRetryPolicy,
		timeout: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *Pool[T]) Start(ctx context.Context, workerCount int) error {
	pending, err := p.store.LoadPendingTasks()
	if err != nil {
		return fmt.Errorf("loading pending tasks: %w", err)
	}

	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}

	for _, t := range pending {
		p.jobs <- t
	}

	return nil
}

func (p *Pool[T]) Submit(ctx context.Context, t Task[T]) error {
	if t.EnqueuedAt.IsZero() {
		t.EnqueuedAt = time.Now()
	}

	if err := p.store.SaveTask(t); err != nil {
		return fmt.Errorf("persisting task %s: %w", t.ID, err)
	}

	select {
	case p.jobs <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool[T]) Results() <-chan Result[T] {
	return p.results
}

func (p *Pool[T]) CloseAndWait() {
	close(p.jobs)
	p.wg.Wait()
	p.rwg.Wait()
	close(p.results)
}

func (p *Pool[T]) worker(ctx context.Context) {
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

func (p *Pool[T]) handle(ctx context.Context, task Task[T]) {
	err := p.runWithRetry(ctx, &task)

	if delErr := p.store.DeleteTask(task.ID); delErr != nil && p.hooks.OnStoreError != nil {
		p.hooks.OnStoreError(fmt.Errorf("deleting completed task %s: %w", task.ID, delErr))
	}

	if err != nil {
		dl := DeadLetter[T]{
			Task:      task,
			Err:       err.Error(),
			Permanent: IsPermanent(err),
			FailedAt:  time.Now(),
		}
		if saveErr := p.store.SaveDeadLetter(dl); saveErr != nil && p.hooks.OnStoreError != nil {
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
		p.results <- Result[T]{Task: task, Err: err}
	}()
}

func (p *Pool[T]) runWithRetry(ctx context.Context, task *Task[T]) error {
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

func (p *Pool[T]) runOnce(parent context.Context, payload T) error {
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
