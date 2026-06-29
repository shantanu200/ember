package ember

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// BatchProcessFunc processes a batch of tasks in a single call and returns one
// error per task, indexed to match the input slice (errs[i] belongs to
// tasks[i]); a nil entry means that task succeeded. Returning a nil slice — or
// one shorter than the batch — treats the unaddressed tasks as successful.
//
// Enable batching by constructing the pool with NewPoolWithBatch.
type BatchProcessFunc func(ctx context.Context, tasks []Task) []error

// batchWorker is the batch-mode counterpart of worker. It repeatedly gathers a
// batch and settles it. Permanent workers loop until ctx is cancelled or the
// jobs channel is closed; ephemeral burst workers additionally retire after
// idleTimeout with no work, letting the dynamic scaler shrink back to its floor.
func (p *Pool) batchWorker(ctx context.Context, ephemeral bool) {
	for {
		batch, ok := p.gatherBatch(ctx, ephemeral)
		if len(batch) > 0 {
			p.handleBatch(ctx, batch)
		}
		if !ok {
			return
		}
	}
}

// gatherBatch blocks for the first task, then collects more until the batch is
// full or maxBatchWait elapses. The second return value is false once the
// worker should stop: the jobs channel was closed with nothing pending, ctx was
// cancelled, or (for ephemeral workers) idleTimeout elapsed before any task
// arrived. A partial batch collected before a stop signal is still returned for
// processing, with ok=true, so the next call observes the stop cleanly.
func (p *Pool) gatherBatch(ctx context.Context, ephemeral bool) ([]Task, bool) {
	var (
		first Task
		ok    bool
	)

	if ephemeral {
		idle := time.NewTimer(p.idleTimeout)
		defer idle.Stop()
		select {
		case <-ctx.Done():
			return nil, false
		case <-idle.C:
			return nil, false
		case first, ok = <-p.jobs:
			if !ok {
				return nil, false
			}
		}
	} else {
		select {
		case <-ctx.Done():
			return nil, false
		case first, ok = <-p.jobs:
			if !ok {
				return nil, false
			}
		}
	}

	batch := make([]Task, 0, p.maxBatchSize)
	batch = append(batch, first)
	if len(batch) >= p.maxBatchSize {
		return batch, true
	}

	// Best-effort mode: take whatever is already buffered without waiting.
	if p.maxBatchWait <= 0 {
		for len(batch) < p.maxBatchSize {
			select {
			case t, ok := <-p.jobs:
				if !ok {
					return batch, true
				}
				batch = append(batch, t)
			default:
				return batch, true
			}
		}
		return batch, true
	}

	timer := time.NewTimer(p.maxBatchWait)
	defer timer.Stop()
	for len(batch) < p.maxBatchSize {
		select {
		case <-ctx.Done():
			return batch, true
		case <-timer.C:
			return batch, true
		case t, ok := <-p.jobs:
			if !ok {
				return batch, true
			}
			batch = append(batch, t)
		}
	}
	return batch, true
}

// handleBatch runs the batch through the retry loop, then settles each task:
// successes fire OnSuccess, failures fire OnDeadLetter, every processed task is
// removed from the pending store, dead letters are persisted, and one Result is
// emitted per task on Results().
func (p *Pool) handleBatch(ctx context.Context, batch []Task) {
	start := time.Now()
	if p.logger != nil {
		p.log(slog.LevelDebug, "batch started", "size", len(batch))
	}

	errs := p.runBatchWithRetry(ctx, batch)

	var deadLetters []RawDeadLetter
	for i := range batch {
		task := batch[i]
		err := errs[i]

		if err == nil {
			if p.logger != nil {
				p.log(
					slog.LevelDebug, "task succeeded",
					"task_id", task.ID,
					"attempts", task.Attempt+1,
				)
			}
			if p.hooks.OnSuccess != nil {
				p.hooks.OnSuccess(task)
			}
			continue
		}

		dl := DeadLetter{
			Task:      task,
			Err:       err.Error(),
			Permanent: IsPermanent(err),
			FailedAt:  time.Now(),
		}
		if p.storeEnabled {
			encoded, _ := p.encode(task.Payload)
			deadLetters = append(deadLetters, RawDeadLetter{
				Task:      RawTask{ID: task.ID, Payload: encoded, EnqueuedAt: task.EnqueuedAt, Attempt: task.Attempt},
				Err:       dl.Err,
				Permanent: dl.Permanent,
				FailedAt:  dl.FailedAt,
			})
		}
		p.log(
			slog.LevelWarn, "task dead-lettered",
			"task_id", task.ID,
			"attempts", task.Attempt+1,
			"permanent", dl.Permanent,
			"error", dl.Err,
		)
		if p.hooks.OnDeadLetter != nil {
			p.hooks.OnDeadLetter(dl)
		}
	}

	if p.storeEnabled {
		// Persist dead letters before removing the batch from the pending
		// store, so a crash can't drop a failed task that is neither pending
		// nor dead-lettered.
		if len(deadLetters) > 0 {
			p.saveDeadLetters(deadLetters)
		}
		ids := make([]string, len(batch))
		for i := range batch {
			ids[i] = batch[i].ID
		}
		p.deleteTasks(ids)
	}

	if p.logger != nil {
		p.log(slog.LevelDebug, "batch settled", "size", len(batch), "elapsed", time.Since(start))
	}

	for i := range batch {
		p.emit(ctx, Result{Task: batch[i], Err: errs[i]})
	}
}

// runBatchWithRetry processes the batch with per-item retry: each attempt calls
// the batch processor with only the tasks still pending, successes and permanent
// failures settle immediately, and transient failures are carried into the next
// attempt after a backoff. The returned slice holds the final error per task,
// indexed to match batch (nil = succeeded). Each task's Attempt field is updated
// in place to the attempt on which it last ran.
func (p *Pool) runBatchWithRetry(ctx context.Context, batch []Task) []error {
	finalErr := make([]error, len(batch))

	pending := make([]int, 0, len(batch))
	for i := range batch {
		// Resume each task from its own carried attempt (a reload after a crash
		// brings the attempt it had reached); clamp a value that already meets
		// the budget so the task still gets one final run before dead-lettering.
		batch[i].Attempt = max(batch[i].Attempt, 0)
		if batch[i].Attempt >= p.policy.MaxAttempts {
			batch[i].Attempt = p.policy.MaxAttempts - 1
		}
		pending = append(pending, i)
	}

	for len(pending) > 0 {
		sub := make([]Task, len(pending))
		for j, idx := range pending {
			sub[j] = batch[idx]
		}

		errs := p.runBatchOnce(ctx, sub)

		var (
			next       []int
			persist    []Task
			minAttempt = p.policy.MaxAttempts
		)
		for j, idx := range pending {
			err := errs[j]
			finalErr[idx] = err
			if err == nil || IsPermanent(err) {
				continue
			}
			// Out of budget: dead-letter, framed like the single-task path.
			if batch[idx].Attempt+1 >= p.policy.MaxAttempts {
				finalErr[idx] = fmt.Errorf("after %d attempts: %w", p.policy.MaxAttempts, err)
				continue
			}
			p.log(
				slog.LevelWarn, "task retrying",
				"task_id", batch[idx].ID,
				"attempt", batch[idx].Attempt+1,
				"max_attempts", p.policy.MaxAttempts,
				"error", err,
			)
			if p.hooks.OnRetry != nil {
				p.hooks.OnRetry(batch[idx], err, batch[idx].Attempt)
			}
			batch[idx].Attempt++
			minAttempt = min(minAttempt, batch[idx].Attempt)
			persist = append(persist, batch[idx])
			next = append(next, idx)
		}

		if len(next) == 0 {
			return finalErr
		}

		// Durably advance attempts so a crash mid-retry resumes each task with
		// its remaining budget rather than a fresh one.
		for i := range persist {
			p.persistAttempt(&persist[i])
		}

		select {
		case <-ctx.Done():
			for _, idx := range next {
				finalErr[idx] = ctx.Err()
			}
			return finalErr
		case <-time.After(p.policy.Delay(minAttempt - 1)):
		}
		pending = next
	}

	return finalErr
}

// runBatchOnce invokes the batch processor once under a per-attempt timeout,
// recovering panics. It always returns a slice aligned to batch: a panic marks
// every task with the panic error, and a nil or short result from the processor
// leaves the unaddressed tasks as nil (success).
func (p *Pool) runBatchOnce(parent context.Context, batch []Task) (errs []error) {
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()

	errs = make([]error, len(batch))

	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic: %v", r)
			for i := range errs {
				errs[i] = err
			}
		}
	}()

	out := p.batchProcess(ctx, batch)
	for i := range errs {
		if i < len(out) {
			errs[i] = out[i]
		}
	}
	return errs
}

// deleteTasks removes processed tasks from the pending store, using a single
// BatchStore call when the store supports it, else one DeleteTask per id.
func (p *Pool) deleteTasks(ids []string) {
	if bs, ok := p.store.(BatchStore); ok {
		if err := bs.DeleteTasks(ids); err != nil && p.hooks.OnStoreError != nil {
			p.hooks.OnStoreError(fmt.Errorf("deleting %d completed tasks: %w", len(ids), err))
		}
		return
	}
	for _, id := range ids {
		if err := p.store.DeleteTask(id); err != nil && p.hooks.OnStoreError != nil {
			p.hooks.OnStoreError(fmt.Errorf("deleting completed task %s: %w", id, err))
		}
	}
}

// saveDeadLetters persists dead letters, using a single BatchStore call when
// the store supports it, else one SaveDeadLetter per entry.
func (p *Pool) saveDeadLetters(dls []RawDeadLetter) {
	if bs, ok := p.store.(BatchStore); ok {
		if err := bs.SaveDeadLetters(dls); err != nil && p.hooks.OnStoreError != nil {
			p.hooks.OnStoreError(fmt.Errorf("saving %d dead letters: %w", len(dls), err))
		}
		return
	}
	for _, dl := range dls {
		if err := p.store.SaveDeadLetter(dl); err != nil && p.hooks.OnStoreError != nil {
			p.hooks.OnStoreError(fmt.Errorf("saving dead letter for task %s: %w", dl.Task.ID, err))
		}
	}
}
