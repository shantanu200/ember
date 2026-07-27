// Command batch-consumer shows quelon.NewPoolWithBatch used as a message
// consumer: it pulls many messages per call for a single bulk write, settles
// each item individually, and dead-letters poison messages to a durable archive.
//
// It uses PersistDeadLettersOnly — the persistence mode to reach for when an
// upstream broker (Kafka, SQS, Pub/Sub) already owns durability of the stream,
// so quelon only needs somewhere to park messages it cannot process.
//
// Run it with:
//
//	go run ./examples/02-batch-consumer
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shantanu200/quelon"
)

// message is one item off the stream. Poison marks one that can never be
// processed, standing in for a malformed / unparseable payload.
type message struct {
	ID     string
	Body   string
	Poison bool
}

// archive is a minimal in-memory Store used only as a dead-letter sink; the
// pending-task methods are no-ops because PersistDeadLettersOnly never persists
// pending work.
type archive struct {
	mu   sync.Mutex
	dead []quelon.RawDeadLetter
}

func (a *archive) SaveTask(quelon.RawTask) error               { return nil }
func (a *archive) DeleteTask(string) error                     { return nil }
func (a *archive) LoadPendingTasks() ([]quelon.RawTask, error) { return nil, nil }
func (a *archive) DeleteDeadLetter(string) error               { return nil }

func (a *archive) SaveDeadLetter(dl quelon.RawDeadLetter) error {
	a.mu.Lock()
	a.dead = append(a.dead, dl)
	a.mu.Unlock()
	return nil
}

func (a *archive) LoadDeadLetters() ([]quelon.RawDeadLetter, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]quelon.RawDeadLetter(nil), a.dead...), nil
}

func main() {
	ctx := context.Background()
	arc := &archive{}
	var inserted atomic.Int64

	pool := quelon.NewPoolWithBatch(
		func(_ context.Context, tasks []quelon.Task) []error {
			// One error slot per task, indexed to match the input (nil = ok).
			errs := make([]error, len(tasks))
			good := 0
			for i, t := range tasks {
				m := t.Payload.(message)
				if m.Poison {
					errs[i] = quelon.NewPermanentError(fmt.Errorf("cannot parse %s", m.ID))
					continue
				}
				good++
			}
			inserted.Add(int64(good)) // stands in for one bulk INSERT per batch
			return errs
		},
		quelon.WithMaxBatchSize(50),
		quelon.WithMaxBatchWait(20*time.Millisecond),
		quelon.WithWorkerCount(4),
		quelon.WithStoreMode(arc, quelon.PersistDeadLettersOnly),
		quelon.WithHooks(quelon.Hooks{
			OnDeadLetter: func(dl quelon.DeadLetter) {
				fmt.Printf("dead-letter: %s (%s)\n", dl.Task.ID, dl.Err)
			},
		}),
	)

	if err := pool.Start(ctx); err != nil {
		panic(err)
	}
	go func() {
		for range pool.Results() {
		}
	}()

	// Feed 200 messages; every 50th is poison (4 total).
	const total = 200
	for i := range total {
		m := message{ID: fmt.Sprintf("msg-%d", i), Body: "payload"}
		if i%50 == 0 {
			m.Poison = true
		}
		if err := pool.Submit(ctx, quelon.Task{ID: m.ID, Payload: m}); err != nil {
			fmt.Printf("submit %s rejected: %v\n", m.ID, err)
		}
	}

	pool.CloseAndWait()

	fmt.Printf("\ninserted %d messages in bulk batches\n", inserted.Load())
	fmt.Printf("dead-lettered %d poison messages (archived durably)\n", len(arc.dead))
}
