package quelon_test

import (
	"context"
	"fmt"

	"github.com/shantanu200/quelon"
)

// The basic lifecycle: construct, Start, Submit, drain Results, CloseAndWait.
func Example() {
	pool := quelon.NewPool(func(_ context.Context, payload any) error {
		return nil // process payload here
	}, quelon.WithWorkerCount(4))

	if err := pool.Start(context.Background()); err != nil {
		panic(err)
	}

	// Drain Results in its own goroutine so workers never block on a full
	// results buffer.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range pool.Results() {
		}
	}()

	_ = pool.Submit(context.Background(), quelon.Task{ID: "t1", Payload: "hello"})

	pool.CloseAndWait() // finish in-flight work; closes Results
	<-done
	fmt.Println("done")
	// Output: done
}

// A malformed payload can never succeed, so a PermanentError skips retries and
// dead-letters it immediately instead of burning the retry budget.
func ExampleNewPermanentError() {
	process := func(_ context.Context, payload any) error {
		s, ok := payload.(string)
		if !ok {
			return quelon.NewPermanentError(fmt.Errorf("want string, got %T", payload))
		}
		fmt.Println("processing", s)
		return nil
	}

	err := process(context.Background(), 42)
	fmt.Println("permanent:", quelon.IsPermanent(err))
	// Output: permanent: true
}

// WithPartitions routes tasks by Key to serial lanes: tasks sharing a Key are
// processed one at a time, in submission order, even across many workers.
func ExampleWithPartitions() {
	pool := quelon.NewPool(func(_ context.Context, payload any) error {
		fmt.Println(payload)
		return nil
	}, quelon.WithPartitions(8))
	if err := pool.Start(context.Background()); err != nil {
		panic(err)
	}
	go func() {
		for range pool.Results() {
		}
	}()

	// Same Key ("acct-1") => same lane => processed in submission order.
	for _, step := range []string{"open", "deposit", "close"} {
		_ = pool.Submit(context.Background(), quelon.Task{ID: step, Key: "acct-1", Payload: step})
	}
	pool.CloseAndWait()
	// Output:
	// open
	// deposit
	// close
}

// Mux routes each event to every handler registered for its type. This is how
// one "batch created" event cascades into the other collections that must stay
// in sync with it: register one handler per downstream concern and the fan-out
// runs them all, in registration order.
//
// Here creating a batch must both (1) assign faculty for the batch and (2) seed
// the subject schema. Both handlers read the same BatchCreated payload; neither
// knows the other exists, so a new downstream concern is just another Handle
// call — no producer or existing handler changes.
func ExampleMux() {
	mux := quelon.NewMux()

	// Faculty module: reserve a faculty slot per subject in the batch.
	mux.Handle(BatchCreated{}, func(_ context.Context, p any) error {
		ev := p.(BatchCreated)
		fmt.Printf("faculty: reserving %d slots for batch %s\n", len(ev.Subjects), ev.BatchID)
		return nil
	})

	// Subject module: seed the schema for each subject in the batch.
	mux.Handle(BatchCreated{}, func(_ context.Context, p any) error {
		ev := p.(BatchCreated)
		for _, s := range ev.Subjects {
			fmt.Printf("subject: seeding schema for %q in batch %s\n", s, ev.BatchID)
		}
		return nil
	})

	// mux.Process is the ProcessFunc you pass to NewPool; called directly here.
	// One emit, both modules react.
	_ = mux.Process(context.Background(), BatchCreated{
		BatchID:  "b1",
		Subjects: []string{"math", "physics"},
	})
	// Output:
	// faculty: reserving 2 slots for batch b1
	// subject: seeding schema for "math" in batch b1
	// subject: seeding schema for "physics" in batch b1
}

// An event with no registered handler is a permanent failure: retrying cannot
// make an unroutable task routable, so it is dead-lettered rather than retried.
func ExampleMux_unroutable() {
	mux := quelon.NewMux()
	mux.Handle(BatchCreated{}, func(context.Context, any) error { return nil })

	err := mux.Process(context.Background(), BatchDeleted{BatchID: "b1"})
	fmt.Println("permanent:", quelon.IsPermanent(err))
	// Output: permanent: true
}

// Wiring a Mux into a pool with persistence. mux.Codec makes persisted events
// replay into their concrete type after a restart. (Illustrative — a real
// program passes a store via WithStore alongside mux.Codec.)
func ExampleMux_pool() {
	mux := quelon.NewMux()
	mux.Handle(BatchCreated{}, func(_ context.Context, p any) error {
		fmt.Println("handling", p.(BatchCreated).BatchID)
		return nil
	})

	pool := quelon.NewPool(mux.Process, mux.Codec(), quelon.WithWorkerCount(2))
	if err := pool.Start(context.Background()); err != nil {
		panic(err)
	}
	go func() {
		for range pool.Results() {
		}
	}()

	_ = pool.Submit(context.Background(), quelon.Task{ID: "batch-created-b1", Payload: BatchCreated{BatchID: "b1"}})
	pool.CloseAndWait()
	// Output: handling b1
}

// BatchCreated and BatchDeleted are example events: each implements
// quelon.Event with a value receiver so the Mux can route it and, with
// persistence, reconstruct it from the store.
type BatchCreated struct {
	BatchID  string   `json:"batchID"`
	Subjects []string `json:"subjects"`
}

func (BatchCreated) EventType() string { return "batch.created" }

type BatchDeleted struct {
	BatchID string `json:"batchID"`
}

func (BatchDeleted) EventType() string { return "batch.deleted" }
