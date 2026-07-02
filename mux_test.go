package quelon

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

// --- test events ---

type batchCreated struct {
	BatchID  string   `json:"batchID"`
	Subjects []string `json:"subjects"`
}

func (batchCreated) EventType() string { return "batch.created" }

type batchDeleted struct {
	BatchID string `json:"batchID"`
}

func (batchDeleted) EventType() string { return "batch.deleted" }

// --- routing ---

func TestMux_RoutesByEventType(t *testing.T) {
	var gotCreated, gotDeleted string

	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, p any) error {
		gotCreated = p.(batchCreated).BatchID
		return nil
	})
	m.Handle(batchDeleted{}, func(_ context.Context, p any) error {
		gotDeleted = p.(batchDeleted).BatchID
		return nil
	})

	if err := m.Process(context.Background(), batchCreated{BatchID: "b1"}); err != nil {
		t.Fatalf("Process(created): %v", err)
	}
	if err := m.Process(context.Background(), batchDeleted{BatchID: "b2"}); err != nil {
		t.Fatalf("Process(deleted): %v", err)
	}

	if gotCreated != "b1" {
		t.Errorf("created handler got %q, want b1", gotCreated)
	}
	if gotDeleted != "b2" {
		t.Errorf("deleted handler got %q, want b2", gotDeleted)
	}
}

func TestMux_FanOut(t *testing.T) {
	var order []string
	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error {
		order = append(order, "faculty")
		return nil
	})
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error {
		order = append(order, "subject")
		return nil
	})

	if err := m.Process(context.Background(), batchCreated{BatchID: "b1"}); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if want := []string{"faculty", "subject"}; !reflect.DeepEqual(order, want) {
		t.Errorf("fan-out order = %v, want %v", order, want)
	}
}

func TestMux_FanOutStopsOnFirstError(t *testing.T) {
	boom := errors.New("boom")
	second := false
	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { return boom })
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error {
		second = true
		return nil
	})

	err := m.Process(context.Background(), batchCreated{})
	if !errors.Is(err, boom) {
		t.Errorf("got err %v, want boom", err)
	}
	if IsPermanent(err) {
		t.Error("a handler's transient error must not be marked permanent")
	}
	if second {
		t.Error("second handler ran after the first errored; fan-out must fail fast")
	}
}

func TestMux_UnknownEventTypeIsPermanent(t *testing.T) {
	m := NewMux()
	// registered something else, but not batchDeleted
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { return nil })

	err := m.Process(context.Background(), batchDeleted{BatchID: "x"})
	if err == nil {
		t.Fatal("want error for unknown event type, got nil")
	}
	if !IsPermanent(err) {
		t.Errorf("unknown event type must be permanent (dead-letter, not retry); got %v", err)
	}
}

func TestMux_NonEventPayloadIsPermanent(t *testing.T) {
	m := NewMux()
	err := m.Process(context.Background(), 42)
	if !IsPermanent(err) {
		t.Errorf("non-Event payload must be permanent; got %v", err)
	}
}

// A pointer payload still satisfies Event (value-receiver method set), so it
// routes — but handlers assert the value type. The Mux must normalize so a
// handler sees the same concrete type whether the task was submitted as a
// value, submitted as a pointer, or reconstructed from the store (a value).
func TestMux_PointerPayloadNormalizedToValue(t *testing.T) {
	var got any
	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, p any) error {
		got = p
		return nil
	})

	if err := m.Process(context.Background(), &batchCreated{BatchID: "b1"}); err != nil {
		t.Fatalf("Process(pointer): %v", err)
	}
	if _, ok := got.(batchCreated); !ok {
		t.Fatalf("handler received %T, want value batchCreated", got)
	}
}

// --- codec (persistence round-trip) ---

func TestMux_CodecRoundTrip(t *testing.T) {
	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { return nil })

	// Apply the mux codec to a pool and exercise the same encode/decode the
	// write-ahead log and crash-replay paths use.
	p := NewPool(m.Process, m.Codec())

	orig := batchCreated{BatchID: "b1", Subjects: []string{"math", "science"}}
	encoded, err := p.encode(orig)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := p.decode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// The decoded value must be the concrete event again (not a generic map),
	// so Process can route it after a restart.
	got, ok := decoded.(batchCreated)
	if !ok {
		t.Fatalf("decoded payload is %T, want batchCreated", decoded)
	}
	if !reflect.DeepEqual(got, orig) {
		t.Errorf("round-trip = %+v, want %+v", got, orig)
	}
}

func TestMux_CodecUnknownTypeErrors(t *testing.T) {
	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { return nil })
	p := NewPool(m.Process, m.Codec())

	encoded, err := p.encode(batchDeleted{BatchID: "b1"}) // never registered
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := p.decode(encoded); err == nil {
		t.Error("decode of unregistered event type must error, got nil")
	}
}

// --- end to end through a running pool ---

func TestMux_EndToEndWithPool(t *testing.T) {
	var faculty, subject atomic.Int64

	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { faculty.Add(1); return nil })
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { subject.Add(1); return nil })

	p := NewPool(m.Process, m.Codec(), WithWorkerCount(4), WithRetryPolicy(fastPolicy(1)))
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	const n = 10
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); collectResults(p) }()
	for i := 0; i < n; i++ {
		if err := p.Submit(context.Background(), Task{ID: "batch-" + string(rune('a'+i)), Payload: batchCreated{BatchID: "b"}}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	p.CloseAndWait()
	wg.Wait()

	if faculty.Load() != n || subject.Load() != n {
		t.Errorf("faculty=%d subject=%d, want %d each", faculty.Load(), subject.Load(), n)
	}
}
