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

// --- On (typed handlers) ---

func TestOn_RoutesTypedEvent(t *testing.T) {
	var gotCreated batchCreated
	var gotDeleted batchDeleted

	m := NewMux()
	On(m, func(_ context.Context, ev batchCreated) error { gotCreated = ev; return nil })
	On(m, func(_ context.Context, ev batchDeleted) error { gotDeleted = ev; return nil })

	want := batchCreated{BatchID: "b1", Subjects: []string{"math"}}
	if err := m.Process(context.Background(), want); err != nil {
		t.Fatalf("Process(created): %v", err)
	}
	if err := m.Process(context.Background(), batchDeleted{BatchID: "b2"}); err != nil {
		t.Fatalf("Process(deleted): %v", err)
	}

	if !reflect.DeepEqual(gotCreated, want) {
		t.Errorf("created handler got %+v, want %+v", gotCreated, want)
	}
	if gotDeleted.BatchID != "b2" {
		t.Errorf("deleted handler got %q, want b2", gotDeleted.BatchID)
	}
}

// On must share one registry with Handle: typed and untyped handlers for the
// same event fan out together, in registration order.
func TestOn_FansOutWithHandle(t *testing.T) {
	var order []string

	m := NewMux()
	On(m, func(_ context.Context, _ batchCreated) error { order = append(order, "typed"); return nil })
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { order = append(order, "untyped"); return nil })

	if err := m.Process(context.Background(), batchCreated{}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if want := []string{"typed", "untyped"}; !reflect.DeepEqual(order, want) {
		t.Errorf("fan-out order = %v, want %v", order, want)
	}
}

// A handler error must reach the pool unwrapped so RetryPolicy applies; the
// typed wrapper must not mark it permanent.
func TestOn_HandlerErrorStaysTransient(t *testing.T) {
	boom := errors.New("boom")
	m := NewMux()
	On(m, func(_ context.Context, _ batchCreated) error { return boom })

	err := m.Process(context.Background(), batchCreated{})
	if !errors.Is(err, boom) {
		t.Errorf("got err %v, want boom", err)
	}
	if IsPermanent(err) {
		t.Error("a typed handler's transient error must not be marked permanent")
	}
}

// Process normalizes a pointer payload to its value form, so a typed handler
// registered for the value type still receives it.
func TestOn_PointerPayloadReachesValueHandler(t *testing.T) {
	var got batchCreated
	m := NewMux()
	On(m, func(_ context.Context, ev batchCreated) error { got = ev; return nil })

	if err := m.Process(context.Background(), &batchCreated{BatchID: "b1"}); err != nil {
		t.Fatalf("Process(pointer): %v", err)
	}
	if got.BatchID != "b1" {
		t.Errorf("handler got %+v, want BatchID b1", got)
	}
}

// A pointer type parameter would register a tag it can never match after store
// replay (decode yields values), so On rejects it loudly at registration
// instead of dead-lettering every event at runtime.
func TestOn_PointerTypeParamPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("On with a pointer type parameter must panic")
		}
	}()

	m := NewMux()
	On(m, func(_ context.Context, _ *batchCreated) error { return nil })
}

func TestOn_InterfaceTypeParamPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("On with an interface type parameter must panic")
		}
	}()

	m := NewMux()
	On(m, func(_ context.Context, _ Event) error { return nil })
}

// On must register the concrete type for the codec too, so events registered
// only through On still replay into their concrete type after a restart.
func TestOn_RegistersTypeForCodec(t *testing.T) {
	m := NewMux()
	On(m, func(_ context.Context, _ batchCreated) error { return nil })
	p := m.NewPool()

	orig := batchCreated{BatchID: "b1", Subjects: []string{"math", "science"}}
	encoded, err := p.encode(orig)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := p.decode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := decoded.(batchCreated)
	if !ok {
		t.Fatalf("decoded payload is %T, want batchCreated", decoded)
	}
	if !reflect.DeepEqual(got, orig) {
		t.Errorf("round-trip = %+v, want %+v", got, orig)
	}
	// And the decoded value must route back through the typed handler.
	if err := m.Process(context.Background(), decoded); err != nil {
		t.Errorf("Process(decoded): %v", err)
	}
}

func TestOn_EndToEndWithPool(t *testing.T) {
	var faculty, subject atomic.Int64

	m := NewMux()
	p := m.NewPool(WithWorkerCount(4), WithRetryPolicy(fastPolicy(1)))
	On(m, func(_ context.Context, ev batchCreated) error {
		if ev.BatchID != "b" {
			t.Errorf("handler got BatchID %q, want b", ev.BatchID)
		}
		faculty.Add(1)
		return nil
	})
	On(m, func(_ context.Context, _ batchCreated) error { subject.Add(1); return nil })

	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	const n = 10
	var wg sync.WaitGroup
	wg.Go(func() { collectResults(p) })
	for i := range n {
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
	wg.Go(func() { collectResults(p) })
	for i := range n {
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

// --- Mux.NewPool ---

// TestMuxNewPool_WiresCodecAutomatically checks that a pool built via
// m.NewPool round-trips events through the same codec as an explicit
// m.Codec(), without the caller passing it.
func TestMuxNewPool_WiresCodecAutomatically(t *testing.T) {
	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { return nil })

	p := m.NewPool()

	orig := batchCreated{BatchID: "b1", Subjects: []string{"math"}}
	encoded, err := p.encode(orig)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := p.decode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := decoded.(batchCreated)
	if !ok {
		t.Fatalf("decoded payload is %T, want batchCreated", decoded)
	}
	if !reflect.DeepEqual(got, orig) {
		t.Errorf("round-trip = %+v, want %+v", got, orig)
	}
}

// TestMuxNewPool_PassesThroughOtherOptions checks that opts besides the
// codec still reach the pool, so m.NewPool composes with the rest of the
// Option surface (WithStore, WithPartitions, WithDynamicWorkers, ...).
func TestMuxNewPool_PassesThroughOtherOptions(t *testing.T) {
	m := NewMux()
	p := m.NewPool(WithWorkerCount(7))

	if p.workerCount != 7 {
		t.Errorf("workerCount = %d, want 7", p.workerCount)
	}
}

// TestMuxNewPool_ExplicitCodecOverrides checks that an explicit WithCodec
// passed in opts still wins over the auto-added mux codec, per Option's
// override-by-order rule (a later option beats an earlier one on the same
// field).
func TestMuxNewPool_ExplicitCodecOverrides(t *testing.T) {
	m := NewMux()
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { return nil })

	sentinel := errors.New("custom codec used")
	p := m.NewPool(WithCodec(
		func(any) ([]byte, error) { return nil, sentinel },
		func([]byte) (any, error) { return nil, sentinel },
	))

	if _, err := p.encode(batchCreated{}); !errors.Is(err, sentinel) {
		t.Errorf("encode err = %v, want sentinel from the explicit codec", err)
	}
}

// TestMuxNewPool_EndToEnd exercises m.NewPool the way a caller actually
// uses it: build the pool before Start, register handlers, submit events,
// and confirm they route and fan out exactly as with NewPool(m.Process,
// m.Codec(), ...).
func TestMuxNewPool_EndToEnd(t *testing.T) {
	var faculty, subject atomic.Int64

	m := NewMux()
	p := m.NewPool(WithWorkerCount(4), WithRetryPolicy(fastPolicy(1)))
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { faculty.Add(1); return nil })
	m.Handle(batchCreated{}, func(_ context.Context, _ any) error { subject.Add(1); return nil })

	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	const n = 10
	var wg sync.WaitGroup
	wg.Go(func() { collectResults(p) })
	for i := range n {
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
