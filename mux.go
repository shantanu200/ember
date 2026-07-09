package quelon

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// Event is a task payload that carries its own type tag. Implement it on the
// command/event structs you Submit so a Mux can route them to handlers and,
// with persistence enabled, replay them back into their concrete type after a
// crash. Implement EventType on a value receiver and Submit the value (not a
// pointer): the Mux reconstructs values when decoding persisted tasks, so a
// pointer-receiver Event would fail to route after a restart.
type Event interface {
	EventType() string
}

// Mux turns a single Pool ProcessFunc into event-type routing with fan-out. It
// solves two problems at once: dispatching a variety of event types through one
// pool, and the construction cycle you hit when a handler's dependencies need
// the Pool (to Submit) while the Pool needs the handler (to process). Because
// Mux.Process is a stable method the moment NewMux returns, you pass it to
// NewPool up front and register handlers afterward, once services are wired:
//
//	mux := quelon.NewMux()
//	pool := mux.NewPool(quelon.WithStore(store)) // codec wired automatically
//	faculty.Register(mux, facultySvc) // mux.Handle(BatchCreated{}, svc.OnBatchCreated)
//	subject.Register(mux, subjectSvc)
//	pool.Start(ctx)
//
// A Mux is configured with Handle before Start and is not safe for concurrent
// registration once the pool is running; Process itself is safe to call from
// many workers concurrently.
type Mux struct {
	handlers map[string][]ProcessFunc
	proto    map[string]reflect.Type
}

// NewMux returns an empty Mux ready for Handle registrations.
func NewMux() *Mux {
	return &Mux{
		handlers: make(map[string][]ProcessFunc),
		proto:    make(map[string]reflect.Type),
	}
}

// Handle registers h for events whose EventType matches sample's. Registering
// more than one handler for the same event type fans out: every handler runs,
// in registration order, when such an event is processed. sample is only used
// for its type tag and concrete type (for persistence decoding); its field
// values are ignored, so pass a zero value like BatchCreated{}.
func (m *Mux) Handle(sample Event, h ProcessFunc) {
	t := reflect.TypeOf(sample)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	key := sample.EventType()
	m.handlers[key] = append(m.handlers[key], h)
	m.proto[key] = t
}

// Process routes payload to the handlers registered for its event type. It is
// the ProcessFunc you pass to NewPool. A payload that is not an Event, or whose
// event type has no registered handler, is a permanent failure (dead-lettered,
// not retried) since retrying cannot make an unroutable task routable. A
// handler's own error is returned unwrapped so the pool's RetryPolicy applies;
// on fan-out the first error stops the remaining handlers.
func (m *Mux) Process(ctx context.Context, payload any) error {
	ev, ok := payload.(Event)
	if !ok {
		return NewPermanentError(fmt.Errorf("quelon: payload %T is not a quelon.Event", payload))
	}
	handlers, ok := m.handlers[ev.EventType()]
	if !ok {
		return NewPermanentError(fmt.Errorf("quelon: no handler registered for event %q", ev.EventType()))
	}
	// Normalize a pointer payload to its value form so handlers see one concrete
	// type regardless of how the task arrived: submitted as a value, submitted as
	// a pointer (still an Event via the value-receiver method set), or replayed
	// from the store (decode always yields a value).
	if rv := reflect.ValueOf(payload); rv.Kind() == reflect.Ptr && !rv.IsNil() {
		payload = rv.Elem().Interface()
	}
	for _, h := range handlers {
		if err := h(ctx, payload); err != nil {
			return err
		}
	}
	return nil
}

// muxEnvelope is the on-disk shape for a persisted event: the type tag plus the
// event's own JSON, so decode knows which concrete type to reconstruct.
type muxEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// Codec returns an Option that makes persisted tasks round-trip through the
// event type tag. Without it, WithStore's default codec decodes payloads into a
// generic any (a map), which Process cannot route after a restart. Pass it
// alongside WithStore/WithStoreMode; it is a no-op when persistence is off.
func (m *Mux) Codec() Option {
	return WithCodec(m.encode, m.decode)
}

func (m *Mux) encode(v any) ([]byte, error) {
	ev, ok := v.(Event)
	if !ok {
		return nil, fmt.Errorf("quelon: cannot encode payload %T: not a quelon.Event", v)
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("quelon: encoding event %q: %w", ev.EventType(), err)
	}
	return json.Marshal(muxEnvelope{Type: ev.EventType(), Data: data})
}

func (m *Mux) decode(b []byte) (any, error) {
	var env muxEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("quelon: decoding event envelope: %w", err)
	}
	t, ok := m.proto[env.Type]
	if !ok {
		return nil, fmt.Errorf("quelon: no registered event for type %q", env.Type)
	}
	ptr := reflect.New(t)
	if err := json.Unmarshal(env.Data, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("quelon: decoding event %q: %w", env.Type, err)
	}
	return ptr.Elem().Interface(), nil
}

// NewPool builds a Pool wired to m: process routes through m.Process and m's
// Codec() is applied automatically, so callers never pass encode/decode by
// hand. Equivalent to:
//
//	quelon.NewPool(m.Process, append([]quelon.Option{m.Codec()}, opts...)...)
//
// opts are applied after the codec, so an explicit WithCodec in opts still
// overrides it (see Option's override-by-order rule). Handle may be called
// before or after NewPool — m.Codec()'s encode/decode read m.proto at call
// time, not at registration time, so registration order doesn't matter here.
func (m *Mux) NewPool(opts ...Option) *Pool {
	return NewPool(m.Process, append([]Option{m.Codec()}, opts...)...)
}
