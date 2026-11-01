package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/google/uuid"
)

func TestRelayMarksOnlyRoutedConfirmsAndContinuesPastReturns(t *testing.T) {
	ctx, s := fixtureStore(t)
	rows := seed(t, ctx, s, 3)
	p := &publisher{publish: func(ctx context.Context, e store.OutboxEvent) error {
		pending, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
		if err != nil || len(pending) != 3 {
			t.Fatalf("mark preceded confirms: rows=%d error=%v", len(pending), err)
		}
		if e.EventID == rows[0].EventID {
			return mq.ErrUnroutable
		}
		return nil
	}}
	r := relay(t, s)
	report, err := r.Step(ctx, 0, p)
	if !errors.Is(err, mq.ErrUnroutable) || report.Published != 2 || report.Marked != 2 {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	pending, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
	if err != nil || len(pending) != 1 || pending[0].EventID != rows[0].EventID {
		t.Fatalf("unroutable row marked: %+v %v", pending, err)
	}
}

func TestRelayTransportLossMarksOnlyEarlierConfirms(t *testing.T) {
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 3)
	lost := errors.New("connection lost before confirm")
	p := &publisher{publish: func(context.Context, store.OutboxEvent) error { return nil }}
	p.publish = func(context.Context, store.OutboxEvent) error {
		if p.calls == 2 {
			return lost
		}
		return nil
	}
	report, err := relay(t, s).Step(ctx, 0, p)
	if !errors.Is(err, lost) || p.calls != 2 || report.Marked != 1 {
		t.Fatalf("report=%+v calls=%d error=%v", report, p.calls, err)
	}
	rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
	if err != nil || len(rows) != 2 {
		t.Fatalf("unknown confirms marked: %d %v", len(rows), err)
	}
}

func TestRelayRetriesExactMarkAfterCommittedResponseIsLost(t *testing.T) {
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 3)
	w := &markFault{Store: s, loseAfterCommit: true}
	r := relay(t, w)
	p := &publisher{}
	if _, err := r.Step(ctx, 0, p); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("lost mark response: %v", err)
	}
	if _, err := r.Step(ctx, 0, p); err != nil {
		t.Fatal(err)
	}
	if p.calls != 3 || w.reads != 1 || len(w.marks) != 2 || !reflect.DeepEqual(w.marks[0], w.marks[1]) {
		t.Fatalf("mark retry changed identity or republished: calls=%d reads=%d marks=%+v", p.calls, w.reads, w.marks)
	}
}

func TestRelayReadsAreStrongBoundedLongPollsAndErrorsAreNotEmpty(t *testing.T) {
	ctx, s := fixtureStore(t)
	w := &readFault{Store: s, t: t}
	p := &publisher{}
	if _, err := relay(t, w).Step(ctx, 0, p); !errors.Is(err, store.ErrUnknownOutcome) || p.calls != 0 {
		t.Fatalf("read failure swallowed: %v calls=%d", err, p.calls)
	}
}

func TestRelayBatchesAtMost100AndKeepsOwningShard(t *testing.T) {
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 101)
	w := &markFault{Store: s}
	r := relay(t, w)
	p := &publisher{}
	for _, want := range []int{100, 1} {
		report, err := r.Step(ctx, 0, p)
		if err != nil || report.Marked != want {
			t.Fatalf("report=%+v want=%d error=%v", report, want, err)
		}
	}
	for _, cmd := range w.marks {
		if cmd.Shard != 0 || len(cmd.EventIDs) > 100 || cmd.OperationID == "" {
			t.Fatalf("invalid mark: %+v", cmd)
		}
	}
	if w.marks[0].OperationID == w.marks[1].OperationID {
		t.Fatal("separate batches reused operation ID")
	}
}

func TestRelayCancelledStepDoesNotPublish(t *testing.T) {
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 1)
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	p := &publisher{}
	if _, err := relay(t, s).Step(ctx, 0, p); !errors.Is(err, context.Canceled) || p.calls != 0 {
		t.Fatalf("cancelled step: %v calls=%d", err, p.calls)
	}
}

func fixtureStore(t *testing.T) (context.Context, *memstore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	s, err := memstore.New(ctx, memstore.WithShards(3), memstore.WithRouter(func(string) int { return 0 }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return ctx, s
}

func seed(t *testing.T, ctx context.Context, s store.Store, count int) []store.OutboxEvent {
	t.Helper()
	var events []store.OutboxEvent
	for range count {
		id, eventID := uuid.NewString(), uuid.NewString()
		body, err := json.Marshal(map[string]any{"schemaVersion": 1, "eventId": eventID, "entityId": id, "entityVersion": 1, "eventType": "request.created", "occurredAt": "2026-10-30T00:00:00Z", "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "payload": map[string]any{"requestId": id, "status": "PENDING", "details": "synthetic relay fixture", "requestType": "WATER", "urgency": "HIGH", "location": map[string]any{"lat": 0, "lng": 0}, "peopleAffected": 1}})
		if err != nil {
			t.Fatal(err)
		}
		e := store.OutboxEvent{EventID: eventID, EventType: "request.created", SchemaVersion: 1, Exchange: "relief.requests", RoutingKey: "request.created.water", Payload: body}
		result, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: uuid.NewString(), At: 1793318400000}, Key: store.RequestKey(id), Request: &store.Request{ID: id, Type: store.TypeWater, Geohash: "s000000", Urgency: 2, PeopleAffected: 1}, Outbox: []store.OutboxEvent{e}})
		if err != nil || result.Outcome != store.OutcomeOK {
			t.Fatalf("seed: %+v %v", result, err)
		}
		events = append(events, e)
	}
	return events
}

func relay(t *testing.T, s store.Store) *outbox.Relay {
	t.Helper()
	r, err := outbox.New(outbox.Config{Store: s})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

type publisher struct {
	publish func(context.Context, store.OutboxEvent) error
	calls   int
}

func (p *publisher) Publish(ctx context.Context, e store.OutboxEvent) error {
	p.calls++
	if p.publish != nil {
		return p.publish(ctx, e)
	}
	return nil
}
func (*publisher) Close() error { return nil }

type markFault struct {
	store.Store
	loseAfterCommit bool
	marks           []store.MarkOutboxPublished
	reads           int
}

func (s *markFault) PendingOutbox(ctx context.Context, q store.OutboxQuery) ([]store.OutboxEvent, error) {
	s.reads++
	return s.Store.PendingOutbox(ctx, q)
}
func (s *markFault) MarkOutboxPublished(ctx context.Context, cmd store.MarkOutboxPublished) (store.Result, error) {
	cmd.EventIDs = append([]string(nil), cmd.EventIDs...)
	s.marks = append(s.marks, cmd)
	r, err := s.Store.MarkOutboxPublished(ctx, cmd)
	if err == nil && s.loseAfterCommit {
		s.loseAfterCommit = false
		return store.Result{}, store.ErrUnknownOutcome
	}
	return r, err
}

type readFault struct {
	store.Store
	t *testing.T
}

func (s *readFault) PendingOutbox(ctx context.Context, q store.OutboxQuery) ([]store.OutboxEvent, error) {
	if _, ok := ctx.Deadline(); !ok || q.Wait != time.Second || q.Consistency != store.ConsistencyStrong || q.Limit != 100 {
		s.t.Fatalf("read options/deadline: %+v", q)
	}
	return nil, store.ErrUnknownOutcome
}
