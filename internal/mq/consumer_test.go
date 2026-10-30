package mq_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
	amqp "github.com/rabbitmq/amqp091-go"
)

const requestID = "550e8400-e29b-41d4-a716-446655440000"

func TestConsumerReadsCurrentStateBeforeDurableHandlerAndAck(t *testing.T) {
	ctx, s := realStore(t)
	ack := &ackLog{t: t}
	calls := 0
	c := mq.Consumer{Store: s, CallTimeout: time.Second, Handle: func(ctx context.Context, rec store.Record) error {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("handler has no deadline")
		}
		if ack.acks != 0 || rec.Request == nil || rec.Request.ID != requestID || rec.Request.Status != store.RequestPending {
			t.Fatalf("ack before handoff or wrong authoritative record: ack=%+v record=%+v", ack, rec)
		}
		// A real committed mutation represents the durable handoff boundary.
		r, err := s.TransitionRequest(ctx, store.TransitionRequest{
			Meta: store.Meta{OperationID: "consumer-handoff", At: 2}, Key: rec.Key,
			ExpectedVersion: rec.Version, ExpectedStatus: store.RequestPending,
			To: store.RequestWaiting, NextAttemptAt: 5000,
		})
		if err != nil || r.Outcome != store.OutcomeOK {
			t.Fatalf("durable transition: %+v %v", r, err)
		}
		return nil
	}}
	if err := c.Process(ctx, eventDelivery(t, ack)); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || ack.acks != 1 || ack.rejects != 0 || ack.nacks != 0 {
		t.Fatalf("calls=%d acknowledgements=%+v", calls, ack)
	}
	r, err := s.ReadRecord(ctx, store.RequestKey(requestID))
	if err != nil || r.Request.Status != store.RequestWaiting {
		t.Fatalf("ack without durable handoff: %+v %v", r, err)
	}
}

func TestConsumerDropsStaleHintAfterReadingTerminalState(t *testing.T) {
	ctx, s := realStore(t)
	rec, err := s.ReadRecord(ctx, store.RequestKey(requestID))
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.TransitionRequest(ctx, store.TransitionRequest{
		Meta: store.Meta{OperationID: "cancel-before-delivery", At: 2}, Key: rec.Key,
		ExpectedVersion: rec.Version, ExpectedStatus: store.RequestPending, To: store.RequestCancelled,
	})
	if err != nil || r.Outcome != store.OutcomeOK {
		t.Fatalf("cancel: %+v %v", r, err)
	}
	ack := &ackLog{t: t}
	c := mq.Consumer{Store: s, Handle: func(context.Context, store.Record) error {
		t.Fatal("stale PENDING event acted on after committed cancellation")
		return nil
	}}
	if err := c.Process(ctx, eventDelivery(t, ack)); err != nil {
		t.Fatal(err)
	}
	if ack.acks != 1 || ack.rejects != 0 {
		t.Fatalf("terminal delivery not acknowledged: %+v", ack)
	}
}

func TestConsumerNeverAcknowledgesFailedHandoffOrStorageRead(t *testing.T) {
	for _, readFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "handoff", true: "read"}[readFailure], func(t *testing.T) {
			ctx, s := realStore(t)
			want := errors.New("storage response lost")
			var authoritative store.Store = s
			if readFailure {
				authoritative = &failedRead{Store: s, err: want, t: t}
			}
			ack := &ackLog{t: t}
			c := mq.Consumer{Store: authoritative, CallTimeout: time.Second, Handle: func(context.Context, store.Record) error {
				if readFailure {
					t.Fatal("handler ran after failed read")
				}
				return want
			}}
			err := c.Process(ctx, eventDelivery(t, ack))
			if !errors.Is(err, want) {
				t.Fatalf("error=%v, want wrapped storage failure", err)
			}
			if ack.acks+ack.rejects+ack.nacks != 0 {
				t.Fatalf("failed durable handoff disposed delivery: %+v", ack)
			}
		})
	}
}

func TestConsumerMalformedEventsRejectWithoutRequeue(t *testing.T) {
	for _, body := range []string{
		`not-json`, `{}`, `{"schemaVersion":99}`, `null`,
		`{"schemaVersion":1,"eventId":"not-a-uuid"}`,
	} {
		t.Run(body, func(t *testing.T) {
			ctx, s := realStore(t)
			ack := &ackLog{t: t}
			c := mq.Consumer{Store: s, Handle: func(context.Context, store.Record) error {
				t.Fatal("malformed event reached handler")
				return nil
			}}
			d := eventDelivery(t, ack)
			d.Body = []byte(body)
			if err := c.Process(ctx, d); err != nil {
				t.Fatal(err)
			}
			if ack.rejects != 1 || ack.acks != 0 || ack.nacks != 0 {
				t.Fatalf("poison was not rejected: %+v", ack)
			}
		})
	}
}

func TestConsumerSurfacesAcknowledgementFailure(t *testing.T) {
	ctx, s := realStore(t)
	want := errors.New("channel closed before ack")
	ack := &ackLog{t: t, err: want}
	c := mq.Consumer{Store: s, Handle: func(context.Context, store.Record) error { return nil }}
	if err := c.Process(ctx, eventDelivery(t, ack)); !errors.Is(err, want) {
		t.Fatalf("ack error lost: %v", err)
	}
}

func TestConsumerAcceptsRealWorkflowRetryEnvelope(t *testing.T) {
	ctx, s := realStore(t)
	m, err := workflow.New(workflow.Config{Store: s, ScanLimit: 100, Now: func() time.Time { return time.UnixMilli(1000) }})
	if err != nil {
		t.Fatal(err)
	}
	r, err := m.Match(ctx, requestID)
	if err != nil || r.Outcome != "NO_CANDIDATE" {
		t.Fatalf("durable retry fixture: %+v %v", r, err)
	}
	var retry []byte
	for shard := range s.Shards() {
		events, err := s.PendingOutbox(ctx, shard, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			if e.EventType == "request.retry" {
				retry = e.Payload
			}
		}
	}
	if len(retry) == 0 {
		t.Fatal("real workflow emitted no retry event")
	}
	ack := &ackLog{t: t}
	calls := 0
	c := mq.Consumer{Store: s, Handle: func(_ context.Context, rec store.Record) error {
		calls++
		if rec.Request.Status != store.RequestWaiting || rec.Request.NextAttemptAt == 0 {
			t.Fatalf("retry did not reread durable schedule: %+v", rec)
		}
		return nil
	}}
	d := eventDelivery(t, ack)
	d.Body = retry
	d.Exchange = "relief.requests"
	d.RoutingKey = "request.retry"
	if err := c.Process(ctx, d); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || ack.acks != 1 || ack.rejects != 0 {
		t.Fatalf("existing retry envelope not supported: calls=%d ack=%+v", calls, ack)
	}
}

func realStore(t *testing.T) (context.Context, *memstore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	s, err := memstore.New(ctx, memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	r, err := s.CreateEntity(ctx, store.CreateEntity{
		Meta: store.Meta{OperationID: "create-consumer-fixture", At: 1}, Key: store.RequestKey(requestID),
		Request: &store.Request{ID: requestID, Type: store.TypeMedicine, PeopleAffected: 4, Urgency: 2, Geohash: "s000000"},
	})
	if err != nil || r.Outcome != store.OutcomeOK {
		t.Fatalf("create fixture: %+v %v", r, err)
	}
	return ctx, s
}

func eventDelivery(t *testing.T, ack amqp.Acknowledger) amqp.Delivery {
	t.Helper()
	b, err := os.ReadFile("../../contracts/events/request_created.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Examples []json.RawMessage `json:"examples"`
	}
	if err := json.Unmarshal(b, &schema); err != nil {
		t.Fatal(err)
	}
	if len(schema.Examples) == 0 {
		t.Fatal("frozen schema has no example")
	}
	return amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, ContentType: "application/json",
		DeliveryMode: amqp.Persistent, Priority: 2, Exchange: "relief.requests", RoutingKey: "request.created.medicine", Body: schema.Examples[0]}
}

type ackLog struct {
	t                    *testing.T
	acks, rejects, nacks int
	err                  error
}

func (a *ackLog) Ack(tag uint64, multiple bool) error {
	if tag != 1 || multiple {
		a.t.Fatalf("unexpected cumulative ack: tag=%d multiple=%v", tag, multiple)
	}
	a.acks++
	return a.err
}
func (a *ackLog) Reject(tag uint64, requeue bool) error {
	if tag != 1 || requeue {
		a.t.Fatalf("poison requeued or wrong tag: %d %v", tag, requeue)
	}
	a.rejects++
	return a.err
}
func (a *ackLog) Nack(uint64, bool, bool) error { a.nacks++; return a.err }

type failedRead struct {
	store.Store
	err error
	t   *testing.T
}

func (s *failedRead) ReadRecord(ctx context.Context, _ string) (store.Record, error) {
	if _, ok := ctx.Deadline(); !ok {
		s.t.Error("storage read has no deadline")
	}
	return store.Record{}, s.err
}
