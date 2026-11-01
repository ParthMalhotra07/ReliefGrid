package mq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"testing"
)

func wakeDelivery(t *testing.T, ack amqp.Acknowledger) amqp.Delivery {
	t.Helper()
	body, err := os.ReadFile("../../contracts/events/request_state_changed.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct{ Examples []map[string]any }
	if err := json.Unmarshal(body, &schema); err != nil || len(schema.Examples) == 0 {
		t.Fatal("schema example missing", err)
	}
	e := schema.Examples[0]
	e["entityId"] = requestID
	e["payload"].(map[string]any)["requestId"] = requestID
	encoded, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, MessageId: e["eventId"].(string), Type: "request.state_changed", Body: encoded}
}
func TestWakeupConsumerCommitsBeforeAck(t *testing.T) {
	ctx, s := realStore(t)
	ack := &ackLog{t: t}
	c := mq.WakeupConsumer{Handle: func(ctx context.Context, w mq.Wakeup) error {
		if ack.acks != 0 || w.EntityID != requestID {
			t.Fatal("ack before authoritative handoff")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded handler")
		}
		rec, err := s.ReadRecord(ctx, store.RequestKey(w.EntityID))
		if err != nil {
			return err
		}
		res, err := s.TransitionRequest(ctx, store.TransitionRequest{Meta: store.Meta{OperationID: "wakeup-handoff", At: 2}, Key: rec.Key, ExpectedVersion: rec.Version, ExpectedStatus: rec.Request.Status, To: store.RequestWaiting, NextAttemptAt: 5000})
		if err != nil || res.Outcome != store.OutcomeOK {
			t.Fatalf("handoff %+v %v", res, err)
		}
		return nil
	}}
	if err := c.Process(ctx, wakeDelivery(t, ack)); err != nil {
		t.Fatal(err)
	}
	if ack.acks != 1 || ack.rejects != 0 {
		t.Fatalf("ack=%+v", ack)
	}
	rec, err := s.ReadRecord(ctx, store.RequestKey(requestID))
	if err != nil || rec.Request.Status != store.RequestWaiting {
		t.Fatal("wake acknowledged without commit")
	}
}
func TestWakeupConsumerRetryRequiresConfirmedHandoff(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{true: "publish failed", false: "confirmed"}[fail], func(t *testing.T) {
			ack := &ackLog{t: t}
			retried := false
			c := mq.WakeupConsumer{Handle: func(context.Context, mq.Wakeup) error { return errors.New("storage unavailable") }, Retry: func(context.Context, amqp.Delivery) error {
				if ack.acks != 0 {
					t.Fatal("ack before retry confirm")
				}
				retried = true
				if fail {
					return errors.New("confirm lost")
				}
				return nil
			}}
			err := c.Process(t.Context(), wakeDelivery(t, ack))
			if (err != nil) != fail || !retried || ack.acks != map[bool]int{true: 0, false: 1}[fail] {
				t.Fatalf("retry=%v ack=%d err=%v", retried, ack.acks, err)
			}
		})
	}
}
func TestWakeupConsumerRejectsPoisonWithoutBusinessOrRetry(t *testing.T) {
	ack := &ackLog{t: t}
	d := wakeDelivery(t, ack)
	d.Body = []byte(`{"schemaVersion":1}`)
	c := mq.WakeupConsumer{Handle: func(context.Context, mq.Wakeup) error { t.Fatal("poison reached workflow"); return nil }, Retry: func(context.Context, amqp.Delivery) error { t.Fatal("poison retried"); return nil }}
	if err := c.Process(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	if ack.acks != 0 || ack.rejects != 1 || ack.nacks != 0 {
		t.Fatalf("poison disposition %+v", ack)
	}
}

type wakePublisher struct {
	event  store.OutboxEvent
	closed bool
}

func (p *wakePublisher) Publish(_ context.Context, e store.OutboxEvent) error {
	p.event = e
	return nil
}
func (p *wakePublisher) Close() error { p.closed = true; return nil }
func TestWakeupTransportPreservesBodyAndReturnsToWakeQueue(t *testing.T) {
	d := wakeDelivery(t, nil)
	p := &wakePublisher{}
	retry := mq.TransportRetry{PublisherFactory: func(context.Context) (mq.RetryPublisher, error) { return p, nil }}
	if err := retry.RetryWakeup(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	if p.event.Exchange != "relief.retry" || p.event.RoutingKey != "wakeup.delay.5s" || !bytes.Equal(p.event.Payload, d.Body) || !p.closed {
		t.Fatalf("wakeup retry sent to wrong path or changed input: %+v", p)
	}
}
func TestWakeupSchemasMatchFrozenContracts(t *testing.T) {
	for _, name := range []string{"request_state_changed", "resource_registered", "resource_state_changed", "match_proposed"} {
		f, err := os.ReadFile("../../contracts/events/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile("schemas/" + name + ".json")
		if err != nil || !bytes.Equal(f, b) {
			t.Errorf("schema %s missing or differs: %v", name, err)
		}
	}
}
func TestWakeupRetryHasOwnDurableDestination(t *testing.T) {
	var d struct {
		Queues []struct {
			Name      string
			Durable   bool
			Arguments map[string]any
		}
		Bindings []struct {
			Source, Destination string
			RoutingKey          string `json:"routing_key"`
		}
	}
	if err := json.Unmarshal(mq.Definitions(), &d); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, q := range d.Queues {
		if q.Name == "wakeup.retry.5s" {
			found = true
			if !q.Durable || q.Arguments["x-message-ttl"] != float64(5000) || q.Arguments["x-dead-letter-exchange"] != "relief.events" || q.Arguments["x-dead-letter-routing-key"] != "reconcile.retry" || q.Arguments["x-overflow"] != "reject-publish" {
				t.Fatalf("unsafe wakeup retry %+v", q)
			}
		}
	}
	if !found {
		t.Fatal("missing dedicated wakeup retry queue")
	}
	for _, want := range [][3]string{{"relief.retry", "wakeup.retry.5s", "wakeup.delay.5s"}, {"relief.events", mq.WakeupQueue, "reconcile.retry"}} {
		found = false
		for _, b := range d.Bindings {
			if [3]string{b.Source, b.Destination, b.RoutingKey} == want {
				found = true
			}
		}
		if !found {
			t.Errorf("missing wake transport binding %v", want)
		}
	}
}
