package mq_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
)

type retryPublisher struct {
	t    *testing.T
	ack  *ackLog
	rows []store.OutboxEvent
	err  error
}

func (p *retryPublisher) Publish(ctx context.Context, e store.OutboxEvent) error {
	if _, ok := ctx.Deadline(); !ok {
		p.t.Error("retry publish has no deadline")
	}
	if p.ack.acks != 0 {
		p.t.Fatal("source was acknowledged before routed confirm")
	}
	p.rows = append(p.rows, e)
	return p.err
}
func (*retryPublisher) Close() error { return nil }

func TestRetryTransportConfirmsBeforeAckAndKeepsPayload(t *testing.T) {
	for _, result := range []error{nil, mq.ErrUnroutable, mq.ErrNack, errors.New("confirm lost")} {
		name := "confirmed"
		if result != nil {
			name = result.Error()
		}
		t.Run(name, func(t *testing.T) {
			ctx, raw := realStore(t)
			ack := &ackLog{t: t}
			pub := &retryPublisher{t: t, ack: ack, err: result}
			r := mq.TransportRetry{PublisherFactory: func(context.Context) (mq.RetryPublisher, error) { return pub, nil }}
			readErr := errors.New("owning shard unavailable")
			c := mq.Consumer{Store: &failedRead{Store: raw, t: t, err: readErr}, Handle: func(context.Context, store.Record) error {
				t.Fatal("handler after failed authoritative read")
				return nil
			}, Retry: r.Retry}
			d := eventDelivery(t, ack)
			d.Headers = amqp.Table{"x-delivery-count": int64(19)}
			err := c.Process(ctx, d)
			if (result == nil) != (err == nil) {
				t.Fatalf("retry result=%v error=%v", result, err)
			}
			wantAck := 0
			if result == nil {
				wantAck = 1
			}
			if ack.acks != wantAck || ack.rejects+ack.nacks != 0 {
				t.Fatalf("wrong disposition after retry: %+v", ack)
			}
			if len(pub.rows) != 1 || !bytes.Equal(pub.rows[0].Payload, d.Body) || pub.rows[0].Exchange != "relief.retry" || pub.rows[0].RoutingKey != "delay.5s" {
				t.Fatalf("transport retry changed the durable hint: %+v", pub.rows)
			}
			rec, readErr := raw.ReadRecord(ctx, store.RequestKey(requestID))
			if readErr != nil || rec.Request.RetryCount != 0 {
				t.Fatal("broker retry was counted as a business retry")
			}
		})
	}
}

func TestRetryTransportDoesNotHidePoisonOrPermanentAbsence(t *testing.T) {
	for _, poison := range []bool{true, false} {
		t.Run(map[bool]string{true: "poison", false: "absent"}[poison], func(t *testing.T) {
			ctx, raw := realStore(t)
			ack := &ackLog{t: t}
			c := mq.Consumer{Store: readOverride{Store: raw, err: store.ErrNotFound}, Handle: func(context.Context, store.Record) error { t.Fatal("permanent work reached handler"); return nil }, Retry: func(context.Context, amqp.Delivery) error {
				t.Fatal("poison or missing request was retried")
				return nil
			}}
			d := eventDelivery(t, ack)
			if poison {
				d.Body = []byte("poison")
			}
			if err := c.Process(ctx, d); err != nil {
				t.Fatal(err)
			}
			if ack.rejects != 1 || ack.acks+ack.nacks != 0 {
				t.Fatalf("permanent work was not rejected once: %+v", ack)
			}
		})
	}
}
