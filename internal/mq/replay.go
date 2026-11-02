package mq

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ReplayEnvelope validates corrected poison input before the tool reads a dead
// message. The destination comes from its supported type, never a supplied URL.
func ReplayEnvelope(body []byte) (store.OutboxEvent, error) {
	e, err := decodeEnvelope(body)
	if err != nil {
		return store.OutboxEvent{}, err
	}
	queue := WakeupQueue
	if e.EventType == "request.created" || e.EventType == "request.retry" {
		e, err = parseEvent(body)
		queue = MatchingQueue
	} else {
		e, err = parseWakeEvent(body)
	}
	if err != nil {
		return store.OutboxEvent{}, err
	}
	return store.OutboxEvent{EventID: e.EventID, EventType: e.EventType, SchemaVersion: e.SchemaVersion, Exchange: "", RoutingKey: queue, Payload: bytes.Clone(body)}, nil
}
func ReplayDead(ctx context.Context, d amqp.Delivery, replacement []byte, p RetryPublisher) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	e, err := ReplayEnvelope(replacement)
	if err != nil {
		return fmt.Errorf("corrected dead input: %w", err)
	}
	if d.Acknowledger == nil || p == nil || d.MessageId != e.EventID {
		return fmt.Errorf("dead replay requires publisher, acknowledger and unchanged event identity")
	}
	if err := p.Publish(ctx, e); err != nil {
		return fmt.Errorf("dead replay routed confirm: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.Ack(false); err != nil {
		return fmt.Errorf("dead replay source ack: %w", err)
	}
	return nil
}

type ReplayReport struct {
	MessageID    string `json:"messageId"`
	Reason       string `json:"reason"`
	Acknowledged bool   `json:"acknowledged"`
}

func ReplayOne(ctx context.Context, conn *amqp.Connection, replacement []byte, expectedID string, p RetryPublisher) (ReplayReport, error) {
	report := ReplayReport{}
	e, err := ReplayEnvelope(replacement)
	if err != nil {
		return report, err
	}
	if expectedID == "" || expectedID != e.EventID {
		return report, fmt.Errorf("replacement eventId must match --message-id")
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	stop := closeOnCancel(ctx, conn)
	defer stop()
	ch, err := conn.Channel()
	if err != nil {
		return report, err
	}
	defer func() { _ = ch.Close() }()
	d, ok, err := ch.Get(DeadQueue, false)
	if err != nil {
		return report, err
	}
	if !ok {
		return report, fmt.Errorf("matching.dead is empty")
	}
	report.MessageID = d.MessageId
	report.Reason = "unknown"
	if reason, ok := d.Headers["x-first-death-reason"].(string); ok {
		switch reason {
		case "rejected", "expired", "maxlen", "delivery_limit":
			report.Reason = reason
		}
	}
	if d.MessageId != expectedID {
		return report, fmt.Errorf("head dead-letter event differs from --message-id; left unacknowledged")
	}
	d.Acknowledger = boundedAcknowledger{Acknowledger: d.Acknowledger, ctx: ctx, conn: conn, timeout: callTimeout}
	err = ReplayDead(ctx, d, replacement, p)
	report.Acknowledged = err == nil
	return report, err
}
func DeadReady(ctx context.Context, conn *amqp.Connection) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	stop := closeOnCancel(ctx, conn)
	defer stop()
	ch, err := conn.Channel()
	if err != nil {
		return 0, err
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(DeadQueue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	return q.Messages, err
}
