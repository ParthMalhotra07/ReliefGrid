package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrUnroutable   = errors.New("mq: mandatory publish returned")
	ErrNack         = errors.New("mq: publisher confirm rejected")
	ErrInvalidEvent = errors.New("mq: invalid event payload")
)

// ConfirmedPublisher owns a dedicated connection. One outstanding publish makes
// the ordered basic.return/confirm pair unambiguous. Unknown outcomes close the
// transport; callers reconnect and retain the original durable outbox row.
type ConfirmedPublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	returns <-chan amqp.Return
	permit  chan struct{}
}

// NewPublisher opens a publisher owned by the lifetime of ctx. Setup is bounded
// independently, so cancelling its setup deadline cannot kill a healthy session.
func NewPublisher(ctx context.Context, address string) (*ConfirmedPublisher, error) {
	conn, err := Dial(ctx, address)
	if err != nil {
		return nil, err
	}
	p := &ConfirmedPublisher{conn: conn, permit: make(chan struct{}, 1)}
	if err := p.setup(ctx); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func (p *ConfirmedPublisher) setup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	stop := closeOnCancel(ctx, p.conn)
	defer stop()
	if err := Declare(ctx, p.conn); err != nil {
		return err
	}
	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("open publisher channel: %w", err)
	}
	p.channel = ch
	p.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	return ctx.Err()
}

// Publish returns nil only after a routed, positive confirm. Return/nack are
// definite publish failures and leave the session usable. All other failures may
// have delivered a message; the caller must allow duplicate-safe retransmission.
func (p *ConfirmedPublisher) Publish(ctx context.Context, e store.OutboxEvent) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	select {
	case p.permit <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.permit }()
	if err := ctx.Err(); err != nil {
		return err
	}
	msg, err := publishing(ctx, e)
	if err != nil {
		return err
	}
	stop := closeOnCancel(ctx, p.conn)
	defer stop()
	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, e.Exchange, e.RoutingKey, true, false, msg)
	if err != nil {
		_ = p.Close()
		return fmt.Errorf("publish %s: %w", e.EventID, err)
	}
	if confirmation == nil {
		_ = p.Close()
		return fmt.Errorf("publish %s: confirmation missing", e.EventID)
	}
	ack, err := confirmation.WaitContext(ctx)
	if err != nil {
		_ = p.Close()
		return fmt.Errorf("confirm %s: %w", e.EventID, err)
	}
	// RabbitMQ sends basic.return before its confirm. amqp091-go v1.16.0
	// dispatch synchronously enqueues returns before resolving deferred confirms.
	select {
	case returned, ok := <-p.returns:
		if !ok {
			_ = p.Close()
			return fmt.Errorf("confirm %s: publisher channel closed", e.EventID)
		}
		return fmt.Errorf("%w: event=%s code=%d reason=%s", ErrUnroutable, e.EventID, returned.ReplyCode, returned.ReplyText)
	default:
	}
	if !ack {
		return fmt.Errorf("%w: event=%s", ErrNack, e.EventID)
	}
	return nil
}

// publishing is the persistent AMQP message for e. It carries ctx's trace
// context (the relay's producer span, or a consumer's span for a transport
// retry) as a traceparent header (S4.4); with none, it has no headers.
func publishing(ctx context.Context, e store.OutboxEvent) (amqp.Publishing, error) {
	priority, err := EventPriority(e.Payload)
	if err != nil {
		return amqp.Publishing{}, err
	}
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: e.EventID,
		Type: e.EventType, Priority: priority, Body: e.Payload,
	}
	headers := amqp.Table{}
	obs.InjectAMQP(ctx, headers)
	if len(headers) != 0 {
		msg.Headers = headers
	}
	return msg, nil
}

func (p *ConfirmedPublisher) Close() error { return p.conn.CloseDeadline(time.Now().Add(callTimeout)) }

// EventPriority preserves the public string urgency and internal retry integer
// urgency on the wire. Events without urgency use explicit NORMAL (1).
func EventPriority(body []byte) (uint8, error) {
	var envelope struct {
		Payload struct {
			Urgency json.RawMessage `json:"urgency"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return 0, fmt.Errorf("%w: event envelope: %w", ErrInvalidEvent, err)
	}
	if len(envelope.Payload.Urgency) == 0 {
		return 1, nil
	}
	var text string
	if json.Unmarshal(envelope.Payload.Urgency, &text) == nil {
		switch text {
		case "NORMAL":
			return 1, nil
		case "HIGH":
			return 2, nil
		case "CRITICAL":
			return 3, nil
		}
	}
	var number uint8
	if json.Unmarshal(envelope.Payload.Urgency, &number) == nil && number >= 1 && number <= 3 {
		return number, nil
	}
	return 0, fmt.Errorf("%w: invalid event urgency: %s", ErrInvalidEvent, envelope.Payload.Urgency)
}
