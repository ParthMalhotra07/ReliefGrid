package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace"
)

// Consumer treats delivery data as a hint. Handle MUST return nil only after
// the required durable state/outbox handoff has committed (or an authoritative
// reread proves no work remains). Unknown outcomes must remain errors. Handle
// must honor its context and use idempotent committed workflow operations.
type Consumer struct {
	Store  store.Store
	Handle func(context.Context, store.Record) error
	// Retry is optional: nil preserves leave-unacknowledged/reconnect behaviour.
	// It may return nil only after a confirmed persistent delayed handoff.
	Retry       func(context.Context, amqp.Delivery) error
	CallTimeout time.Duration
	WorkTimeout time.Duration
	// TracerProvider records consumer spans (S4.4); nil uses the global one.
	TracerProvider trace.TracerProvider
}

func (c Consumer) validate() error {
	if c.Store == nil || c.Handle == nil {
		return fmt.Errorf("mq consumer requires store and durable handler")
	}
	if c.CallTimeout < 0 || c.WorkTimeout < 0 {
		return fmt.Errorf("mq consumer timeouts must be positive")
	}
	if _, err := requestCreatedSchema(); err != nil {
		return fmt.Errorf("mq consumer event schema: %w", err)
	}
	return nil
}

func (c Consumer) timeouts() (time.Duration, time.Duration) {
	call, work := c.CallTimeout, c.WorkTimeout
	if call == 0 {
		call = callTimeout
	}
	if work == 0 {
		work = 15 * time.Second
	}
	return call, work
}

// Process disposes one delivery. A malformed event is rejected into the DLX.
// A valid event's failed read/handler may hand off the same hint through Retry.
// Without a successful confirmed handoff it stays unacknowledged: stop the
// channel and back off before reconnecting, never nack-loop it.
func (c Consumer) Process(ctx context.Context, d amqp.Delivery) (err error) {
	if err := c.validate(); err != nil {
		return err
	}
	if d.Acknowledger == nil {
		return fmt.Errorf("mq delivery has no acknowledger")
	}
	call, work := c.timeouts()
	ctx, cancel := context.WithTimeout(ctx, work)
	defer cancel()
	ctx, span := startConsume(ctx, c.TracerProvider, MatchingQueue, d)
	defer func() { endSpan(span, err) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	e, err := parseEvent(d.Body)
	if err != nil {
		if err := d.Reject(false); err != nil {
			return fmt.Errorf("reject malformed event: %w", err)
		}
		slog.WarnContext(ctx, "rejected malformed matching event", "messageId", d.MessageId, "error", err)
		return nil
	}
	readCtx, readCancel := context.WithTimeout(ctx, call)
	rec, err := c.Store.ReadRecord(readCtx, store.RequestKey(e.EntityID))
	readCancel()
	if errors.Is(err, store.ErrNotFound) || (err == nil && rec.Deleted) {
		// An authoritative absence is permanent for this delivery, unlike a
		// failed read. Reject it once so reconnects do not burn delivery-limit.
		if err := d.Reject(false); err != nil {
			return fmt.Errorf("reject absent request event %s: %w", e.EventID, err)
		}
		slog.WarnContext(ctx, "rejected matching event for absent request", "eventId", e.EventID, "requestId", e.EntityID)
		return nil
	}
	if err != nil {
		return c.deferDelivery(ctx, d, fmt.Errorf("read current request for event %s: %w", e.EventID, err))
	}
	if rec.Request == nil {
		return fmt.Errorf("event %s has no current request", e.EventID)
	}
	switch rec.Request.Status {
	case store.RequestCancelled, store.RequestFulfilled, store.RequestMatched, store.RequestInProgress:
		// This delivery's matching work is already superseded. No claim action.
	default:
		if err := c.Handle(ctx, rec); err != nil {
			return c.deferDelivery(ctx, d, fmt.Errorf("durable event handoff %s: %w", e.EventID, err))
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.Ack(false); err != nil {
		return fmt.Errorf("ack event %s: %w", e.EventID, err)
	}
	return nil
}

// Consume processes matching.requests sequentially with manual ack/prefetch 8.
// It owns one channel, not the connection. On an error it closes that channel,
// returning unacknowledged deliveries to RabbitMQ. A supervisor must back off
// before calling again; business retry scheduling is the workflow's job (A2.2).
// A stalled AMQP RPC is bounded by closing the connection, so use a dedicated
// consumer connection opened with Dial. No automatic connection recovery runs.
func (c Consumer) Consume(ctx context.Context, conn *amqp.Connection) error {
	if err := c.validate(); err != nil {
		return err
	}
	if conn == nil {
		return fmt.Errorf("mq consumer requires a connection")
	}
	call, work := c.timeouts()
	setupCtx, setupCancel := context.WithTimeout(ctx, call)
	stop := closeOnCancel(setupCtx, conn)
	ch, err := conn.Channel()
	if err != nil {
		stop()
		setupCancel()
		return fmt.Errorf("open consumer channel: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), call)
		defer cancel()
		stop := closeOnCancel(cleanupCtx, conn)
		defer stop()
		_ = ch.Close()
	}()
	if err := ch.Qos(Prefetch, 0, false); err != nil {
		stop()
		setupCancel()
		return fmt.Errorf("consumer prefetch: %w", err)
	}
	// Own cancellation here: ConsumeWithContext starts a concurrent Cancel
	// RPC, which can race this channel's Close while a delivery is held.
	// Setup is bounded above; the loop and handler honor ctx, then Close
	// returns every unacknowledged delivery on this dedicated channel.
	deliveries, err := ch.Consume(MatchingQueue, "", false, false, false, false, nil)
	stop()
	setupCancel()
	if err != nil {
		return fmt.Errorf("start matching consumer: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				if err := ctx.Err(); err != nil {
					return err
				}
				return fmt.Errorf("matching delivery channel closed")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			// Drain only this admitted delivery. Prefetched work stays unacked
			// and is returned by channel Close after this bounded handoff.
			deliveryCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), work)
			d.Acknowledger = boundedAcknowledger{Acknowledger: d.Acknowledger, ctx: deliveryCtx, conn: conn, timeout: call}
			err := c.Process(deliveryCtx, d)
			finish()
			if err != nil {
				return err
			}
		}
	}
}

type boundedAcknowledger struct {
	amqp.Acknowledger
	ctx     context.Context
	conn    *amqp.Connection
	timeout time.Duration
}

func (a boundedAcknowledger) Ack(tag uint64, multiple bool) error {
	return a.call(func() error { return a.Acknowledger.Ack(tag, multiple) })
}
func (a boundedAcknowledger) Reject(tag uint64, requeue bool) error {
	return a.call(func() error { return a.Acknowledger.Reject(tag, requeue) })
}
func (a boundedAcknowledger) call(fn func() error) error {
	ctx, cancel := context.WithTimeout(a.ctx, a.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := closeOnCancel(ctx, a.conn)
	defer stop()
	return fn()
}
