package mq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
	"time"
)

// ProjectEvents owns an exclusive auto-delete CLASSIC queue per API connection.
// This is a best-effort copy of committed events, never the matching/wakeup
// queues or a durable handoff. API outages cannot consume workflow messages.
func ProjectEvents(ctx context.Context, address string, deliver func([]byte) error, state func(bool)) error {
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := projectOnce(ctx, address, deliver, state)
		state(false)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.WarnContext(ctx, "event projection disconnected", "error", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(5*time.Second, backoff*2)
	}
	return ctx.Err()
}
func projectOnce(ctx context.Context, address string, deliver func([]byte) error, state func(bool)) error {
	conn, err := Dial(ctx, address)
	if err != nil {
		return err
	}
	defer func() { _ = conn.CloseDeadline(time.Now().Add(time.Second)) }()
	setup, cancel := context.WithTimeout(ctx, 5*time.Second)
	stop := closeOnCancel(setup, conn)
	defer cancel()
	defer stop()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("projection channel: %w", err)
	}
	if err = ch.Qos(Prefetch, 0, false); err != nil {
		return fmt.Errorf("projection qos: %w", err)
	}
	q, err := ch.QueueDeclare("", false, true, true, false, amqp.Table{"x-queue-type": "classic"})
	if err != nil {
		return fmt.Errorf("projection queue: %w", err)
	}
	if err = ch.ExchangeDeclare(ProjectionExchange, "fanout", false, false, false, false, nil); err != nil {
		return fmt.Errorf("projection exchange: %w", err)
	}
	if err = ch.QueueBind(q.Name, "", ProjectionExchange, false, nil); err != nil {
		return fmt.Errorf("projection binding: %w", err)
	}

	deliveries, err := ch.Consume(q.Name, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("projection consume: %w", err)
	}
	stop()
	cancel()
	state(true)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("projection delivery channel closed")
			}
			if err = deliver(d.Body); err != nil {
				slog.WarnContext(ctx, "ignored invalid event projection", "error", err)
			}
			// Ack only this temporary copy. The canonical outbox and workflow consumers
			// retain their existing durable-handoff rules.
			ackCtx, ackCancel := context.WithTimeout(ctx, 5*time.Second)
			ackStop := closeOnCancel(ackCtx, conn)
			err = d.Ack(false)
			ackStop()
			ackCancel()
			if err != nil {
				return fmt.Errorf("projection ack: %w", err)
			}
		}
	}
}
