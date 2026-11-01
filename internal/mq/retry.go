package mq

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RetryPublisher returns nil only after a persistent, mandatory routed confirm.
type RetryPublisher interface {
	Publish(context.Context, store.OutboxEvent) error
	Close() error
}

// TransportRetry preserves the original durable hint when no domain handoff
// can be committed. Its fresh persistent copy is confirmed into the fixed 5s
// tier before the source can be acknowledged. Broker death/delivery headers are
// not forwarded; they are separate from the persisted business retry counter.
type TransportRetry struct {
	PublisherFactory func(context.Context) (RetryPublisher, error)
}

func (r TransportRetry) Retry(ctx context.Context, d amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if r.PublisherFactory == nil {
		return fmt.Errorf("mq transport retry requires a confirmed publisher factory")
	}
	e, err := parseEvent(d.Body)
	if err != nil {
		return fmt.Errorf("transport retry event: %w", err)
	}
	return r.publishRetry(ctx, d, e, "delay.5s")
}

// RetryWakeup uses its own fixed-TTL path back to matching.wakeups. Resource and
// cleanup hints must never be dead-lettered into the request-created consumer.
func (r TransportRetry) RetryWakeup(ctx context.Context, d amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if r.PublisherFactory == nil {
		return fmt.Errorf("wakeup retry requires confirmed publisher factory")
	}
	e, err := parseWakeEvent(d.Body)
	if err != nil {
		return fmt.Errorf("wakeup retry event: %w", err)
	}
	return r.publishRetry(ctx, d, e, "wakeup.delay.5s")
}
func (r TransportRetry) publishRetry(ctx context.Context, d amqp.Delivery, e event, route string) error {
	p, err := r.PublisherFactory(ctx)
	if err != nil {
		return fmt.Errorf("transport retry publisher: %w", err)
	}
	if p == nil {
		return fmt.Errorf("transport retry publisher is nil")
	}
	defer func() { _ = p.Close() }()
	if err := p.Publish(ctx, store.OutboxEvent{EventID: e.EventID, EventType: e.EventType, SchemaVersion: e.SchemaVersion,
		Exchange: "relief.retry", RoutingKey: route, Payload: bytes.Clone(d.Body)}); err != nil {
		return fmt.Errorf("transport retry routed confirm: %w", err)
	}
	slog.InfoContext(ctx, "matching delivery deferred", "eventId", e.EventID,
		"retryClass", "transport", "brokerDeliveryCount", d.Headers["x-delivery-count"], "brokerDeaths", d.Headers["x-death"])
	return ctx.Err()
}

func (c Consumer) deferDelivery(ctx context.Context, d amqp.Delivery, cause error) error {
	if c.Retry == nil {
		return cause
	}
	call, _ := c.timeouts()
	deferCtx, cancel := context.WithTimeout(ctx, call)
	defer cancel()
	if err := c.Retry(deferCtx, d); err != nil {
		return fmt.Errorf("defer matching delivery (%w): %w", cause, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.Ack(false); err != nil {
		return fmt.Errorf("ack after confirmed delayed handoff: %w", err)
	}
	return nil
}
