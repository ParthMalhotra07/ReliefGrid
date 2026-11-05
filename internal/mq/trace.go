package mq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
)

// startConsume starts the consumer span for d (S4.4). A first delivery
// continues the publisher's trace (its traceparent header). A broker
// redelivery, or a copy dead-lettered back from a retry queue (x-death), is
// a span of its own linked to the publisher, so retries do not stretch one
// trace over every backoff.
func startConsume(ctx context.Context, tp trace.TracerProvider, queue string, d amqp.Delivery) (context.Context, trace.Span) {
	_, retried := d.Headers["x-death"]
	return obs.StartConsume(ctx, d.Headers, d.Redelivered || retried, obs.WithTracerProvider(tp),
		obs.WithAttributes(attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", queue),
			attribute.String("messaging.message.id", d.MessageId)))
}

// endSpan records err (if any) on span and ends it.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
