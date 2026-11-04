package mq_test

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// The producer (relay publish span) whose context a delivery carries.
const (
	producerTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	producerSpan  = "00f067aa0ba902b7"
	producerTP    = "00-" + producerTrace + "-" + producerSpan + "-01"
)

func traceRecorder(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp, sr
}

// deliveryCase is how a message reaches a consumer: a first delivery
// continues the producer's trace; a broker redelivery or a message back from
// a TTL retry queue (x-death) is a span of its own linked to the producer.
var deliveryCases = []struct {
	name string
	mod  func(*amqp.Delivery)
	link bool
}{
	{"first delivery", func(*amqp.Delivery) {}, false},
	{"redelivered", func(d *amqp.Delivery) { d.Redelivered = true }, true},
	{"from a retry queue", func(d *amqp.Delivery) {
		d.Headers["x-death"] = []any{amqp.Table{"count": int64(1), "queue": "relief.retry.5s", "reason": "expired"}}
	}, true},
}

// checkConsumerSpan checks the handler ran under exactly one recorded
// consumer span, related to the producer as the case requires.
func checkConsumerSpan(t *testing.T, sr *tracetest.SpanRecorder, handled trace.SpanContext, link bool) {
	t.Helper()
	var spans []sdktrace.ReadOnlySpan
	for _, sp := range sr.Ended() {
		if sp.SpanKind() == trace.SpanKindConsumer {
			spans = append(spans, sp)
		}
	}
	if len(spans) != 1 {
		t.Fatalf("%d consumer spans, want 1", len(spans))
	}
	sp := spans[0]
	if handled.SpanID() != sp.SpanContext().SpanID() {
		t.Errorf("handler ran under span %s, want the consumer span %s", handled.SpanID(), sp.SpanContext().SpanID())
	}
	inProducerTrace := sp.SpanContext().TraceID().String() == producerTrace && sp.Parent().SpanID().String() == producerSpan
	linked := len(sp.Links()) == 1 && sp.Links()[0].SpanContext.TraceID().String() == producerTrace &&
		sp.Links()[0].SpanContext.SpanID().String() == producerSpan
	switch {
	case !link && (!inProducerTrace || len(sp.Links()) != 0):
		t.Errorf("consumer span %s parent %s links %v, want a child of the producer %s", sp.SpanContext().TraceID(), sp.Parent().SpanID(), sp.Links(), producerTP)
	case link && (sp.SpanContext().TraceID().String() == producerTrace || !linked):
		t.Errorf("consumer span %s links %v, want its own trace linked to the producer %s", sp.SpanContext().TraceID(), sp.Links(), producerTP)
	}
}

func TestMatchingConsumerSpanFollowsTheProducer(t *testing.T) {
	for _, tc := range deliveryCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, s := realStore(t)
			tp, sr := traceRecorder(t)
			var handled trace.SpanContext
			c := mq.Consumer{Store: s, TracerProvider: tp, Handle: func(ctx context.Context, _ store.Record) error {
				handled = trace.SpanContextFromContext(ctx)
				return nil
			}}
			d := eventDelivery(t, &ackLog{t: t})
			d.Headers = amqp.Table{"traceparent": producerTP}
			tc.mod(&d)
			if err := c.Process(ctx, d); err != nil {
				t.Fatal(err)
			}
			checkConsumerSpan(t, sr, handled, tc.link)
		})
	}
}

func TestWakeupConsumerSpanFollowsTheProducer(t *testing.T) {
	for _, tc := range deliveryCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := realStore(t)
			tp, sr := traceRecorder(t)
			var handled trace.SpanContext
			c := mq.WakeupConsumer{TracerProvider: tp, Handle: func(ctx context.Context, _ mq.Wakeup) error {
				handled = trace.SpanContextFromContext(ctx)
				return nil
			}}
			d := wakeDelivery(t, &ackLog{t: t})
			d.Headers = amqp.Table{"traceparent": producerTP}
			tc.mod(&d)
			if err := c.Process(ctx, d); err != nil {
				t.Fatal(err)
			}
			checkConsumerSpan(t, sr, handled, tc.link)
		})
	}
}
