package mq

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

var traceEvent = store.OutboxEvent{EventID: "e-1", EventType: "request.created", Exchange: "relief.requests", RoutingKey: "request.created.water",
	Payload: []byte(`{"payload":{"urgency":"HIGH"}}`)}

// The published message carries the publishing context's trace (the relay's
// producer span) as a traceparent header, for the consumer to continue.
func TestPublishingCarriesTheContextTraceparent(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "publish")
	defer span.End()

	msg, err := publishing(ctx, traceEvent)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("00-%s-%s-01", span.SpanContext().TraceID(), span.SpanContext().SpanID())
	if got := msg.Headers["traceparent"]; got != want {
		t.Errorf("traceparent header %v, want the publishing span %q", got, want)
	}
}

// With no trace in the context, the message is exactly what it was before
// S4.4: no headers at all.
func TestPublishingWithoutTraceIsUnchanged(t *testing.T) {
	msg, err := publishing(context.Background(), traceEvent)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Headers != nil {
		t.Errorf("headers %v, want none without a trace", msg.Headers)
	}
	want := publishingFields{DeliveryMode: 2, ContentType: "application/json", MessageID: "e-1", Type: "request.created", Priority: 2, Body: traceEvent.Payload}
	if got := (publishingFields{msg.DeliveryMode, msg.ContentType, msg.MessageId, msg.Type, msg.Priority, msg.Body}); !reflect.DeepEqual(got, want) {
		t.Errorf("message %+v, want %+v", got, want)
	}
}

type publishingFields struct {
	DeliveryMode uint8
	ContentType  string
	MessageID    string
	Type         string
	Priority     uint8
	Body         []byte
}
