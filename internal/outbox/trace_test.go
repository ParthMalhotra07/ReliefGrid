package outbox_test

import (
	"context"
	"errors"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/outbox"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// seed's stored traceparent.
const (
	storedTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	storedSpan  = "00f067aa0ba902b7"
)

// Each publish is a producer span the publisher receives in its context. A
// row's first publish continues the trace stored in its envelope (the API
// request's); a retry after a failed publish is a span of its own, linked
// to the stored context, so a stuck row does not stretch the request's
// trace over every backoff (plan §25).
func TestRelayPublishIsAProducerSpanLinkedOnRetry(t *testing.T) {
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 1)
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	r, err := outbox.New(outbox.Config{Store: s, TracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}

	var seen []trace.SpanContext
	fail := true
	p := &publisher{publish: func(ctx context.Context, _ store.OutboxEvent) error {
		seen = append(seen, trace.SpanContextFromContext(ctx))
		if fail {
			fail = false
			return errors.New("broker unavailable")
		}
		return nil
	}}
	if _, err := r.Step(ctx, 0, p); err == nil {
		t.Fatal("first step: want the publish failure")
	}
	if _, err := r.Step(ctx, 0, p); err != nil {
		t.Fatalf("retry step: %v", err)
	}

	ended := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, sp := range sr.Ended() {
		ended[sp.SpanContext().SpanID()] = sp
	}
	if len(seen) != 2 {
		t.Fatalf("publisher called %d times, want 2", len(seen))
	}
	first, retry := ended[seen[0].SpanID()], ended[seen[1].SpanID()]
	if first == nil || retry == nil {
		t.Fatalf("publisher contexts %v are not recorded relay spans (recorded %d)", seen, len(ended))
	}
	for _, sp := range []sdktrace.ReadOnlySpan{first, retry} {
		if sp.SpanKind() != trace.SpanKindProducer {
			t.Errorf("span %q kind %v, want producer", sp.Name(), sp.SpanKind())
		}
	}
	if first.SpanContext().TraceID().String() != storedTrace || first.Parent().SpanID().String() != storedSpan {
		t.Errorf("first publish %s parent %s, want a child of the stored %s/%s",
			first.SpanContext().TraceID(), first.Parent().SpanID(), storedTrace, storedSpan)
	}
	if retry.SpanContext().TraceID().String() == storedTrace {
		t.Errorf("retry publish joined the stored trace; want a span of its own")
	}
	links := retry.Links()
	if len(links) != 1 || links[0].SpanContext.TraceID().String() != storedTrace || links[0].SpanContext.SpanID().String() != storedSpan {
		t.Errorf("retry links %v, want one link to the stored %s/%s", links, storedTrace, storedSpan)
	}
}
