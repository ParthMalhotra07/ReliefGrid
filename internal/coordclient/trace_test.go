package coordclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Every call to the coordinator is a client span under the caller's span,
// named by store method (never by key), and the coordinator receives that
// span's context as traceparent.
func TestCallsAreClientSpansWithTraceparent(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	f := newFake(t)
	c := newClient(t, f, coordclient.WithTracerProvider(tp))

	ctx, parent := tp.Tracer("test").Start(testCtx(t), "caller")
	if _, err := c.CreateEntity(ctx, store.CreateEntity{Meta: meta("op-1"), Key: "kv:a", Value: json.RawMessage(`1`)}); err != nil {
		t.Fatalf("CreateEntity: %v", err)
	}
	parent.End()

	var spans []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.SpanContext().TraceID() == parent.SpanContext().TraceID() && s.SpanKind() == trace.SpanKindClient {
			spans = append(spans, s)
		}
	}
	if len(spans) != 1 {
		t.Fatalf("%d client spans in the caller's trace, want 1", len(spans))
	}
	s := spans[0]
	if s.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("client span parent %s, want the caller %s", s.Parent().SpanID(), parent.SpanContext().SpanID())
	}
	if s.Name() != "coordclient.CreateEntity" {
		t.Errorf("span name %q, want coordclient.CreateEntity", s.Name())
	}
	reqs := f.requests()
	if len(reqs) != 1 {
		t.Fatalf("coordinator got %d requests, want 1", len(reqs))
	}
	want := fmt.Sprintf("00-%s-%s-01", s.SpanContext().TraceID(), s.SpanContext().SpanID())
	if got := reqs[0].header.Get("traceparent"); got != want {
		t.Errorf("coordinator received traceparent %q, want the client span's %q", got, want)
	}
}
