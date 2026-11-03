package coordinator_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
)

const incomingTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func tracedHarness(t *testing.T) (*harness, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	h := newHarness(t, func(c *coordinator.Config) { c.TracerProvider = tp })
	return h, sr
}

func traceAttr(s sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func traceparentOf(s sdktrace.ReadOnlySpan) string {
	return fmt.Sprintf("00-%s-%s-01", s.SpanContext().TraceID(), s.SpanContext().SpanID())
}

// A routed call continues the caller's trace in a server span; each forward
// attempt is its own child span naming the shard, the attempt number and the
// leader hint it followed, and the storage node receives that attempt's
// context. Registration and heartbeats are not traced.
func TestRoutedCallTracesEachForwardAttempt(t *testing.T) {
	h, sr := tracedHarness(t)
	h.ready()
	old, cur := h.nodes[2][0], h.nodes[2][1]
	old.setHandler(notLeader(cur, 2))
	r := routedCalls(t, h, 2)[7] // claim
	if r.name != "claim" {
		t.Fatalf("case %q", r.name)
	}
	r.call.header = map[string]string{"traceparent": incomingTrace}
	if rec := h.do(r.call); rec.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rec.Code, rec.Body.String())
	}

	in := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": incomingTrace}))
	var server sdktrace.ReadOnlySpan
	var attempts []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		switch {
		case s.SpanKind() == trace.SpanKindServer:
			if server != nil {
				t.Errorf("second server span %q: registration or heartbeats were traced", s.Name())
			}
			server = s
		case s.Name() == "coordinator.forward":
			attempts = append(attempts, s)
		default:
			t.Errorf("unexpected span %q (%v)", s.Name(), s.SpanKind())
		}
	}
	if server == nil {
		t.Fatal("no server span")
	}
	if server.Name() != "POST /internal/resources/{id}/claim" {
		t.Errorf("server span name %q, want the route pattern", server.Name())
	}
	if server.Parent().SpanID() != in.SpanID() || server.SpanContext().TraceID() != in.TraceID() {
		t.Errorf("server span parent %s, want the incoming %s", server.Parent().SpanID(), in.SpanID())
	}
	if len(attempts) != 2 {
		t.Fatalf("%d forward spans, want 2 (NOT_LEADER, then the hinted leader)", len(attempts))
	}
	for i, a := range attempts {
		if a.Parent().SpanID() != server.SpanContext().SpanID() {
			t.Errorf("attempt %d parent %s, want the server span", i+1, a.Parent().SpanID())
		}
		if a.SpanKind() != trace.SpanKindClient {
			t.Errorf("attempt %d kind %v, want client", i+1, a.SpanKind())
		}
		if v, _ := traceAttr(a, "reliefgrid.shard"); v.AsInt64() != 2 {
			t.Errorf("attempt %d reliefgrid.shard = %d, want 2", i+1, v.AsInt64())
		}
		if v, _ := traceAttr(a, "coordinator.attempt"); v.AsInt64() != int64(i+1) {
			t.Errorf("attempt %d coordinator.attempt = %d", i+1, v.AsInt64())
		}
	}
	if v, ok := traceAttr(attempts[0], "coordinator.leader_hint"); ok {
		t.Errorf("first attempt followed hint %q, want none", v.AsString())
	}
	if v, _ := traceAttr(attempts[1], "coordinator.leader_hint"); v.AsString() != cur.pod {
		t.Errorf("second attempt coordinator.leader_hint = %q, want %q", v.AsString(), cur.pod)
	}
	if v, _ := traceAttr(attempts[0], "http.response.status_code"); v.AsInt64() != http.StatusServiceUnavailable {
		t.Errorf("first attempt status = %d, want 503", v.AsInt64())
	}

	o, c := old.requests(), cur.requests()
	if len(o) != 1 || len(c) != 1 {
		t.Fatalf("old leader got %d, new leader %d requests; want 1 and 1", len(o), len(c))
	}
	if got := o[0].header.Get("traceparent"); got != traceparentOf(attempts[0]) {
		t.Errorf("old leader received traceparent %q, want attempt 1's %q", got, traceparentOf(attempts[0]))
	}
	if got := c[0].header.Get("traceparent"); got != traceparentOf(attempts[1]) {
		t.Errorf("new leader received traceparent %q, want attempt 2's %q", got, traceparentOf(attempts[1]))
	}
}
