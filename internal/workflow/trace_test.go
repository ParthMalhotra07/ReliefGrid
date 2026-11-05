package workflow

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Envelopes the pre-S4.4b workflow wrote (captured from 415fa2c), one per
// line sorted, for the fixed flows below. With no span in the context they
// must stay byte-identical: traceparent derived from the operationId.
const (
	goldenMatchEvents = `{"schemaVersion":1,"eventId":"3d45242e-0c15-5fa1-a97d-93720f388bd5","entityId":"R","entityVersion":3,"eventType":"match.completed","occurredAt":"2026-10-26T12:00:00Z","traceparent":"00-7688d4db149afe0e1e7820b4d4e6f654-9318a3c8d2499001-00","payload":{"requestId":"R","attemptId":"test-id-1","resourceId":"Res","fingerprint":"rg1:1793016000000:1:9721b5fcc731d9abc19efd893ea437c64e161e854810dd959972e897cd6f73c0"}}
{"schemaVersion":1,"eventId":"b859b909-7293-5011-b925-304df6c860d5","entityId":"R","entityVersion":2,"eventType":"match.proposed","occurredAt":"2026-10-26T12:00:00Z","traceparent":"00-f323c837293c4258096f3876c78c1a9d-22a11f758a04b817-00","payload":{"requestId":"R","attemptId":"test-id-1","resourceId":"Res","fingerprint":"rg1:1793016000000:1:9721b5fcc731d9abc19efd893ea437c64e161e854810dd959972e897cd6f73c0"}}`
	goldenCleanupEvents = `{"schemaVersion":1,"eventId":"a5890ddf-747f-5351-ba18-45e6812183ca","entityId":"550e8400-e29b-41d4-a716-446655440000","entityVersion":4,"eventType":"request.state_changed","occurredAt":"2026-10-26T12:01:00Z","traceparent":"00-880c36c810ad4fb295ba18979ccc5ab8-05038a72b1bab2ac-00","payload":{"requestId":"550e8400-e29b-41d4-a716-446655440000","previousStatus":"CANCELLING","newStatus":"CANCELLED","operationId":"8297f756-b851-547a-88db-6d89c386019b","attemptId":"770e8400-e29b-41d4-a716-446655440000"}}`
)

// workflowEvents is every pending outbox envelope of the given types, sorted.
func workflowEvents(t *testing.T, ctx context.Context, s store.Store, types ...string) []string {
	t.Helper()
	var out []string
	for shard := range s.Shards() {
		rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if slices.Contains(types, row.EventType) {
				out = append(out, string(row.Payload))
			}
		}
	}
	slices.Sort(out)
	return out
}

// spanCtx is ctx under a recorded span (the matcher's consumer span in
// production) and the traceparent the events must carry.
func spanCtx(t *testing.T, ctx context.Context) (context.Context, string) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(ctx, "consume")
	t.Cleanup(func() { span.End() })
	sc := trace.SpanContextFromContext(ctx)
	return ctx, fmt.Sprintf(`"traceparent":"00-%s-%s-01"`, sc.TraceID(), sc.SpanID())
}

func matchFlow(t *testing.T, traced bool) []string {
	t.Helper()
	ctx, s := testStore(t)
	createRequest(t, ctx, s, request("R"))
	createResource(t, ctx, s, resource("Res", 0))
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	callCtx, want := ctx, ""
	if traced {
		callCtx, want = spanCtx(t, ctx)
	}
	if _, err := m.Match(callCtx, "R"); err != nil {
		t.Fatal(err)
	}
	matched(t, ctx, s, "R")
	got := workflowEvents(t, ctx, s, "match.proposed", "match.completed")
	if len(got) != 2 {
		t.Fatalf("match events %q, want proposed and completed", got)
	}
	if traced {
		for _, e := range got {
			if !strings.Contains(e, want) {
				t.Errorf("match event %s does not carry the caller's span (%s)", e, want)
			}
		}
	}
	return got
}

func cleanupFlow(t *testing.T, traced bool) []string {
	t.Helper()
	ctx, s := testStore(t)
	cleanupFixture(t, ctx, s, false, false, true, false)
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	callCtx, want := ctx, ""
	if traced {
		callCtx, want = spanCtx(t, ctx)
	}
	if _, err := m.Cleanup(callCtx, cleanupRequest); err != nil {
		t.Fatal(err)
	}
	got := workflowEvents(t, ctx, s, "request.state_changed")
	var terminal []string
	for _, e := range got {
		if strings.Contains(e, `"newStatus":"CANCELLED"`) {
			terminal = append(terminal, e)
		}
	}
	if len(terminal) != 1 {
		t.Fatalf("cleanup terminal events %q, want one CANCELLED", got)
	}
	if traced && !strings.Contains(terminal[0], want) {
		t.Errorf("cleanup event %s does not carry the caller's span (%s)", terminal[0], want)
	}
	return terminal
}

// Workflow events carry the trace of the context the work ran under (the
// matcher's consumer span), so the next hop joins the request's trace.
func TestWorkflowEventsCarryTheCallerSpan(t *testing.T) {
	t.Run("match", func(t *testing.T) { matchFlow(t, true) })
	t.Run("cleanup", func(t *testing.T) { cleanupFlow(t, true) })
}

// With no span, the envelopes are byte-identical to before S4.4b.
func TestWorkflowEventsWithoutSpanAreByteIdentical(t *testing.T) {
	if got := strings.Join(matchFlow(t, false), "\n"); got != goldenMatchEvents {
		t.Errorf("match events changed with no span:\n got %s\nwant %s", got, goldenMatchEvents)
	}
	if got := strings.Join(cleanupFlow(t, false), "\n"); got != goldenCleanupEvents {
		t.Errorf("cleanup events changed with no span:\n got %s\nwant %s", got, goldenCleanupEvents)
	}
}
