package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	traceCreateBody = `{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"Synthetic trace test"}`
	incomingTrace   = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

var traceAt = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

// Outbox envelopes the pre-S4.4b API wrote (captured from 415fa2c) for
// traceCreateBody under Idempotency-Key fixtureKey("trace"), caller
// "trace-caller" and traceAt. With no span they must stay byte-identical.
const (
	// No traceparent header: derived from the operationId, unsampled.
	goldenDerived = `{"schemaVersion":1,"eventId":"ff237536-d753-59a6-89da-eb7ce8196955","entityId":"4238edd8-d7e4-5fbb-a17f-fdc9cf0e416d","entityVersion":1,"eventType":"request.created","occurredAt":"2026-11-04T12:00:00Z","traceparent":"00-4f3df39420092e602f007b68bbb68112-e698da878fd07ca0-00","payload":{"details":"Synthetic trace test","location":{"lat":0,"lng":0},"peopleAffected":1,"requestId":"4238edd8-d7e4-5fbb-a17f-fdc9cf0e416d","requestType":"WATER","status":"PENDING","urgency":"NORMAL"}}`
	// A valid incoming traceparent header: passed through.
	goldenIncoming = `{"schemaVersion":1,"eventId":"ff237536-d753-59a6-89da-eb7ce8196955","entityId":"4238edd8-d7e4-5fbb-a17f-fdc9cf0e416d","entityVersion":1,"eventType":"request.created","occurredAt":"2026-11-04T12:00:00Z","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","payload":{"details":"Synthetic trace test","location":{"lat":0,"lng":0},"peopleAffected":1,"requestId":"4238edd8-d7e4-5fbb-a17f-fdc9cf0e416d","requestType":"WATER","status":"PENDING","urgency":"NORMAL"}}`
)

// createAndReadOutbox serves one create through h and returns the request
// row's committed outbox payload.
func createAndReadOutbox(t *testing.T, h http.Handler, s store.Store, ctx context.Context, header string) string {
	t.Helper()
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/v1/requests", strings.NewReader(traceCreateBody))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", fixtureKey("trace"))
	if header != "" {
		r.Header.Set("traceparent", header)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	var rows []store.OutboxEvent
	for shard := range s.Shards() {
		got, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, got...)
	}
	if len(rows) != 1 || rows[0].EventType != "request.created" {
		t.Fatalf("outbox rows %+v, want one request.created", rows)
	}
	return string(rows[0].Payload)
}

func envelopeTraceparent(t *testing.T, payload string) string {
	t.Helper()
	var env struct {
		Traceparent string `json:"traceparent"`
	}
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		t.Fatal(err)
	}
	return env.Traceparent
}

// With no span in the request context and no header, the event is exactly
// what the API wrote before tracing: traceparent derived from the operation.
func TestEventWithoutSpanIsByteIdentical(t *testing.T) {
	ctx, s := realStore(t)
	got := createAndReadOutbox(t, handler(t, s, traceAt, "trace-caller"), s, ctx, "")
	if got != goldenDerived {
		t.Errorf("outbox envelope changed with no span:\n got %s\nwant %s", got, goldenDerived)
	}
}

// With tracing off (no-op provider) behind obs.Handler, an incoming
// traceparent still reaches the event unchanged, as the header did before.
func TestEventWithTracingOffKeepsIncomingTraceparent(t *testing.T) {
	ctx, s := realStore(t)
	h := obs.Handler(handler(t, s, traceAt, "trace-caller"), obs.WithTracerProvider(noop.NewTracerProvider()))
	got := createAndReadOutbox(t, h, s, ctx, incomingTrace)
	if got != goldenIncoming {
		t.Errorf("outbox envelope changed with tracing off:\n got %s\nwant %s", got, goldenIncoming)
	}
}

// With tracing on, the event carries the API server span's context, so the
// relay's producer span (and everything after it) joins the request's trace.
func TestEventCarriesTheRequestSpan(t *testing.T) {
	ctx, s := realStore(t)
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	h := obs.Handler(handler(t, s, traceAt, "trace-caller"), obs.WithTracerProvider(tp))
	got := envelopeTraceparent(t, createAndReadOutbox(t, h, s, ctx, incomingTrace))

	var server sdktrace.ReadOnlySpan
	for _, sp := range sr.Ended() {
		if sp.SpanKind() == trace.SpanKindServer {
			server = sp
		}
	}
	if server == nil {
		t.Fatal("no API server span")
	}
	want := fmt.Sprintf("00-%s-%s-01", server.SpanContext().TraceID(), server.SpanContext().SpanID())
	if got != want {
		t.Errorf("event traceparent %q, want the API server span %q", got, want)
	}
}
