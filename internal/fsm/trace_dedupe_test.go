package fsm_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	traceA = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	traceB = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
)

// envelopeEvent is an outbox event whose payload is an A0.1 event envelope
// carrying trace.
func envelopeEvent(eventID, entityID, trace string) store.OutboxEvent {
	body, _ := json.Marshal(map[string]any{
		"schemaVersion": 1, "eventId": eventID, "entityId": entityID, "entityVersion": 1,
		"eventType": "request.created", "occurredAt": "2026-10-30T00:00:00Z", "traceparent": trace,
		"payload": map[string]any{"requestId": entityID, "status": "PENDING"},
	})
	return store.OutboxEvent{EventID: eventID, EventType: "request.created", SchemaVersion: 1,
		Exchange: "relief.events", RoutingKey: "request.created", Payload: body}
}

func withTrace(evs []store.OutboxEvent, trace string) []store.OutboxEvent {
	out := make([]store.OutboxEvent, len(evs))
	for i, ev := range evs {
		var m map[string]any
		if err := json.Unmarshal(ev.Payload, &m); err != nil {
			panic(err)
		}
		m["traceparent"] = trace
		ev.Payload, _ = json.Marshal(m)
		out[i] = ev
	}
	return out
}

func outboxTrace(t *testing.T, h *harness, eventID string) string {
	t.Helper()
	dump, err := h.f.Dump(context.Background())
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	for _, row := range dump["outbox"] {
		if strings.Contains(row, eventID) {
			for _, tr := range []string{traceA, traceB} {
				if strings.Contains(row, tr) {
					return tr
				}
			}
			t.Fatalf("outbox row %s has neither test trace", row)
		}
	}
	t.Fatalf("no outbox row for %s", eventID)
	return ""
}

// Verify (S1.x): a retry of the same operationId that differs only in an
// outbox envelope's traceparent replays the committed result, and the stored
// row keeps the first committed traceparent. Covers every command that
// carries outbox events; a stored rejection is the documented exception.
func TestRetryWithNewTraceparentReplays(t *testing.T) {
	h := newHarness(t)
	createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))

	// CreateEntity.
	create := store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater),
		Outbox: []store.OutboxEvent{envelopeEvent("ev-c-q1", "q1", traceA)}}
	first := h.apply(create)
	expectOutcome(t, "create", first, store.OutcomeOK)
	create.Outbox = withTrace(create.Outbox, traceB)
	again := h.apply(create)
	if again.Outcome != store.OutcomeOK || !again.Replayed || again.Version != first.Version || again.AppliedIndex != first.AppliedIndex {
		t.Fatalf("create retry with new trace = %+v (%s), want replay of %+v", again, again.Reason, first)
	}
	if got := outboxTrace(t, h, "ev-c-q1"); got != traceA {
		t.Fatalf("stored traceparent = %s, want the first committed %s", got, traceA)
	}

	// TransitionRequest.
	a1 := attempt("a1", "r1")
	tr := store.TransitionRequest{Meta: meta("t-q1"), Key: store.RequestKey("q1"), ExpectedVersion: 1,
		ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a1,
		Outbox: []store.OutboxEvent{envelopeEvent("ev-t-q1", "q1", traceA)}}
	expectOutcome(t, "transition", h.apply(tr), store.OutcomeOK)
	tr.Outbox = withTrace(tr.Outbox, traceB)
	if res := h.apply(tr); res.Outcome != store.OutcomeOK || !res.Replayed {
		t.Fatalf("transition retry with new trace = %+v (%s), want replay", res, res.Reason)
	}

	// RetryWorkflow (q2 is WAITING first, so a retry may be recorded).
	expectOutcome(t, "create q2", h.apply(store.CreateEntity{Meta: meta("c-q2"), Key: store.RequestKey("q2"), Request: request("q2", store.TypeWater)}), store.OutcomeOK)
	expectOutcome(t, "q2 waiting", h.apply(store.TransitionRequest{Meta: meta("t-q2"), Key: store.RequestKey("q2"), ExpectedVersion: 1,
		ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: t0 + 30_000}), store.OutcomeOK)
	retry := store.RetryWorkflow{Meta: meta("retry-q2"), Key: store.RequestKey("q2"), ExpectedVersion: 2, NextAttemptAt: t0 + 60_000,
		Outbox: []store.OutboxEvent{envelopeEvent("ev-retry-q2", "q2", traceA)}}
	expectOutcome(t, "retry workflow", h.apply(retry), store.OutcomeOK)
	retry.Outbox = withTrace(retry.Outbox, traceB)
	if res := h.apply(retry); res.Outcome != store.OutcomeOK || !res.Replayed {
		t.Fatalf("retry workflow retry with new trace = %+v (%s), want replay", res, res.Reason)
	}

	// Limitation: a stored rejection wrote no outbox rows, so there is no
	// committed traceparent to compare against. A trace-only retry of a
	// rejection stays OPERATION_ID_REUSED (an identical retry still replays).
	stale := store.TransitionRequest{Meta: meta("t-q1-stale"), Key: store.RequestKey("q1"), ExpectedVersion: 1,
		ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: t0 + 30_000,
		Outbox: []store.OutboxEvent{envelopeEvent("ev-stale", "q1", traceA)}}
	expectOutcome(t, "stale", h.apply(stale), store.OutcomeVersionConflict)
	if res := h.apply(stale); res.Outcome != store.OutcomeVersionConflict || !res.Replayed {
		t.Fatalf("identical retry of a rejection = %+v (%s), want replayed VERSION_CONFLICT", res, res.Reason)
	}
	stale.Outbox = withTrace(stale.Outbox, traceB)
	expectOutcome(t, "trace-only retry of a rejection (limitation)", h.apply(stale), store.OutcomeOperationIDReuse)

	// TransitionResource.
	res := store.TransitionResource{Meta: meta("tr-r1"), Key: store.ResourceKey("r1"), ExpectedVersion: 1,
		Status: store.ResourceOffline, Outbox: []store.OutboxEvent{envelopeEvent("ev-tr-r1", "r1", traceA)}}
	first = h.apply(res)
	expectOutcome(t, "resource transition", first, store.OutcomeOK)
	res.Outbox = withTrace(res.Outbox, traceB)
	if again := h.apply(res); again.Outcome != first.Outcome || !again.Replayed {
		t.Fatalf("resource transition retry with new trace = %+v (%s), want replay", again, again.Reason)
	}
}

// Verify (S1.x): any change other than an envelope's traceparent value is
// still OPERATION_ID_REUSED, even when the trace changes too.
func TestOtherOutboxChangesStillReuse(t *testing.T) {
	base := func() store.CreateEntity {
		return store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater),
			Outbox: []store.OutboxEvent{envelopeEvent("ev-c-q1", "q1", traceA)}}
	}
	setField := func(field string, v any) func(*store.CreateEntity) {
		return func(c *store.CreateEntity) {
			var m map[string]any
			_ = json.Unmarshal(c.Outbox[0].Payload, &m)
			if v == nil {
				delete(m, field)
			} else {
				m[field] = v
			}
			c.Outbox[0].Payload, _ = json.Marshal(m)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*store.CreateEntity)
	}{
		{"envelope payload", setField("payload", map[string]any{"requestId": "q1", "status": "WAITING"})},
		{"envelope entityVersion", setField("entityVersion", 2)},
		{"envelope eventType", setField("eventType", "request.state_changed")},
		{"traceparent removed", setField("traceparent", nil)},
		{"extra envelope field", setField("traceid", traceB)},
		{"nested traceparent", setField("payload", map[string]any{"requestId": "q1", "status": "PENDING", "traceparent": traceB})},
		{"eventId", func(c *store.CreateEntity) { c.Outbox[0].EventID = "ev-other" }},
		{"routingKey", func(c *store.CreateEntity) { c.Outbox[0].RoutingKey = "request.created.water" }},
		{"exchange", func(c *store.CreateEntity) { c.Outbox[0].Exchange = "relief.retry" }},
		{"extra event", func(c *store.CreateEntity) { c.Outbox = append(c.Outbox, envelopeEvent("ev-2", "q1", traceA)) }},
		{"event removed", func(c *store.CreateEntity) { c.Outbox = nil }},
		{"request body", func(c *store.CreateEntity) { c.Request.PeopleAffected = 5 }},
		{"timestamp", func(c *store.CreateEntity) { c.At = t0 + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			expectOutcome(t, "first", h.apply(base()), store.OutcomeOK)
			retry := base()
			retry.Outbox = withTrace(retry.Outbox, traceB)
			tc.mutate(&retry)
			expectOutcome(t, "retry", h.apply(retry), store.OutcomeOperationIDReuse)
		})
	}
}

// A payload that is not a JSON object (no envelope) is hashed as is: an
// identical retry replays and a changed one is still REUSED.
func TestNonEnvelopeOutboxPayloadDedupe(t *testing.T) {
	h := newHarness(t)
	ev := store.OutboxEvent{EventID: "ev-raw", EventType: "request.created", SchemaVersion: 1,
		Exchange: "relief.events", RoutingKey: "request.created", Payload: json.RawMessage(`["traceparent","x"]`)}
	cmd := store.CreateEntity{Meta: meta("c-raw"), Key: store.RequestKey("q9"), Request: request("q9", store.TypeWater), Outbox: []store.OutboxEvent{ev}}
	expectOutcome(t, "first", h.apply(cmd), store.OutcomeOK)
	if res := h.apply(cmd); !res.Replayed {
		t.Fatalf("identical retry = %+v, want replay", res)
	}
	cmd.Outbox[0].Payload = json.RawMessage(`["traceparent","y"]`)
	expectOutcome(t, "changed array payload", h.apply(cmd), store.OutcomeOperationIDReuse)
}
