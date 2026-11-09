package fsm_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Use the proposed wire envelope so these tests compile before the new Go API
// exists. Red must be a missing-behaviour assertion, not a compiler error.
func agingBody(t *testing.T, rec store.Record, op string, at, interval int64, priority int) map[string]any {
	t.Helper()
	payload := map[string]any{"schemaVersion": 1, "eventId": "event-" + op, "entityId": rec.Request.ID, "entityVersion": rec.Version + 1, "eventType": "request.retry", "occurredAt": "2026-10-14T00:00:00Z", "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "payload": map[string]any{"requestId": rec.Request.ID, "workflowGeneration": rec.Request.WorkflowGeneration, "nextAttemptAt": at, "urgency": priority}}
	// Envelope time must correspond to the proposer timestamp.
	payload["occurredAt"] = time.UnixMilli(at).UTC().Format(time.RFC3339Nano)
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"operationId": op, "at": at, "key": rec.Key, "expectedVersion": rec.Version, "agingIntervalMS": interval, "outbox": []store.OutboxEvent{{EventID: "event-" + op, EventType: "request.retry", SchemaVersion: 1, Exchange: "relief.requests", RoutingKey: "request.retry", Payload: b}}}
}

func agingEntry(t *testing.T, body map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"op": "AgeRequest", "body": body})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func applyAging(h *harness, body map[string]any) store.Result {
	h.t.Helper()
	return h.applyEntry(agingEntry(h.t, body))
}
func agingRequest(t *testing.T, urgency int) (*harness, store.Record) {
	t.Helper()
	h := newHarness(t)
	q := request("aging", store.TypeWater)
	q.Urgency = urgency
	expectOutcome(t, "create", h.apply(store.CreateEntity{Meta: meta("create-aging"), Key: store.RequestKey(q.ID), Request: q}), store.OutcomeOK)
	return h, h.read(store.RequestKey(q.ID))
}
func effective(t *testing.T, q *store.Request) int {
	t.Helper()
	b, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	var fields struct {
		Urgency   int `json:"urgency"`
		Effective int `json:"effectiveUrgency"`
	}
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	if fields.Effective == 0 {
		return fields.Urgency
	}
	return fields.Effective
}
func ageRows(t *testing.T, h *harness) []store.OutboxEvent {
	t.Helper()
	rows, err := h.f.PendingOutbox(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}
func unchangedAging(t *testing.T, h *harness, before store.Record) {
	t.Helper()
	if got := h.read(before.Key); !reflect.DeepEqual(got, before) {
		t.Fatalf("request changed on rejection/no-op: got %+v want %+v", got, before)
	}
	if rows := ageRows(t, h); len(rows) != 0 {
		t.Fatalf("unexpected outbox rows: %+v", rows)
	}
}

func TestAgeRequestThresholdsAndCap(t *testing.T) {
	for _, tc := range []struct {
		name    string
		base    int
		elapsed int64
		want    int
		changed bool
	}{
		{"before", 1, 999, 1, false}, {"exact", 1, 1000, 2, true}, {"second", 1, 2000, 3, true}, {"high", 2, 1000, 3, true}, {"critical", 3, 1000, 3, false}, {"far_future", 1, math.MaxInt64 - t0, 3, true}, {"before_creation", 1, -1, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, before := agingRequest(t, tc.base)
			at := t0 + tc.elapsed
			got := applyAging(h, agingBody(t, before, "age", at, 1000, tc.want))
			expectOutcome(t, "age", got, store.OutcomeOK)
			after := h.read(before.Key)
			if !tc.changed {
				unchangedAging(t, h, before)
				return
			}
			if effective(t, after.Request) != tc.want || after.Request.Urgency != tc.base || after.Version != before.Version+1 || got.Version != after.Version {
				t.Fatalf("priority/version: %+v / %+v", after, got)
			}
			expected := *before.Request
			expected.UpdatedAt = at
			expected.NextAttemptAt = at
			actual := *after.Request
			// Compare every original field, leaving only the proposed new field aside.
			b, _ := json.Marshal(actual)
			var fields map[string]json.RawMessage
			_ = json.Unmarshal(b, &fields)
			delete(fields, "effectiveUrgency")
			b, _ = json.Marshal(fields)
			var oldShape store.Request
			_ = json.Unmarshal(b, &oldShape)
			if !reflect.DeepEqual(oldShape, expected) {
				t.Fatalf("unrelated request fields changed: got %+v want %+v", oldShape, expected)
			}
			if after.UpdatedAt != at {
				t.Fatalf("record timestamp %d, want %d", after.UpdatedAt, at)
			}
			rows := ageRows(t, h)
			if len(rows) != 1 || rows[0].EntityVersion != after.Version || rows[0].EntityKey != before.Key || rows[0].CreatedAt != at || rows[0].Published {
				t.Fatalf("outbox not reciprocal: %+v", rows)
			}
		})
	}
}

func TestAgeRequestUsesCreatedAtAndShortensWaitingBackoff(t *testing.T) {
	h, r := agingRequest(t, 1)
	expectOutcome(t, "wait", h.apply(store.TransitionRequest{Meta: store.Meta{OperationID: "wait", At: t0 + 900}, Key: r.Key, ExpectedVersion: r.Version, ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: t0 + 60_000}), store.OutcomeOK)
	before := h.read(r.Key)
	expectOutcome(t, "age", applyAging(h, agingBody(t, before, "age", t0+1000, 1000, 2)), store.OutcomeOK)
	after := h.read(r.Key)
	if after.Request.NextAttemptAt != t0+1000 || after.Request.RetryCount != before.Request.RetryCount || after.Request.Status != store.RequestWaiting || effective(t, after.Request) != 2 {
		t.Fatalf("waiting wake lost state: %+v", after)
	}
}

func TestAgeRequestDedupeCompetingVersionsAndNoop(t *testing.T) {
	h, before := agingRequest(t, 1)
	cmd := agingBody(t, before, "age", t0+1000, 1000, 2)
	first := applyAging(h, cmd)
	expectOutcome(t, "first", first, store.OutcomeOK)
	replay := applyAging(h, cmd)
	if !replay.Replayed || replay.Outcome != store.OutcomeOK || replay.Version != first.Version {
		t.Fatalf("lost-response replay: %+v", replay)
	}
	expectOutcome(t, "competing scan", applyAging(h, agingBody(t, before, "other", t0+1000, 1000, 2)), store.OutcomeVersionConflict)
	after := h.read(before.Key)
	expectOutcome(t, "achieved target", applyAging(h, agingBody(t, after, "noop", t0+1000, 1000, 2)), store.OutcomeOK)
	if got := h.read(before.Key); !reflect.DeepEqual(got, after) {
		t.Fatal("new operation re-aged achieved target")
	}
	if len(ageRows(t, h)) != 1 {
		t.Fatal("duplicate wake event")
	}
	cmd["at"] = t0 + 1001
	expectOutcome(t, "changed retry", applyAging(h, cmd), store.OutcomeOperationIDReuse)
}

func TestAgeRequestCancellationBothOrderings(t *testing.T) {
	for _, cancelFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelFirst), func(t *testing.T) {
			h, before := agingRequest(t, 1)
			age := agingBody(t, before, "age", t0+1000, 1000, 2)
			cancel := store.TransitionRequest{Meta: meta("cancel"), Key: before.Key, ExpectedVersion: before.Version, ExpectedStatus: store.RequestPending, To: store.RequestCancelled}
			if cancelFirst {
				expectOutcome(t, "cancel", h.apply(cancel), store.OutcomeOK)
				expectOutcome(t, "stale age", applyAging(h, age), store.OutcomeVersionConflict)
				terminal := h.read(before.Key)
				expectOutcome(t, "late age", applyAging(h, agingBody(t, terminal, "late", t0+2000, 1000, 3)), store.OutcomeDomainConflict)
				unchangedAging(t, h, terminal)
			} else {
				expectOutcome(t, "age", applyAging(h, age), store.OutcomeOK)
				expectOutcome(t, "stale cancel", h.apply(cancel), store.OutcomeVersionConflict)
				cancel.OperationID = "cancel-fresh"
				cancel.ExpectedVersion = h.read(before.Key).Version
				expectOutcome(t, "fresh cancel", h.apply(cancel), store.OutcomeOK)
				terminal := h.read(before.Key)
				res := applyAging(h, age)
				if !res.Replayed || res.Outcome != store.OutcomeOK {
					t.Fatalf("historical replay: %+v", res)
				}
				if !reflect.DeepEqual(h.read(before.Key), terminal) || len(ageRows(t, h)) != 1 {
					t.Fatal("replay resurrected request or repeated wake")
				}
			}
		})
	}
}

func TestAgeRequestRejectsIneligibleStates(t *testing.T) {
	for _, state := range requestStatuses {
		if state == store.RequestPending || state == store.RequestWaiting {
			continue
		}
		t.Run(string(state), func(t *testing.T) {
			h := requestAt(t, state)
			before := h.read(store.RequestKey(tableRequest))
			expectOutcome(t, "ineligible", applyAging(h, agingBody(t, before, "age", t0+1000, 1000, 3)), store.OutcomeDomainConflict)
			unchangedAging(t, h, before)
		})
	}
}

func TestAgeRequestValidationIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"zero_interval", func(c map[string]any) { c["agingIntervalMS"] = 0 }},
		{"negative_interval", func(c map[string]any) { c["agingIntervalMS"] = -1 }},
		{"zero_at", func(c map[string]any) { c["at"] = 0 }},
		{"missing_event", func(c map[string]any) { delete(c, "outbox") }},
		{"wrong_priority", func(c map[string]any) {
			ev := c["outbox"].([]store.OutboxEvent)
			var p map[string]any
			_ = json.Unmarshal(ev[0].Payload, &p)
			p["payload"].(map[string]any)["urgency"] = 1
			ev[0].Payload, _ = json.Marshal(p)
		}},
		{"duplicate_event", func(c map[string]any) { ev := c["outbox"].([]store.OutboxEvent); c["outbox"] = append(ev, ev[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, before := agingRequest(t, 1)
			c := agingBody(t, before, "age", t0+1000, 1000, 2)
			tc.change(c)
			expectOutcome(t, "invalid", applyAging(h, c), store.OutcomeInvalid)
			unchangedAging(t, h, before)
		})
	}
}

func TestAgeRequestReplayAndSnapshotPreserveState(t *testing.T) {
	h, before := agingRequest(t, 1)
	expectOutcome(t, "age", applyAging(h, agingBody(t, before, "age", t0+1000, 1000, 2)), store.OutcomeOK)
	replay := newHarness(t)
	for _, entry := range h.log {
		replay.applyEntry(entry)
	}
	want, err := h.f.Dump(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := replay.f.Dump(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("log replay diverged")
	}
	path := filepath.Join(t.TempDir(), "snapshot.db")
	if err := h.f.VacuumInto(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	restored, err := fsm.OpenImage(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	got, err = restored.Dump(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("snapshot lost aging state")
	}
}

func TestAgeRequestOutboxFailureRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fsm.db")
	f, err := fsm.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := &harness{t: t, f: f}
	q := request("aging", store.TypeWater)
	q.Urgency = 1
	expectOutcome(t, "create", h.apply(store.CreateEntity{Meta: meta("create"), Key: store.RequestKey(q.ID), Request: q}), store.OutcomeOK)
	before := h.read(store.RequestKey(q.ID))
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), `CREATE TRIGGER reject_aging_event BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT, 'injected outbox failure'); END`); err != nil {
		t.Fatal(err)
	}
	cmd := agingBody(t, before, "age", t0+1000, 1000, 2)
	_, err = f.Apply(h.idx+1, agingEntry(t, cmd))
	if err == nil {
		t.Fatal("expected injected storage failure, not committed rejection")
	}
	unchangedAging(t, h, before)
	if _, err := f.Operation(context.Background(), "age"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed transaction retained result: %v", err)
	}
	if f.AppliedIndex() != h.idx {
		t.Fatal("failed transaction advanced applied index")
	}
}

func TestAgeRequestEffectiveUrgencyCompatibility(t *testing.T) {
	field, ok := reflect.TypeOf(store.Request{}).FieldByName("EffectiveUrgency")
	if !ok {
		t.Fatal("Request is missing persisted EffectiveUrgency")
	}
	if field.Tag.Get("json") != "effectiveUrgency,omitempty" {
		t.Fatalf("effective urgency must preserve legacy command hashes: tag=%q", field.Tag.Get("json"))
	}
	q := request("legacy", store.TypeWater)
	b, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["effectiveUrgency"]; ok {
		t.Fatal("zero effective urgency changed legacy serialization")
	}
	if effective(t, q) != q.Urgency {
		t.Fatal("legacy request lost original priority")
	}
}
