//go:build schedules

package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

type outboxHandoff struct {
	meta      store.Meta
	key       string
	version   uint64
	eventType string
	route     string
	events    []store.OutboxEvent
}

func validateHandoff(h outboxHandoff, rows []store.OutboxEvent, result store.Result) error {
	if result.Outcome != store.OutcomeOK || result.OperationID != h.meta.OperationID || result.Key != h.key || result.Version != h.version {
		return fmt.Errorf("outbox operation proof mismatches committed handoff %s", h.meta.OperationID)
	}
	if len(h.events) != 1 {
		return fmt.Errorf("handoff %s has %d events, want one %s", h.meta.OperationID, len(h.events), h.eventType)
	}
	event := h.events[0]
	exchange := "relief.events"
	if h.eventType == "request.retry" {
		exchange = "relief.retry"
	}
	if event.EventID == "" || event.EventType != h.eventType || event.SchemaVersion != 1 || event.Exchange != exchange || event.RoutingKey != h.route {
		return fmt.Errorf("wrong event type/route for %s", h.meta.OperationID)
	}
	var body struct {
		SchemaVersion int    `json:"schemaVersion"`
		EventID       string `json:"eventId"`
		EntityID      string `json:"entityId"`
		EntityVersion uint64 `json:"entityVersion"`
		EventType     string `json:"eventType"`
		OccurredAt    string `json:"occurredAt"`
		Payload       struct {
			RequestID   string `json:"requestId"`
			OperationID string `json:"operationId"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(event.Payload, &body); err != nil {
		return fmt.Errorf("event envelope: %w", err)
	}
	at, err := time.Parse(time.RFC3339Nano, body.OccurredAt)
	request := strings.TrimPrefix(h.key, store.RequestKeyPrefix)
	if err != nil || at.UnixMilli() != h.meta.At || body.SchemaVersion != 1 || body.EventID != event.EventID || body.EntityID != request || body.EntityVersion != h.version || body.EventType != h.eventType || body.Payload.RequestID != request {
		return fmt.Errorf("event envelope detached from command %s", h.meta.OperationID)
	}
	if h.eventType == "request.state_changed" && body.Payload.OperationID != h.meta.OperationID {
		return fmt.Errorf("terminal event operation identity changed")
	}
	expected := event
	expected.EntityKey = h.key
	expected.EntityVersion = h.version
	expected.CreatedAt = h.meta.At
	count := 0
	for _, row := range rows {
		if row.EventID != event.EventID {
			continue
		}
		count++
		if !reflect.DeepEqual(row, expected) {
			return fmt.Errorf("durable outbox row differs from committed command %s", h.meta.OperationID)
		}
	}
	if count != 1 {
		return fmt.Errorf("durable event %s count=%d, want one", event.EventID, count)
	}
	return nil
}
func validateClosedAttempt(c store.Closure, resource, request string, a store.Attempt, at int64) error {
	if c.ResourceKey != resource || c.RequestID != request || c.AttemptID != a.AttemptID || c.Fingerprint != a.Fingerprint || c.ClosedAt != at || c.Reason != "retrying" || c.Disposition != store.ResourceAvailable {
		return fmt.Errorf("SCH04 attemptA closure missing, foreign or changed: %+v", c)
	}
	return nil
}

func TestScheduleOutboxRejectsHollowEvidence(t *testing.T) {
	payload := json.RawMessage(`{"schemaVersion":1,"eventId":"event","entityId":"R","entityVersion":4,"eventType":"request.state_changed","occurredAt":"1970-01-01T00:00:01Z","payload":{"requestId":"R","operationId":"operation"}}`)
	event := store.OutboxEvent{EventID: "event", EventType: "request.state_changed", SchemaVersion: 1, Exchange: "relief.events", RoutingKey: "request.status.cancelled", Payload: payload}
	h := outboxHandoff{meta: store.Meta{OperationID: "operation", At: 1000}, key: store.RequestKey("R"), version: 4, eventType: event.EventType, route: event.RoutingKey, events: []store.OutboxEvent{event}}
	committed := event
	committed.EntityKey = h.key
	committed.EntityVersion = h.version
	committed.CreatedAt = h.meta.At
	result := store.Result{Outcome: store.OutcomeOK, OperationID: h.meta.OperationID, Key: h.key, Version: h.version}
	if err := validateHandoff(h, []store.OutboxEvent{committed}, result); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing-row", "duplicate-row", "changed-payload", "wrong-version", "wrong-timestamp", "wrong-operation", "missing-command-event"} {
		t.Run(name, func(t *testing.T) {
			candidate := h
			rows := []store.OutboxEvent{committed}
			proof := result
			switch name {
			case "missing-row":
				rows = nil
			case "duplicate-row":
				rows = append(rows, committed)
			case "changed-payload":
				rows[0].Payload = json.RawMessage(`{}`)
			case "wrong-version":
				rows[0].EntityVersion++
			case "wrong-timestamp":
				rows[0].CreatedAt++
			case "wrong-operation":
				proof.OperationID = "other"
			case "missing-command-event":
				candidate.events = nil
			}
			if err := validateHandoff(candidate, rows, proof); err == nil {
				t.Fatal("hollow outbox evidence was accepted")
			}
		})
	}
}

func TestSCH04ClosureRejectsMissingOrForeignFence(t *testing.T) {
	a := store.Attempt{AttemptID: "attemptA", ResourceID: "Res", Fingerprint: "fingerprintA"}
	c := store.Closure{ResourceKey: store.ResourceKey(a.ResourceID), RequestID: "R", AttemptID: a.AttemptID, Fingerprint: a.Fingerprint, Reason: "retrying", Disposition: store.ResourceAvailable, ClosedAt: 1000}
	if err := validateClosedAttempt(c, c.ResourceKey, c.RequestID, a, c.ClosedAt); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"absent", "foreign-request", "foreign-fingerprint", "foreign-attempt", "wrong-time", "wrong-disposition", "wrong-reason"} {
		t.Run(name, func(t *testing.T) {
			bad := c
			switch name {
			case "absent":
				bad = store.Closure{}
			case "foreign-request":
				bad.RequestID = "other"
			case "foreign-fingerprint":
				bad.Fingerprint = "other"
			case "foreign-attempt":
				bad.AttemptID = "attemptB"
			case "wrong-time":
				bad.ClosedAt++
			case "wrong-disposition":
				bad.Disposition = store.ResourceConsumed
			case "wrong-reason":
				bad.Reason = "other"
			}
			if err := validateClosedAttempt(bad, c.ResourceKey, c.RequestID, a, c.ClosedAt); err == nil {
				t.Fatal("missing or foreign closure was accepted")
			}
		})
	}
}

// Only this test decorator omits event persistence. The real FSM still applies
// and commits the terminal entity transition, so final-state checks alone pass.
type omitOutboxStore struct{ store.Store }

func (s omitOutboxStore) TransitionRequest(ctx context.Context, c store.TransitionRequest) (store.Result, error) {
	c.Outbox = nil
	return s.Store.TransitionRequest(ctx, c)
}

func TestScheduleOutboxDetectsCommittedEventOmission(t *testing.T) {
	t.Setenv("SCHEDULE_BACKEND", "m8") // Never run a deliberately omitted event against kind.
	fixture := Schedules[9]            // SCH10 starts with a reciprocal MATCHED claim.
	x := newExecution(t, fixture, 1, "outbox-omission-control")
	x.move(store.RequestFulfilling, nil)
	if _, err := x.matcher(omitOutboxStore{Store: x.s}).Cleanup(x.ctx, x.entities["R"]); err != nil {
		t.Fatal(err)
	}
	assertFixtureFinal(x, fixture)
	rows, err := x.s.PendingOutbox(x.ctx, store.OutboxQuery{Shard: x.requestShard, Limit: 100, Consistency: store.ConsistencyStrong})
	if err != nil {
		t.Fatal(err)
	}
	if len(x.observed) != 1 || len(rows) != 0 {
		t.Fatal("omission control did not exercise a committed missing event")
	}
	h := x.observed[0]
	result, err := x.s.Operation(x.ctx, h.key, h.meta.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateHandoff(h.outboxHandoff, rows, result); err == nil {
		t.Fatal("final fixture passed but omitted durable event was accepted")
	}
	t.Log("real FSM final fixture passed; durable event omission was rejected")
}
