package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func operationID(parts ...string) string {
	// JSON preserves field boundaries even if an entity ID contains punctuation.
	encoded, err := json.Marshal(parts)
	if err != nil {
		panic(err) // []string cannot fail JSON encoding.
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, append([]byte("reliefgrid/A1.2/"), encoded...)).String()
}

func transitionID(cmd store.TransitionRequest) string {
	return operationID("transition", cmd.Key, strconv.FormatUint(cmd.ExpectedVersion, 10),
		string(cmd.To), cmd.ExpectedAttemptID, strconv.FormatInt(cmd.At, 10), attemptIdentity(cmd.Attempt))
}

func attemptIdentity(a *store.Attempt) string {
	if a == nil {
		return ""
	}
	return operationID(a.AttemptID, a.ResourceID, a.Fingerprint)
}

func (m *Matcher) attempt(rec store.Record, resource store.Resource, at int64) (store.Attempt, error) {
	input := struct {
		RequestID       string
		RequestVersion  uint64
		Type            store.ResourceType
		RequestLocation store.Location
		Resource        store.Resource
		At              int64
		Seed            uint64
	}{rec.Request.ID, rec.Version, rec.Request.Type, rec.Request.Location, resource, at, m.cfg.Seed}
	encoded, err := json.Marshal(input)
	if err != nil {
		return store.Attempt{}, fmt.Errorf("workflow eligibility fingerprint: %w", err)
	}
	digest := sha256.Sum256(encoded)
	id := m.id()
	if id == "" {
		return store.Attempt{}, fmt.Errorf("workflow: ID source returned an empty attempt ID")
	}
	// The versioned hash envelope retains the proposer timestamp and selection
	// seed. UpdatedAt can later change during RetryWorkflow, so it cannot be
	// used to reconstruct an unresolved claim after a restart.
	return store.Attempt{
		AttemptID: id, ResourceID: resource.ID,
		Fingerprint: fmt.Sprintf("rg1:%d:%d:%x", at, m.cfg.Seed, digest),
	}, nil
}

func claimTime(rec store.Record) (int64, error) {
	fingerprint := rec.Request.CurrentAttempt.Fingerprint
	if !strings.HasPrefix(fingerprint, "rg1:") {
		// Unversioned imported intents use the immutable creation timestamp.
		// Every intent produced here uses rg1 and carries its actual proposal time.
		return rec.Request.CreatedAt, nil
	}
	parts := strings.Split(fingerprint, ":")
	if len(parts) != 4 {
		return 0, fmt.Errorf("workflow: malformed rg1 intent fingerprint")
	}
	at, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("workflow intent timestamp: %w", err)
	}
	if _, err := strconv.ParseUint(parts[2], 10, 64); err != nil {
		return 0, fmt.Errorf("workflow intent seed: %w", err)
	}
	digest, err := hex.DecodeString(parts[3])
	if err != nil || len(digest) != sha256.Size {
		return 0, fmt.Errorf("workflow: malformed rg1 eligibility digest")
	}
	return at, nil
}

type eventEnvelope struct {
	SchemaVersion int             `json:"schemaVersion"`
	EventID       string          `json:"eventId"`
	EntityID      string          `json:"entityId"`
	EntityVersion uint64          `json:"entityVersion"`
	EventType     string          `json:"eventType"`
	OccurredAt    string          `json:"occurredAt"`
	Traceparent   string          `json:"traceparent"`
	Payload       json.RawMessage `json:"payload"`
}

func events(rec store.Record, cmd store.TransitionRequest, eventType string, a *store.Attempt) ([]store.OutboxEvent, error) {
	var payload any
	if a != nil {
		payload = struct {
			RequestID   string `json:"requestId"`
			AttemptID   string `json:"attemptId"`
			ResourceID  string `json:"resourceId"`
			Fingerprint string `json:"fingerprint"`
		}{rec.Request.ID, a.AttemptID, a.ResourceID, a.Fingerprint}
	} else {
		payload = struct {
			RequestID          string `json:"requestId"`
			WorkflowGeneration uint64 `json:"workflowGeneration"`
			NextAttemptAt      int64  `json:"nextAttemptAt"`
			Urgency            int    `json:"urgency"`
		}{rec.Request.ID, rec.Request.WorkflowGeneration, cmd.NextAttemptAt, rec.Request.Urgency}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("workflow event payload: %w", err)
	}
	eventID := operationID("event", cmd.OperationID, eventType)
	trace := sha256.Sum256([]byte(cmd.OperationID))
	encoded, err := json.Marshal(eventEnvelope{
		SchemaVersion: 1, EventID: eventID, EntityID: rec.Request.ID, EntityVersion: rec.Version + 1,
		EventType: eventType, OccurredAt: time.UnixMilli(cmd.At).UTC().Format(time.RFC3339Nano),
		Traceparent: fmt.Sprintf("00-%x-%x-00", trace[:16], trace[16:24]), Payload: body,
	})
	if err != nil {
		return nil, fmt.Errorf("workflow event envelope: %w", err)
	}
	exchange, route := "relief.events", eventType
	if eventType == "request.retry" {
		exchange, route = "relief.retry", "delay.5s"
		if cmd.NextAttemptAt-cmd.At >= 120000 {
			route = "delay.120s"
		} else if cmd.NextAttemptAt-cmd.At >= 30000 {
			route = "delay.30s"
		}
	}
	return []store.OutboxEvent{{
		EventID: eventID, EventType: eventType, SchemaVersion: 1,
		Exchange: exchange, RoutingKey: route, Payload: encoded,
	}}, nil
}
