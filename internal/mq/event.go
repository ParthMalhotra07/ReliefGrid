package mq

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// The byte-for-byte copy is checked against the frozen contract in tests.
//
//go:embed schemas/request_created.json
var requestCreatedJSON []byte

var requestCreatedSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	var document any
	if err := json.Unmarshal(requestCreatedJSON, &document); err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	const location = "https://reliefgrid.internal/events/request_created.json"
	if err := c.AddResource(location, document); err != nil {
		return nil, err
	}
	return c.Compile(location)
})

type event struct {
	SchemaVersion int             `json:"schemaVersion"`
	EventID       string          `json:"eventId"`
	EntityID      string          `json:"entityId"`
	EntityVersion uint64          `json:"entityVersion"`
	EventType     string          `json:"eventType"`
	OccurredAt    string          `json:"occurredAt"`
	Traceparent   string          `json:"traceparent"`
	Payload       json.RawMessage `json:"payload"`
}

var traceparent = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// parseEvent validates transport identity and supported version before any
// state read. Payload fields are hints, never inputs to an ownership decision.
func parseEvent(body []byte) (event, error) {
	var e event
	if len(body) > 256*1024 {
		return e, fmt.Errorf("event exceeds 256 KiB")
	}
	if err := uniqueJSON(body); err != nil {
		return e, err
	}
	if err := exactFields(body, "schemaVersion", "eventId", "entityId", "entityVersion", "eventType", "occurredAt", "traceparent", "payload"); err != nil {
		return e, err
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return e, fmt.Errorf("event JSON: %w", err)
	}
	if e.SchemaVersion != 1 || !validUUID(e.EventID) || !validUUID(e.EntityID) || e.EntityVersion < 1 || !traceparent.MatchString(e.Traceparent) {
		return e, fmt.Errorf("invalid event envelope")
	}
	if _, err := time.Parse(time.RFC3339Nano, e.OccurredAt); err != nil {
		return e, fmt.Errorf("event occurredAt: %w", err)
	}
	var payload struct {
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		return e, fmt.Errorf("event payload: %w", err)
	}
	if payload.RequestID != e.EntityID {
		return e, fmt.Errorf("event payload requestId differs from entityId")
	}
	switch e.EventType {
	case "request.created":
		schema, err := requestCreatedSchema()
		if err != nil {
			return e, fmt.Errorf("compile embedded event schema: %w", err)
		}
		var value any
		if err := json.Unmarshal(body, &value); err != nil {
			return e, err
		}
		if err := schema.Validate(value); err != nil {
			return e, fmt.Errorf("request.created schema: %w", err)
		}
	case "request.retry":
		// A1.2's committed internal retry envelope (not a new public contract).
		if err := exactFields(e.Payload, "requestId", "workflowGeneration", "nextAttemptAt", "urgency"); err != nil {
			return e, err
		}
		var retry struct {
			RequestID          string  `json:"requestId"`
			WorkflowGeneration *uint64 `json:"workflowGeneration"`
			NextAttemptAt      *int64  `json:"nextAttemptAt"`
			Urgency            *int    `json:"urgency"`
		}
		d := json.NewDecoder(bytes.NewReader(e.Payload))
		d.DisallowUnknownFields()
		if err := d.Decode(&retry); err != nil {
			return e, fmt.Errorf("retry payload: %w", err)
		}
		if retry.WorkflowGeneration == nil || retry.NextAttemptAt == nil || *retry.NextAttemptAt <= 0 || retry.Urgency == nil || *retry.Urgency < 1 || *retry.Urgency > 3 {
			return e, fmt.Errorf("invalid retry payload")
		}
	default:
		return e, fmt.Errorf("unsupported work event type %q", e.EventType)
	}
	return e, nil
}

func validUUID(id string) bool {
	if len(id) != 36 {
		return false
	}
	_, err := uuid.Parse(id)
	return err == nil
}

// DisallowUnknownFields still accepts case aliases for Go struct fields.
// Require the precise versioned wire keys before decoding their values.
func exactFields(body []byte, names ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return err
	}
	if len(fields) != len(names) {
		return fmt.Errorf("event has missing or unknown properties")
	}
	for _, name := range names {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("event property %s is required", name)
		}
	}
	return nil
}

// Reject duplicate/case-aliased properties and trailing documents rather than
// allowing encoding/json's last-field-wins behavior to choose an identity.
func uniqueJSON(body []byte) error {
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if err := jsonValue(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing event JSON")
	}
	return nil
}

func jsonValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return fmt.Errorf("object property is not a string")
			}
			if seen[name] {
				return fmt.Errorf("duplicate event property %q", name)
			}
			seen[name] = true
			if err := jsonValue(d); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := jsonValue(d); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected event delimiter")
	}
	_, err = d.Token()
	return err
}
