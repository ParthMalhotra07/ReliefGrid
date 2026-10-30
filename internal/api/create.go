package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// A durable intent freezes server metadata before creating the domain record.
// Both steps are committed commands. A crashed API can resume either step on
// another instance. The immutable response is replayed only after CreateEntity
// returns its committed OK result, never from the intent alone.
type creationIntent struct {
	SchemaVersion int                `json:"schemaVersion"`
	PayloadHash   string             `json:"payloadHash"`
	Command       store.CreateEntity `json:"command"`
	Response      json.RawMessage    `json:"response"`
	Location      string             `json:"location"`
}

func (s *Server) createRequest(w http.ResponseWriter, r *http.Request) error {
	data, err := requestData(w, r)
	if err != nil {
		return err
	}
	return s.create(w, r, "requests", store.CreateEntity{Request: &data})
}

func (s *Server) createResource(w http.ResponseWriter, r *http.Request) error {
	data, err := resourceData(w, r)
	if err != nil {
		return err
	}
	return s.create(w, r, "resources", store.CreateEntity{Resource: &data})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request, operation string, command store.CreateEntity) error {
	keys := r.Header.Values("Idempotency-Key")
	key := r.Header.Get("Idempotency-Key")
	if len(keys) > 1 {
		return invalid("Idempotency-Key", "must be a single UUID")
	}
	if len(keys) == 1 {
		parsed, err := uuid.Parse(key)
		if err != nil || len(key) != 36 {
			return invalid("Idempotency-Key", "must be a 36-character UUID")
		}
		key = parsed.String()
	}
	// This body contains only normalized client data and fixed domain defaults:
	// IDs, timestamps, trace context and outbox rows have not been added yet.
	canonical, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("canonical creation payload: %w", err)
	}
	payloadHash := fmt.Sprintf("%x", sha256.Sum256(canonical))
	id, operationID, intentKey := uuid.NewString(), uuid.NewString(), ""
	if len(keys) == 1 {
		namespace, err := json.Marshal([]string{"reliefgrid/api/create/v1", s.cfg.CallerNamespace, operation, key})
		if err != nil {
			return fmt.Errorf("creation namespace: %w", err)
		}
		id = uuid.NewSHA1(uuid.NameSpaceURL, namespace).String()
		operationID = uuid.NewSHA1(uuid.NameSpaceOID, namespace).String()
		intentKey = "api-create:" + id
	}
	command.Meta = store.Meta{OperationID: operationID, At: s.cfg.Now().UnixMilli()}
	if command.At <= 0 {
		return fmt.Errorf("api: invalid proposer timestamp")
	}
	var response any
	var payload map[string]any
	eventType := "request.created"
	if command.Request != nil {
		data := command.Request
		data.ID, data.CreatedAt, data.UpdatedAt = id, command.At, command.At
		command.Key = store.RequestKey(id)
		response, err = renderRequest(store.Record{Version: 1, Request: data})
		payload = map[string]any{"requestId": id, "requestType": string(data.Type), "status": "PENDING",
			"location": data.Location, "details": details(data.Details), "peopleAffected": data.PeopleAffected, "urgency": urgency(data.Urgency)}
	} else {
		data := command.Resource
		data.ID, data.CreatedAt, data.UpdatedAt = id, command.At, command.At
		command.Key = store.ResourceKey(id)
		response, err = renderResource(store.Record{Version: 1, Resource: data})
		eventType = "resource.registered"
		payload = map[string]any{"resourceId": id, "resourceType": string(data.Type), "name": data.Name, "status": "AVAILABLE",
			"location": data.Location, "windowStart": time.UnixMilli(data.AvailableFrom).UTC(),
			"windowEnd": time.UnixMilli(data.AvailableUntil).UTC(), "reusable": data.Reusable}
	}
	if err != nil {
		return err
	}
	outbox, err := event(eventType, id, 1, command.Meta, r.Header.Get("traceparent"), payload)
	if err != nil {
		return err
	}
	command.Outbox = []store.OutboxEvent{outbox}
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("creation response: %w", err)
	}
	intent := creationIntent{SchemaVersion: 1, PayloadHash: payloadHash, Command: command,
		Response: encoded, Location: "/api/v1/" + operation + "/" + id}
	if intentKey != "" {
		intent, err = s.reserve(r.Context(), intentKey, intent)
		if err != nil {
			return err
		}
	}
	result, err := invoke(r.Context(), s.cfg.CallTimeout, func(ctx context.Context) (store.Result, error) {
		return s.cfg.Store.CreateEntity(ctx, intent.Command)
	})
	if err != nil {
		return remoteError(err, true)
	}
	if intentKey != "" && (result.Outcome == store.OutcomeExists || result.Outcome == store.OutcomeOperationIDReuse) {
		return issue(409, "CONFLICT_IDEMPOTENCY_KEY", "creation identity has a different committed payload")
	}
	if err := committed(result); err != nil {
		return err
	}
	w.Header().Set("Location", intent.Location)
	writeJSON(w, 201, intent.Response)
	return nil
}

// reserve stores immutable idempotency metadata. api-create: records must not
// be deleted or independently expired: they retain the exact command and the
// original response needed after edits, crashes or lost replies. A tombstone
// prevents create-if-absent reuse; deleting one requires an explicit migration.
func (s *Server) reserve(ctx context.Context, key string, proposed creationIntent) (creationIntent, error) {
	rec, err := s.read(ctx, key)
	var p *problem
	if errors.As(err, &p) && p.code == "NOT_FOUND" {
		body, encodeErr := json.Marshal(proposed)
		if encodeErr != nil {
			return creationIntent{}, fmt.Errorf("durable creation intent: %w", encodeErr)
		}
		command := store.CompareAndSet{Meta: store.Meta{OperationID: uuid.NewString(), At: proposed.Command.At}, Key: key, Value: body}
		result, writeErr := invoke(ctx, s.cfg.CallTimeout, func(ctx context.Context) (store.Result, error) {
			return s.cfg.Store.CompareAndSet(ctx, command)
		})
		if writeErr != nil {
			return creationIntent{}, remoteError(writeErr, true)
		}
		if result.Outcome == store.OutcomeOK {
			return proposed, nil
		}
		if result.Outcome != store.OutcomeVersionConflict {
			return creationIntent{}, committed(result)
		}
		rec, err = s.read(ctx, key)
	}
	if err != nil {
		return creationIntent{}, err
	}
	var existing creationIntent
	if rec.Kind != store.KindKV || rec.Version != 1 || json.Unmarshal(rec.Value, &existing) != nil || existing.SchemaVersion != 1 ||
		existing.Command.Key != proposed.Command.Key || existing.Command.OperationID != proposed.Command.OperationID ||
		existing.Location != proposed.Location || !json.Valid(existing.Response) || existing.Command.At <= 0 {
		return creationIntent{}, unavailable("STORAGE_UNAVAILABLE", false)
	}
	if existing.PayloadHash != proposed.PayloadHash {
		return creationIntent{}, issue(409, "CONFLICT_IDEMPOTENCY_KEY", "idempotency key has a different payload")
	}
	return existing, nil
}
