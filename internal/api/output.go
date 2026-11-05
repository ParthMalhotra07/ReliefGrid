package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api/gen"
	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Generated types mark read-only fields omitempty. The frozen public contract
// still requires their null/zero values, so explicit outer fields preserve them.
type requestResponse struct {
	gen.RequestResponse
	AssignedResourceID *uuid.UUID `json:"assignedResourceId"`
	NextAttemptAt      *time.Time `json:"nextAttemptAt"`
	RetryCount         int        `json:"retryCount"`
	LastError          string     `json:"lastError"`
}

type claimResponse struct {
	RequestID uuid.UUID `json:"requestId"`
	AttemptID uuid.UUID `json:"attemptId"`
}

type resourceResponse struct {
	gen.ResourceResponse
	ActiveClaim      *claimResponse `json:"activeClaim"`
	LocationRevision uint64         `json:"locationRevision"`
}

func urgency(value int) gen.Urgency {
	switch value {
	case 2:
		return gen.Urgency("HIGH")
	case 3:
		return gen.Urgency("CRITICAL")
	default:
		return gen.Urgency("NORMAL")
	}
}

func details(value json.RawMessage) string {
	var text string
	if len(value) == 0 || string(value) == "null" {
		return ""
	}
	if err := json.Unmarshal(value, &text); err != nil {
		// Older internal fixtures may have structured details. Preserve their
		// content as text without exposing internal workflow fields.
		return string(value)
	}
	return text
}

func renderRequest(rec store.Record) (requestResponse, error) {
	if rec.Request == nil {
		return requestResponse{}, fmt.Errorf("api: expected request record")
	}
	r := rec.Request
	id, err := uuid.Parse(r.ID)
	if err != nil {
		return requestResponse{}, fmt.Errorf("stored request ID: %w", err)
	}
	response := requestResponse{RequestResponse: gen.RequestResponse{
		RequestId: id, RequestType: gen.RequestType(r.Type), Status: gen.RequestStatus(r.Status),
		Location: gen.Location{Lat: r.Location.Lat, Lng: r.Location.Lng},
		Details:  details(r.Details), PeopleAffected: int32(r.PeopleAffected), Urgency: urgency(r.Urgency),
		Version: fmt.Sprintf("v%d", rec.Version), CreatedAt: time.UnixMilli(r.CreatedAt).UTC(),
		UpdatedAt: time.UnixMilli(r.UpdatedAt).UTC(),
	}, RetryCount: r.RetryCount, LastError: r.LastError}
	if r.AssignedResourceID != "" && r.Status != store.RequestCancelled && r.Status != store.RequestFulfilled {
		assigned, err := uuid.Parse(r.AssignedResourceID)
		if err != nil {
			return requestResponse{}, fmt.Errorf("stored assignment ID: %w", err)
		}
		response.AssignedResourceID = &assigned
	}
	if r.NextAttemptAt != 0 {
		due := time.UnixMilli(r.NextAttemptAt).UTC()
		response.NextAttemptAt = &due
	}
	return response, nil
}

func renderResource(rec store.Record) (resourceResponse, error) {
	if rec.Resource == nil {
		return resourceResponse{}, fmt.Errorf("api: expected resource record")
	}
	r := rec.Resource
	id, err := uuid.Parse(r.ID)
	if err != nil {
		return resourceResponse{}, fmt.Errorf("stored resource ID: %w", err)
	}
	response := resourceResponse{ResourceResponse: gen.ResourceResponse{
		ResourceId: id, ResourceType: gen.ResourceType(r.Type), Name: r.Name, Status: gen.ResourceStatus(r.Status),
		Location: gen.Location{Lat: r.Location.Lat, Lng: r.Location.Lng}, Reusable: r.Reusable,
		WindowStart: time.UnixMilli(r.AvailableFrom).UTC(), WindowEnd: time.UnixMilli(r.AvailableUntil).UTC(),
		CreatedAt: time.UnixMilli(r.CreatedAt).UTC(), UpdatedAt: time.UnixMilli(r.UpdatedAt).UTC(),
		Version: fmt.Sprintf("v%d", rec.Version),
	}, LocationRevision: r.LocationRevision}
	if r.ActiveClaim != nil {
		requestID, err := uuid.Parse(r.ActiveClaim.RequestID)
		if err != nil {
			return resourceResponse{}, fmt.Errorf("stored claim request ID: %w", err)
		}
		attemptID, err := uuid.Parse(r.ActiveClaim.AttemptID)
		if err != nil {
			return resourceResponse{}, fmt.Errorf("stored claim attempt ID: %w", err)
		}
		response.ActiveClaim = &claimResponse{RequestID: requestID, AttemptID: attemptID}
	}
	return response, nil
}

type eventEnvelope struct {
	SchemaVersion int    `json:"schemaVersion"`
	EventID       string `json:"eventId"`
	EntityID      string `json:"entityId"`
	EntityVersion uint64 `json:"entityVersion"`
	EventType     string `json:"eventType"`
	OccurredAt    string `json:"occurredAt"`
	Traceparent   string `json:"traceparent"`
	Payload       any    `json:"payload"`
}

// event is the outbox row for one committed change. Its traceparent is the
// request's span (obs.Handler continues the caller's trace, S4.4); with no
// span or remote parent in ctx it is derived from the operationId, as before.
func event(ctx context.Context, eventType, id string, version uint64, meta store.Meta, payload any) (store.OutboxEvent, error) {
	trace := obs.Traceparent(ctx, meta.OperationID)
	eventID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("reliefgrid/api/event/"+meta.OperationID+"/"+eventType)).String()
	body, err := json.Marshal(eventEnvelope{
		SchemaVersion: 1, EventID: eventID, EntityID: id, EntityVersion: version,
		EventType: eventType, OccurredAt: time.UnixMilli(meta.At).UTC().Format(time.RFC3339Nano), Traceparent: trace, Payload: payload,
	})
	if err != nil {
		return store.OutboxEvent{}, fmt.Errorf("API outbox envelope: %w", err)
	}
	exchange, route := "relief.events", eventType
	switch eventType {
	case "request.created":
		exchange = "relief.requests"
		route += "." + strings.ToLower(payload.(map[string]any)["requestType"].(string))
	case "resource.registered":
		route += "." + strings.ToLower(payload.(map[string]any)["resourceType"].(string))
	case "request.state_changed":
		route = "request.status." + strings.ToLower(payload.(map[string]any)["newStatus"].(string))
	}
	return store.OutboxEvent{EventID: eventID, EventType: eventType, SchemaVersion: 1,
		Exchange: exchange, RoutingKey: route, Payload: body}, nil
}
