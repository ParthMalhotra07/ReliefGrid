package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
)

func (s *Server) getRequest(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r, "requestId")
	if err != nil {
		return err
	}
	rec, err := s.read(r.Context(), store.RequestKey(id))
	if err != nil {
		return err
	}
	response, err := renderRequest(rec)
	if err != nil {
		return err
	}
	writeJSON(w, 200, response)
	return nil
}

func (s *Server) updateRequest(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r, "requestId")
	if err != nil {
		return err
	}
	var in struct {
		Status string `json:"status"`
	}
	if err := decodeBody(w, r, &in, "status"); err != nil {
		return err
	}
	to := store.RequestStatus(in.Status)
	if to != store.RequestInProgress && to != store.RequestCancelled && to != store.RequestFulfilled {
		return invalid("status", "must be IN_PROGRESS, CANCELLED or FULFILLED")
	}
	rec, err := s.read(r.Context(), store.RequestKey(id))
	if err != nil {
		return err
	}
	if rec.Request == nil {
		return issue(404, "NOT_FOUND", "request not found")
	}
	data := *rec.Request
	if data.Status == to || (to == store.RequestCancelled && data.Status == store.RequestCancelling) ||
		(to == store.RequestFulfilled && data.Status == store.RequestFulfilling) {
		return respondRequest(w, rec)
	}
	command := store.TransitionRequest{
		Meta: store.Meta{OperationID: uuid.NewString(), At: s.cfg.Now().UnixMilli()},
		Key:  rec.Key, ExpectedVersion: rec.Version, ExpectedStatus: data.Status, To: to,
	}
	if data.CurrentAttempt != nil {
		command.ExpectedAttemptID = data.CurrentAttempt.AttemptID
	}
	if to == store.RequestCancelled && (data.CurrentAttempt != nil || data.AssignedResourceID != "") {
		command.To = store.RequestCancelling
	}
	if to == store.RequestFulfilled {
		command.To = store.RequestFulfilling
	}
	if command.To == store.RequestCancelling || command.To == store.RequestFulfilling {
		if data.CurrentAttempt == nil {
			return issue(409, "DOMAIN_CONFLICT", "cleanup requires a recorded attempt")
		}
		resource, err := s.read(r.Context(), store.ResourceKey(data.CurrentAttempt.ResourceID))
		if err != nil {
			return err
		}
		if resource.Resource == nil {
			return issue(409, "DOMAIN_CONFLICT", "attempt resource is not present")
		}
		command.Disposition = store.ResourceAvailable
		if to == store.RequestFulfilled && !resource.Resource.Reusable {
			command.Disposition = store.ResourceConsumed
		} else if command.At >= resource.Resource.AvailableUntil {
			command.Disposition = store.ResourceOffline
		}
		if data.Disposition != "" {
			command.Disposition = data.Disposition
		}
	}
	outbox, err := event("request.state_changed", id, rec.Version+1, command.Meta, r.Header.Get("traceparent"), map[string]any{
		"requestId": id, "previousStatus": string(data.Status), "newStatus": string(command.To), "operationId": command.OperationID,
	})
	if err != nil {
		return err
	}
	command.Outbox = []store.OutboxEvent{outbox}
	result, err := invoke(r.Context(), s.cfg.CallTimeout, func(ctx context.Context) (store.Result, error) {
		return s.cfg.Store.TransitionRequest(ctx, command)
	})
	if err != nil {
		return remoteError(err, true)
	}
	if err := committed(result); err != nil {
		return err
	}
	// Report this committed transition, not a later unrelated mutation. No claim
	// is released here: A2.1 completes closure before the terminal state.
	data.Status, data.UpdatedAt = command.To, command.At
	if command.Disposition != "" {
		data.Disposition = command.Disposition
	}
	rec.Request, rec.Version = &data, result.Version
	return respondRequest(w, rec)
}

func respondRequest(w http.ResponseWriter, rec store.Record) error {
	response, err := renderRequest(rec)
	if err != nil {
		return err
	}
	writeJSON(w, 200, response)
	return nil
}

func (s *Server) updateResource(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r, "resourceId")
	if err != nil {
		return err
	}
	var in struct {
		Status *string `json:"status"`
		From   *string `json:"windowStart"`
		Until  *string `json:"windowEnd"`
	}
	if err := decodeBody(w, r, &in, "status", "windowStart", "windowEnd"); err != nil {
		return err
	}
	if in.Status == nil && in.From == nil && in.Until == nil {
		return invalid("body", "at least one update field is required")
	}
	if in.Status != nil {
		switch store.ResourceStatus(*in.Status) {
		case store.ResourceAssigned, store.ResourceConsumed:
			return issue(409, "DOMAIN_CONFLICT", "claims and cleanup control this status")
		case store.ResourceAvailable, store.ResourceOffline:
		default:
			return invalid("status", "unknown resource status")
		}
	}
	rec, err := s.read(r.Context(), store.ResourceKey(id))
	if err != nil {
		return err
	}
	if rec.Resource == nil {
		return issue(404, "NOT_FOUND", "resource not found")
	}
	data := *rec.Resource
	if data.Status == store.ResourceAssigned || data.ActiveClaim != nil {
		return issue(409, "DOMAIN_CONFLICT", "an assigned resource cannot be publicly updated")
	}
	command := store.TransitionResource{Meta: store.Meta{OperationID: uuid.NewString(), At: s.cfg.Now().UnixMilli()}, Key: rec.Key, ExpectedVersion: rec.Version}
	if in.Status != nil {
		command.Status = store.ResourceStatus(*in.Status)
	}
	if in.From != nil || in.Until != nil {
		window := store.Window{From: data.AvailableFrom, Until: data.AvailableUntil}
		if in.From != nil {
			window.From, err = utcTime(*in.From)
			if err != nil {
				return err
			}
		}
		if in.Until != nil {
			window.Until, err = utcTime(*in.Until)
			if err != nil {
				return err
			}
		}
		if window.From >= window.Until {
			return invalid("window", "windowStart must precede windowEnd")
		}
		command.Availability = &window
	}
	newStatus := data.Status
	if command.Status != "" {
		newStatus = command.Status
	}
	outbox, err := event("resource.state_changed", id, rec.Version+1, command.Meta, r.Header.Get("traceparent"), map[string]any{
		"resourceId": id, "previousStatus": string(data.Status), "newStatus": string(newStatus), "operationId": command.OperationID,
	})
	if err != nil {
		return err
	}
	command.Outbox = []store.OutboxEvent{outbox}
	result, err := invoke(r.Context(), s.cfg.CallTimeout, func(ctx context.Context) (store.Result, error) {
		return s.cfg.Store.TransitionResource(ctx, command)
	})
	if err != nil {
		return remoteError(err, true)
	}
	if err := committed(result); err != nil {
		return err
	}
	data.Status, data.UpdatedAt = newStatus, command.At
	if command.Availability != nil {
		data.AvailableFrom, data.AvailableUntil = command.Availability.From, command.Availability.Until
	}
	rec.Resource, rec.Version = &data, result.Version
	response, err := renderResource(rec)
	if err != nil {
		return err
	}
	writeJSON(w, 200, response)
	return nil
}

func (s *Server) matchRequest(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r, "requestId")
	if err != nil {
		return err
	}
	rec, err := s.read(r.Context(), store.RequestKey(id))
	if err != nil {
		return err
	}
	if rec.Request == nil {
		return issue(404, "NOT_FOUND", "request not found")
	}
	if rec.Request.Status == store.RequestCancelled || rec.Request.Status == store.RequestFulfilled {
		return issue(409, "DOMAIN_CONFLICT", "a terminal request cannot be matched")
	}
	result, matchErr := s.matcher.Match(r.Context(), id)
	if matchErr != nil && !errors.Is(matchErr, workflow.ErrNeedsRecovery) && !errors.Is(matchErr, workflow.ErrWorkBudget) {
		if errors.Is(matchErr, workflow.ErrIncompleteScan) || errors.Is(matchErr, workflow.ErrConcurrentChange) {
			return unavailable("STORAGE_UNAVAILABLE", false)
		}
		return remoteError(matchErr, errors.Is(matchErr, store.ErrUnknownOutcome) || errors.Is(matchErr, workflow.ErrOwnershipUnknown))
	}
	rec, err = s.read(r.Context(), store.RequestKey(id))
	if err != nil {
		return err
	}
	if rec.Request == nil {
		return unavailable("STORAGE_UNAVAILABLE", false)
	}
	if rec.Request.Status == store.RequestCancelled || rec.Request.Status == store.RequestFulfilled {
		return issue(409, "DOMAIN_CONFLICT", "request became terminal while matching")
	}
	status, outcome := 202, "IN_PROGRESS"
	if result.Outcome == "NO_CANDIDATE" && rec.Request.Status == store.RequestWaiting {
		status, outcome = 200, "NO_CANDIDATE"
	}
	if status == 202 {
		if rec.Request.Status == store.RequestPending {
			return unavailable("STORAGE_UNAVAILABLE", false)
		}
		w.Header().Set("Location", "/api/v1/requests/"+id)
	}
	response, err := renderRequest(rec)
	if err != nil {
		return err
	}
	// A record read after Match may have progressed, so return an existing match
	// whenever current state has one, including a concurrently completed retry.
	if rec.Request.Status == store.RequestMatched || rec.Request.Status == store.RequestInProgress {
		status, outcome = 200, "MATCHED"
	}
	writeJSON(w, status, struct {
		Outcome string          `json:"outcome"`
		Request requestResponse `json:"request"`
	}{Outcome: outcome, Request: response})
	return nil
}
