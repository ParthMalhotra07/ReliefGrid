package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Cleanup resumes an already committed CANCELLING/FULFILLING request. The API
// chooses and persists the disposition before this method can close a claim.
// This is bounded protocol work, not a background reconciliation loop.
func (m *Matcher) Cleanup(ctx context.Context, requestID string) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WorkTimeout)
	defer cancel()
	rec, err := m.read(ctx, store.RequestKey(requestID))
	if err != nil {
		return Result{}, err
	}
	return m.cleanup(ctx, rec)
}

func cleanupTerminal(rec store.Record) (store.RequestStatus, error) {
	if rec.Request == nil {
		return "", fmt.Errorf("workflow: cleanup requires a request")
	}
	switch rec.Request.Status {
	case store.RequestCancelling, store.RequestCancelled:
		return store.RequestCancelled, nil
	case store.RequestFulfilling, store.RequestFulfilled:
		return store.RequestFulfilled, nil
	default:
		return "", ErrNeedsRecovery
	}
}

func (m *Matcher) cleanup(ctx context.Context, rec store.Record) (Result, error) {
	target, err := cleanupTerminal(rec)
	if err != nil {
		return Result{}, err
	}
	if rec.Request.Status == target {
		return Result{Outcome: string(target)}, nil
	}
	a := rec.Request.CurrentAttempt
	if a == nil || a.AttemptID == "" || a.ResourceID == "" || a.Fingerprint == "" {
		return Result{}, fmt.Errorf("workflow: cleanup requires a complete recorded intent")
	}
	switch rec.Request.Disposition {
	case store.ResourceAvailable, store.ResourceConsumed, store.ResourceOffline:
	default:
		return Result{}, fmt.Errorf("workflow: cleanup requires a committed disposition")
	}
	closed, err := m.closure(ctx, rec)
	if err != nil {
		return Result{}, err
	}
	if !closed {
		// This committed request version supplies the command timestamp. Moving
		// the worker's clock or restarting cannot change the payload under its ID.
		// A RetryWorkflow that changes the version starts a new conditional
		// recovery proposal, still bound to the same permanent attempt closure
		// and already-chosen disposition. No eligibility is recomputed here.
		cmd := store.CloseAttempt{
			Meta: store.Meta{
				OperationID: operationID("cleanup-close", rec.Key, a.AttemptID, string(rec.Request.Status),
					strconv.FormatUint(rec.Version, 10), strconv.FormatInt(rec.UpdatedAt, 10), string(rec.Request.Disposition)),
				At: rec.UpdatedAt,
			},
			ResourceKey: store.ResourceKey(a.ResourceID), RequestID: rec.Request.ID,
			Attempt: *a, Disposition: rec.Request.Disposition, Reason: strings.ToLower(string(target)),
		}
		result, err := m.mutate(ctx, func(ctx context.Context) (store.Result, error) {
			return m.cfg.Store.CloseAttempt(ctx, cmd)
		})
		if err != nil {
			return Result{}, err
		}
		if result.Outcome != store.OutcomeOK {
			return Result{}, fmt.Errorf("workflow close rejected with %s: %s", result.Outcome, result.Reason)
		}
		// A stored historical OK cannot replace proof of this exact fence.
		closed, err = m.closure(ctx, rec)
		if err != nil {
			return Result{}, err
		}
		if !closed {
			return Result{}, ErrOwnershipUnknown
		}
	}
	cmd := transition(rec, target, rec.UpdatedAt)
	cmd.OperationID = transitionID(cmd)
	cmd.Outbox, err = cleanupEvent(rec, cmd)
	if err != nil {
		return Result{}, err
	}
	current, err := m.applyTransition(ctx, rec, cmd)
	if err != nil && !errors.Is(err, ErrConcurrentChange) {
		return Result{}, err
	}
	// A competing helper may already have committed the same terminal state.
	// Current state is authoritative; the loser never repeats a release/event.
	if current.Request != nil && current.Request.Status == target {
		return Result{Outcome: string(target)}, nil
	}
	return Result{}, ErrConcurrentChange
}

func cleanupEvent(rec store.Record, cmd store.TransitionRequest) ([]store.OutboxEvent, error) {
	// Use the frozen request.state_changed schema and the existing API routing
	// convention for the plan's cancelled/fulfilled notification semantics.
	payload, err := json.Marshal(struct {
		RequestID      string              `json:"requestId"`
		PreviousStatus store.RequestStatus `json:"previousStatus"`
		NewStatus      store.RequestStatus `json:"newStatus"`
		OperationID    string              `json:"operationId"`
		AttemptID      string              `json:"attemptId"`
	}{rec.Request.ID, rec.Request.Status, cmd.To, cmd.OperationID, cmd.ExpectedAttemptID})
	if err != nil {
		return nil, fmt.Errorf("workflow cleanup event payload: %w", err)
	}
	eventID := operationID("event", cmd.OperationID, "request.state_changed")
	trace := sha256.Sum256([]byte(cmd.OperationID))
	body, err := json.Marshal(eventEnvelope{
		SchemaVersion: 1, EventID: eventID, EntityID: rec.Request.ID, EntityVersion: rec.Version + 1,
		EventType: "request.state_changed", OccurredAt: time.UnixMilli(cmd.At).UTC().Format(time.RFC3339Nano),
		Traceparent: fmt.Sprintf("00-%x-%x-00", trace[:16], trace[16:24]), Payload: payload,
	})
	if err != nil {
		return nil, fmt.Errorf("workflow cleanup event envelope: %w", err)
	}
	return []store.OutboxEvent{{EventID: eventID, EventType: "request.state_changed", SchemaVersion: 1,
		Exchange: "relief.events", RoutingKey: "request.status." + strings.ToLower(string(cmd.To)), Payload: body}}, nil
}
