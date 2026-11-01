package workflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func retryDelay(count int) int64 {
	if count <= 0 {
		return 5000
	}
	if count == 1 {
		return 30000
	}
	return 120000
}

func workState(status store.RequestStatus) bool {
	switch status {
	case store.RequestPending, store.RequestWaiting, store.RequestMatching, store.RequestRetrying, store.RequestCancelling, store.RequestFulfilling:
		return true
	}
	return false
}

func retryReason(cause error) string {
	switch {
	case errors.Is(cause, store.ErrUnknownOutcome):
		return "OUTCOME_UNKNOWN"
	case errors.Is(cause, ErrIncompleteScan):
		return "INCOMPLETE_SCAN"
	case errors.Is(cause, ErrConcurrentChange):
		return "CONCURRENT_CHANGE"
	case errors.Is(cause, ErrNeedsRecovery), errors.Is(cause, ErrOwnershipUnknown):
		return "RECOVERY_PENDING"
	default:
		return "WORKFLOW_RETRY"
	}
}

// Handle is a consumer's durable boundary: a final/obsolete/deferred result, or
// a committed state-preserving retry handoff. A failed handoff stays an error.
func (m *Matcher) Handle(ctx context.Context, requestID string) error {
	_, err := m.Match(ctx, requestID)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return err
	}
	if _, retryErr := m.ScheduleRetry(ctx, requestID, err); retryErr != nil {
		return errors.Join(err, retryErr)
	}
	return nil
}

// ScheduleRetry retains the current status, attempt and disposition. It never
// interprets an unknown claim as rejection or clears an intent to enter WAITING.
func (m *Matcher) ScheduleRetry(ctx context.Context, requestID string, cause error) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WorkTimeout)
	defer cancel()
	rec, err := m.read(ctx, store.RequestKey(requestID))
	if err != nil {
		return Result{}, err
	}
	if rec.Request == nil {
		return Result{}, fmt.Errorf("workflow: retry requires a request")
	}
	if !workState(rec.Request.Status) {
		return m.obsolete(rec)
	}
	at := m.cfg.Now().UnixMilli()
	if rec.Request.NextAttemptAt > at {
		return Result{Outcome: "DEFERRED", NextAttemptAt: rec.Request.NextAttemptAt}, nil
	}
	cmd, err := retryCommand(rec, at, retryReason(cause))
	if err != nil {
		return Result{}, err
	}
	current, applyErr := m.applyRetry(ctx, rec, cmd)
	if applyErr != nil && !errors.Is(applyErr, ErrConcurrentChange) {
		return Result{}, applyErr
	}
	if current.Request == nil {
		return Result{}, ErrConcurrentChange
	}
	if !workState(current.Request.Status) {
		return m.obsolete(current)
	}
	if applyErr == nil && current.Version > rec.Version && current.Request.NextAttemptAt == cmd.NextAttemptAt {
		return Result{Outcome: "RETRY_SCHEDULED", NextAttemptAt: cmd.NextAttemptAt}, nil
	}
	if current.Request.NextAttemptAt > at {
		return Result{Outcome: "DEFERRED", NextAttemptAt: current.Request.NextAttemptAt}, nil
	}
	return Result{}, ErrConcurrentChange
}

// Abandon voluntarily fences an exact MATCHING attempt with RETRYING before
// closing it. A confirmer that won first keeps its resource; this helper must
// never close a now-MATCHED attempt based on an old read.
func (m *Matcher) Abandon(ctx context.Context, requestID string) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WorkTimeout)
	defer cancel()
	rec, err := m.read(ctx, store.RequestKey(requestID))
	if err != nil {
		return Result{}, err
	}
	if rec.Request == nil {
		return Result{}, ErrConcurrentChange
	}
	if rec.Request.Status == store.RequestRetrying {
		return m.retrying(ctx, rec)
	}
	if rec.Request.Status != store.RequestMatching {
		return m.obsolete(rec)
	}
	a := rec.Request.CurrentAttempt
	if a == nil {
		return Result{}, ErrNeedsRecovery
	}
	cmd := transition(rec, store.RequestRetrying, m.cfg.Now().UnixMilli())
	cmd.OperationID = transitionID(cmd)
	cmd.Outbox, err = cleanupEvent(rec, cmd)
	if err != nil {
		return Result{}, err
	}
	current, err := m.applyTransition(ctx, rec, cmd)
	if err != nil && !errors.Is(err, ErrConcurrentChange) {
		return Result{}, err
	}
	if current.Request != nil && current.Request.Status == store.RequestRetrying && current.Request.CurrentAttempt != nil && *current.Request.CurrentAttempt == *a {
		return m.retrying(ctx, current)
	}
	if current.Request != nil && (current.Request.Status == store.RequestMatched || current.Request.Status == store.RequestInProgress || current.Request.Status == store.RequestCancelled || current.Request.Status == store.RequestFulfilled) {
		return m.obsolete(current)
	}
	return Result{}, ErrConcurrentChange
}

func (m *Matcher) retrying(ctx context.Context, rec store.Record) (Result, error) {
	a := rec.Request.CurrentAttempt
	if a == nil || a.AttemptID == "" || a.ResourceID == "" || a.Fingerprint == "" {
		return Result{}, ErrNeedsRecovery
	}
	closed, err := m.closure(ctx, rec)
	if err != nil {
		return Result{}, err
	}
	if !closed {
		resource, err := m.read(ctx, store.ResourceKey(a.ResourceID))
		if err != nil {
			return Result{}, err
		}
		if resource.Resource == nil {
			return Result{}, ErrOwnershipUnknown
		}
		disposition := store.ResourceAvailable
		if rec.UpdatedAt >= resource.Resource.AvailableUntil {
			disposition = store.ResourceOffline
		}
		cmd := store.CloseAttempt{
			Meta: store.Meta{At: rec.UpdatedAt, OperationID: operationID("retry-close", rec.Key, a.AttemptID,
				strconv.FormatUint(rec.Version, 10), strconv.FormatInt(rec.UpdatedAt, 10), string(disposition))},
			ResourceKey: store.ResourceKey(a.ResourceID), RequestID: rec.Request.ID, Attempt: *a, Disposition: disposition, Reason: "retrying",
		}
		result, err := m.mutate(ctx, func(ctx context.Context) (store.Result, error) { return m.cfg.Store.CloseAttempt(ctx, cmd) })
		if err != nil {
			return Result{}, err
		}
		if result.Outcome != store.OutcomeOK {
			return Result{}, fmt.Errorf("workflow retry close rejected with %s: %s", result.Outcome, result.Reason)
		}
		closed, err = m.closure(ctx, rec)
		if err != nil {
			return Result{}, err
		}
		if !closed {
			return Result{}, ErrOwnershipUnknown
		}
	}
	at := m.cfg.Now().UnixMilli()
	cmd := transition(rec, store.RequestWaiting, at)
	cmd.NextAttemptAt = at + retryDelay(rec.Request.RetryCount)
	cmd.LastError = "ATTEMPT_CLOSED"
	cmd.OperationID = transitionID(cmd)
	cmd.Outbox, err = events(rec, cmd, "request.retry", nil)
	if err != nil {
		return Result{}, err
	}
	current, err := m.applyTransition(ctx, rec, cmd)
	if err != nil {
		return Result{}, err
	}
	if current.Request != nil && current.Request.Status == store.RequestWaiting && current.Request.CurrentAttempt == nil && current.Request.NextAttemptAt == cmd.NextAttemptAt {
		return Result{Outcome: "RETRY_SCHEDULED", NextAttemptAt: cmd.NextAttemptAt}, nil
	}
	return m.obsolete(current)
}

func retryCommand(rec store.Record, at int64, reason string) (store.RetryWorkflow, error) {
	cmd := store.RetryWorkflow{Meta: store.Meta{At: at}, Key: rec.Key, ExpectedVersion: rec.Version,
		NextAttemptAt: at + retryDelay(rec.Request.RetryCount), LastError: reason}
	cmd.OperationID = operationID("retry", rec.Key, strconv.FormatUint(rec.Version, 10),
		strconv.FormatInt(cmd.At, 10), strconv.FormatInt(cmd.NextAttemptAt, 10), cmd.LastError)
	var err error
	cmd.Outbox, err = events(rec, store.TransitionRequest{Meta: cmd.Meta, NextAttemptAt: cmd.NextAttemptAt}, "request.retry", nil)
	return cmd, err
}

func (m *Matcher) applyRetry(ctx context.Context, rec store.Record, cmd store.RetryWorkflow) (store.Record, error) {
	result, err := m.mutate(ctx, func(ctx context.Context) (store.Result, error) { return m.cfg.Store.RetryWorkflow(ctx, cmd) })
	if err != nil {
		return store.Record{}, err
	}
	if result.Outcome != store.OutcomeOK && result.Outcome != store.OutcomeVersionConflict {
		return store.Record{}, fmt.Errorf("workflow retry rejected with %s: %s", result.Outcome, result.Reason)
	}
	current, err := m.read(ctx, rec.Key)
	if err != nil {
		return current, err
	}
	if result.Outcome == store.OutcomeVersionConflict {
		return current, ErrConcurrentChange
	}
	return current, nil
}
