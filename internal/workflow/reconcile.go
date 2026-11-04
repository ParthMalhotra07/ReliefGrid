package workflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Recover is the periodic worker's bounded repair entry point. An old MATCHING
// attempt is fenced from its exact read version before closure. UpdatedAt is
// not its age: RetryWorkflow may have moved it while the claim stayed unknown.
func (m *Matcher) Recover(ctx context.Context, id string, staleAfter time.Duration) error {
	if staleAfter <= 0 {
		return fmt.Errorf("workflow: recovery age must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WorkTimeout)
	defer cancel()
	rec, err := m.read(ctx, store.RequestKey(id))
	if err != nil {
		return err
	}
	if rec.Request == nil {
		return fmt.Errorf("workflow: recovery requires a request")
	}
	if rec.Request.Status == store.RequestMatching && rec.Request.CurrentAttempt != nil {
		at, err := claimTime(rec)
		if err != nil {
			return err
		}
		if m.cfg.Now().UnixMilli()-at >= staleAfter.Milliseconds() {
			_, err := m.abandon(ctx, rec)
			if err == nil {
				return nil
			}
			return m.handoffError(ctx, id, err)
		}
	}
	if workState(rec.Request.Status) && rec.Request.NextAttemptAt > m.cfg.Now().UnixMilli() {
		return nil
	}
	return m.Handle(ctx, id)
}

// Wake durably shortens a waiting request's backoff after a resource hint. The
// caller first reads the CURRENT resource and filters compatible nearby rows.
// This only schedules work; claim/ownership decisions stay in Match. Duplicate
// and unknown deliveries preserve the exact command until its outcome is known.
func (m *Matcher) Wake(ctx context.Context, id, sourceID string, sourceAt int64) error {
	if sourceID == "" || sourceAt <= 0 {
		return fmt.Errorf("workflow: wake source identity/time required")
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WorkTimeout)
	defer cancel()
	rec, err := m.read(ctx, store.RequestKey(id))
	if err != nil {
		return err
	}
	if rec.Request == nil {
		return fmt.Errorf("workflow: wake requires a request")
	}
	if rec.Request.Status != store.RequestWaiting || rec.Request.NextAttemptAt <= m.cfg.Now().UnixMilli() {
		return m.Handle(ctx, id)
	}
	if rec.Request.CurrentAttempt != nil {
		return ErrNeedsRecovery
	}
	at := max(rec.UpdatedAt, min(sourceAt, m.cfg.Now().UnixMilli()))
	cmd := store.RetryWorkflow{Meta: store.Meta{At: at}, Key: rec.Key, ExpectedVersion: rec.Version, NextAttemptAt: at + 1, LastError: "RESOURCE_AVAILABLE"}
	cmd.OperationID = operationID("resource-wake", rec.Key, strconv.FormatUint(rec.Version, 10), sourceID, strconv.FormatInt(at, 10))
	cmd.Outbox, err = events(ctx, rec, store.TransitionRequest{Meta: cmd.Meta, NextAttemptAt: cmd.NextAttemptAt}, "request.retry", nil)
	if err != nil {
		return err
	}
	current, err := m.applyRetry(ctx, rec, cmd)
	if err != nil && !errors.Is(err, ErrConcurrentChange) {
		return err
	}
	if err == nil && current.Request != nil && current.Request.Status == store.RequestWaiting && current.Request.NextAttemptAt == cmd.NextAttemptAt {
		return nil
	}
	return m.Handle(ctx, id)
}
