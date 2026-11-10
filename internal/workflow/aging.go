package workflow

import (
	"context"
	"fmt"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"strconv"
	"time"
)

// Age proposes at most one new aging boundary per pass. Boundary time and the
// read version, rather than the worker clock, freeze every retry byte across
// replicas and restarts. Already-due work is still decided from a current read.
func (m *Matcher) Age(ctx context.Context, id string, interval time.Duration) (store.Record, error) {
	if interval < time.Millisecond {
		return store.Record{}, fmt.Errorf("workflow: aging interval must be at least one millisecond")
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.WorkTimeout)
	defer cancel()
	rec, err := m.read(ctx, store.RequestKey(id))
	if err != nil {
		return rec, err
	}
	q := rec.Request
	if q == nil {
		return rec, ErrNeedsRecovery
	}
	if (q.Status != store.RequestPending && q.Status != store.RequestWaiting) || q.CurrentAttempt != nil || q.AssignedResourceID != "" {
		return rec, nil
	}
	current := max(q.Urgency, q.EffectiveUrgency)
	if q.Urgency < 1 || current > 3 || q.CreatedAt <= 0 {
		return rec, ErrNeedsRecovery
	}
	if current == 3 {
		return rec, nil
	}
	now := m.cfg.Now().UnixMilli()
	unit := interval.Milliseconds()
	if now < q.CreatedAt || (now-q.CreatedAt)/unit < int64(current-q.Urgency+1) {
		return rec, nil
	}
	// Eligibility above proves this multiplication/addition cannot overflow.
	at := max(rec.UpdatedAt, q.CreatedAt+int64(current-q.Urgency+1)*unit)
	if at > now {
		return rec, nil
	}
	target := q.Urgency + int(min((at-q.CreatedAt)/unit, int64(3-q.Urgency)))
	cmd := store.AgeRequest{Meta: store.Meta{At: at}, Key: rec.Key, ExpectedVersion: rec.Version, AgingIntervalMS: unit}
	cmd.OperationID = operationID("age", rec.Key, strconv.FormatUint(rec.Version, 10), strconv.FormatInt(unit, 10), strconv.FormatInt(at, 10))
	copyRequest := *q
	copyRequest.EffectiveUrgency = target
	eventRecord := rec
	eventRecord.Request = &copyRequest
	// The persisted work envelope has a stable operation-seeded root; caller
	// tracing may differ between replicas or after a restart. No remote call
	// uses this background context.
	cmd.Outbox, err = events(context.Background(), eventRecord, store.TransitionRequest{Meta: cmd.Meta, NextAttemptAt: at}, "request.retry", nil)
	if err != nil {
		return rec, err
	}
	cmd.Outbox[0].Exchange, cmd.Outbox[0].RoutingKey = store.AgingWakeExchange, store.AgingWakeRoutingKey
	result, err := m.mutate(ctx, func(ctx context.Context) (store.Result, error) { return m.cfg.Store.AgeRequest(ctx, cmd) })
	if err != nil {
		return rec, err
	}
	if result.Outcome != store.OutcomeOK && result.Outcome != store.OutcomeVersionConflict {
		return rec, fmt.Errorf("workflow age rejected with %s: %s", result.Outcome, result.Reason)
	}
	// A historical result never authorizes current matching or ownership.
	return m.read(ctx, rec.Key)
}
