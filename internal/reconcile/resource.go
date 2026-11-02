package reconcile

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ParthMalhotra07/ReliefGrid/internal/geo"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
)

func (e *Engine) read(ctx context.Context, key string) (store.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.CallTimeout)
	defer cancel()
	return e.cfg.Store.ReadRecord(ctx, key)
}
func (e *Engine) Repair(ctx context.Context, id string) error {
	return e.cfg.Matcher.Recover(ctx, id, e.cfg.StaleAfter)
}

// Wake consumes identity-only hints. A terminal cleanup can expose a released
// resource, but the current resource record must still prove availability.
func (e *Engine) Wake(ctx context.Context, id, eventID, kind string, at int64) error {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.WorkTimeout)
	defer cancel()
	switch kind {
	case "resource.registered", "resource.state_changed":
		return e.WakeResource(ctx, id, eventID, at)
	case "request.state_changed", "match.proposed", "match.completed":
		if err := e.Repair(ctx, id); err != nil {
			return err
		}
		rec, err := e.read(ctx, store.RequestKey(id))
		if err != nil {
			return err
		}
		if rec.Request != nil && (rec.Request.Status == store.RequestCancelled || rec.Request.Status == store.RequestFulfilled) && rec.Request.CurrentAttempt != nil {
			return e.WakeResource(ctx, rec.Request.CurrentAttempt.ResourceID, eventID, at)
		}
		return nil
	default:
		return fmt.Errorf("reconcile: unsupported wakeup kind %q", kind)
	}
}

// WakeResource accelerates a bounded batch. One resource has capacity for one
// claim; due scans remain the fallback for other waiters. A partial/unfinished
// negative walk is an error, never evidence that no compatible request exists.
func (e *Engine) WakeResource(ctx context.Context, id, eventID string, at int64) error {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.WorkTimeout)
	defer cancel()
	rec, err := e.read(ctx, store.ResourceKey(id))
	if err != nil {
		return err
	}
	r := rec.Resource
	now := e.cfg.Now().UnixMilli()
	if rec.Deleted || r == nil || r.Status != store.ResourceAvailable || r.ActiveClaim != nil || now < r.AvailableFrom || now >= r.AvailableUntil {
		return nil
	}
	prefixes, err := geo.Cover(r.Location, 50)
	if err != nil {
		return err
	}
	q := store.ScanQuery{Kind: store.KindRequest, Type: r.Type, Statuses: []string{string(store.RequestWaiting)}, GeohashPrefixes: prefixes, Limit: e.cfg.PageSize, Consistency: store.ConsistencyStrong}
	expected, seen := e.allShards(), make(map[string]bool)
	scheduled := 0
	for range 1024 {
		page, err := e.scan(ctx, q)
		if err != nil {
			return fmt.Errorf("%w: resource wake: %w", ErrIncompleteScan, err)
		}
		more, err := pagePositions(page, expected, e.cfg.PageSize)
		if err != nil {
			return err
		}
		if page.NextCursor != "" && (page.NextCursor == q.Cursor || seen[page.NextCursor]) {
			return fmt.Errorf("%w: resource cursor did not advance", ErrIncompleteScan)
		}
		for _, request := range page.Records {
			if request.Deleted || request.Request == nil || request.Request.Type != r.Type || geo.HaversineKM(request.Request.Location, r.Location) > 50 {
				continue
			}
			if err := e.cfg.Matcher.Wake(ctx, request.Request.ID, eventID, at); err != nil {
				return fmt.Errorf("resource wake %s: %w", request.Key, err)
			}
			scheduled++
			if scheduled >= e.cfg.BatchSize {
				slog.InfoContext(ctx, "resource wake batch committed", "resourceId", id, "scheduled", scheduled)
				return nil
			}
		}
		if page.NextCursor == "" {
			return nil
		}
		seen[page.NextCursor] = true
		q.Cursor = page.NextCursor
		expected = more
	}
	return fmt.Errorf("reconcile resource wake: %w", workflow.ErrWorkBudget)
}
