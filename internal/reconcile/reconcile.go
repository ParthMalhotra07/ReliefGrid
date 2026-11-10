// Package reconcile resumes durable application work; storage remains authoritative.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
)

var ErrIncompleteScan = errors.New("reconcile: incomplete scan")

type Config struct {
	AgingInterval                                      time.Duration // zero disables aging until the storage rollout is complete
	Store                                              store.Store
	Matcher                                            *workflow.Matcher
	Now                                                func() time.Time
	PageSize, BatchSize                                int
	PollInterval, StaleAfter, CallTimeout, WorkTimeout time.Duration
}

type Engine struct {
	cfg      Config
	permit   chan struct{}
	cursor   string
	expected map[int]bool
	seen     map[string]bool
	pending  []store.Record
	offset   int
	next     string
	more     map[int]bool
}

type Report struct {
	Scanned, Visited, Attempted, Failed int
	Complete                            bool
}

func New(cfg Config) (*Engine, error) {
	if cfg.Store == nil || cfg.Store.Shards() < 1 {
		return nil, fmt.Errorf("reconcile: sharded authoritative store required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.PageSize == 0 {
		cfg.PageSize = 32
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 16
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.StaleAfter == 0 {
		cfg.StaleAfter = 30 * time.Second
	}
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = 2 * time.Second
	}
	if cfg.WorkTimeout == 0 {
		cfg.WorkTimeout = 15 * time.Second
	}
	if cfg.AgingInterval < 0 || (cfg.AgingInterval > 0 && cfg.AgingInterval < time.Millisecond) {
		return nil, fmt.Errorf("reconcile: aging interval must be zero or at least one millisecond")
	}
	if cfg.PageSize < 1 || cfg.PageSize > store.MaxScanLimit || cfg.BatchSize < 1 || cfg.BatchSize > 100 || cfg.PollInterval <= 0 || cfg.StaleAfter <= 0 || cfg.CallTimeout <= 0 || cfg.WorkTimeout <= 0 {
		return nil, fmt.Errorf("reconcile: invalid batch size or timeout")
	}
	if cfg.Matcher == nil {
		m, err := workflow.New(workflow.Config{Store: cfg.Store, Now: cfg.Now, ScanLimit: cfg.PageSize})
		if err != nil {
			return nil, err
		}
		cfg.Matcher = m
	}
	e := &Engine{cfg: cfg, permit: make(chan struct{}, 1)}
	e.reset()
	return e, nil
}
func (e *Engine) allShards() map[int]bool {
	out := make(map[int]bool)
	for shard := range e.cfg.Store.Shards() {
		out[shard] = true
	}
	return out
}
func (e *Engine) reset() {
	e.cursor = ""
	e.expected = e.allShards()
	e.seen = make(map[string]bool)
	e.pending = nil
	e.offset = 0
	e.next = ""
	e.more = nil
}

func pagePositions(page store.ScanResult, expected map[int]bool, limit int) (map[int]bool, error) {
	if page.Partial || len(page.FailedShards) != 0 {
		return nil, fmt.Errorf("%w: failed shards %v", ErrIncompleteScan, page.FailedShards)
	}
	answered, more := make(map[int]bool), make(map[int]bool)
	for _, p := range page.Positions {
		if !expected[p.Shard] || answered[p.Shard] {
			return nil, fmt.Errorf("%w: unexpected/duplicate shard position", ErrIncompleteScan)
		}
		answered[p.Shard] = true
		if p.HasMore {
			more[p.Shard] = true
		}
	}
	if len(answered) != len(expected) || (len(more) == 0) != (page.NextCursor == "") || len(page.Records) > limit*len(expected) {
		return nil, fmt.Errorf("%w: missing position or inconsistent continuation", ErrIncompleteScan)
	}
	return more, nil
}
func (e *Engine) scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.CallTimeout)
	defer cancel()
	return e.cfg.Store.Scan(ctx, q)
}

// Step retains the unprocessed tail of a page. Cursor advances only after its
// whole page was visited, so a small batch never silently loses the other rows.
// Failed repairs stay persisted and are revisited on the next walk; scan failure
// never advances the cursor or becomes evidence of no work.
func (e *Engine) Step(ctx context.Context) (Report, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.WorkTimeout)
	defer cancel()
	select {
	case e.permit <- struct{}{}:
	case <-ctx.Done():
		return Report{}, ctx.Err()
	}
	defer func() { <-e.permit }()
	report := Report{}
	if e.pending == nil {
		q := store.ScanQuery{Kind: store.KindRequest, Statuses: []string{"PENDING", "WAITING", "MATCHING", "RETRYING", "CANCELLING", "FULFILLING"}, Limit: e.cfg.PageSize, Cursor: e.cursor, Consistency: store.ConsistencyStrong}
		page, err := e.scan(ctx, q)
		if err != nil {
			if errors.Is(err, store.ErrRingVersionMismatch) {
				e.reset()
			}
			return report, fmt.Errorf("%w: %w", ErrIncompleteScan, err)
		}
		more, err := pagePositions(page, e.expected, e.cfg.PageSize)
		if err != nil {
			return report, err
		}
		if page.NextCursor != "" && (page.NextCursor == e.cursor || e.seen[page.NextCursor]) {
			return report, fmt.Errorf("%w: cursor did not advance", ErrIncompleteScan)
		}
		if page.NextCursor != "" {
			e.seen[page.NextCursor] = true
		}
		e.pending = append([]store.Record{}, page.Records...)
		e.offset = 0
		e.next = page.NextCursor
		e.more = more
		report.Scanned = len(page.Records)
	}
	var failures error
	for e.offset < len(e.pending) && report.Visited < e.cfg.BatchSize {
		if ctx.Err() != nil {
			return report, errors.Join(failures, ctx.Err())
		}
		rec := e.pending[e.offset]
		e.offset++
		report.Visited++
		if rec.Deleted || rec.Request == nil {
			continue
		}
		if e.cfg.AgingInterval > 0 && (rec.Request.Status == store.RequestPending || rec.Request.Status == store.RequestWaiting) {
			current, err := e.cfg.Matcher.Age(ctx, rec.Request.ID, e.cfg.AgingInterval)
			if err != nil {
				report.Failed++
				failures = errors.Join(failures, fmt.Errorf("age %s: %w", rec.Key, err))
				continue
			}
			rec = current
			if rec.Deleted || rec.Request == nil {
				continue
			}
		}
		// MATCHING age comes from immutable intent time, even during backoff.
		if rec.Request.Status != store.RequestMatching && rec.Request.NextAttemptAt > e.cfg.Now().UnixMilli() {
			continue
		}
		report.Attempted++
		if err := e.cfg.Matcher.Recover(ctx, rec.Request.ID, e.cfg.StaleAfter); err != nil {
			report.Failed++
			failures = errors.Join(failures, fmt.Errorf("repair %s: %w", rec.Key, err))
		}
	}
	if e.offset == len(e.pending) {
		e.pending = nil
		e.offset = 0
		e.cursor = e.next
		e.expected = e.more
		if e.next == "" {
			report.Complete = true
			e.reset()
		}
	}
	return report, failures
}

// Run bounds every pass and backs off on failure. Persistent repair errors are
// visible in logs; durable RetryWorkflow metadata remains the work-state record.
func (e *Engine) Run(ctx context.Context) error {
	backoff := e.cfg.PollInterval
	for ctx.Err() == nil {
		report, err := e.Step(ctx)
		if err != nil {
			slog.WarnContext(ctx, "reconcile pass failed", "error", err, "attempted", report.Attempted, "failed", report.Failed, "backoff", backoff)
			backoff = min(2*backoff, 30*time.Second)
		} else {
			backoff = e.cfg.PollInterval
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}
