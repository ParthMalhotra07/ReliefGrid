// Package invcheck checks the cross-shard claim invariants (AGENTS.md 3) over
// a full read of the store: no resource with two active claims, no request
// with two live claims, a reciprocal claim for every MATCHED/IN_PROGRESS
// request, no live claim held for a terminal request, and intent kept by the
// intermediate states. It also reports the unpublished outbox backlog.
//
// It reads through store.Store, so it runs on memstore and on the cluster
// through coordclient. The scans of different shards are not one snapshot:
// run it after writers are paused and workflows have settled (v3.1 §26), or
// transient workflow states may be reported. Run reads every shard's applied
// index before and after the walk and marks the report not Quiescent if any
// moved, so a check of a busy cluster is never mistaken for one of a still one.
package invcheck

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Rule names one invariant.
type Rule string

const (
	// ResourceDoubleClaim: two MATCHED/IN_PROGRESS requests hold the same
	// resource as their confirmed assignment.
	ResourceDoubleClaim Rule = "resource-double-claim"
	// RequestDoubleClaim: two resources hold a live claim for one request.
	RequestDoubleClaim Rule = "request-double-claim"
	// MissingReciprocal: a MATCHED/IN_PROGRESS request whose assigned
	// resource does not hold the claim for its attempt.
	MissingReciprocal Rule = "missing-reciprocal-claim"
	// TerminalHoldsClaim: a resource holds a live claim for a CANCELLED or
	// FULFILLED request.
	TerminalHoldsClaim Rule = "terminal-holds-claim"
	// LostIntent: a MATCHING/RETRYING/CANCELLING/FULFILLING request without
	// its attempt, a CANCELLING/FULFILLING one without its disposition, or a
	// MATCHED/IN_PROGRESS one whose assignment is not its attempt's resource.
	LostIntent Rule = "intermediate-lost-intent"
	// ClaimStatus: an ASSIGNED resource without a claim, or a claim on a
	// resource that is not ASSIGNED.
	ClaimStatus Rule = "claim-status-mismatch"
	// OrphanClaim: a claim whose request is missing, or does not hold that
	// attempt on that resource.
	OrphanClaim Rule = "orphan-claim"
	// OutboxBacklog: a shard has more unpublished events than
	// Options.MaxBacklog allows.
	OutboxBacklog Rule = "outbox-backlog"
)

// Violation is one broken invariant at one key.
type Violation struct {
	Rule   Rule   `json:"rule"`
	Key    string `json:"key"`
	Detail string `json:"detail"`
}

// Backlog is one shard's unpublished outbox. Pending counts at most
// OutboxLimit events; AtLeast is set when the page was full.
type Backlog struct {
	Shard    int   `json:"shard"`
	Pending  int   `json:"pending"`
	AtLeast  bool  `json:"atLeast,omitempty"`
	OldestAt int64 `json:"oldestAt,omitempty"` // CreatedAt of the oldest pending event
}

// Report is the result of one complete check.
type Report struct {
	Requests     int         `json:"requests"`
	Resources    int         `json:"resources"`
	Pages        int         `json:"pages"`
	PartialPages int         `json:"partialPages"` // partial pages resumed by the cursor
	Violations   []Violation `json:"violations"`
	Outbox       []Backlog   `json:"outbox"`
	// AppliedBefore and AppliedAfter are each shard's applied index read
	// before and after the walk. Quiescent is set when they are equal: no
	// write committed while the check read, so its records are one state.
	AppliedBefore map[int]uint64 `json:"appliedBefore"`
	AppliedAfter  map[int]uint64 `json:"appliedAfter"`
	Quiescent     bool           `json:"quiescent"`
}

// OK reports whether the check found no violation. Only a Report returned
// with a nil error is a complete check, and only a Quiescent one is
// evidence about a single state.
func (r Report) OK() bool { return len(r.Violations) == 0 }

// Verdict is "clean" or "violations", with " (not quiescent)" appended
// when a write committed during the walk.
func (r Report) Verdict() string {
	v := "clean"
	if !r.OK() {
		v = "violations"
	}
	if !r.Quiescent {
		v += " (not quiescent)"
	}
	return v
}

// OutboxLimit is the page read per shard for the backlog: the largest page
// coordclient serves.
const OutboxLimit = 100

// Options tune Run; zero values take the defaults.
type Options struct {
	// PageLimit is the scan limit per shard per page (default 500).
	PageLimit int
	// MaxPartialPages is how many partial pages in a row Run resumes before
	// it gives up with ErrIncomplete (default 20).
	MaxPartialPages int
	// PartialBackoff is the wait before resuming a partial page (default 200ms).
	PartialBackoff time.Duration
	// MaxBacklog, if set, makes a shard with more unpublished events than
	// this an OutboxBacklog violation. It must be below OutboxLimit, since
	// only one outbox page is read. Unset, the backlog is only reported.
	MaxBacklog *int
}

// ErrIncomplete: some shard could not be read in full, so no verdict is
// given. A partial scan is never a clean result (AGENTS.md 7).
var ErrIncomplete = errors.New("invcheck: incomplete read")

func incomplete(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrIncomplete}, args...)...)
}

// claimHolding are the states in which a request may hold a live claim for
// its current attempt.
var claimHolding = map[store.RequestStatus]bool{
	store.RequestMatching: true, store.RequestMatched: true, store.RequestInProgress: true,
	store.RequestRetrying: true, store.RequestCancelling: true, store.RequestFulfilling: true,
}

func terminal(s store.RequestStatus) bool {
	return s == store.RequestCancelled || s == store.RequestFulfilled
}

func validDisposition(d store.ResourceStatus) bool {
	return d == store.ResourceAvailable || d == store.ResourceConsumed || d == store.ResourceOffline
}

func completeAttempt(a *store.Attempt) bool {
	return a != nil && a.AttemptID != "" && a.ResourceID != "" && a.Fingerprint != ""
}

// Check returns every violation in records, sorted by rule then key.
// Tombstones are skipped.
func Check(records []store.Record) []Violation {
	type keyed[T any] struct {
		key string
		v   *T
	}
	reqs := map[string]keyed[store.Request]{}
	res := map[string]keyed[store.Resource]{}
	var resOrder []string
	for _, r := range records {
		switch {
		case r.Deleted:
		case r.Request != nil:
			reqs[r.Request.ID] = keyed[store.Request]{r.Key, r.Request}
		case r.Resource != nil:
			res[r.Resource.ID] = keyed[store.Resource]{r.Key, r.Resource}
			resOrder = append(resOrder, r.Resource.ID)
		}
	}
	var vs []Violation
	add := func(rule Rule, key, format string, args ...any) {
		vs = append(vs, Violation{Rule: rule, Key: key, Detail: fmt.Sprintf(format, args...)})
	}

	claimsFor := map[string][]string{} // request ID -> resources holding a claim for it
	for _, id := range resOrder {
		r := res[id]
		c := r.v.ActiveClaim
		if (r.v.Status == store.ResourceAssigned) != (c != nil) {
			add(ClaimStatus, r.key, "status %s with claim %v", r.v.Status, c != nil)
		}
		if c == nil {
			continue
		}
		claimsFor[c.RequestID] = append(claimsFor[c.RequestID], id)
		q, ok := reqs[c.RequestID]
		switch {
		case !ok:
			add(OrphanClaim, r.key, "claim for request %s, which does not exist", c.RequestID)
		case terminal(q.v.Status):
			add(TerminalHoldsClaim, r.key, "claim for %s request %s, attempt %s", q.v.Status, c.RequestID, c.AttemptID)
		case !claimHolding[q.v.Status] || q.v.CurrentAttempt == nil ||
			q.v.CurrentAttempt.AttemptID != c.AttemptID || q.v.CurrentAttempt.ResourceID != id:
			add(OrphanClaim, r.key, "claim for request %s attempt %s; the request is %s with attempt %+v",
				c.RequestID, c.AttemptID, q.v.Status, q.v.CurrentAttempt)
		}
	}
	for rid, holders := range claimsFor {
		if len(holders) > 1 {
			key := store.RequestKey(rid)
			if q, ok := reqs[rid]; ok {
				key = q.key
			}
			add(RequestDoubleClaim, key, "claimed by %d resources: %v", len(holders), holders)
		}
	}

	assignedTo := map[string][]string{} // resource ID -> MATCHED/IN_PROGRESS requests assigned it
	for rid, q := range reqs {
		a := q.v.CurrentAttempt
		switch q.v.Status {
		case store.RequestMatching, store.RequestRetrying, store.RequestCancelling, store.RequestFulfilling:
			if !completeAttempt(a) {
				add(LostIntent, q.key, "%s without a complete attempt: %+v", q.v.Status, a)
			}
			if (q.v.Status == store.RequestCancelling || q.v.Status == store.RequestFulfilling) && !validDisposition(q.v.Disposition) {
				add(LostIntent, q.key, "%s without a disposition (have %q)", q.v.Status, q.v.Disposition)
			}
		case store.RequestMatched, store.RequestInProgress:
			if !completeAttempt(a) || a.ResourceID != q.v.AssignedResourceID {
				add(LostIntent, q.key, "%s assigned %q, attempt %+v", q.v.Status, q.v.AssignedResourceID, a)
			}
			assignedTo[q.v.AssignedResourceID] = append(assignedTo[q.v.AssignedResourceID], rid)
			attemptID := ""
			if a != nil {
				attemptID = a.AttemptID
			}
			r, ok := res[q.v.AssignedResourceID]
			if !ok || r.v.ActiveClaim == nil || r.v.ActiveClaim.RequestID != rid || r.v.ActiveClaim.AttemptID != attemptID {
				var held *store.Claim
				if ok {
					held = r.v.ActiveClaim
				}
				add(MissingReciprocal, q.key, "%s on resource %q attempt %s, which holds %+v", q.v.Status, q.v.AssignedResourceID, attemptID, held)
			}
		}
	}
	for id, holders := range assignedTo {
		if len(holders) > 1 {
			key := store.ResourceKey(id)
			if r, ok := res[id]; ok {
				key = r.key
			}
			slices.Sort(holders)
			add(ResourceDoubleClaim, key, "assigned to %d MATCHED/IN_PROGRESS requests: %v", len(holders), holders)
		}
	}
	sortViolations(vs)
	return vs
}

func sortViolations(vs []Violation) {
	slices.SortFunc(vs, func(a, b Violation) int {
		if c := cmp.Compare(a.Rule, b.Rule); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return cmp.Compare(a.Detail, b.Detail)
	})
}

// walker reads pages, retrying failed and partial ones against one budget.
type walker struct {
	s      store.Store
	opt    Options
	streak int // failed or partial pages in a row
}

// scan reads one page. A failed or partial page counts against the budget
// and waits PartialBackoff; once the budget is spent, or the ring changed,
// it returns ErrIncomplete. Otherwise an error means: retry the same page.
func (w *walker) scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	res, err := w.s.Scan(ctx, q)
	switch {
	case err == nil && !res.Partial:
		w.streak = 0
		return res, nil
	case errors.Is(err, store.ErrRingVersionMismatch):
		return res, incomplete("the ring changed during the walk: %w", err)
	case ctx.Err() != nil:
		return res, incomplete("%w", ctx.Err())
	}
	w.streak++
	if w.streak > w.opt.MaxPartialPages {
		if err != nil {
			return res, incomplete("%d failed pages in a row: %w", w.streak, err)
		}
		return res, incomplete("shards %v still failing after %d partial pages", res.FailedShards, w.streak)
	}
	t := time.NewTimer(w.opt.PartialBackoff)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
		return res, incomplete("%w", ctx.Err())
	}
	return res, err
}

// applied reads every shard's applied index with a one-record page.
func (w *walker) applied(ctx context.Context) (map[int]uint64, error) {
	for {
		res, err := w.scan(ctx, store.ScanQuery{Kind: store.KindRequest, Limit: 1})
		if errors.Is(err, ErrIncomplete) {
			return nil, err
		}
		if err != nil || res.Partial {
			continue
		}
		out := map[int]uint64{}
		for _, p := range res.Positions {
			out[p.Shard] = p.AppliedIndex
		}
		if len(out) != w.s.Shards() {
			return nil, incomplete("positions for %d of %d shards", len(out), w.s.Shards())
		}
		return out, nil
	}
}

// walk reads every record of kind, following the cursor to its end.
func (w *walker) walk(ctx context.Context, kind store.Kind, rep *Report) ([]store.Record, error) {
	var out []store.Record
	q := store.ScanQuery{Kind: kind, Limit: w.opt.PageLimit}
	for {
		res, err := w.scan(ctx, q)
		if errors.Is(err, ErrIncomplete) {
			return nil, err
		}
		if err != nil {
			continue
		}
		rep.Pages++
		if res.Partial {
			rep.PartialPages++
		}
		out = append(out, res.Records...)
		if res.NextCursor == "" {
			if res.Partial {
				return nil, incomplete("partial page %v with no cursor", res.FailedShards)
			}
			return out, nil
		}
		q.Cursor = res.NextCursor
	}
}

// Run reads every request and resource by walking Scan to an empty cursor,
// checks them, and reads each shard's outbox backlog. A shard that stays
// failed, a scan that keeps failing or an outbox read error returns
// ErrIncomplete. Each shard's applied index is read before and after the
// walk; if any moved, the Report is not Quiescent.
func Run(ctx context.Context, s store.Store, opt Options) (Report, error) {
	if opt.PageLimit == 0 {
		opt.PageLimit = 500
	}
	if opt.MaxPartialPages == 0 {
		opt.MaxPartialPages = 20
	}
	if opt.PartialBackoff == 0 {
		opt.PartialBackoff = 200 * time.Millisecond
	}
	if m := opt.MaxBacklog; m != nil && (*m < 0 || *m >= OutboxLimit) {
		return Report{}, fmt.Errorf("invcheck: max backlog %d: must be 0 to %d", *m, OutboxLimit-1)
	}
	w := &walker{s: s, opt: opt}
	var rep Report
	var err error
	if rep.AppliedBefore, err = w.applied(ctx); err != nil {
		return rep, err
	}
	var recs []store.Record
	for _, kind := range []store.Kind{store.KindRequest, store.KindResource} {
		got, err := w.walk(ctx, kind, &rep)
		if err != nil {
			return rep, err
		}
		recs = append(recs, got...)
	}
	for _, r := range recs {
		switch {
		case r.Deleted:
		case r.Request != nil:
			rep.Requests++
		case r.Resource != nil:
			rep.Resources++
		}
	}
	rep.Violations = Check(recs)
	for shard := range s.Shards() {
		evs, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: OutboxLimit})
		if err != nil {
			return rep, incomplete("outbox of shard %d: %w", shard, err)
		}
		b := Backlog{Shard: shard, Pending: len(evs), AtLeast: len(evs) == OutboxLimit}
		if len(evs) > 0 {
			b.OldestAt = evs[0].CreatedAt
		}
		rep.Outbox = append(rep.Outbox, b)
		if m := opt.MaxBacklog; m != nil && b.Pending > *m {
			rep.Violations = append(rep.Violations, Violation{Rule: OutboxBacklog, Key: fmt.Sprintf("outbox:shard-%d", shard),
				Detail: fmt.Sprintf("%d unpublished events, at most %d allowed", b.Pending, *m)})
		}
	}
	sortViolations(rep.Violations)
	if rep.AppliedAfter, err = w.applied(ctx); err != nil {
		return rep, err
	}
	rep.Quiescent = maps.Equal(rep.AppliedBefore, rep.AppliedAfter)
	return rep, nil
}
