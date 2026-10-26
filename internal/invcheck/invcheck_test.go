package invcheck_test

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck/invchecktest"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newStore(t *testing.T, opts ...memstore.Option) *memstore.Store {
	t.Helper()
	s, err := memstore.New(context.Background(), opts...)
	if err != nil {
		t.Fatalf("memstore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// seeded is a memstore holding the clean workflow fixture.
func seeded(t *testing.T, extra int, opts ...memstore.Option) (*memstore.Store, invchecktest.Seeded) {
	t.Helper()
	s := newStore(t, opts...)
	sd, err := invchecktest.Seed(context.Background(), s, "ic", extra)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return s, sd
}

// records reads every request and resource of s.
func records(t *testing.T, s store.Store) []store.Record {
	t.Helper()
	var out []store.Record
	for _, kind := range []store.Kind{store.KindRequest, store.KindResource} {
		q := store.ScanQuery{Kind: kind, Limit: store.MaxScanLimit}
		for {
			res, err := s.Scan(testCtx(t), q)
			if err != nil || res.Partial {
				t.Fatalf("scan %s: partial=%v, %v", kind, res.Partial, err)
			}
			out = append(out, res.Records...)
			if res.NextCursor == "" {
				break
			}
			q.Cursor = res.NextCursor
		}
	}
	return out
}

// edit returns a copy of recs with fn applied to the record at key.
func edit(t *testing.T, recs []store.Record, key string, fn func(r *store.Record)) []store.Record {
	t.Helper()
	out := slices.Clone(recs)
	for i := range out {
		if out[i].Key == key {
			r := out[i]
			if r.Request != nil {
				q := *r.Request
				r.Request = &q
			}
			if r.Resource != nil {
				res := *r.Resource
				r.Resource = &res
			}
			fn(&r)
			out[i] = r
			return out
		}
	}
	t.Fatalf("no record %s", key)
	return nil
}

func rules(vs []invcheck.Violation) map[invcheck.Rule]bool {
	out := map[invcheck.Rule]bool{}
	for _, v := range vs {
		out[v.Rule] = true
	}
	return out
}

func set(rs ...invcheck.Rule) map[invcheck.Rule]bool {
	out := map[invcheck.Rule]bool{}
	for _, r := range rs {
		out[r] = true
	}
	return out
}

const (
	reqMatched    = "req:ic-q-matched"
	resMatched    = "res:ic-r-matched"
	reqPending    = "req:ic-q-pending"
	reqCancelled  = "req:ic-q-cancelled"
	resCancelled  = "res:ic-r-cancelled"
	resFulfilled  = "res:ic-r-fulfilled"
	reqFulfilled  = "req:ic-q-fulfilled"
	reqRetrying   = "req:ic-q-retrying-closed"
	reqCancelling = "req:ic-q-cancelling"
	resExtra0     = "res:ic-r-extra-0000"
	resExtra1     = "res:ic-r-extra-0001"
)

// The clean workflow, through every request state, passes every invariant.
func TestCheckCleanWorkflow(t *testing.T) {
	s, sd := seeded(t, 4)
	recs := records(t, s)
	if len(recs) != sd.Requests+sd.Resources {
		t.Fatalf("read %d records, seeded %d", len(recs), sd.Requests+sd.Resources)
	}
	for _, st := range []store.RequestStatus{store.RequestPending, store.RequestWaiting, store.RequestMatching, store.RequestMatched,
		store.RequestInProgress, store.RequestRetrying, store.RequestCancelling, store.RequestCancelled, store.RequestFulfilling, store.RequestFulfilled} {
		if sd.ByStatus[st] == 0 {
			t.Fatalf("fixture has no %s request: %v", st, sd.ByStatus)
		}
	}
	if vs := invcheck.Check(recs); len(vs) != 0 {
		t.Fatalf("clean workflow: %+v", vs)
	}
}

// Verify (S2.2): each deliberately corrupted fixture is caught, by exactly
// the rules it breaks.
func TestCheckCatchesCorruptedFixtures(t *testing.T) {
	s, _ := seeded(t, 4)
	clean := records(t, s)
	claimOf := func(key string) store.Claim {
		for _, r := range clean {
			if r.Key == key && r.Resource != nil && r.Resource.ActiveClaim != nil {
				return *r.Resource.ActiveClaim
			}
		}
		t.Fatalf("%s holds no claim", key)
		return store.Claim{}
	}
	attemptOf := func(key string) store.Attempt {
		for _, r := range clean {
			if r.Key == key && r.Request != nil && r.Request.CurrentAttempt != nil {
				return *r.Request.CurrentAttempt
			}
		}
		t.Fatalf("%s has no attempt", key)
		return store.Attempt{}
	}
	assign := func(c store.Claim) func(r *store.Record) {
		return func(r *store.Record) { r.Resource.Status, r.Resource.ActiveClaim = store.ResourceAssigned, &c }
	}

	for _, tc := range []struct {
		name  string
		recs  []store.Record
		want  map[invcheck.Rule]bool
		where string // a key the first violation must name
	}{
		{
			name:  "double claim: one request claimed by two resources",
			recs:  edit(t, clean, resExtra0, assign(claimOf(resMatched))),
			want:  set(invcheck.RequestDoubleClaim, invcheck.OrphanClaim),
			where: reqMatched,
		},
		{
			name: "double claim: two requests MATCHED on one resource",
			recs: edit(t, clean, reqPending, func(r *store.Record) {
				a := attemptOf(reqMatched)
				a.AttemptID = "ic-a-intruder"
				r.Request.Status, r.Request.CurrentAttempt, r.Request.AssignedResourceID = store.RequestMatched, &a, a.ResourceID
			}),
			want:  set(invcheck.ResourceDoubleClaim, invcheck.MissingReciprocal),
			where: resMatched,
		},
		{
			name: "missing reciprocal claim: MATCHED request, resource released",
			recs: edit(t, clean, resMatched, func(r *store.Record) {
				r.Resource.Status, r.Resource.ActiveClaim = store.ResourceAvailable, nil
			}),
			want:  set(invcheck.MissingReciprocal),
			where: reqMatched,
		},
		{
			name: "missing reciprocal claim: resource holds another attempt",
			recs: edit(t, clean, resMatched, func(r *store.Record) {
				c := *r.Resource.ActiveClaim
				c.AttemptID = "ic-a-stale"
				r.Resource.ActiveClaim = &c
			}),
			want:  set(invcheck.MissingReciprocal, invcheck.OrphanClaim),
			where: reqMatched,
		},
		{
			name:  "terminal request holding a claim: CANCELLED",
			recs:  edit(t, clean, resCancelled, assign(store.Claim{RequestID: "ic-q-cancelled", AttemptID: attemptOf(reqCancelled).AttemptID})),
			want:  set(invcheck.TerminalHoldsClaim),
			where: resCancelled,
		},
		{
			name:  "terminal request holding a claim: FULFILLED",
			recs:  edit(t, clean, resFulfilled, assign(store.Claim{RequestID: "ic-q-fulfilled", AttemptID: attemptOf(reqFulfilled).AttemptID})),
			want:  set(invcheck.TerminalHoldsClaim),
			where: resFulfilled,
		},
		{
			name:  "intermediate state lost its intent",
			recs:  edit(t, clean, reqRetrying, func(r *store.Record) { r.Request.CurrentAttempt = nil }),
			want:  set(invcheck.LostIntent),
			where: reqRetrying,
		},
		{
			name:  "CANCELLING lost its disposition",
			recs:  edit(t, clean, reqCancelling, func(r *store.Record) { r.Request.Disposition = "" }),
			want:  set(invcheck.LostIntent),
			where: reqCancelling,
		},
		{
			name:  "ASSIGNED resource with no claim",
			recs:  edit(t, clean, resExtra0, func(r *store.Record) { r.Resource.Status = store.ResourceAssigned }),
			want:  set(invcheck.ClaimStatus),
			where: resExtra0,
		},
		{
			name:  "claim for a request that does not exist",
			recs:  edit(t, clean, resExtra1, assign(store.Claim{RequestID: "ic-q-ghost", AttemptID: "ic-a-ghost"})),
			want:  set(invcheck.OrphanClaim),
			where: resExtra1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vs := invcheck.Check(tc.recs)
			if got := rules(vs); !maps.Equal(got, tc.want) {
				t.Fatalf("rules = %v, want %v\n%+v", got, tc.want, vs)
			}
			if !slices.ContainsFunc(vs, func(v invcheck.Violation) bool { return v.Key == tc.where && v.Detail != "" }) {
				t.Fatalf("no violation names %s with a detail: %+v", tc.where, vs)
			}
		})
	}
}

// Violations are reported in a stable order: by rule, then key.
func TestCheckIsSorted(t *testing.T) {
	s, _ := seeded(t, 4)
	recs := edit(t, records(t, s), resExtra0, func(r *store.Record) { r.Resource.Status = store.ResourceAssigned })
	recs = edit(t, recs, resExtra1, func(r *store.Record) { r.Resource.Status = store.ResourceAssigned })
	recs = edit(t, recs, reqRetrying, func(r *store.Record) { r.Request.CurrentAttempt = nil })
	slices.Reverse(recs)
	vs := invcheck.Check(recs)
	if len(vs) != 3 || !slices.IsSortedFunc(vs, func(a, b invcheck.Violation) int {
		if a.Rule != b.Rule {
			return cmp.Compare(a.Rule, b.Rule)
		}
		return cmp.Compare(a.Key, b.Key)
	}) {
		t.Fatalf("violations = %+v, want 3 sorted by rule then key", vs)
	}
}

// Verify (S2.2): a clean run passes. Run pages through every shard with the
// cursor (a small limit forces many pages) and reports each shard's backlog.
func TestRunCleanOnMemstore(t *testing.T) {
	s, sd := seeded(t, 40)
	rep, err := invcheck.Run(testCtx(t), s, invcheck.Options{PageLimit: 7})
	if err != nil || !rep.OK() || !rep.Quiescent || rep.Verdict() != "clean" {
		t.Fatalf("Run = %s %+v, %v; want clean and quiescent", rep.Verdict(), rep.Violations, err)
	}
	if rep.Requests != sd.Requests || rep.Resources != sd.Resources {
		t.Fatalf("read %d requests and %d resources, seeded %d and %d", rep.Requests, rep.Resources, sd.Requests, sd.Resources)
	}
	if len(rep.AppliedBefore) != s.Shards() || rep.AppliedBefore[0] == 0 {
		t.Fatalf("applied before %v, want every shard's index", rep.AppliedBefore)
	}
	if rep.Pages < 3 { // one page per kind would mean the walk never used a cursor
		t.Fatalf("%d pages for %d records at limit 7; the walk did not page", rep.Pages, sd.Requests+sd.Resources)
	}
	if len(rep.Outbox) != s.Shards() {
		t.Fatalf("backlog for %d shards, want %d: %+v", len(rep.Outbox), s.Shards(), rep.Outbox)
	}
	total := 0
	for i, b := range rep.Outbox {
		if b.Shard != i || b.AtLeast || (b.Pending > 0) != (b.OldestAt == invchecktest.T0) {
			t.Fatalf("backlog %d = %+v", i, b)
		}
		total += b.Pending
	}
	if total != sd.Events {
		t.Fatalf("backlog %d events, seeded %d unpublished", total, sd.Events)
	}
}

// A backlog larger than one outbox page is reported as at least that page.
func TestRunBacklogAtLeast(t *testing.T) {
	s, sd := seeded(t, invcheck.OutboxLimit, memstore.WithShards(1))
	rep, err := invcheck.Run(testCtx(t), s, invcheck.Options{})
	if err != nil || len(rep.Outbox) != 1 {
		t.Fatalf("Run = %+v, %v", rep.Outbox, err)
	}
	if b := rep.Outbox[0]; sd.Events <= invcheck.OutboxLimit || b.Pending != invcheck.OutboxLimit || !b.AtLeast {
		t.Fatalf("backlog %+v for %d events, want at least %d", b, sd.Events, invcheck.OutboxLimit)
	}
}

// tampered serves Scan pages with fn applied to each record: a corruption
// the FSM itself would refuse to commit.
type tampered struct {
	store.Store
	fn func(r *store.Record)
}

func (t tampered) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	res, err := t.Store.Scan(ctx, q)
	for i := range res.Records {
		r := res.Records[i]
		if r.Request != nil {
			q := *r.Request
			r.Request = &q
		}
		if r.Resource != nil {
			x := *r.Resource
			r.Resource = &x
		}
		t.fn(&r)
		res.Records[i] = r
	}
	return res, err
}

// Verify (S2.2): Run, reading through store.Store, catches each corrupted
// fixture: a double claim, a missing reciprocal claim and a terminal request
// holding a claim. Each is a completed check that is not OK, not an error.
func TestRunCatchesCorruption(t *testing.T) {
	s, _ := seeded(t, 4)
	recs := records(t, s)
	var matchedClaim, cancelledAttempt store.Claim
	for _, r := range recs {
		if r.Key == resMatched {
			matchedClaim = *r.Resource.ActiveClaim
		}
		if r.Key == reqCancelled {
			cancelledAttempt = store.Claim{RequestID: r.Request.ID, AttemptID: r.Request.CurrentAttempt.AttemptID}
		}
	}
	for _, tc := range []struct {
		name string
		fn   func(r *store.Record)
		want invcheck.Rule
	}{
		{"double claim", func(r *store.Record) {
			if r.Key == resExtra0 {
				c := matchedClaim
				r.Resource.Status, r.Resource.ActiveClaim = store.ResourceAssigned, &c
			}
		}, invcheck.RequestDoubleClaim},
		{"missing reciprocal claim", func(r *store.Record) {
			if r.Key == resMatched {
				r.Resource.Status, r.Resource.ActiveClaim = store.ResourceAvailable, nil
			}
		}, invcheck.MissingReciprocal},
		{"terminal request holding a claim", func(r *store.Record) {
			if r.Key == resCancelled {
				c := cancelledAttempt
				r.Resource.Status, r.Resource.ActiveClaim = store.ResourceAssigned, &c
			}
		}, invcheck.TerminalHoldsClaim},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep, err := invcheck.Run(testCtx(t), tampered{Store: s, fn: tc.fn}, invcheck.Options{PageLimit: 5})
			if err != nil {
				t.Fatalf("Run: %v; a corruption is a finding, not an error", err)
			}
			if rep.OK() || !rules(rep.Violations)[tc.want] {
				t.Fatalf("violations = %+v, want %s", rep.Violations, tc.want)
			}
		})
	}
}

var errBroken = errors.New("broken on purpose")

// Run never reports success from a partial read: a shard that stays down,
// a scan that keeps failing or a failing outbox read is ErrIncomplete. A
// shard that fails for a few pages and comes back is resumed by the cursor,
// and the check is complete.
func TestRunRefusesAPartialRead(t *testing.T) {
	s, sd := seeded(t, 20)
	opt := invcheck.Options{PageLimit: 5, MaxPartialPages: 3, PartialBackoff: time.Millisecond}
	for _, tc := range []struct {
		name string
		hook func(scans *atomic.Int64) memstore.Hook
	}{
		{"shard down for the whole walk", func(*atomic.Int64) memstore.Hook {
			return func(c memstore.Call) memstore.Fault {
				if c.Method == "Scan" {
					return memstore.Fault{FailShards: []int{1}}
				}
				return memstore.Fault{}
			}
		}},
		{"scan failing from the third page on", func(scans *atomic.Int64) memstore.Hook {
			return func(c memstore.Call) memstore.Fault {
				if c.Method == "Scan" && scans.Add(1) >= 3 {
					return memstore.Fault{Before: func(context.Context) error { return errBroken }}
				}
				return memstore.Fault{}
			}
		}},
		{"outbox read error", func(*atomic.Int64) memstore.Hook {
			return func(c memstore.Call) memstore.Fault {
				if c.Method == "PendingOutbox" {
					return memstore.Fault{Before: func(context.Context) error { return errBroken }}
				}
				return memstore.Fault{}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scans atomic.Int64
			rep, err := invcheck.Run(testCtx(t), memstore.WithFaults(s, tc.hook(&scans)), opt)
			if !errors.Is(err, invcheck.ErrIncomplete) {
				t.Fatalf("Run = OK %v, %v; want ErrIncomplete", rep.OK(), err)
			}
		})
	}

	t.Run("shard back after two partial pages", func(t *testing.T) {
		var scans atomic.Int64
		flaky := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
			if c.Method == "Scan" {
				if n := scans.Add(1); n == 2 || n == 3 {
					return memstore.Fault{FailShards: []int{1}}
				}
			}
			return memstore.Fault{}
		})
		rep, err := invcheck.Run(testCtx(t), flaky, opt)
		if err != nil || !rep.OK() || rep.PartialPages != 2 {
			t.Fatalf("Run = %d partial pages, %+v, %v; want OK after 2 resumed pages", rep.PartialPages, rep.Violations, err)
		}
		if rep.Requests != sd.Requests || rep.Resources != sd.Resources {
			t.Fatalf("read %d requests and %d resources, seeded %d and %d", rep.Requests, rep.Resources, sd.Requests, sd.Resources)
		}
	})
}

// A write committing during the walk moves a shard's applied index: the
// verdict says so, clean or not, so a busy cluster's check is never read as
// evidence about one state.
func TestRunNotQuiescent(t *testing.T) {
	s, _ := seeded(t, 20)
	busy := func() store.Store {
		var scans atomic.Int64
		return memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
			if c.Method == "Scan" && scans.Add(1) == 3 {
				return memstore.Fault{Before: func(ctx context.Context) error {
					_, err := s.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: "busy-" + t.Name(), At: invchecktest.T0},
						Key: "kv:busy-" + t.Name(), Value: []byte(`1`)})
					return err
				}}
			}
			return memstore.Fault{}
		})
	}
	dropClaim := func(r *store.Record) {
		if r.Key == resMatched {
			r.Resource.Status, r.Resource.ActiveClaim = store.ResourceAvailable, nil
		}
	}
	t.Run("clean", func(t *testing.T) {
		rep, err := invcheck.Run(testCtx(t), busy(), invcheck.Options{PageLimit: 5})
		if err != nil || !rep.OK() || rep.Quiescent || rep.Verdict() != "clean (not quiescent)" {
			t.Fatalf("Run = %q, %v", rep.Verdict(), err)
		}
		moved := 0
		for sh, before := range rep.AppliedBefore {
			if rep.AppliedAfter[sh] != before {
				moved++
			}
		}
		if moved != 1 {
			t.Fatalf("applied %v -> %v, want exactly one shard moved", rep.AppliedBefore, rep.AppliedAfter)
		}
	})
	t.Run("violations", func(t *testing.T) {
		rep, err := invcheck.Run(testCtx(t), tampered{Store: busy(), fn: dropClaim}, invcheck.Options{PageLimit: 5})
		if err != nil || rep.Verdict() != "violations (not quiescent)" {
			t.Fatalf("Run = %q, %v", rep.Verdict(), err)
		}
	})
}

// MaxBacklog, when set, turns a larger backlog into a violation per shard;
// unset, the backlog is only reported. It must be below one outbox page.
func TestRunMaxBacklog(t *testing.T) {
	s, _ := seeded(t, 4)
	ptr := func(n int) *int { return &n }
	rep, err := invcheck.Run(testCtx(t), s, invcheck.Options{})
	if err != nil || !rep.OK() {
		t.Fatalf("unset: %+v, %v", rep.Violations, err)
	}
	most, busy := 0, 0
	for _, b := range rep.Outbox {
		most = max(most, b.Pending)
		if b.Pending > 0 {
			busy++
		}
	}

	rep, err = invcheck.Run(testCtx(t), s, invcheck.Options{MaxBacklog: ptr(0)})
	if err != nil || len(rep.Violations) != busy || !maps.Equal(rules(rep.Violations), set(invcheck.OutboxBacklog)) {
		t.Fatalf("max 0: %+v, %v; want one outbox-backlog violation for each of %d shards", rep.Violations, err, busy)
	}
	if rep, err := invcheck.Run(testCtx(t), s, invcheck.Options{MaxBacklog: ptr(most)}); err != nil || !rep.OK() {
		t.Fatalf("max %d: %+v, %v; want OK", most, rep.Violations, err)
	}
	if rep, err := invcheck.Run(testCtx(t), s, invcheck.Options{MaxBacklog: ptr(most - 1)}); err != nil || rep.OK() {
		t.Fatalf("max %d: %+v, %v; want a violation", most-1, rep.Violations, err)
	}
	for _, bad := range []int{-1, invcheck.OutboxLimit} {
		if _, err := invcheck.Run(testCtx(t), s, invcheck.Options{MaxBacklog: ptr(bad)}); err == nil || errors.Is(err, invcheck.ErrIncomplete) {
			t.Fatalf("max %d: %v, want an option error", bad, err)
		}
	}
}
