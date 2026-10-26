package workflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestMatchFiltersTypeRadiusWindowAndStatus(t *testing.T) {
	ctx, s := testStore(t)
	createRequest(t, ctx, s, request("R"))
	wrongType := resource("wrong-type", 0)
	wrongType.Type = store.TypeFood
	future := resource("future", 0)
	future.AvailableFrom = testNow.Add(time.Minute).UnixMilli()
	expired := resource("expired", 0)
	expired.AvailableUntil = testNow.UnixMilli()
	offline := resource("offline", 0)
	offline.Status = store.ResourceOffline
	consumed := resource("consumed", 0)
	consumed.Status = store.ResourceConsumed
	invalid := []store.Resource{wrongType, resource("outside-50-km", .6), future, expired, offline, consumed}
	for _, r := range append(invalid, resource("eligible", .01)) {
		createResource(t, ctx, s, r)
	}
	m, err := New(testConfig(s, 7))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	if got := matched(t, ctx, s, "R"); got != "eligible" {
		t.Errorf("matched %s; want eligible", got)
	}
	for _, r := range invalid {
		if got := read(t, ctx, s, store.ResourceKey(r.ID)); got.Resource.ActiveClaim != nil {
			t.Errorf("ineligible resource claimed: %+v", got.Resource)
		}
	}
}

func TestMatchAvailabilityInclusiveStartExclusiveEnd(t *testing.T) {
	for _, tc := range []struct {
		name        string
		from, until int64
		eligible    bool
	}{
		{"start", testNow.UnixMilli(), testNow.Add(time.Minute).UnixMilli(), true},
		{"before end", testNow.Add(-time.Minute).UnixMilli(), testNow.UnixMilli() + 1, true},
		{"at end", testNow.Add(-time.Minute).UnixMilli(), testNow.UnixMilli(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, s := testStore(t)
			createRequest(t, ctx, s, request("R"))
			r := resource("Res", 0)
			r.AvailableFrom, r.AvailableUntil = tc.from, tc.until
			createResource(t, ctx, s, r)
			m, err := New(testConfig(s, 1))
			if err != nil {
				t.Fatal(err)
			}
			result, err := m.Match(ctx, "R")
			if err != nil {
				t.Fatal(err)
			}
			if tc.eligible {
				matched(t, ctx, s, "R")
			} else {
				if result.Outcome != "NO_CANDIDATE" {
					t.Errorf("outcome = %s; want NO_CANDIDATE", result.Outcome)
				}
				if got := read(t, ctx, s, store.ResourceKey("Res")); got.Resource.ActiveClaim != nil {
					t.Fatal("expired resource was claimed")
				}
			}
		})
	}
}

func TestMatchAcrossGeohashBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		r, x   store.Location
		rh, xh string
	}{
		{"prime meridian", store.Location{Lng: -.000001}, store.Location{Lng: .000001}, "ebpbpbp", "s000000"},
		{"dateline", store.Location{Lng: 179.99}, store.Location{Lng: -179.99}, "xbpbpb0", "800000p"},
		{"pole", store.Location{Lat: 89.9, Lng: 90}, store.Location{Lat: 89.9, Lng: -90}, "ypbp2jb", "fpbp2jb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, s := testStore(t)
			r := request("R")
			r.Location, r.Geohash = tc.r, tc.rh
			createRequest(t, ctx, s, r)
			x := resource("Res", 0)
			x.Location, x.Geohash = tc.x, tc.xh
			createResource(t, ctx, s, x)
			m, err := New(testConfig(s, 1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.Match(ctx, "R"); err != nil {
				t.Fatal(err)
			}
			matched(t, ctx, s, "R")
		})
	}
}

func TestMatchSeededTopThreeStableAcrossEqualDistanceInsertionOrder(t *testing.T) {
	seen := make(map[string]bool)
	for seed := uint64(1); seed <= 12; seed++ {
		var first string
		for order, ids := range [][]string{{"d", "c", "b", "a"}, {"b", "a", "d", "c"}} {
			t.Run(fmt.Sprintf("seed-%d/order-%d", seed, order), func(t *testing.T) {
				ctx, s := testStore(t)
				createRequest(t, ctx, s, request("R"))
				for _, id := range ids {
					createResource(t, ctx, s, resource(id, .01))
				}
				m, err := New(testConfig(s, seed))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := m.Match(ctx, "R"); err != nil {
					t.Fatal(err)
				}
				got := matched(t, ctx, s, "R")
				if got != "a" && got != "b" && got != "c" {
					t.Fatalf("selected %s outside nearest three after ID tie-break", got)
				}
				if order == 0 {
					first = got
				} else if got != first {
					t.Errorf("same seed changed selection with insertion order: %s vs %s", first, got)
				}
				seen[got] = true
			})
		}
	}
	if len(seen) < 2 {
		t.Errorf("seeds 1..12 never spread work: %v", seen)
	}
}

func TestMatchPagesBeyondIneligibleFirstPage(t *testing.T) {
	ctx, raw := testStore(t, memstore.WithRouter(func(string) int { return 1 }))
	createRequest(t, ctx, raw, request("R"))
	future := resource("a-future", .01)
	future.AvailableFrom = testNow.Add(time.Minute).UnixMilli()
	createResource(t, ctx, raw, future)
	createResource(t, ctx, raw, resource("z-eligible", .02))
	s := &traceStore{Store: raw}
	cfg := testConfig(s, 1)
	cfg.ScanLimit = 1
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	if got := matched(t, ctx, raw, "R"); got != "z-eligible" {
		t.Fatalf("matched %s; want second-page candidate", got)
	}
	paged := false
	for _, q := range s.scans {
		if q.Limit != 1 {
			t.Errorf("scan limit = %d; want configured limit 1", q.Limit)
		}
		paged = paged || q.Cursor != ""
	}
	if !paged {
		t.Fatal("no cursor continuation was used")
	}
}

func TestMatchPartialScanNeverCommitsNoCandidate(t *testing.T) {
	f := schedule(t, "SCH17")
	ctx, raw := testStore(t, memstore.WithRouter(func(key string) int {
		if key == store.ResourceKey("Res") {
			return 1
		}
		return 0
	}))
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	assertStates(t, ctx, raw, f.InitialState, "")
	fail := true
	s := memstore.WithFaults(raw, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" && fail {
			return memstore.Fault{FailShards: []int{1}}
		}
		return memstore.Fault{}
	})
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	result, err := m.Match(ctx, "R")
	if err == nil || result.Outcome == "NO_CANDIDATE" {
		t.Fatalf("partial scan = %+v, %v; want incomplete work", result, err)
	}
	assertStates(t, ctx, raw, f.InitialState, "")
	for shard := range raw.Shards() {
		events, err := raw.PendingOutbox(ctx, shard, 100)
		if err != nil || len(events) != 0 {
			t.Fatalf("partial scan persisted handoff: %v, %v", events, err)
		}
	}
	fail = false
	if _, err := m.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	matched(t, ctx, raw, "R")
	a := read(t, ctx, raw, store.RequestKey("R")).Request.CurrentAttempt
	assertStates(t, ctx, raw, f.FinalState, a.AttemptID)
}

func TestMatchEmptyHintScanRequiresAuthoritativeFallback(t *testing.T) {
	ctx, raw := testStore(t)
	_, hints := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	authoritative := &traceStore{Store: raw}
	cfg := testConfig(authoritative, 1)
	cfg.CandidateScanner = hints
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	matched(t, ctx, raw, "R")
	if len(authoritative.scans) == 0 {
		t.Fatal("stale empty hint scan was not checked authoritatively")
	}
}

func TestMatchNoCandidateHasCommittedRetryHandoff(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	s := &traceStore{Store: raw}
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	result, err := m.Match(ctx, "R")
	if err != nil || result.Outcome != "NO_CANDIDATE" {
		t.Fatalf("empty complete scan = %+v, %v", result, err)
	}
	r := read(t, ctx, raw, store.RequestKey("R"))
	if len(s.scans) == 0 || r.Request.Status != store.RequestWaiting || r.Request.CurrentAttempt != nil ||
		r.Request.NextAttemptAt <= testNow.UnixMilli() {
		t.Fatalf("no durable waiting schedule: %+v", r.Request)
	}
	retry := false
	for shard := range raw.Shards() {
		events, err := raw.PendingOutbox(ctx, shard, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			retry = retry || (e.EventType == "request.retry" && e.EntityKey == r.Key && e.EntityVersion == r.Version)
		}
	}
	if !retry {
		t.Fatal("NO_CANDIDATE returned without retry outbox at committed request version")
	}
}

func TestMatchClaimsOnlyAfterCommittedBoundIntent(t *testing.T) {
	ctx, raw := testStore(t)
	createRequest(t, ctx, raw, request("R"))
	createResource(t, ctx, raw, resource("Res", .01))
	s := &traceStore{Store: raw}
	s.beforeClaim = func(ctx context.Context, cmd store.TryClaim) {
		r := read(t, ctx, raw, store.RequestKey(cmd.RequestID))
		if r.Request.Status != store.RequestMatching || r.Request.CurrentAttempt == nil || *r.Request.CurrentAttempt != cmd.Attempt ||
			cmd.ResourceKey != store.ResourceKey(cmd.Attempt.ResourceID) || cmd.Attempt.Fingerprint == "" {
			t.Fatalf("claim precedes immutable matching intent: request=%+v command=%+v", r.Request, cmd)
		}
	}
	m, err := New(testConfig(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Match(ctx, "R"); err != nil {
		t.Fatal(err)
	}
	matched(t, ctx, raw, "R")
	if len(s.claims) != 1 {
		t.Fatalf("claim calls = %d; want 1", len(s.claims))
	}
}
