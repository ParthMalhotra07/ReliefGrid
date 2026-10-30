package workflow

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	fixtures "github.com/ParthMalhotra07/ReliefGrid/tests/workflow"
)

var testNow = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

func testStore(t *testing.T, opts ...memstore.Option) (context.Context, *memstore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	s, err := memstore.New(ctx, append([]memstore.Option{memstore.WithShards(3)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return ctx, s
}

func testConfig(s store.Store, seed uint64) Config {
	sequence := 0
	return Config{
		Store: s, Seed: seed, ScanLimit: 1000,
		Now: func() time.Time { return testNow },
		NewID: func() string {
			sequence++
			return fmt.Sprintf("test-id-%d", sequence)
		},
	}
}

func request(id string) store.Request {
	return store.Request{
		ID: id, Type: store.TypeWater, Location: store.Location{}, Geohash: "s000000",
		PeopleAffected: 1, Urgency: 1,
	}
}

// Fixed index cells are independent fixtures, not a second matching implementation.
func resource(id string, lng float64) store.Resource {
	hashes := map[float64]string{
		0: "s000000", .01: "s00000p", .02: "s00002n", .03: "s00008j",
		.04: "s0000bj", .6: "s002j8h", -.000001: "ebpbpbp", .000001: "s000000",
		179.99: "xbpbpb0", -179.99: "800000p",
	}
	hash, ok := hashes[lng]
	if !ok {
		panic(fmt.Sprintf("missing index fixture for longitude %v", lng))
	}
	return store.Resource{
		ID: id, Type: store.TypeWater, Name: id, Location: store.Location{Lng: lng},
		Geohash: hash, AvailableFrom: testNow.Add(-time.Hour).UnixMilli(),
		AvailableUntil: testNow.Add(time.Hour).UnixMilli(), Status: store.ResourceAvailable,
		Reusable: true, LocationRevision: 1,
	}
}

func createRequest(t *testing.T, ctx context.Context, s store.Store, r store.Request) {
	t.Helper()
	result, err := s.CreateEntity(ctx, store.CreateEntity{
		Meta: store.Meta{OperationID: "create-request-" + r.ID, At: testNow.UnixMilli()},
		Key:  store.RequestKey(r.ID), Request: &r,
	})
	mustOutcome(t, result, err, store.OutcomeOK)
}

func createResource(t *testing.T, ctx context.Context, s store.Store, r store.Resource) {
	t.Helper()
	result, err := s.CreateEntity(ctx, store.CreateEntity{
		Meta: store.Meta{OperationID: "create-resource-" + r.ID, At: testNow.UnixMilli()},
		Key:  store.ResourceKey(r.ID), Resource: &r,
	})
	mustOutcome(t, result, err, store.OutcomeOK)
}

func mustOutcome(t *testing.T, result store.Result, err error, want store.Outcome) {
	t.Helper()
	if err != nil || result.Outcome != want {
		t.Fatalf("outcome = %+v, %v; want %s", result, err, want)
	}
}

func read(t *testing.T, ctx context.Context, s store.Store, key string) store.Record {
	t.Helper()
	r, err := s.ReadRecord(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func matched(t *testing.T, ctx context.Context, s store.Store, requestID string) string {
	t.Helper()
	r := read(t, ctx, s, store.RequestKey(requestID))
	if r.Request == nil || r.Request.Status != store.RequestMatched || r.Request.CurrentAttempt == nil {
		t.Fatalf("request not matched: %+v", r)
	}
	a := r.Request.CurrentAttempt
	x := read(t, ctx, s, store.ResourceKey(a.ResourceID))
	if x.Resource == nil || x.Resource.Status != store.ResourceAssigned || x.Resource.ActiveClaim == nil {
		t.Fatalf("missing resource claim: %+v", x)
	}
	c := x.Resource.ActiveClaim
	if c.RequestID != requestID || c.AttemptID != a.AttemptID || c.Fingerprint != a.Fingerprint ||
		a.Fingerprint == "" || r.Request.AssignedResourceID != a.ResourceID {
		t.Fatalf("nonreciprocal claim: request=%+v resource=%+v", r.Request, x.Resource)
	}
	return a.ResourceID
}

// traceStore observes calls and injects interleavings; every outcome comes from
// S's actual SQLite FSM. It does not implement claim, CAS or dedupe rules.
type traceStore struct {
	store.Store
	claims           []store.TryClaim
	scans            []store.ScanQuery
	transitions      []store.TransitionRequest
	beforeTransition func(context.Context, store.TransitionRequest)
	beforeClaim      func(context.Context, store.TryClaim)
	afterClaim       func(context.Context, store.TryClaim, store.Result, error) (store.Result, error)
}

func (s *traceStore) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	s.scans = append(s.scans, q)
	return s.Store.Scan(ctx, q)
}

func (s *traceStore) TransitionRequest(ctx context.Context, cmd store.TransitionRequest) (store.Result, error) {
	s.transitions = append(s.transitions, cmd)
	if s.beforeTransition != nil {
		s.beforeTransition(ctx, cmd)
	}
	return s.Store.TransitionRequest(ctx, cmd)
}

func (s *traceStore) TryClaim(ctx context.Context, cmd store.TryClaim) (store.Result, error) {
	s.claims = append(s.claims, cmd)
	if s.beforeClaim != nil {
		s.beforeClaim(ctx, cmd)
	}
	r, err := s.Store.TryClaim(ctx, cmd)
	if s.afterClaim != nil {
		return s.afterClaim(ctx, cmd, r, err)
	}
	return r, err
}

func schedule(t *testing.T, prefix string) fixtures.ScheduleFixture {
	t.Helper()
	for _, f := range fixtures.Schedules {
		if strings.HasPrefix(f.Tag, prefix+"_") {
			return f
		}
	}
	t.Fatalf("missing A0.2 schedule %s", prefix)
	return fixtures.ScheduleFixture{}
}

func assertStates(t *testing.T, ctx context.Context, s store.Store, states []fixtures.EntityState, attemptID string) {
	t.Helper()
	for _, want := range states {
		key := store.ResourceKey("Res")
		if want.Entity == "R" {
			key = store.RequestKey("R")
		}
		got := read(t, ctx, s, key)
		if got.Version != uint64(want.Version) {
			t.Errorf("%s version = %d; want %d", key, got.Version, want.Version)
		}
		if got.Request != nil {
			if got.Request.Status != want.ReqStatus {
				t.Errorf("%s status = %s; want %s", key, got.Request.Status, want.ReqStatus)
			}
			if want.ActiveClaim != "" && (got.Request.CurrentAttempt == nil || got.Request.CurrentAttempt.AttemptID != attemptID) {
				t.Errorf("%s missing intent %s", key, attemptID)
			}
		} else if got.Resource != nil {
			if got.Resource.Status != want.ResStatus {
				t.Errorf("%s status = %s; want %s", key, got.Resource.Status, want.ResStatus)
			}
			claim := got.Resource.ActiveClaim
			if (want.ActiveClaim == "" && claim != nil) || (want.ActiveClaim != "" && (claim == nil || claim.AttemptID != attemptID)) {
				t.Errorf("%s unexpected claim: %+v", key, claim)
			}
		}
	}
}
