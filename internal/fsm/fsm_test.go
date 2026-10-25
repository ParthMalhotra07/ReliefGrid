package fsm_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// t0 is a fixed command timestamp; the FSM never reads a clock.
const (
	t0   = int64(1_792_000_000_000)
	hour = int64(3_600_000)
)

var dbSeq atomic.Int64

type harness struct {
	t   *testing.T
	f   *fsm.FSM
	idx uint64
	log [][]byte
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dsn := fmt.Sprintf("file:fsmtest%d?mode=memory", dbSeq.Add(1))
	f, err := fsm.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return &harness{t: t, f: f}
}

// apply encodes body as the next log entry and applies it.
func (h *harness) apply(body any) store.Result {
	h.t.Helper()
	entry, err := fsm.Encode(body)
	if err != nil {
		h.t.Fatalf("Encode(%T): %v", body, err)
	}
	return h.applyEntry(entry)
}

func (h *harness) applyEntry(entry []byte) store.Result {
	h.t.Helper()
	h.idx++
	res, err := h.f.Apply(h.idx, entry)
	if err != nil {
		h.t.Fatalf("Apply(%d): %v", h.idx, err)
	}
	h.log = append(h.log, entry)
	return res
}

func (h *harness) read(key string) store.Record {
	h.t.Helper()
	rec, err := h.f.ReadRecord(context.Background(), key)
	if err != nil {
		h.t.Fatalf("ReadRecord(%s): %v", key, err)
	}
	return rec
}

func expectOutcome(t *testing.T, what string, got store.Result, want store.Outcome) {
	t.Helper()
	if got.Outcome != want {
		t.Fatalf("%s: outcome = %s (%s), want %s", what, got.Outcome, got.Reason, want)
	}
}

func meta(op string) store.Meta { return store.Meta{OperationID: op, At: t0} }

func resource(id string, typ store.ResourceType, status store.ResourceStatus) *store.Resource {
	return &store.Resource{
		ID: id, Type: typ, Name: "unit " + id,
		Location: store.Location{Lat: 28.61, Lng: 77.21}, Geohash: "ttnfv",
		AvailableFrom: t0 - hour, AvailableUntil: t0 + 24*hour,
		Status: status, Reusable: true,
	}
}

func request(id string, typ store.ResourceType) *store.Request {
	return &store.Request{
		ID: id, Type: typ,
		Location: store.Location{Lat: 28.62, Lng: 77.22}, Geohash: "ttnfv",
		PeopleAffected: 4, Urgency: 2, Status: store.RequestPending,
	}
}

func attempt(id, resourceID string) store.Attempt {
	return store.Attempt{AttemptID: id, ResourceID: resourceID, Fingerprint: "fp-" + id}
}

func createResource(h *harness, r *store.Resource) {
	h.t.Helper()
	res := h.apply(store.CreateEntity{Meta: meta("create-" + r.ID), Key: store.ResourceKey(r.ID), Resource: r})
	expectOutcome(h.t, "create "+r.ID, res, store.OutcomeOK)
}

func tryClaim(op, resourceID, requestID string, a store.Attempt, typ store.ResourceType) store.TryClaim {
	return store.TryClaim{Meta: meta(op), ResourceKey: store.ResourceKey(resourceID), RequestID: requestID, Attempt: a, Type: typ}
}

func closeAttempt(op, resourceID, requestID string, a store.Attempt) store.CloseAttempt {
	return store.CloseAttempt{
		Meta: meta(op), ResourceKey: store.ResourceKey(resourceID), RequestID: requestID,
		Attempt: a, Disposition: store.ResourceAvailable, Reason: "cancelled",
	}
}

// Verify: for two same-version CAS commands, exactly one wins. Both carry
// expected version 1, as if both were proposed before either applied; the
// second sees the first one's new version.
func TestCompareAndSetSameVersionExactlyOneWins(t *testing.T) {
	h := newHarness(t)
	created := h.apply(store.CompareAndSet{Meta: meta("kv-create"), Key: "kv:counter", ExpectedVersion: 0, Value: json.RawMessage(`"v0"`)})
	expectOutcome(t, "create", created, store.OutcomeOK)
	if created.Version != 1 {
		t.Fatalf("create version = %d, want 1", created.Version)
	}

	first := h.apply(store.CompareAndSet{Meta: meta("kv-a"), Key: "kv:counter", ExpectedVersion: 1, Value: json.RawMessage(`"a"`)})
	second := h.apply(store.CompareAndSet{Meta: meta("kv-b"), Key: "kv:counter", ExpectedVersion: 1, Value: json.RawMessage(`"b"`)})

	expectOutcome(t, "first", first, store.OutcomeOK)
	expectOutcome(t, "second", second, store.OutcomeVersionConflict)
	if first.Version != 2 || second.Version != 2 {
		t.Fatalf("versions = %d, %d; want 2 (new) and 2 (current on conflict)", first.Version, second.Version)
	}
	rec := h.read("kv:counter")
	if rec.Version != 2 || string(rec.Value) != `"a"` {
		t.Fatalf("record = v%d %s, want v2 \"a\"", rec.Version, rec.Value)
	}
}

func TestCompareAndSetRejectsDomainKeys(t *testing.T) {
	h := newHarness(t)
	createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
	res := h.apply(store.CompareAndSet{Meta: meta("kv-bypass"), Key: store.ResourceKey("r1"), ExpectedVersion: 1, Delete: true})
	expectOutcome(t, "generic CAS on res:", res, store.OutcomeDomainConflict)
	if rec := h.read(store.ResourceKey("r1")); rec.Deleted || rec.Version != 1 {
		t.Fatalf("resource changed by generic CAS: %+v", rec)
	}
}

// Verify: replaying the same log gives identical rows. The log exercises
// every command and every table.
func TestReplaySameLogGivesIdenticalRows(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	created := store.OutboxEvent{EventID: "ev-q1-created", EventType: "request.created", SchemaVersion: 1,
		Exchange: "relief.requests", RoutingKey: "request.created.water", Payload: json.RawMessage(`{"id":"q1"}`)}
	matched := store.OutboxEvent{EventID: "ev-q1-matched", EventType: "match.completed", SchemaVersion: 1,
		Exchange: "relief.events", RoutingKey: "match.completed", Payload: json.RawMessage(`{"id":"q1"}`)}
	retry := store.OutboxEvent{EventID: "ev-q2-retry", EventType: "request.retry", SchemaVersion: 1,
		Exchange: "relief.retry", RoutingKey: "delay.30s", Payload: json.RawMessage(`{"id":"q2"}`)}
	a1, a2, a3 := attempt("a1", "r1"), attempt("a2", "r1"), attempt("a3", "r2")

	results := []store.Result{
		h.apply(store.CreateEntity{Meta: meta("c-r1"), Key: store.ResourceKey("r1"), Resource: resource("r1", store.TypeWater, store.ResourceAvailable)}),
		h.apply(store.CreateEntity{Meta: meta("c-r2"), Key: store.ResourceKey("r2"), Resource: resource("r2", store.TypeWater, store.ResourceAvailable)}),
		h.apply(store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater), Outbox: []store.OutboxEvent{created}}),
		h.apply(store.CreateEntity{Meta: meta("c-q2"), Key: store.RequestKey("q2"), Request: request("q2", store.TypeWater)}),
		h.apply(store.CompareAndSet{Meta: meta("kv-1"), Key: "kv:demo", Value: json.RawMessage(`1`)}),
		h.apply(store.CompareAndSet{Meta: meta("kv-2"), Key: "kv:demo", ExpectedVersion: 1, Value: json.RawMessage(`2`)}),
		h.apply(store.CompareAndSet{Meta: meta("kv-3"), Key: "kv:demo", ExpectedVersion: 1, Value: json.RawMessage(`3`)}), // conflict
		h.apply(store.TransitionRequest{Meta: meta("t-q1-matching"), Key: store.RequestKey("q1"), ExpectedVersion: 1,
			ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a1}),
		h.apply(tryClaim("claim-a1", "r1", "q1", a1, store.TypeWater)),
		h.apply(store.TransitionRequest{Meta: meta("t-q1-matched"), Key: store.RequestKey("q1"), ExpectedVersion: 2,
			ExpectedStatus: store.RequestMatching, ExpectedAttemptID: "a1", To: store.RequestMatched, Outbox: []store.OutboxEvent{matched}}),
		h.apply(tryClaim("claim-a2", "r1", "q2", a2, store.TypeWater)), // CONFLICT_CLOSED
		h.apply(closeAttempt("close-a3", "r2", "q2", a3)),              // closure before any claim
		h.apply(store.TransitionRequest{Meta: meta("t-q2-waiting"), Key: store.RequestKey("q2"), ExpectedVersion: 1,
			ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: t0 + 30_000, Outbox: []store.OutboxEvent{retry}}),
		h.apply(store.RetryWorkflow{Meta: meta("retry-q2"), Key: store.RequestKey("q2"), ExpectedVersion: 2, NextAttemptAt: t0 + 120_000, LastError: "no candidate"}),
		h.apply(store.MarkOutboxPublished{Meta: meta("mark-1"), EventIDs: []string{"ev-q1-created"}}),
	}
	want := []store.Outcome{
		store.OutcomeOK, store.OutcomeOK, store.OutcomeOK, store.OutcomeOK, store.OutcomeOK, store.OutcomeOK,
		store.OutcomeVersionConflict, store.OutcomeOK, store.ClaimSuccess, store.OutcomeOK,
		store.ClaimConflictClosed, store.OutcomeOK, store.OutcomeOK, store.OutcomeOK, store.OutcomeOK,
	}
	for i, r := range results {
		expectOutcome(t, fmt.Sprintf("entry %d", i+1), r, want[i])
	}

	original, err := h.f.Dump(ctx)
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	for _, table := range []string{"records", "operation_results", "attempt_closures", "outbox", "meta"} {
		if len(original[table]) == 0 {
			t.Errorf("table %s is empty after the log; replay would compare nothing", table)
		}
	}

	replica := newHarness(t)
	var replayed []store.Result
	for _, entry := range h.log {
		replayed = append(replayed, replica.applyEntry(entry))
	}
	if !reflect.DeepEqual(results, replayed) {
		t.Errorf("replay results differ:\n got %+v\nwant %+v", replayed, results)
	}
	got, err := replica.f.Dump(ctx)
	if err != nil {
		t.Fatalf("replica Dump: %v", err)
	}
	if !reflect.DeepEqual(original, got) {
		t.Fatalf("replayed rows differ:\n got %v\nwant %v", got, original)
	}
	if replica.f.AppliedIndex() != h.f.AppliedIndex() || h.f.AppliedIndex() != uint64(len(h.log)) {
		t.Fatalf("applied index = %d / %d, want %d", replica.f.AppliedIndex(), h.f.AppliedIndex(), len(h.log))
	}
}

// Verify: the same operation ID with a different payload is rejected, and
// neither the entity nor the stored result changes.
func TestOperationIDDedupe(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.apply(store.CompareAndSet{Meta: meta("kv-create"), Key: "kv:x", Value: json.RawMessage(`"v0"`)})

	cmd := store.CompareAndSet{Meta: meta("op-1"), Key: "kv:x", ExpectedVersion: 1, Value: json.RawMessage(`"a"`)}
	first := h.apply(cmd)
	expectOutcome(t, "first", first, store.OutcomeOK)

	again := h.apply(cmd)
	if !again.Replayed || again.Outcome != first.Outcome || again.Version != first.Version {
		t.Fatalf("identical retry = %+v, want replay of %+v", again, first)
	}

	changed := cmd
	changed.Value = json.RawMessage(`"b"`)
	expectOutcome(t, "changed payload", h.apply(changed), store.OutcomeOperationIDReuse)

	otherOp := store.CompareAndSet{Meta: meta("op-1"), Key: "kv:y", Value: json.RawMessage(`"z"`)}
	expectOutcome(t, "same ID, other key", h.apply(otherOp), store.OutcomeOperationIDReuse)

	rec := h.read("kv:x")
	if rec.Version != 2 || string(rec.Value) != `"a"` {
		t.Fatalf("record = v%d %s, want v2 \"a\"", rec.Version, rec.Value)
	}
	if _, err := h.f.ReadRecord(ctx, "kv:y"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("kv:y read err = %v, want ErrNotFound", err)
	}
	stored, err := h.f.Operation(ctx, "op-1")
	if err != nil {
		t.Fatalf("Operation: %v", err)
	}
	if stored.Outcome != store.OutcomeOK || stored.Version != 2 || stored.Key != "kv:x" {
		t.Fatalf("stored result = %+v, want the original OK at v2", stored)
	}
}

// The five TryClaim outcomes of v3.1 §17.
func TestTryClaimOutcomes(t *testing.T) {
	ctx := context.Background()
	a, b := attempt("a", "r1"), attempt("b", "r1")

	t.Run("closure exists: CLOSED, no claim", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		expectOutcome(t, "close", h.apply(closeAttempt("close-a", "r1", "q1", a)), store.OutcomeOK)
		expectOutcome(t, "claim", h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater)), store.ClaimClosed)
		r := h.read(store.ResourceKey("r1")).Resource
		if r.ActiveClaim != nil || r.Status != store.ResourceAvailable {
			t.Fatalf("resource after CLOSED = %s %+v, want AVAILABLE with no claim", r.Status, r.ActiveClaim)
		}
	})

	t.Run("same attempt holds it: SUCCESS, no second claim", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		first := h.apply(tryClaim("claim-a-1", "r1", "q1", a, store.TypeWater))
		expectOutcome(t, "first", first, store.ClaimSuccess)
		// A different worker re-issues the same bound attempt under a new ID.
		again := h.apply(tryClaim("claim-a-2", "r1", "q1", a, store.TypeWater))
		expectOutcome(t, "again", again, store.ClaimSuccess)
		if again.Replayed {
			t.Fatal("new operation ID must be evaluated, not replayed")
		}
		rec := h.read(store.ResourceKey("r1"))
		if rec.Version != first.Version {
			t.Fatalf("version %d -> %d: re-claim by the holder changed the resource", first.Version, rec.Version)
		}
		if c := rec.Resource.ActiveClaim; c == nil || c.AttemptID != "a" || c.RequestID != "q1" {
			t.Fatalf("active claim = %+v, want q1/a", c)
		}
	})

	t.Run("available and eligible: SUCCESS, ASSIGNED", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		res := h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater))
		expectOutcome(t, "claim", res, store.ClaimSuccess)
		rec := h.read(store.ResourceKey("r1"))
		want := &store.Claim{RequestID: "q1", AttemptID: "a", Fingerprint: a.Fingerprint}
		if rec.Resource.Status != store.ResourceAssigned || !reflect.DeepEqual(rec.Resource.ActiveClaim, want) {
			t.Fatalf("resource = %s %+v, want ASSIGNED %+v", rec.Resource.Status, rec.Resource.ActiveClaim, want)
		}
		if rec.Version != 2 || res.Version != 2 {
			t.Fatalf("versions record=%d result=%d, want 2", rec.Version, res.Version)
		}
	})

	t.Run("another attempt holds it: CONFLICT_CLOSED with closure", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		expectOutcome(t, "holder", h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater)), store.ClaimSuccess)
		expectOutcome(t, "rival", h.apply(tryClaim("claim-b", "r1", "q2", b, store.TypeWater)), store.ClaimConflictClosed)

		if c := h.read(store.ResourceKey("r1")).Resource.ActiveClaim; c == nil || c.AttemptID != "a" {
			t.Fatalf("holder changed: %+v", c)
		}
		cl, err := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "b")
		if err != nil {
			t.Fatalf("closure for rejected attempt: %v", err)
		}
		if cl.RequestID != "q2" || cl.Fingerprint != b.Fingerprint {
			t.Fatalf("closure = %+v, want bound to q2/%s", cl, b.Fingerprint)
		}
		if _, err := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "a"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("holder's attempt was closed: err = %v", err)
		}
		// Even after the holder releases, the rejected attempt stays fenced.
		expectOutcome(t, "release a", h.apply(closeAttempt("close-a", "r1", "q1", a)), store.OutcomeOK)
		expectOutcome(t, "late b", h.apply(tryClaim("claim-b-late", "r1", "q2", b, store.TypeWater)), store.ClaimClosed)
	})

	ineligible := []struct {
		name string
		res  *store.Resource
		typ  store.ResourceType
	}{
		{"offline", resource("r1", store.TypeWater, store.ResourceOffline), store.TypeWater},
		{"consumed", resource("r1", store.TypeWater, store.ResourceConsumed), store.TypeWater},
		{"wrong type", resource("r1", store.TypeFood, store.ResourceAvailable), store.TypeWater},
		{"window not started", func() *store.Resource {
			r := resource("r1", store.TypeWater, store.ResourceAvailable)
			r.AvailableFrom = t0 + 1
			return r
		}(), store.TypeWater},
		{"window ended", func() *store.Resource {
			r := resource("r1", store.TypeWater, store.ResourceAvailable)
			r.AvailableUntil = t0 // exclusive
			return r
		}(), store.TypeWater},
	}
	for _, tc := range ineligible {
		t.Run("ineligible "+tc.name+": REJECTED_CLOSED with closure", func(t *testing.T) {
			h := newHarness(t)
			createResource(h, tc.res)
			before := h.read(store.ResourceKey("r1"))
			expectOutcome(t, "claim", h.apply(tryClaim("claim-a", "r1", "q1", a, tc.typ)), store.ClaimRejectedClosed)
			after := h.read(store.ResourceKey("r1"))
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("rejection changed the resource:\n before %+v\n after  %+v", before.Resource, after.Resource)
			}
			if _, err := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "a"); err != nil {
				t.Fatalf("no closure recorded: %v", err)
			}
		})
	}
}

// Verify: a TryClaim after CloseAttempt returns CLOSED, whichever of the two
// the claim was racing (cancellation before and after the claim, v3.1 §19).
func TestTryClaimAfterCloseAttemptReturnsClosed(t *testing.T) {
	ctx := context.Background()
	a := attempt("a", "r1")

	t.Run("close before claim arrives", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		expectOutcome(t, "close", h.apply(closeAttempt("close-a", "r1", "q1", a)), store.OutcomeOK)
		cl, err := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "a")
		if err != nil || cl.RequestID != "q1" || cl.Disposition != store.ResourceAvailable {
			t.Fatalf("closure = %+v, %v; want q1 with disposition AVAILABLE", cl, err)
		}
		expectOutcome(t, "late claim", h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater)), store.ClaimClosed)
		if r := h.read(store.ResourceKey("r1")).Resource; r.ActiveClaim != nil || r.Status != store.ResourceAvailable {
			t.Fatalf("late claim took the resource: %s %+v", r.Status, r.ActiveClaim)
		}
	})

	t.Run("claim then close releases, retry gets CLOSED", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		expectOutcome(t, "claim", h.apply(tryClaim("claim-a-1", "r1", "q1", a, store.TypeWater)), store.ClaimSuccess)
		expectOutcome(t, "close", h.apply(closeAttempt("close-a", "r1", "q1", a)), store.OutcomeOK)
		if r := h.read(store.ResourceKey("r1")).Resource; r.ActiveClaim != nil || r.Status != store.ResourceAvailable {
			t.Fatalf("close did not release: %s %+v", r.Status, r.ActiveClaim)
		}
		expectOutcome(t, "retry", h.apply(tryClaim("claim-a-2", "r1", "q1", a, store.TypeWater)), store.ClaimClosed)
	})
}

// Invariant 4: replaying an old SUCCESS after the attempt was closed returns
// the historical result but does not recreate the claim.
func TestHistoricalClaimSuccessIsNotOwnership(t *testing.T) {
	h := newHarness(t)
	a := attempt("a", "r1")
	createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
	claim := tryClaim("claim-a", "r1", "q1", a, store.TypeWater)
	expectOutcome(t, "claim", h.apply(claim), store.ClaimSuccess)
	expectOutcome(t, "close", h.apply(closeAttempt("close-a", "r1", "q1", a)), store.OutcomeOK)
	before := h.read(store.ResourceKey("r1"))

	old := h.apply(claim)
	if old.Outcome != store.ClaimSuccess || !old.Replayed {
		t.Fatalf("retry of old op = %+v, want replayed SUCCESS", old)
	}
	if after := h.read(store.ResourceKey("r1")); !reflect.DeepEqual(before, after) {
		t.Fatalf("replayed result changed the resource: %+v", after.Resource)
	}
}

// An immutable-input mismatch is a domain error; it never overwrites another
// attempt's closure or claim.
func TestTryClaimImmutableMismatchIsDomainConflict(t *testing.T) {
	ctx := context.Background()
	a := attempt("a", "r1")

	t.Run("closure bound to another request", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		h.apply(closeAttempt("close-a", "r1", "q1", a))
		expectOutcome(t, "claim", h.apply(tryClaim("claim-a", "r1", "q2", a, store.TypeWater)), store.OutcomeDomainConflict)
		if cl, _ := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "a"); cl.RequestID != "q1" {
			t.Fatalf("closure rebound to %q", cl.RequestID)
		}
	})

	t.Run("claim held with another fingerprint", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater))
		forged := a
		forged.Fingerprint = "fp-other"
		expectOutcome(t, "claim", h.apply(tryClaim("claim-a-forged", "r1", "q1", forged, store.TypeWater)), store.OutcomeDomainConflict)
		if c := h.read(store.ResourceKey("r1")).Resource.ActiveClaim; c == nil || c.Fingerprint != a.Fingerprint {
			t.Fatalf("claim changed: %+v", c)
		}
		if _, err := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "a"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("mismatch closed the live attempt: err = %v", err)
		}
	})
}

// CloseAttempt releases only its own claim (v3.1 §18).
func TestCloseAttemptNeverReleasesAnotherClaim(t *testing.T) {
	h := newHarness(t)
	a, b := attempt("a", "r1"), attempt("b", "r1")
	createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
	h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater))
	expectOutcome(t, "close b", h.apply(closeAttempt("close-b", "r1", "q2", b)), store.OutcomeOK)
	r := h.read(store.ResourceKey("r1")).Resource
	if r.Status != store.ResourceAssigned || r.ActiveClaim == nil || r.ActiveClaim.AttemptID != "a" {
		t.Fatalf("closing b disturbed a's claim: %s %+v", r.Status, r.ActiveClaim)
	}
}

func transitionResource(op, id string, version uint64, status store.ResourceStatus) store.TransitionResource {
	return store.TransitionResource{Meta: meta(op), Key: store.ResourceKey(id), ExpectedVersion: version, Status: status}
}

// Public resource updates follow v3.1 §19: they cannot set ASSIGNED, cannot
// touch activeClaim and cannot take an assigned resource offline.
func TestTransitionResourceRules(t *testing.T) {
	a := attempt("a", "r1")
	assigned := func(t *testing.T) (*harness, store.Record) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		expectOutcome(t, "claim", h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater)), store.ClaimSuccess)
		return h, h.read(store.ResourceKey("r1"))
	}

	t.Run("cannot set ASSIGNED", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		before := h.read(store.ResourceKey("r1"))
		expectOutcome(t, "update", h.apply(transitionResource("u1", "r1", 1, store.ResourceAssigned)), store.OutcomeDomainConflict)
		if after := h.read(store.ResourceKey("r1")); !reflect.DeepEqual(before, after) {
			t.Fatalf("rejected update changed the resource: %+v", after.Resource)
		}
	})

	t.Run("cannot set CONSUMED", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		expectOutcome(t, "update", h.apply(transitionResource("u1", "r1", 1, store.ResourceConsumed)), store.OutcomeDomainConflict)
	})

	for _, to := range []store.ResourceStatus{store.ResourceOffline, store.ResourceAvailable} {
		t.Run("assigned resource cannot become "+string(to), func(t *testing.T) {
			h, before := assigned(t)
			res := h.apply(transitionResource("u1", "r1", before.Version, to))
			expectOutcome(t, "update", res, store.OutcomeDomainConflict)
			if after := h.read(store.ResourceKey("r1")); !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected update changed the assigned resource: %s %+v", after.Resource.Status, after.Resource.ActiveClaim)
			}
		})
	}

	t.Run("window update keeps the active claim", func(t *testing.T) {
		h, before := assigned(t)
		upd := store.TransitionResource{Meta: meta("u1"), Key: store.ResourceKey("r1"), ExpectedVersion: before.Version,
			Availability: &store.Window{From: t0 - hour, Until: t0 + 48*hour}}
		expectOutcome(t, "update", h.apply(upd), store.OutcomeOK)
		r := h.read(store.ResourceKey("r1")).Resource
		if r.AvailableUntil != t0+48*hour {
			t.Fatalf("window not updated: until = %d", r.AvailableUntil)
		}
		if r.Status != store.ResourceAssigned || !reflect.DeepEqual(r.ActiveClaim, before.Resource.ActiveClaim) {
			t.Fatalf("window update disturbed the claim: %s %+v", r.Status, r.ActiveClaim)
		}
	})

	t.Run("available and offline toggle", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		off := h.apply(transitionResource("u1", "r1", 1, store.ResourceOffline))
		expectOutcome(t, "offline", off, store.OutcomeOK)
		if off.Version != 2 {
			t.Fatalf("version = %d, want 2", off.Version)
		}
		expectOutcome(t, "available", h.apply(transitionResource("u2", "r1", 2, store.ResourceAvailable)), store.OutcomeOK)
		if s := h.read(store.ResourceKey("r1")).Resource.Status; s != store.ResourceAvailable {
			t.Fatalf("status = %s, want AVAILABLE", s)
		}
	})

	t.Run("stale version conflicts", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		expectOutcome(t, "update", h.apply(transitionResource("u1", "r1", 7, store.ResourceOffline)), store.OutcomeVersionConflict)
	})

	t.Run("empty window is invalid", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		upd := store.TransitionResource{Meta: meta("u1"), Key: store.ResourceKey("r1"), ExpectedVersion: 1,
			Availability: &store.Window{From: t0, Until: t0}}
		expectOutcome(t, "update", h.apply(upd), store.OutcomeInvalid)
	})
}

// CloseAttempt releases the holder to the chosen disposition, records that
// disposition once, and keeps it on later closes of the same attempt.
func TestCloseAttemptDisposition(t *testing.T) {
	ctx := context.Background()
	a := attempt("a", "r1")
	for _, d := range []store.ResourceStatus{store.ResourceAvailable, store.ResourceConsumed, store.ResourceOffline} {
		t.Run(string(d), func(t *testing.T) {
			h := newHarness(t)
			createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
			h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater))
			c := closeAttempt("close-a", "r1", "q1", a)
			c.Disposition = d
			expectOutcome(t, "close", h.apply(c), store.OutcomeOK)
			r := h.read(store.ResourceKey("r1")).Resource
			if r.Status != d || r.ActiveClaim != nil {
				t.Fatalf("resource = %s %+v, want %s with no claim", r.Status, r.ActiveClaim, d)
			}
			if cl, err := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "a"); err != nil || cl.Disposition != d {
				t.Fatalf("closure = %+v, %v; want disposition %s", cl, err, d)
			}
		})
	}

	t.Run("ASSIGNED is not a disposition", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		c := closeAttempt("close-a", "r1", "q1", a)
		c.Disposition = store.ResourceAssigned
		expectOutcome(t, "close", h.apply(c), store.OutcomeInvalid)
		if _, err := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "a"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("invalid close recorded a closure: err = %v", err)
		}
	})

	t.Run("first disposition survives a later close", func(t *testing.T) {
		h := newHarness(t)
		createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
		h.apply(tryClaim("claim-a", "r1", "q1", a, store.TypeWater))
		first := closeAttempt("close-a-1", "r1", "q1", a)
		first.Disposition = store.ResourceConsumed
		expectOutcome(t, "first close", h.apply(first), store.OutcomeOK)
		expectOutcome(t, "second close", h.apply(closeAttempt("close-a-2", "r1", "q1", a)), store.OutcomeOK)
		if cl, _ := h.f.ReadClosure(ctx, store.ResourceKey("r1"), "a"); cl.Disposition != store.ResourceConsumed {
			t.Fatalf("closure disposition = %s, want CONSUMED", cl.Disposition)
		}
		if s := h.read(store.ResourceKey("r1")).Resource.Status; s != store.ResourceConsumed {
			t.Fatalf("status = %s, want CONSUMED", s)
		}
	})
}

// Scan filters by kind, type, status and geohash-prefix ranges, in key order.
func TestScanFilters(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	for _, r := range []struct {
		id, geohash string
		typ         store.ResourceType
		status      store.ResourceStatus
	}{
		{"r1", "ttnfv1", store.TypeWater, store.ResourceAvailable},
		{"r2", "tsq4k2", store.TypeWater, store.ResourceAvailable},
		{"r3", "ttnfv9", store.TypeWater, store.ResourceOffline},
		{"r4", "ttnfv2", store.TypeFood, store.ResourceAvailable},
		{"r5", "tu0000", store.TypeWater, store.ResourceAvailable}, // outside the cover
		{"r6", "ttn", store.TypeWater, store.ResourceAvailable},    // prefix equals the hash
	} {
		res := resource(r.id, r.typ, r.status)
		res.Geohash = r.geohash
		createResource(h, res)
	}
	req := request("q1", store.TypeWater)
	req.Geohash = "ttnfv1"
	h.apply(store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: req})

	got, err := h.f.Scan(ctx, store.ScanQuery{
		Kind: store.KindResource, Type: store.TypeWater,
		Statuses:        []string{string(store.ResourceAvailable)},
		GeohashPrefixes: []string{"ttn", "tsq4"},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var keys []string
	for _, rec := range got {
		keys = append(keys, rec.Key)
	}
	want := []string{"res:r1", "res:r2", "res:r6"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("scan keys = %v, want %v", keys, want)
	}

	all, err := h.f.Scan(ctx, store.ScanQuery{Kind: store.KindRequest})
	if err != nil || len(all) != 1 || all[0].Key != "req:q1" {
		t.Fatalf("request scan = %+v, %v; want only req:q1", all, err)
	}
}

// A rejected transition writes neither the entity nor its outbox rows.
func TestRejectedTransitionWritesNoOutbox(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.apply(store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater)})
	a := attempt("a", "r1")
	ev := []store.OutboxEvent{{EventID: "ev-1", EventType: "request.status.matching", Payload: json.RawMessage(`{}`)}}

	stale := store.TransitionRequest{Meta: meta("t-stale"), Key: store.RequestKey("q1"), ExpectedVersion: 9,
		ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a, Outbox: ev}
	expectOutcome(t, "stale", h.apply(stale), store.OutcomeVersionConflict)
	illegal := store.TransitionRequest{Meta: meta("t-illegal"), Key: store.RequestKey("q1"), ExpectedVersion: 1,
		ExpectedStatus: store.RequestPending, To: store.RequestMatched, Outbox: ev}
	expectOutcome(t, "PENDING -> MATCHED", h.apply(illegal), store.OutcomeDomainConflict)

	if pending, err := h.f.PendingOutbox(ctx, 0); err != nil || len(pending) != 0 {
		t.Fatalf("outbox after rejections = %+v, %v; want empty", pending, err)
	}
	if rec := h.read(store.RequestKey("q1")); rec.Version != 1 || rec.Request.Status != store.RequestPending {
		t.Fatalf("request changed: v%d %s", rec.Version, rec.Request.Status)
	}
}

// Log entries decode strictly: a forged field such as activeClaim is
// rejected, not ignored.
func TestForgedEntryFieldIsRejected(t *testing.T) {
	h := newHarness(t)
	createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
	forged := []byte(`{"op":"TransitionResource","body":{"operationId":"u1","at":1792000000000,"key":"res:r1","expectedVersion":1,"status":"OFFLINE","activeClaim":{"requestId":"q9","attemptId":"z","fingerprint":"f"}}}`)
	expectOutcome(t, "forged", h.applyEntry(forged), store.OutcomeInvalid)
	if r := h.read(store.ResourceKey("r1")).Resource; r.Status != store.ResourceAvailable || r.ActiveClaim != nil {
		t.Fatalf("forged entry changed the resource: %s %+v", r.Status, r.ActiveClaim)
	}
}

// A VacuumInto image reopened with OpenImage has the same rows and applied
// index, and keeps applying from the next index.
func TestVacuumIntoOpenImageRoundTrip(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.apply(store.CompareAndSet{Meta: meta("kv-1"), Key: "kv:a", Value: json.RawMessage(`1`)})
	h.apply(store.CreateEntity{Meta: meta("c-r1"), Key: store.ResourceKey("r1"), Resource: resource("r1", store.TypeWater, store.ResourceAvailable)})
	h.apply(closeAttempt("close-a1", "r1", "q1", attempt("a1", "r1")))

	path := filepath.Join(t.TempDir(), "image.db")
	if err := h.f.VacuumInto(ctx, path); err != nil {
		t.Fatalf("VacuumInto: %v", err)
	}
	if err := h.f.VacuumInto(ctx, path); err == nil {
		t.Fatal("VacuumInto over an existing file succeeded")
	}
	img, err := fsm.OpenImage(ctx, path)
	if err != nil {
		t.Fatalf("OpenImage: %v", err)
	}
	t.Cleanup(func() { _ = img.Close() })

	want, err := h.f.Dump(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := img.Dump(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("image rows differ:\n got %v\nwant %v", got, want)
	}
	if img.AppliedIndex() != h.f.AppliedIndex() {
		t.Fatalf("image applied index = %d, want %d", img.AppliedIndex(), h.f.AppliedIndex())
	}
	if _, err := img.Apply(h.f.AppliedIndex(), mustEncode(t, store.CompareAndSet{Meta: meta("kv-dup"), Key: "kv:b"})); err == nil {
		t.Fatal("image accepted an entry at its own applied index")
	}
	entry := mustEncode(t, store.CompareAndSet{Meta: meta("kv-2"), Key: "kv:a", ExpectedVersion: 1, Value: json.RawMessage(`2`)})
	res, err := img.Apply(h.f.AppliedIndex()+1, entry)
	if err != nil {
		t.Fatalf("Apply after image: %v", err)
	}
	expectOutcome(t, "CAS after image", res, store.OutcomeOK)
}

func TestOpenImageRejectsForeignDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "foreign.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if f, err := fsm.OpenImage(ctx, path); err == nil {
		_ = f.Close()
		t.Fatal("OpenImage accepted a database without the FSM schema")
	}
	if f, err := fsm.OpenImage(ctx, filepath.Join(t.TempDir(), "missing.db")); err == nil {
		_ = f.Close()
		t.Fatal("OpenImage accepted a missing file")
	}
}

func mustEncode(t *testing.T, body any) []byte {
	t.Helper()
	b, err := fsm.Encode(body)
	if err != nil {
		t.Fatalf("Encode(%T): %v", body, err)
	}
	return b
}

// openFileFSM opens an FSM on a database file, as the storage node does,
// and returns a second FSM opened from its image.
func openFileFSM(t *testing.T) (*fsm.FSM, *fsm.FSM) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	f, err := fsm.Open(ctx, "file:"+filepath.Join(dir, "fsm-0.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.Apply(1, mustEncode(t, store.CompareAndSet{Meta: meta("kv-1"), Key: "kv:a", Value: json.RawMessage(`1`)})); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "image.db")
	if err := f.VacuumInto(ctx, path); err != nil {
		t.Fatal(err)
	}
	img, err := fsm.OpenImage(ctx, path)
	if err != nil {
		t.Fatalf("OpenImage: %v", err)
	}
	t.Cleanup(func() { _ = img.Close() })
	return f, img
}

// On a database file, reads use their own read-only pool: a long read never
// holds the connection Apply needs, and the read pool cannot write.
func TestFileReadsDoNotBlockApply(t *testing.T) {
	ctx := context.Background()
	f, img := openFileFSM(t)
	for name, db := range map[string]*fsm.FSM{"Open": f, "OpenImage": img} {
		release, err := db.HoldRead(ctx)
		if err != nil {
			t.Fatalf("%s: HoldRead: %v", name, err)
		}
		done := make(chan error, 1)
		go func() {
			res, err := db.Apply(db.AppliedIndex()+1, mustEncode(t, store.CompareAndSet{Meta: meta("kv-2-" + name), Key: "kv:b", Value: json.RawMessage(`2`)}))
			if err == nil && res.Outcome != store.OutcomeOK {
				err = fmt.Errorf("outcome %s", res.Outcome)
			}
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s: Apply during a read: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("%s: Apply blocked behind an open read", name)
			release()
			if err := <-done; err != nil {
				t.Errorf("%s: Apply after the read ended: %v", name, err)
			}
			continue
		}
		release()
		if _, err := db.ReadRecord(ctx, "kv:b"); err != nil {
			t.Errorf("%s: read after apply: %v", name, err)
		}
		if err := db.ExecOnReadPool(ctx, `DELETE FROM records`); err == nil {
			t.Errorf("%s: the read pool accepted a write", name)
		}
	}
}
