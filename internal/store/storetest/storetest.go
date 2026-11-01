// Package storetest is the store.Store contract suite. Every implementation
// (internal/fsm/memstore, internal/coordclient) runs it, so the API and
// matcher can swap one for the other without code changes.
//
// The suite checks the behaviour callers rely on, fault-free: a committed
// rejection is a Result with a nil error (never an error), an absent read is
// ErrNotFound, retries replay, and a complete scan covers every shard. Each
// subtest uses its own keys, operation IDs and geohash, so one store may be
// shared by all of them.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// T0 is the proposer timestamp every command carries (unix ms).
const T0 = int64(1_792_000_000_000)

const hour = int64(3_600_000)

// Run runs the contract suite against the store newStore returns. newStore
// is called once per subtest and may return the same store every time.
func Run(t *testing.T, newStore func(t *testing.T) store.Store) {
	for _, c := range []struct {
		name string
		fn   func(t *testing.T, s store.Store, n ns)
	}{
		{"CreateReadAndExists", testCreateReadAndExists},
		{"ReadAbsentIsNotFound", testReadAbsentIsNotFound},
		{"CompareAndSet", testCompareAndSet},
		{"Tombstone", testTombstone},
		{"OperationReplayAndReuse", testOperationReplayAndReuse},
		{"ReplayIgnoresOutboxTraceparent", testReplayIgnoresOutboxTraceparent},
		{"TransitionRequestConflicts", testTransitionRequestConflicts},
		{"ClaimAndCloseLifecycle", testClaimAndCloseLifecycle},
		{"TransitionResourceAndRetry", testTransitionResourceAndRetry},
		{"InvalidIsACommittedResult", testInvalidIsACommittedResult},
		{"ScanCoversEveryShard", testScanCoversEveryShard},
		{"ScanCursorPaging", testScanCursorPaging},
		{"OutboxPendingAndPublished", testOutboxPendingAndPublished},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.fn(t, newStore(t), newNS(t))
		})
	}
}

// ns namespaces one subtest's IDs so subtests can share a store.
type ns string

func newNS(t *testing.T) ns {
	h := fnv.New32a()
	_, _ = h.Write([]byte(t.Name()))
	return ns(fmt.Sprintf("st%08x", h.Sum32()))
}

func (n ns) id(s string) string { return string(n) + "-" + s }
func (n ns) op(s string) store.Meta {
	return store.Meta{OperationID: string(n) + "-op-" + s, At: T0}
}
func (n ns) geohash() string { return "zz" + string(n) }

func ctxFor(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func (n ns) resource(id string) *store.Resource {
	return &store.Resource{
		ID: n.id(id), Type: store.TypeWater, Name: "unit " + id,
		Location: store.Location{Lat: 28.61, Lng: 77.21}, Geohash: n.geohash(),
		AvailableFrom: T0 - hour, AvailableUntil: T0 + 24*hour,
		Status: store.ResourceAvailable, Reusable: true,
	}
}

func (n ns) request(id string) *store.Request {
	return &store.Request{
		ID: n.id(id), Type: store.TypeWater,
		Location: store.Location{Lat: 28.62, Lng: 77.22}, Geohash: n.geohash(),
		PeopleAffected: 4, Urgency: 2, Status: store.RequestPending,
	}
}

func (n ns) attempt(id, resourceID string) store.Attempt {
	return store.Attempt{AttemptID: n.id(id), ResourceID: n.id(resourceID), Fingerprint: "fp-" + id}
}

// expect returns a check of one call: it fails unless the call committed
// outcome want with a nil error. Use as expect(t, what, want)(s.Call(...)).
func expect(t *testing.T, what string, want store.Outcome) func(store.Result, error) store.Result {
	return func(res store.Result, err error) store.Result {
		t.Helper()
		return check(t, what, res, err, want)
	}
}

func check(t *testing.T, what string, res store.Result, err error, want store.Outcome) store.Result {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: err = %v, want committed %s (a rejection is a Result, never an error)", what, err, want)
	}
	if res.Outcome != want {
		t.Fatalf("%s: outcome = %s (%s), want %s", what, res.Outcome, res.Reason, want)
	}
	return res
}

func checkShard(t *testing.T, s store.Store, res store.Result) {
	t.Helper()
	if res.Shard < 0 || res.Shard >= s.Shards() {
		t.Fatalf("result shard %d outside 0..%d", res.Shard, s.Shards()-1)
	}
}

func testCreateReadAndExists(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	key := "kv:" + n.id("a")
	res := expect(t, "create", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c1"), Key: key, Value: json.RawMessage(`{"x":1}`)}))
	if res.Key != key || res.Version != 1 || res.OperationID != n.op("c1").OperationID || res.Replayed {
		t.Fatalf("create result = %+v", res)
	}
	checkShard(t, s, res)

	rec, err := s.ReadRecord(ctx, key)
	if err != nil || rec.Key != key || rec.Kind != store.KindKV || rec.Version != 1 || rec.Deleted || !jsonEqual(rec.Value, `{"x":1}`) {
		t.Fatalf("read = %+v, %v", rec, err)
	}

	expect(t, "create again", store.OutcomeExists)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c2"), Key: key, Value: json.RawMessage(`2`)}))

	rkey := store.ResourceKey(n.id("r1"))
	expect(t, "create resource", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c3"), Key: rkey, Resource: n.resource("r1")}))
	rec, err = s.ReadRecord(ctx, rkey)
	if err != nil || rec.Kind != store.KindResource || rec.Resource == nil || rec.Resource.ID != n.id("r1") || rec.Resource.Status != store.ResourceAvailable {
		t.Fatalf("read resource = %+v, %v", rec, err)
	}
}

func testReadAbsentIsNotFound(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	if _, err := s.ReadRecord(ctx, "kv:"+n.id("absent")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadRecord absent: err = %v, want ErrNotFound", err)
	}
	if _, err := s.ReadClosure(ctx, store.ResourceKey(n.id("r")), n.id("a")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadClosure absent: err = %v, want ErrNotFound", err)
	}
	if _, err := s.Operation(ctx, "kv:"+n.id("absent"), n.op("never").OperationID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Operation absent: err = %v, want ErrNotFound", err)
	}
}

func testCompareAndSet(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	key := "kv:" + n.id("cas")
	expect(t, "CAS create (expected 0)", store.OutcomeOK)(s.CompareAndSet(ctx, store.CompareAndSet{Meta: n.op("1"), Key: key, Value: json.RawMessage(`0`)}))
	res := expect(t, "CAS v1", store.OutcomeOK)(s.CompareAndSet(ctx, store.CompareAndSet{Meta: n.op("2"), Key: key, ExpectedVersion: 1, Value: json.RawMessage(`1`)}))
	if res.Version != 2 {
		t.Fatalf("version after CAS = %d, want 2", res.Version)
	}
	res = expect(t, "stale CAS", store.OutcomeVersionConflict)(s.CompareAndSet(ctx, store.CompareAndSet{Meta: n.op("3"), Key: key, ExpectedVersion: 1, Value: json.RawMessage(`9`)}))
	if res.Version != 2 {
		t.Fatalf("stale CAS reports version %d, want the current 2", res.Version)
	}
	if rec, err := s.ReadRecord(ctx, key); err != nil || rec.Version != 2 || !jsonEqual(rec.Value, `1`) {
		t.Fatalf("after stale CAS = %+v, %v; want v2 value 1", rec, err)
	}
	expect(t, "CAS on a domain key", store.OutcomeDomainConflict)(s.CompareAndSet(ctx, store.CompareAndSet{Meta: n.op("4"), Key: store.RequestKey(n.id("q")), Value: json.RawMessage(`1`)}))
}

func testTombstone(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	key := "kv:" + n.id("del")
	expect(t, "create", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c"), Key: key, Value: json.RawMessage(`1`)}))
	expect(t, "delete", store.OutcomeOK)(s.CompareAndSet(ctx, store.CompareAndSet{Meta: n.op("d"), Key: key, ExpectedVersion: 1, Delete: true}))
	rec, err := s.ReadRecord(ctx, key)
	if err != nil || !rec.Deleted || rec.Version != 2 {
		t.Fatalf("tombstone read = %+v, %v; want deleted at v2", rec, err)
	}
	expect(t, "create over tombstone", store.OutcomeExists)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c2"), Key: key, Value: json.RawMessage(`2`)}))
}

func testOperationReplayAndReuse(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	key := "kv:" + n.id("op")
	cmd := store.CreateEntity{Meta: n.op("same"), Key: key, Value: json.RawMessage(`1`)}
	first := expect(t, "first", store.OutcomeOK)(s.CreateEntity(ctx, cmd))
	again := expect(t, "replay", store.OutcomeOK)(s.CreateEntity(ctx, cmd))
	if !again.Replayed || again.Version != first.Version || again.AppliedIndex != first.AppliedIndex {
		t.Fatalf("replay = %+v, want first %+v with replayed=true", again, first)
	}
	reused := cmd
	reused.Value = json.RawMessage(`2`)
	expect(t, "same operationId, other payload", store.OutcomeOperationIDReuse)(s.CreateEntity(ctx, reused))

	got, err := s.Operation(ctx, key, cmd.OperationID)
	if err != nil || got.Outcome != store.OutcomeOK || got.Version != first.Version || got.Key != key || got.Shard != first.Shard {
		t.Fatalf("Operation = %+v, %v; want %+v", got, err, first)
	}

	// A rejection is stored and replayed too.
	stale := store.CompareAndSet{Meta: n.op("stale"), Key: key, ExpectedVersion: 7, Value: json.RawMessage(`3`)}
	expect(t, "stale", store.OutcomeVersionConflict)(s.CompareAndSet(ctx, stale))
	if res := expect(t, "stale replay", store.OutcomeVersionConflict)(s.CompareAndSet(ctx, stale)); !res.Replayed {
		t.Fatalf("rejection replay = %+v, want replayed", res)
	}
}

// testReplayIgnoresOutboxTraceparent: an outbox envelope's traceparent is
// not part of an operation's identity. A retry that changes only that value
// replays the committed result, and the stored event keeps the first
// committed traceparent; any other change is still OPERATION_ID_REUSED.
func testReplayIgnoresOutboxTraceparent(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	const (
		first  = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		second = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	)
	key := store.RequestKey(n.id("q-trace"))
	eventID := n.id("ev-trace")
	cmd := func(trace string, people int) store.CreateEntity {
		body, err := json.Marshal(map[string]any{"schemaVersion": 1, "eventId": eventID, "entityId": n.id("q-trace"),
			"entityVersion": 1, "eventType": "request.created", "occurredAt": "2026-10-30T00:00:00Z", "traceparent": trace,
			"payload": map[string]any{"requestId": n.id("q-trace"), "status": "PENDING"}})
		if err != nil {
			t.Fatal(err)
		}
		req := n.request("q-trace")
		req.PeopleAffected = people
		return store.CreateEntity{Meta: n.op("create-trace"), Key: key, Request: req,
			Outbox: []store.OutboxEvent{{EventID: eventID, EventType: "request.created", SchemaVersion: 1,
				Exchange: "relief.events", RoutingKey: "request.created", Payload: body}}}
	}
	orig := expect(t, "first", store.OutcomeOK)(s.CreateEntity(ctx, cmd(first, 3)))
	again := expect(t, "trace-only retry", store.OutcomeOK)(s.CreateEntity(ctx, cmd(second, 3)))
	if !again.Replayed || again.Version != orig.Version || again.AppliedIndex != orig.AppliedIndex {
		t.Fatalf("trace-only retry = %+v, want replay of %+v", again, orig)
	}
	expect(t, "trace and payload changed", store.OutcomeOperationIDReuse)(s.CreateEntity(ctx, cmd(second, 4)))

	pending, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: orig.Shard, Limit: 100})
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	ev := find(pending, eventID)
	if ev == nil {
		t.Fatalf("event %s not pending", eventID)
	}
	var env struct {
		Traceparent string `json:"traceparent"`
	}
	if err := json.Unmarshal(ev.Payload, &env); err != nil || env.Traceparent != first {
		t.Fatalf("stored traceparent = %q (%v), want the first committed %q", env.Traceparent, err, first)
	}
}

func testTransitionRequestConflicts(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	key := store.RequestKey(n.id("q1"))
	expect(t, "create request", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c"), Key: key, Request: n.request("q1")}))
	a := n.attempt("a", "r1")
	expect(t, "stale version", store.OutcomeVersionConflict)(s.TransitionRequest(ctx, store.TransitionRequest{Meta: n.op("t1"), Key: key, ExpectedVersion: 9,
		ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a}))
	expect(t, "illegal transition", store.OutcomeDomainConflict)(s.TransitionRequest(ctx, store.TransitionRequest{Meta: n.op("t2"), Key: key, ExpectedVersion: 1,
		ExpectedStatus: store.RequestPending, To: store.RequestMatched}))
	expect(t, "absent request", store.OutcomeNotFound)(s.TransitionRequest(ctx, store.TransitionRequest{Meta: n.op("t3"), Key: store.RequestKey(n.id("nope")), ExpectedVersion: 1,
		ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a}))
	res := expect(t, "PENDING -> MATCHING", store.OutcomeOK)(s.TransitionRequest(ctx, store.TransitionRequest{Meta: n.op("t4"), Key: key, ExpectedVersion: 1,
		ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a}))
	rec, err := s.ReadRecord(ctx, key)
	if err != nil || rec.Version != res.Version || rec.Request.Status != store.RequestMatching || rec.Request.CurrentAttempt == nil || rec.Request.CurrentAttempt.AttemptID != a.AttemptID {
		t.Fatalf("after transition = %+v, %v", rec, err)
	}
}

func testClaimAndCloseLifecycle(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	rkey := store.ResourceKey(n.id("r1"))
	expect(t, "create resource", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c"), Key: rkey, Resource: n.resource("r1")}))
	a, b := n.attempt("a", "r1"), n.attempt("b", "r1")
	claim := func(op, req string, at store.Attempt) store.TryClaim {
		return store.TryClaim{Meta: n.op(op), ResourceKey: rkey, RequestID: n.id(req), Attempt: at, Type: store.TypeWater}
	}

	expect(t, "claim a", store.ClaimSuccess)(s.TryClaim(ctx, claim("claim-a", "q1", a)))
	rec, err := s.ReadRecord(ctx, rkey)
	if err != nil || rec.Resource.Status != store.ResourceAssigned || rec.Resource.ActiveClaim == nil || rec.Resource.ActiveClaim.AttemptID != a.AttemptID {
		t.Fatalf("after claim = %+v, %v", rec.Resource, err)
	}
	expect(t, "claim b while a holds", store.ClaimConflictClosed)(s.TryClaim(ctx, claim("claim-b", "q2", b)))
	if cl, err := s.ReadClosure(ctx, rkey, b.AttemptID); err != nil || cl.RequestID != n.id("q2") || cl.AttemptID != b.AttemptID {
		t.Fatalf("closure for b = %+v, %v", cl, err)
	}

	expect(t, "close a", store.OutcomeOK)(s.CloseAttempt(ctx, store.CloseAttempt{Meta: n.op("close-a"), ResourceKey: rkey, RequestID: n.id("q1"),
		Attempt: a, Disposition: store.ResourceAvailable, Reason: "cancelled"}))
	if rec, err := s.ReadRecord(ctx, rkey); err != nil || rec.Resource.Status != store.ResourceAvailable || rec.Resource.ActiveClaim != nil {
		t.Fatalf("after close = %+v, %v; want released", rec.Resource, err)
	}
	if cl, err := s.ReadClosure(ctx, rkey, a.AttemptID); err != nil || cl.Disposition != store.ResourceAvailable || cl.Reason != "cancelled" {
		t.Fatalf("closure for a = %+v, %v", cl, err)
	}
	expect(t, "claim a after close", store.ClaimClosed)(s.TryClaim(ctx, claim("claim-a-2", "q1", a)))

	// CONTRIBUTING.md, Invariants, 4: the historical SUCCESS is still returned by Operation,
	// and is not ownership.
	if old, err := s.Operation(ctx, rkey, n.op("claim-a").OperationID); err != nil || old.Outcome != store.ClaimSuccess {
		t.Fatalf("historical claim = %+v, %v; want SUCCESS", old, err)
	}
}

func testTransitionResourceAndRetry(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	rkey := store.ResourceKey(n.id("r1"))
	expect(t, "create resource", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c"), Key: rkey, Resource: n.resource("r1")}))
	expect(t, "offline", store.OutcomeOK)(s.TransitionResource(ctx, store.TransitionResource{Meta: n.op("off"), Key: rkey, ExpectedVersion: 1, Status: store.ResourceOffline}))
	expect(t, "to ASSIGNED", store.OutcomeDomainConflict)(s.TransitionResource(ctx, store.TransitionResource{Meta: n.op("asg"), Key: rkey, ExpectedVersion: 2, Status: store.ResourceAssigned}))

	qkey := store.RequestKey(n.id("q1"))
	expect(t, "create request", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("cq"), Key: qkey, Request: n.request("q1")}))
	expect(t, "retry", store.OutcomeOK)(s.RetryWorkflow(ctx, store.RetryWorkflow{Meta: n.op("retry"), Key: qkey, ExpectedVersion: 1, NextAttemptAt: T0 + 30_000, LastError: "no candidate"}))
	if rec, err := s.ReadRecord(ctx, qkey); err != nil || rec.Request.NextAttemptAt != T0+30_000 || rec.Request.Status != store.RequestPending {
		t.Fatalf("after retry = %+v, %v", rec.Request, err)
	}
	expect(t, "stale retry", store.OutcomeVersionConflict)(s.RetryWorkflow(ctx, store.RetryWorkflow{Meta: n.op("retry2"), Key: qkey, ExpectedVersion: 1, NextAttemptAt: T0}))
}

func testInvalidIsACommittedResult(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	rkey := store.ResourceKey(n.id("r1"))
	expect(t, "create resource", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c"), Key: rkey, Resource: n.resource("r1")}))
	expect(t, "bad disposition", store.OutcomeInvalid)(s.CloseAttempt(ctx, store.CloseAttempt{Meta: n.op("close"), ResourceKey: rkey, RequestID: n.id("q1"),
		Attempt: n.attempt("a", "r1"), Disposition: store.ResourceAssigned, Reason: "x"}))
	bad := store.Window{From: T0, Until: T0}
	expect(t, "empty window", store.OutcomeInvalid)(s.TransitionResource(ctx, store.TransitionResource{Meta: n.op("win"), Key: rkey, ExpectedVersion: 1, Availability: &bad}))
}

func testScanCoversEveryShard(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	shards := map[int]bool{}
	var want []string
	for i := 0; len(shards) < s.Shards() || i < 6; i++ {
		if i >= 200 {
			t.Fatalf("200 resources reached only shards %v of %d", shards, s.Shards())
		}
		id := fmt.Sprintf("r%d", i)
		res := expect(t, "create "+id, store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c" + id), Key: store.ResourceKey(n.id(id)), Resource: n.resource(id)}))
		shards[res.Shard] = true
		want = append(want, res.Key)
	}

	got, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindResource, Type: store.TypeWater, GeohashPrefixes: []string{n.geohash()}, Limit: 1000})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got.Partial || len(got.FailedShards) != 0 {
		t.Fatalf("fault-free scan partial=%v failed=%v", got.Partial, got.FailedShards)
	}
	if len(got.Positions) != s.Shards() {
		t.Fatalf("positions = %+v, want one per shard (%d)", got.Positions, s.Shards())
	}
	var keys []string
	for _, r := range got.Records {
		keys = append(keys, r.Key)
	}
	slices.Sort(keys)
	slices.Sort(want)
	if !slices.Equal(keys, want) {
		t.Fatalf("scan keys = %v, want %v", keys, want)
	}

	none, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindResource, GeohashPrefixes: []string{n.geohash() + "-none"}, Limit: 10})
	if err != nil || none.Partial || len(none.Records) != 0 || len(none.Positions) != s.Shards() {
		t.Fatalf("empty complete scan = %+v, %v", none, err)
	}
	for _, p := range got.Positions {
		if p.HasMore {
			t.Fatalf("shard %d: hasMore with fewer than limit records", p.Shard)
		}
	}

	// A full page (exactly Limit records) is flagged: more may match.
	page, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindResource, GeohashPrefixes: []string{n.geohash()}, Limit: 1})
	if err != nil || page.Partial {
		t.Fatalf("limit-1 scan = %+v, %v", page, err)
	}
	for _, p := range page.Positions {
		if !p.HasMore { // every shard holds at least one of these records
			t.Fatalf("limit-1 page: shard %d hasMore=false with a full page", p.Shard)
		}
	}
	if len(page.Records) > s.Shards() {
		t.Fatalf("limit-1 scan returned %d records over %d shards", len(page.Records), s.Shards())
	}

	// Limit must be 1 to MaxScanLimit: 0 is not "no limit".
	for _, limit := range []int{0, -1, store.MaxScanLimit + 1} {
		if _, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindResource, Limit: limit}); err == nil ||
			errors.Is(err, store.ErrUnknownOutcome) || errors.Is(err, store.ErrNotFound) {
			t.Fatalf("scan limit %d: err = %v, want a plain error", limit, err)
		}
	}
}

// S1.6: paging with NextCursor returns every matching record exactly once,
// in key order per shard, and ends with an empty NextCursor. A cursor from
// another ring version is ErrRingVersionMismatch; a malformed one, or one
// continued with another limit, is a plain error. None is read as "start
// over".
func testScanCursorPaging(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	shardOf := map[string]int{}
	for i := range 40 {
		id := fmt.Sprintf("p%02d", i)
		res := expect(t, "create "+id, store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c" + id), Key: store.ResourceKey(n.id(id)), Resource: n.resource(id)}))
		shardOf[res.Key] = res.Shard
	}
	perShard := map[int]int{}
	for _, sh := range shardOf {
		perShard[sh]++
	}
	if len(perShard) != s.Shards() {
		t.Fatalf("40 resources reached only shards %v of %d", perShard, s.Shards())
	}

	q := store.ScanQuery{Kind: store.KindResource, Type: store.TypeWater, GeohashPrefixes: []string{n.geohash()}, Limit: 3}
	seen := map[string]int{}
	last := map[int]string{}
	pages := 0
	for {
		if pages++; pages > 40 {
			t.Fatalf("still paging after 40 pages: %d of 40 records", len(seen))
		}
		page, err := s.Scan(ctx, q)
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		if page.Partial || len(page.FailedShards) != 0 {
			t.Fatalf("fault-free page %d partial=%v failed=%v", pages, page.Partial, page.FailedShards)
		}
		for _, r := range page.Records {
			sh, ok := shardOf[r.Key]
			if !ok {
				t.Fatalf("page %d returned %s, which matches no created record", pages, r.Key)
			}
			if r.Key <= last[sh] {
				t.Fatalf("shard %d returned %s after %s", sh, r.Key, last[sh])
			}
			last[sh] = r.Key
			seen[r.Key]++
		}
		if page.NextCursor == "" {
			break
		}
		q.Cursor = page.NextCursor
	}
	for k := range shardOf {
		if seen[k] != 1 {
			t.Fatalf("%s returned %d times over %d pages, want once", k, seen[k], pages)
		}
	}
	maxPerShard := 0
	for _, c := range perShard {
		maxPerShard = max(maxPerShard, c)
	}
	if want := maxPerShard/3 + 1; pages != want {
		t.Fatalf("walk took %d pages, want %d (largest shard has %d records, limit 3)", pages, want, maxPerShard)
	}

	first, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindResource, GeohashPrefixes: []string{n.geohash()}, Limit: 3})
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page = nextCursor %q, %v; want one", first.NextCursor, err)
	}
	cur, err := store.DecodeScanCursor(first.NextCursor)
	if err != nil {
		t.Fatalf("decode a store's own cursor: %v", err)
	}
	cur.RingVersion += 1 << 20
	stale := store.ScanQuery{Kind: store.KindResource, GeohashPrefixes: []string{n.geohash()}, Limit: 3, Cursor: store.EncodeScanCursor(cur)}
	if res, err := s.Scan(ctx, stale); !errors.Is(err, store.ErrRingVersionMismatch) || errors.Is(err, store.ErrUnknownOutcome) || len(res.Records) != 0 {
		t.Fatalf("stale-ring cursor = %d records, %v; want ErrRingVersionMismatch", len(res.Records), err)
	}
	other := store.ScanQuery{Kind: store.KindResource, GeohashPrefixes: []string{n.geohash()}, Limit: 4, Cursor: first.NextCursor}
	if res, err := s.Scan(ctx, other); err == nil || errors.Is(err, store.ErrRingVersionMismatch) || errors.Is(err, store.ErrUnknownOutcome) || len(res.Records) != 0 {
		t.Fatalf("cursor continued with another limit = %d records, %v; want a plain error", len(res.Records), err)
	}
	bad := stale
	bad.Cursor = "not-a-cursor"
	if res, err := s.Scan(ctx, bad); err == nil || errors.Is(err, store.ErrRingVersionMismatch) || errors.Is(err, store.ErrUnknownOutcome) ||
		errors.Is(err, store.ErrNotFound) || len(res.Records) != 0 {
		t.Fatalf("malformed cursor = %d records, %v; want a plain error", len(res.Records), err)
	}
}

func testOutboxPendingAndPublished(t *testing.T, s store.Store, n ns) {
	ctx := ctxFor(t)
	key := store.ResourceKey(n.id("r1"))
	ev := store.OutboxEvent{EventID: n.id("ev1"), EventType: "resource.registered", SchemaVersion: 1,
		Exchange: "relief.events", RoutingKey: "resource.registered.water", Payload: json.RawMessage(`{"id":1}`)}
	res := expect(t, "create with outbox", store.OutcomeOK)(s.CreateEntity(ctx, store.CreateEntity{Meta: n.op("c"), Key: key, Resource: n.resource("r1"), Outbox: []store.OutboxEvent{ev}}))

	pending, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: res.Shard, Limit: 100})
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	found := find(pending, ev.EventID)
	if found == nil || found.EntityKey != key || found.EntityVersion != 1 || found.CreatedAt != T0 || found.Published {
		t.Fatalf("pending event = %+v in %d events", found, len(pending))
	}

	expect(t, "mark published", store.OutcomeOK)(s.MarkOutboxPublished(ctx, store.MarkOutboxPublished{Meta: n.op("pub"), Shard: res.Shard, EventIDs: []string{ev.EventID}}))
	pending, err = s.PendingOutbox(ctx, store.OutboxQuery{Shard: res.Shard, Limit: 100})
	if err != nil || find(pending, ev.EventID) != nil {
		t.Fatalf("after publish: %v, event still pending = %v", err, find(pending, ev.EventID) != nil)
	}
}

func find(evs []store.OutboxEvent, id string) *store.OutboxEvent {
	for i := range evs {
		if evs[i].EventID == id {
			return &evs[i]
		}
	}
	return nil
}

func jsonEqual(raw json.RawMessage, want string) bool {
	var a, b any
	return json.Unmarshal(raw, &a) == nil && json.Unmarshal([]byte(want), &b) == nil && fmt.Sprint(a) == fmt.Sprint(b)
}
