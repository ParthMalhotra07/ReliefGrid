package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Each mutation route builds exactly the store command the contract names,
// with the key taken from the path, and returns the committed Result.
func TestMutationRoutesBuildStoreCommands(t *testing.T) {
	rg := loadRing(t)
	reqKey, reqID := keyOn(t, rg, testShard, store.RequestKeyPrefix, "r")
	resKey, resID := keyOn(t, rg, testShard, store.ResourceKeyPrefix, "s")
	kvKey, _ := keyOn(t, rg, testShard, "", "kv:cfg")
	attempt := store.Attempt{AttemptID: "a-1", ResourceID: resID, Fingerprint: "fp-1"}
	resource := &store.Resource{ID: resID, Type: store.TypeWater, Name: "Tanker", Geohash: "ttnfv8k",
		AvailableFrom: t0, AvailableUntil: t0 + 86_400_000, Status: store.ResourceAvailable, Reusable: true}
	outbox := []store.OutboxEvent{{EventID: "ev-1", EventType: "resource.registered", SchemaVersion: 1,
		Exchange: "relief.resources", RoutingKey: "resource.registered.water", Payload: json.RawMessage(`{"id":"x"}`)}}
	value := json.RawMessage(`{"radiusKm":50}`)
	window := &store.Window{From: t0, Until: t0 + 1000}

	cases := []struct {
		name     string
		call     call
		want     any
		wantETag bool
	}{
		{
			name: "create entity",
			call: call{method: http.MethodPut, path: "/internal/data/" + resKey, header: map[string]string{"If-None-Match": "*"},
				body: map[string]any{"operationId": "op-c", "at": t0, "ringVersion": 1, "resource": resource, "outbox": outbox}},
			want:     store.CreateEntity{Meta: meta("op-c"), Key: resKey, Resource: resource, Outbox: outbox},
			wantETag: true,
		},
		{
			name: "compare and set",
			call: call{method: http.MethodPut, path: "/internal/data/" + kvKey, header: map[string]string{"If-Match": `"4"`},
				body: map[string]any{"operationId": "op-cas", "at": t0, "ringVersion": 1, "value": value}},
			want:     store.CompareAndSet{Meta: meta("op-cas"), Key: kvKey, ExpectedVersion: 4, Value: value},
			wantETag: true,
		},
		{
			name: "delete",
			call: call{method: http.MethodDelete, path: fmt.Sprintf("/internal/data/%s?operationId=op-del&at=%d&ringVersion=1", kvKey, t0),
				header: map[string]string{"If-Match": `"5"`}},
			want:     store.CompareAndSet{Meta: meta("op-del"), Key: kvKey, ExpectedVersion: 5, Delete: true},
			wantETag: true,
		},
		{
			name: "transition request",
			call: call{method: http.MethodPost, path: "/internal/requests/" + reqID + "/transition",
				body: map[string]any{"operationId": "op-t", "at": t0, "ringVersion": 1, "expectedVersion": 2,
					"expectedStatus": "PENDING", "expectedAttemptId": "", "to": "MATCHING", "attempt": attempt, "outbox": outbox}},
			want: store.TransitionRequest{Meta: meta("op-t"), Key: reqKey, ExpectedVersion: 2, ExpectedStatus: store.RequestPending,
				To: store.RequestMatching, Attempt: &attempt, Outbox: outbox},
		},
		{
			name: "retry workflow",
			call: call{method: http.MethodPost, path: "/internal/requests/" + reqID + "/retry",
				body: map[string]any{"operationId": "op-r", "at": t0, "ringVersion": 1, "expectedVersion": 4,
					"nextAttemptAt": t0 + 30_000, "lastError": "no candidate"}},
			want: store.RetryWorkflow{Meta: meta("op-r"), Key: reqKey, ExpectedVersion: 4, NextAttemptAt: t0 + 30_000, LastError: "no candidate"},
		},
		{
			name: "transition resource",
			call: call{method: http.MethodPost, path: "/internal/resources/" + resID + "/transition",
				body: map[string]any{"operationId": "op-tr", "at": t0, "ringVersion": 1, "expectedVersion": 1,
					"status": "OFFLINE", "availability": window}},
			want: store.TransitionResource{Meta: meta("op-tr"), Key: resKey, ExpectedVersion: 1, Status: store.ResourceOffline, Availability: window},
		},
		{
			name: "try claim",
			call: call{method: http.MethodPost, path: "/internal/resources/" + resID + "/claim",
				body: map[string]any{"operationId": "op-claim", "at": t0, "ringVersion": 1, "requestId": reqID, "attempt": attempt, "type": "WATER"}},
			want: store.TryClaim{Meta: meta("op-claim"), ResourceKey: resKey, RequestID: reqID, Attempt: attempt, Type: store.TypeWater},
		},
		{
			name: "close attempt",
			call: call{method: http.MethodPost, path: "/internal/resources/" + resID + "/close-attempt",
				body: map[string]any{"operationId": "op-close", "at": t0, "ringVersion": 1, "requestId": reqID, "attempt": attempt,
					"disposition": "AVAILABLE", "reason": "cancelled"}},
			want: store.CloseAttempt{Meta: meta("op-close"), ResourceKey: resKey, RequestID: reqID, Attempt: attempt,
				Disposition: store.ResourceAvailable, Reason: "cancelled"},
		},
		{
			name: "mark outbox published",
			call: call{method: http.MethodPost, path: fmt.Sprintf("/internal/outbox/published?shard=%d", testShard),
				body: map[string]any{"operationId": "op-pub", "at": t0, "ringVersion": 1, "eventIds": []string{"ev-1", "ev-2"}}},
			want: store.MarkOutboxPublished{Meta: meta("op-pub"), Shard: testShard, EventIDs: []string{"ev-1", "ev-2"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newFakeNode(t)
			rec := do(t, newTestServer(t, n).dataRoutes(), tc.call)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
			}
			got := n.appliedCmds()
			if len(got) != 1 || !reflect.DeepEqual(got[0], tc.want) {
				t.Fatalf("proposed %#v\nwant      %#v", got, tc.want)
			}
			resp := decodeJSON[internalapi.MutationResponse](t, rec)
			if resp.Result != n.applyRes || resp.RingVersion != 1 {
				t.Fatalf("response = %+v, want result %+v at ringVersion 1", resp, n.applyRes)
			}
			if etag := rec.Header().Get("ETag"); tc.wantETag && etag != internalapi.FormatETag(n.applyRes.Version) {
				t.Fatalf("ETag = %q, want %q", etag, internalapi.FormatETag(n.applyRes.Version))
			}
		})
	}
}

// Requests that must never be proposed: bad input, a stale ringVersion, or
// a key this shard does not own.
func TestMutationsRejectedBeforeProposal(t *testing.T) {
	rg := loadRing(t)
	kvKey, _ := keyOn(t, rg, testShard, "", "kv:cfg")
	otherKey, _ := keyOn(t, rg, (testShard+1)%rg.Shards(), "", "kv:cfg")
	_, resID := keyOn(t, rg, testShard, store.ResourceKeyPrefix, "s")
	_, otherResID := keyOn(t, rg, (testShard+1)%rg.Shards(), store.ResourceKeyPrefix, "s")
	ok := map[string]any{"operationId": "op-1", "at": t0, "ringVersion": 1, "value": 1}
	claim := func(ringVersion int) map[string]any {
		return map[string]any{"operationId": "op-c", "at": t0, "ringVersion": ringVersion, "requestId": "r-1",
			"attempt": store.Attempt{AttemptID: "a-1", ResourceID: resID, Fingerprint: "fp"}, "type": "WATER"}
	}
	var tooMany []string
	for i := range 101 {
		tooMany = append(tooMany, fmt.Sprintf("ev-%d", i))
	}

	cases := []struct {
		name   string
		call   call
		status int
		code   internalapi.ErrorCode
	}{
		{"stale ringVersion", call{method: http.MethodPost, path: "/internal/resources/" + resID + "/claim", body: claim(2)},
			http.StatusConflict, internalapi.RINGVERSIONMISMATCH},
		{"key on another shard", call{method: http.MethodPost, path: "/internal/resources/" + otherResID + "/claim", body: claim(1)},
			http.StatusBadRequest, internalapi.INVALID},
		{"generic key on another shard", call{method: http.MethodPut, path: "/internal/data/" + otherKey,
			header: map[string]string{"If-None-Match": "*"}, body: ok}, http.StatusBadRequest, internalapi.INVALID},
		{"unknown body field", call{method: http.MethodPut, path: "/internal/data/" + kvKey, header: map[string]string{"If-None-Match": "*"},
			body: map[string]any{"operationId": "op-1", "at": t0, "ringVersion": 1, "value": 1, "activeClaim": map[string]any{}}},
			http.StatusBadRequest, internalapi.INVALID},
		{"malformed JSON", call{method: http.MethodPut, path: "/internal/data/" + kvKey, header: map[string]string{"If-None-Match": "*"}, body: "{"},
			http.StatusBadRequest, internalapi.INVALID},
		{"both preconditions", call{method: http.MethodPut, path: "/internal/data/" + kvKey,
			header: map[string]string{"If-None-Match": "*", "If-Match": `"1"`}, body: ok}, http.StatusBadRequest, internalapi.INVALID},
		{"no precondition", call{method: http.MethodPut, path: "/internal/data/" + kvKey, body: ok}, http.StatusBadRequest, internalapi.INVALID},
		{"weak entity tag", call{method: http.MethodPut, path: "/internal/data/" + kvKey, header: map[string]string{"If-Match": `W/"1"`}, body: ok},
			http.StatusBadRequest, internalapi.INVALID},
		{"If-None-Match not *", call{method: http.MethodPut, path: "/internal/data/" + kvKey, header: map[string]string{"If-None-Match": `"1"`}, body: ok},
			http.StatusBadRequest, internalapi.INVALID},
		{"delete without If-Match", call{method: http.MethodDelete, path: fmt.Sprintf("/internal/data/%s?operationId=op&at=%d&ringVersion=1", kvKey, t0)},
			http.StatusBadRequest, internalapi.INVALID},
		{"delete without operationId", call{method: http.MethodDelete, path: fmt.Sprintf("/internal/data/%s?at=%d&ringVersion=1", kvKey, t0),
			header: map[string]string{"If-Match": `"1"`}}, http.StatusBadRequest, internalapi.INVALID},
		{"delete with stale ringVersion", call{method: http.MethodDelete, path: fmt.Sprintf("/internal/data/%s?operationId=op&at=%d&ringVersion=7", kvKey, t0),
			header: map[string]string{"If-Match": `"1"`}}, http.StatusConflict, internalapi.RINGVERSIONMISMATCH},
		{"mark published on another shard", call{method: http.MethodPost, path: fmt.Sprintf("/internal/outbox/published?shard=%d", testShard+1),
			body: map[string]any{"operationId": "op", "at": t0, "ringVersion": 1, "eventIds": []string{"ev-1"}}}, http.StatusBadRequest, internalapi.INVALID},
		{"mark published over 100 events", call{method: http.MethodPost, path: fmt.Sprintf("/internal/outbox/published?shard=%d", testShard),
			body: map[string]any{"operationId": "op", "at": t0, "ringVersion": 1, "eventIds": tooMany}}, http.StatusBadRequest, internalapi.INVALID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newFakeNode(t)
			rec := do(t, newTestServer(t, n).dataRoutes(), tc.call)
			e := expectError(t, rec, tc.status, tc.code)
			if got := n.appliedCmds(); len(got) != 0 {
				t.Fatalf("proposed %#v; this request must never reach Raft", got)
			}
			if e.OutcomeUnknown || e.Result != nil {
				t.Fatalf("error = %+v: a rejection before proposal has no result and a known outcome", e)
			}
			if tc.code == internalapi.RINGVERSIONMISMATCH && (!e.Retryable || e.RingVersion == nil || *e.RingVersion != 1) {
				t.Fatalf("ring mismatch = %+v, want retryable with the server's ringVersion 1", e)
			}
		})
	}
}

// contracts/README.md: committed outcomes and their HTTP status on a domain
// route.
func TestCommittedOutcomeMapping(t *testing.T) {
	rg := loadRing(t)
	_, resID := keyOn(t, rg, testShard, store.ResourceKeyPrefix, "s")
	body := map[string]any{"operationId": "op-c", "at": t0, "ringVersion": 1, "requestId": "r-1",
		"attempt": store.Attempt{AttemptID: "a-1", ResourceID: resID, Fingerprint: "fp"}, "type": "WATER"}
	cases := []struct {
		outcome store.Outcome
		status  int
		code    internalapi.ErrorCode
	}{
		{store.OutcomeOK, http.StatusOK, ""},
		{store.ClaimSuccess, http.StatusOK, ""},
		{store.ClaimConflictClosed, http.StatusOK, ""},
		{store.ClaimRejectedClosed, http.StatusOK, ""},
		{store.ClaimClosed, http.StatusOK, ""},
		{store.OutcomeInvalid, http.StatusBadRequest, internalapi.INVALID},
		{store.OutcomeNotFound, http.StatusNotFound, internalapi.NOTFOUND},
		{store.OutcomeDomainConflict, http.StatusConflict, internalapi.DOMAINCONFLICT},
		{store.OutcomeOperationIDReuse, http.StatusConflict, internalapi.OPERATIONIDREUSED},
		{store.OutcomeVersionConflict, http.StatusConflict, internalapi.VERSIONCONFLICT},
		{store.OutcomeExists, http.StatusPreconditionFailed, internalapi.EXISTS},
	}
	for _, tc := range cases {
		t.Run(string(tc.outcome), func(t *testing.T) {
			status, code := outcomeStatus(tc.outcome, false)
			if status != tc.status || code != tc.code {
				t.Fatalf("outcomeStatus = %d %q, want %d %q", status, code, tc.status, tc.code)
			}
			n := newFakeNode(t)
			n.applyRes = store.Result{Outcome: tc.outcome, OperationID: "op-c", Key: "res:" + resID, Version: 2, Reason: "why", Shard: testShard, AppliedIndex: 9}
			rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodPost, path: "/internal/resources/" + resID + "/claim", body: body})
			if tc.status == http.StatusOK {
				if rec.Code != http.StatusOK || decodeJSON[internalapi.MutationResponse](t, rec).Result != n.applyRes {
					t.Fatalf("status %d body %s, want 200 with the result", rec.Code, rec.Body.String())
				}
				return
			}
			e := expectError(t, rec, tc.status, tc.code)
			if e.Result == nil || *e.Result != n.applyRes || e.Retryable || e.OutcomeUnknown {
				t.Fatalf("error = %+v, want the committed result, retryable=false, outcomeUnknown=false", e)
			}
		})
	}
}

// Failures that are not committed outcomes: not leader, no leader, unknown
// outcome (invariant 5), unavailable reads.
func TestFailureMapping(t *testing.T) {
	rg := loadRing(t)
	kvKey, _ := keyOn(t, rg, testShard, "", "kv:cfg")
	put := call{method: http.MethodPut, path: "/internal/data/" + kvKey, header: map[string]string{"If-Match": `"1"`},
		body: map[string]any{"operationId": "op-1", "at": t0, "ringVersion": 1, "value": 2}}
	get := call{method: http.MethodGet, path: "/internal/data/" + kvKey}

	t.Run("not leader with hint", func(t *testing.T) {
		n := newFakeNode(t)
		n.term = 7
		n.applyErr = fmt.Errorf("apply: %w", &raftstore.NotLeaderError{LeaderID: "storage-1-0/5f0c", LeaderAddr: "storage-1-0.storage-1.ns.svc.cluster.local:7000"})
		e := expectError(t, do(t, newTestServer(t, n).dataRoutes(), put), http.StatusServiceUnavailable, internalapi.NOTLEADER)
		want := internalapi.LeaderHint{NodeId: "storage-1-0", Address: "http-of-storage-1-0.storage-1.ns.svc.cluster.local:7000"}
		if e.LeaderHint == nil || e.LeaderHint.NodeId != want.NodeId || e.LeaderHint.Address != want.Address ||
			e.LeaderHint.Term == nil || *e.LeaderHint.Term != 7 {
			t.Fatalf("leaderHint = %+v, want %+v at term 7", e.LeaderHint, want)
		}
		if n.statuses != 0 {
			t.Fatalf("leaderHint called Status %d times; the term lookup must be cheap", n.statuses)
		}
		if !e.Retryable || e.OutcomeUnknown {
			t.Fatalf("not leader = %+v, want retryable and a known outcome", e)
		}
	})
	t.Run("no leader known", func(t *testing.T) {
		n := newFakeNode(t)
		n.applyErr = &raftstore.NotLeaderError{}
		rec := do(t, newTestServer(t, n).dataRoutes(), put)
		e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.NOLEADER)
		if e.LeaderHint != nil || !e.Retryable || e.OutcomeUnknown || rec.Header().Get("Retry-After") == "" {
			t.Fatalf("no leader = %+v (Retry-After %q)", e, rec.Header().Get("Retry-After"))
		}
	})
	t.Run("unknown outcome", func(t *testing.T) {
		n := newFakeNode(t)
		n.applyErr = fmt.Errorf("%w: leadership lost while committing", store.ErrUnknownOutcome)
		rec := do(t, newTestServer(t, n).dataRoutes(), put)
		e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN)
		if !e.OutcomeUnknown || !e.Retryable || e.Result != nil || rec.Header().Get("Retry-After") == "" {
			t.Fatalf("unknown outcome = %+v (Retry-After %q)", e, rec.Header().Get("Retry-After"))
		}
	})
	t.Run("read cannot be fenced", func(t *testing.T) {
		n := newFakeNode(t)
		n.readErr = fmt.Errorf("%w: barrier: timeout", raftstore.ErrUnavailable)
		e := expectError(t, do(t, newTestServer(t, n).dataRoutes(), get), http.StatusServiceUnavailable, internalapi.NOLEADER)
		if e.OutcomeUnknown {
			t.Fatal("a read never has an unknown outcome")
		}
	})
	t.Run("local read failure", func(t *testing.T) {
		n := newFakeNode(t)
		n.readErr = fmt.Errorf("query entity: %w", errors.New("sqlite: disk I/O error"))
		rec := do(t, newTestServer(t, n).dataRoutes(), get)
		e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.STORAGEUNAVAILABLE)
		if !e.Retryable || e.OutcomeUnknown || e.LeaderHint != nil || rec.Header().Get("Retry-After") == "" {
			t.Fatalf("local read failure = %+v (Retry-After %q)", e, rec.Header().Get("Retry-After"))
		}
	})
	t.Run("read on follower", func(t *testing.T) {
		n := newFakeNode(t)
		n.readErr = &raftstore.NotLeaderError{LeaderID: "storage-1-0/5f0c", LeaderAddr: "storage-1-0:7000"}
		expectError(t, do(t, newTestServer(t, n).dataRoutes(), get), http.StatusServiceUnavailable, internalapi.NOTLEADER)
	})
	t.Run("absent key", func(t *testing.T) {
		n := newFakeNode(t)
		expectError(t, do(t, newTestServer(t, n).dataRoutes(), get), http.StatusNotFound, internalapi.NOTFOUND)
	})
}

// Strong by default (M5); linearizable is served strong until S4.1; stale
// only on request, marked stalePossible.
func TestReadConsistency(t *testing.T) {
	rg := loadRing(t)
	kvKey, _ := keyOn(t, rg, testShard, "", "kv:cfg")
	cases := []struct {
		query     string
		mode      raftstore.ReadMode
		reported  internalapi.ReadInfoConsistency
		stalePoss bool
	}{
		{"?", raftstore.ReadStrong, internalapi.ReadInfoConsistencyStrong, false},
		{"?consistency=strong", raftstore.ReadStrong, internalapi.ReadInfoConsistencyStrong, false},
		{"?consistency=linearizable", raftstore.ReadStrong, internalapi.ReadInfoConsistencyStrong, false},
		{"?consistency=stale", raftstore.ReadStale, internalapi.ReadInfoConsistencyStale, true},
	}
	for _, tc := range cases {
		t.Run("consistency="+strings.TrimPrefix(tc.query, "?"), func(t *testing.T) {
			n := newFakeNode(t)
			n.seed(t, store.CompareAndSet{Meta: meta("op-1"), Key: kvKey, Value: json.RawMessage(`7`)})
			rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: "/internal/data/" + kvKey + tc.query})
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if len(n.modes) != 1 || n.modes[0] != tc.mode {
				t.Fatalf("read modes = %v, want [%v]", n.modes, tc.mode)
			}
			resp := decodeJSON[internalapi.RecordResponse](t, rec)
			if resp.Read.Consistency != tc.reported || resp.Read.StalePossible != tc.stalePoss || resp.Read.ServedBy != testPod ||
				resp.Read.AppliedIndex != 1 || resp.RingVersion != 1 {
				t.Fatalf("read = %+v ringVersion %d", resp.Read, resp.RingVersion)
			}
			if resp.Record.Key != kvKey || resp.Record.Version != 1 || string(resp.Record.Value) != "7" {
				t.Fatalf("record = %+v", resp.Record)
			}
			if etag := rec.Header().Get("ETag"); etag != `"1"` {
				t.Fatalf("ETag = %q", etag)
			}
		})
	}
	t.Run("unknown mode", func(t *testing.T) {
		n := newFakeNode(t)
		expectError(t, do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: "/internal/data/" + kvKey + "?consistency=eventual"}),
			http.StatusBadRequest, internalapi.INVALID)
		if len(n.modes) != 0 {
			t.Fatal("a bad consistency still read")
		}
	})
}

// A key another shard owns is 400 INVALID on every read, never a 404
// "absent", and is never read. Scan takes no key: it reads only this shard,
// and its one position names this shard.
func TestReadsRejectForeignKeys(t *testing.T) {
	rg := loadRing(t)
	other := (testShard + 1) % rg.Shards()
	kvKey, _ := keyOn(t, rg, other, "", "kv:cfg")
	resKey, resID := keyOn(t, rg, other, store.ResourceKeyPrefix, "s")
	for _, mode := range []string{"", "?consistency=stale"} {
		for name, path := range map[string]string{
			"record":    "/internal/data/" + kvKey + mode,
			"domain":    "/internal/data/" + resKey + mode,
			"closure":   "/internal/resources/" + resID + "/closures/a-1" + mode,
			"operation": "/internal/operations/op-1?key=" + resKey + strings.Replace(mode, "?", "&", 1),
		} {
			t.Run(name+mode, func(t *testing.T) {
				n := newFakeNode(t)
				e := expectError(t, do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: path}),
					http.StatusBadRequest, internalapi.INVALID)
				if !strings.Contains(e.Message, fmt.Sprintf("shard %d", other)) {
					t.Fatalf("message %q does not name the owning shard", e.Message)
				}
				if len(n.modes) != 0 {
					t.Fatal("a foreign key was read")
				}
			})
		}
	}
	t.Run("scan reports only this shard", func(t *testing.T) {
		n := newFakeNode(t)
		rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: "/internal/scan?kind=kv&limit=10"})
		resp := decodeJSON[internalapi.ScanResponse](t, rec)
		if rec.Code != http.StatusOK || len(resp.Positions) != 1 || resp.Positions[0].Shard != testShard || resp.Partial {
			t.Fatalf("%d %+v", rec.Code, resp)
		}
	})
}

func TestOtherReadRoutes(t *testing.T) {
	rg := loadRing(t)
	resKey, resID := keyOn(t, rg, testShard, store.ResourceKeyPrefix, "s")
	_, otherResID := keyOn(t, rg, (testShard+1)%rg.Shards(), store.ResourceKeyPrefix, "s")
	n := newFakeNode(t)
	attempt := store.Attempt{AttemptID: "a-1", ResourceID: resID, Fingerprint: "fp"}
	ev := store.OutboxEvent{EventID: "ev-1", EventType: "resource.registered", SchemaVersion: 1, Exchange: "relief.resources",
		RoutingKey: "resource.registered.water", Payload: json.RawMessage(`{"id":"x"}`)}
	n.seed(t, store.CreateEntity{Meta: meta("op-create"), Key: resKey, Outbox: []store.OutboxEvent{ev},
		Resource: &store.Resource{ID: resID, Type: store.TypeWater, Name: "T", Geohash: "ttnfv8k", AvailableFrom: t0,
			AvailableUntil: t0 + 86_400_000, Status: store.ResourceAvailable, Reusable: true}})
	n.seed(t, store.CloseAttempt{Meta: meta("op-close"), ResourceKey: resKey, RequestID: "r-1", Attempt: attempt,
		Disposition: store.ResourceAvailable, Reason: "cancelled"})
	h := newTestServer(t, n).dataRoutes()

	t.Run("closure", func(t *testing.T) {
		rec := do(t, h, call{method: http.MethodGet, path: "/internal/resources/" + resID + "/closures/a-1"})
		resp := decodeJSON[internalapi.ClosureResponse](t, rec)
		if rec.Code != http.StatusOK || resp.Closure.ResourceKey != resKey || resp.Closure.AttemptID != "a-1" || resp.Read.Consistency != "strong" {
			t.Fatalf("%d %+v", rec.Code, resp)
		}
		expectError(t, do(t, h, call{method: http.MethodGet, path: "/internal/resources/" + resID + "/closures/a-404"}), http.StatusNotFound, internalapi.NOTFOUND)
		expectError(t, do(t, h, call{method: http.MethodGet, path: "/internal/resources/" + otherResID + "/closures/a-1"}), http.StatusBadRequest, internalapi.INVALID)
	})
	t.Run("operation", func(t *testing.T) {
		rec := do(t, h, call{method: http.MethodGet, path: "/internal/operations/op-close?key=" + resKey})
		resp := decodeJSON[internalapi.OperationResponse](t, rec)
		if rec.Code != http.StatusOK || resp.Result.OperationID != "op-close" || !resp.Result.Replayed {
			t.Fatalf("%d %+v: a historical result is marked replayed", rec.Code, resp)
		}
		expectError(t, do(t, h, call{method: http.MethodGet, path: "/internal/operations/op-close"}), http.StatusBadRequest, internalapi.INVALID)
		expectError(t, do(t, h, call{method: http.MethodGet, path: "/internal/operations/op-404?key=" + resKey}), http.StatusNotFound, internalapi.NOTFOUND)
	})
	t.Run("outbox", func(t *testing.T) {
		rec := do(t, h, call{method: http.MethodGet, path: fmt.Sprintf("/internal/outbox?shard=%d&limit=10", testShard)})
		resp := decodeJSON[internalapi.OutboxPage](t, rec)
		if rec.Code != http.StatusOK || resp.Shard != testShard || len(resp.Events) != 1 || resp.Events[0].EventID != "ev-1" || resp.Events[0].EntityKey != resKey {
			t.Fatalf("%d %+v", rec.Code, resp)
		}
		for _, q := range []string{fmt.Sprintf("shard=%d&limit=10", testShard+1), fmt.Sprintf("shard=%d", testShard),
			fmt.Sprintf("shard=%d&limit=0", testShard), fmt.Sprintf("shard=%d&limit=101", testShard), "limit=10"} {
			expectError(t, do(t, h, call{method: http.MethodGet, path: "/internal/outbox?" + q}), http.StatusBadRequest, internalapi.INVALID)
		}
	})
	t.Run("scan", func(t *testing.T) {
		rec := do(t, h, call{method: http.MethodGet, path: "/internal/scan?kind=resource&type=WATER&statuses=AVAILABLE&geohashPrefixes=ttn&limit=5"})
		resp := decodeJSON[internalapi.ScanResponse](t, rec)
		if rec.Code != http.StatusOK || len(resp.Records) != 1 || resp.Records[0].Key != resKey || resp.Partial ||
			len(resp.Positions) != 1 || resp.Positions[0] != (store.ShardPosition{Shard: testShard, AppliedIndex: 2}) ||
			resp.Consistency != "strong" || resp.RingVersion != 1 {
			t.Fatalf("%d %+v", rec.Code, resp)
		}
		expectError(t, do(t, h, call{method: http.MethodGet, path: "/internal/scan"}), http.StatusBadRequest, internalapi.INVALID)
		expectError(t, do(t, h, call{method: http.MethodGet, path: "/internal/scan?kind=planet"}), http.StatusBadRequest, internalapi.INVALID)
	})
}

// §40: VERSION_CONFLICT is 412 for a failed generic CAS (PUT/DELETE
// /internal/data) and 409 on the domain transition routes, where it also
// covers a status or attempt mismatch.
func TestVersionConflictStatusFollowsRoute(t *testing.T) {
	rg := loadRing(t)
	_, reqID := keyOn(t, rg, testShard, store.RequestKeyPrefix, "r")
	_, resID := keyOn(t, rg, testShard, store.ResourceKeyPrefix, "s")
	kvKey, _ := keyOn(t, rg, testShard, "", "kv:cfg")
	cases := []struct {
		name   string
		call   call
		status int
	}{
		{"transition request", call{method: http.MethodPost, path: "/internal/requests/" + reqID + "/transition",
			body: map[string]any{"operationId": "op-t", "at": t0, "ringVersion": 1, "expectedVersion": 2,
				"expectedStatus": "PENDING", "expectedAttemptId": "", "to": "CANCELLED"}}, http.StatusConflict},
		{"retry workflow", call{method: http.MethodPost, path: "/internal/requests/" + reqID + "/retry",
			body: map[string]any{"operationId": "op-r", "at": t0, "ringVersion": 1, "expectedVersion": 4, "nextAttemptAt": t0}}, http.StatusConflict},
		{"transition resource", call{method: http.MethodPost, path: "/internal/resources/" + resID + "/transition",
			body: map[string]any{"operationId": "op-tr", "at": t0, "ringVersion": 1, "expectedVersion": 1, "status": "OFFLINE"}}, http.StatusConflict},
		{"put if-match", call{method: http.MethodPut, path: "/internal/data/" + kvKey, header: map[string]string{"If-Match": `"4"`},
			body: map[string]any{"operationId": "op-cas", "at": t0, "ringVersion": 1, "value": 1}}, http.StatusPreconditionFailed},
		{"delete", call{method: http.MethodDelete, path: fmt.Sprintf("/internal/data/%s?operationId=op-del&at=%d&ringVersion=1", kvKey, t0),
			header: map[string]string{"If-Match": `"5"`}}, http.StatusPreconditionFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newFakeNode(t)
			n.applyRes = store.Result{Outcome: store.OutcomeVersionConflict, OperationID: "op", Key: "k", Version: 3, Reason: "mismatch", Shard: testShard, AppliedIndex: 9}
			e := expectError(t, do(t, newTestServer(t, n).dataRoutes(), tc.call), tc.status, internalapi.VERSIONCONFLICT)
			if e.Result == nil || *e.Result != n.applyRes || e.Retryable || e.OutcomeUnknown {
				t.Fatalf("error = %+v, want the committed result", e)
			}
		})
	}
}

// Scan is bounded (limit 1 to 1000, required) and checks an optional
// ringVersion before reading.
func TestScanLimitAndRingVersion(t *testing.T) {
	for _, q := range []string{"kind=kv", "kind=kv&limit=0", "kind=kv&limit=1001", "kind=kv&limit=x", "kind=kv&limit=-1",
		"kind=kv&limit=10&ringVersion=x"} {
		t.Run(q, func(t *testing.T) {
			n := newFakeNode(t)
			expectError(t, do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: "/internal/scan?" + q}),
				http.StatusBadRequest, internalapi.INVALID)
			if len(n.modes) != 0 {
				t.Fatal("a bad scan still read")
			}
		})
	}
	t.Run("stale ringVersion", func(t *testing.T) {
		n := newFakeNode(t)
		e := expectError(t, do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: "/internal/scan?kind=kv&limit=10&ringVersion=2"}),
			http.StatusConflict, internalapi.RINGVERSIONMISMATCH)
		if !e.Retryable || e.RingVersion == nil || *e.RingVersion != 1 || len(n.modes) != 0 {
			t.Fatalf("mismatch = %+v (reads %v), want retryable, ringVersion 1, nothing read", e, n.modes)
		}
	})
	for _, q := range []string{"kind=kv&limit=1000", "kind=kv&limit=1&ringVersion=1"} {
		t.Run(q, func(t *testing.T) {
			n := newFakeNode(t)
			if rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: "/internal/scan?" + q}); rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// GET /internal/outbox: consistency (strong default), the read block, and
// wait (0 to 1 s) long-polling outside the read.
func TestOutboxConsistencyAndLongPoll(t *testing.T) {
	rg := loadRing(t)
	resKey, resID := keyOn(t, rg, testShard, store.ResourceKeyPrefix, "s")
	create := store.CreateEntity{Meta: meta("op-create"), Key: resKey, Outbox: []store.OutboxEvent{{EventID: "ev-1", EventType: "resource.registered",
		SchemaVersion: 1, Exchange: "relief.resources", RoutingKey: "resource.registered.water", Payload: json.RawMessage(`{"id":"x"}`)}},
		Resource: &store.Resource{ID: resID, Type: store.TypeWater, Name: "T", Geohash: "ttnfv8k", AvailableFrom: t0,
			AvailableUntil: t0 + 86_400_000, Status: store.ResourceAvailable, Reusable: true}}
	path := func(q string) string { return fmt.Sprintf("/internal/outbox?shard=%d&limit=10%s", testShard, q) }

	for _, tc := range []struct {
		query string
		mode  raftstore.ReadMode
		read  internalapi.ReadInfoConsistency
	}{
		{"", raftstore.ReadStrong, internalapi.ReadInfoConsistencyStrong},
		{"&consistency=linearizable", raftstore.ReadStrong, internalapi.ReadInfoConsistencyStrong},
		{"&consistency=stale", raftstore.ReadStale, internalapi.ReadInfoConsistencyStale},
	} {
		t.Run("consistency"+tc.query, func(t *testing.T) {
			n := newFakeNode(t)
			n.seed(t, create)
			rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: path(tc.query + "&wait=1")})
			resp := decodeJSON[internalapi.OutboxPage](t, rec)
			if rec.Code != http.StatusOK || len(resp.Events) != 1 || len(n.modes) != 1 || n.modes[0] != tc.mode {
				t.Fatalf("%d %+v modes %v", rec.Code, resp, n.modes)
			}
			if resp.Read.Consistency != tc.read || resp.Read.ServedBy != testPod || resp.Read.AppliedIndex != 1 ||
				resp.Read.StalePossible != (tc.mode == raftstore.ReadStale) {
				t.Fatalf("read = %+v", resp.Read)
			}
			if len(n.waits) != 0 {
				t.Fatal("waited although events were pending")
			}
		})
	}
	for _, q := range []string{"&consistency=eventual", "&wait=2", "&wait=-1", "&wait=x"} {
		t.Run("bad"+q, func(t *testing.T) {
			n := newFakeNode(t)
			expectError(t, do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: path(q)}), http.StatusBadRequest, internalapi.INVALID)
			if len(n.modes) != 0 {
				t.Fatal("a bad request still read")
			}
		})
	}
	t.Run("wait=0 returns at once", func(t *testing.T) {
		n := newFakeNode(t)
		rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: path("")})
		if resp := decodeJSON[internalapi.OutboxPage](t, rec); rec.Code != http.StatusOK || len(resp.Events) != 0 || len(n.waits) != 0 {
			t.Fatalf("%d %+v waits %v", rec.Code, resp, n.waits)
		}
	})
	t.Run("a commit during the wait is returned", func(t *testing.T) {
		n := newFakeNode(t)
		n.onWait = func() { n.seed(t, create) }
		start := time.Now()
		rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: path("&wait=1")})
		resp := decodeJSON[internalapi.OutboxPage](t, rec)
		if rec.Code != http.StatusOK || len(resp.Events) != 1 || resp.Events[0].EventID != "ev-1" || resp.Read.AppliedIndex != 1 {
			t.Fatalf("%d %+v", rec.Code, resp)
		}
		if !reflect.DeepEqual(n.waits, []uint64{0}) || n.waitedInRead || len(n.modes) != 2 {
			t.Fatalf("waits %v (inside a read: %v), reads %v: want one wait after index 0, outside the read, then a re-read", n.waits, n.waitedInRead, n.modes)
		}
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Fatalf("took %v; a commit must end the wait", d)
		}
	})
	t.Run("nothing pending after the wait is an empty page", func(t *testing.T) {
		n := newFakeNode(t)
		start := time.Now()
		rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: path("&wait=1")})
		resp := decodeJSON[internalapi.OutboxPage](t, rec)
		if d := time.Since(start); rec.Code != http.StatusOK || len(resp.Events) != 0 || d < 900*time.Millisecond || d > 3*time.Second {
			t.Fatalf("%d %+v after %v, want an empty 200 after about 1 s", rec.Code, resp, d)
		}
	})
}
