package coordclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const t0 = int64(1_792_800_000_000)

var ringV1 = internalapi.Ring{RingVersion: 1, Hash: internalapi.Xxhash64, Shards: 3, VnodesPerShard: 128,
	Checksum: "sha256:670e1eca690b168c83fd910b2bdf1e2fbf81f4cd8de91994d8d5ac2989de6360"}

type seen struct {
	method, path, query string
	header              http.Header
	body                []byte
}

// fakeCoordinator serves GET /internal/ring and answers every other call
// with answer (default: 200 with a committed OK).
type fakeCoordinator struct {
	mu     sync.Mutex
	ring   internalapi.Ring
	rings  int
	reqs   []seen
	answer func(w http.ResponseWriter, r *http.Request, n int) // n counts non-ring calls from 1
	srv    *httptest.Server
}

func newFake(t *testing.T) *fakeCoordinator {
	f := &fakeCoordinator{ring: ringV1}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		if r.Method == http.MethodGet && r.URL.Path == "/internal/ring" {
			f.rings++
			ring := f.ring
			f.mu.Unlock()
			writeJSON(w, http.StatusOK, ring)
			return
		}
		f.reqs = append(f.reqs, seen{r.Method, r.URL.EscapedPath(), r.URL.RawQuery, r.Header.Clone(), body})
		n, answer := len(f.reqs), f.answer
		f.mu.Unlock()
		if answer == nil {
			writeJSON(w, http.StatusOK, internalapi.MutationResponse{RingVersion: 1, Result: okResult})
			return
		}
		answer(w, r, n)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCoordinator) requests() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seen(nil), f.reqs...)
}

func (f *fakeCoordinator) setAnswer(a func(w http.ResponseWriter, r *http.Request, n int)) {
	f.mu.Lock()
	f.answer = a
	f.mu.Unlock()
}

var okResult = store.Result{Outcome: store.OutcomeOK, OperationID: "op-1", Key: "kv:a", Version: 2, Shard: 1, AppliedIndex: 40}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newClient(t *testing.T, f *fakeCoordinator, opts ...coordclient.Option) *coordclient.Client {
	t.Helper()
	c, err := coordclient.New(testCtx(t), f.srv.URL, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func meta(op string) store.Meta { return store.Meta{OperationID: op, At: t0} }

func TestNewLoadsTheRing(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f)
	if c.Shards() != 3 {
		t.Fatalf("Shards = %d, want 3", c.Shards())
	}
	f.srv.Close()
	if _, err := coordclient.New(testCtx(t), f.srv.URL); err == nil {
		t.Fatal("New with no coordinator succeeded")
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, "nonsense") }))
	defer bad.Close()
	if _, err := coordclient.New(testCtx(t), bad.URL); err == nil {
		t.Fatal("New with a garbled ring succeeded")
	}
}

// Each Store method sends its contract route: method, path (IDs taken from
// the req:/res: key, path-escaped), conditional headers, query, and a body
// that is the command plus ringVersion.
func TestRequestShapes(t *testing.T) {
	a := store.Attempt{AttemptID: "a-1", ResourceID: "s 7", Fingerprint: "fp"}
	win := store.Window{From: t0, Until: t0 + 1}
	cases := []struct {
		name              string
		call              func(context.Context, *coordclient.Client) error
		method, path, qry string
		header            map[string]string
		body              string // JSON; "" for none
	}{
		{"CreateEntity", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.CreateEntity(ctx, store.CreateEntity{Meta: meta("op-c"), Key: "kv:a b/c", Value: json.RawMessage(`{"x":1}`)})
			return err
		}, http.MethodPut, "/internal/data/kv:a%20b%2Fc", "", map[string]string{"If-None-Match": "*", "If-Match": ""},
			`{"operationId":"op-c","at":1792800000000,"ringVersion":1,"value":{"x":1}}`},
		{"CompareAndSet", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.CompareAndSet(ctx, store.CompareAndSet{Meta: meta("op-s"), Key: "kv:a", ExpectedVersion: 4, Value: json.RawMessage(`2`)})
			return err
		}, http.MethodPut, "/internal/data/kv:a", "", map[string]string{"If-Match": `"4"`, "If-None-Match": ""},
			`{"operationId":"op-s","at":1792800000000,"ringVersion":1,"value":2}`},
		{"CompareAndSet expected 0", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.CompareAndSet(ctx, store.CompareAndSet{Meta: meta("op-s0"), Key: "kv:a", Value: json.RawMessage(`2`)})
			return err
		}, http.MethodPut, "/internal/data/kv:a", "", map[string]string{"If-Match": `"0"`},
			`{"operationId":"op-s0","at":1792800000000,"ringVersion":1,"value":2}`},
		{"CompareAndSet delete", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.CompareAndSet(ctx, store.CompareAndSet{Meta: meta("op d"), Key: "kv:a", ExpectedVersion: 5, Delete: true})
			return err
		}, http.MethodDelete, "/internal/data/kv:a", "at=1792800000000&operationId=op+d&ringVersion=1", map[string]string{"If-Match": `"5"`}, ""},
		{"TransitionRequest", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.TransitionRequest(ctx, store.TransitionRequest{Meta: meta("op-t"), Key: "req:q 1", ExpectedVersion: 2, ExpectedStatus: store.RequestPending,
				To: store.RequestMatching, Attempt: &a})
			return err
		}, http.MethodPost, "/internal/requests/q%201/transition", "", nil,
			`{"operationId":"op-t","at":1792800000000,"ringVersion":1,"expectedVersion":2,"expectedStatus":"PENDING","expectedAttemptId":"","to":"MATCHING","attempt":{"attemptId":"a-1","resourceId":"s 7","fingerprint":"fp"}}`},
		{"RetryWorkflow", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.RetryWorkflow(ctx, store.RetryWorkflow{Meta: meta("op-r"), Key: "req:q1", ExpectedVersion: 3, NextAttemptAt: t0 + 5, LastError: "none"})
			return err
		}, http.MethodPost, "/internal/requests/q1/retry", "", nil,
			`{"operationId":"op-r","at":1792800000000,"ringVersion":1,"expectedVersion":3,"nextAttemptAt":1792800000005,"lastError":"none"}`},
		{"TransitionResource", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.TransitionResource(ctx, store.TransitionResource{Meta: meta("op-tr"), Key: "res:s7", ExpectedVersion: 1, Status: store.ResourceOffline, Availability: &win})
			return err
		}, http.MethodPost, "/internal/resources/s7/transition", "", nil,
			`{"operationId":"op-tr","at":1792800000000,"ringVersion":1,"expectedVersion":1,"status":"OFFLINE","availability":{"from":1792800000000,"until":1792800000001}}`},
		{"TryClaim", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.TryClaim(ctx, store.TryClaim{Meta: meta("op-k"), ResourceKey: "res:s 7", RequestID: "q1", Attempt: a, Type: store.TypeWater})
			return err
		}, http.MethodPost, "/internal/resources/s%207/claim", "", nil,
			`{"operationId":"op-k","at":1792800000000,"ringVersion":1,"requestId":"q1","attempt":{"attemptId":"a-1","resourceId":"s 7","fingerprint":"fp"},"type":"WATER"}`},
		{"CloseAttempt", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.CloseAttempt(ctx, store.CloseAttempt{Meta: meta("op-x"), ResourceKey: "res:s7", RequestID: "q1", Attempt: a, Disposition: store.ResourceAvailable, Reason: "done"})
			return err
		}, http.MethodPost, "/internal/resources/s7/close-attempt", "", nil,
			`{"operationId":"op-x","at":1792800000000,"ringVersion":1,"requestId":"q1","attempt":{"attemptId":"a-1","resourceId":"s 7","fingerprint":"fp"},"disposition":"AVAILABLE","reason":"done"}`},
		{"MarkOutboxPublished", func(ctx context.Context, c *coordclient.Client) error {
			_, err := c.MarkOutboxPublished(ctx, store.MarkOutboxPublished{Meta: meta("op-p"), Shard: 2, EventIDs: []string{"e1", "e2"}})
			return err
		}, http.MethodPost, "/internal/outbox/published", "shard=2", nil,
			`{"operationId":"op-p","at":1792800000000,"ringVersion":1,"eventIds":["e1","e2"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			c := newClient(t, f)
			if err := tc.call(testCtx(t), c); err != nil {
				t.Fatalf("call: %v", err)
			}
			reqs := f.requests()
			if len(reqs) != 1 {
				t.Fatalf("%d requests, want 1", len(reqs))
			}
			got := reqs[0]
			if got.method != tc.method || got.path != tc.path || got.query != tc.qry {
				t.Fatalf("sent %s %s?%s, want %s %s?%s", got.method, got.path, got.query, tc.method, tc.path, tc.qry)
			}
			for k, v := range tc.header {
				if got.header.Get(k) != v {
					t.Fatalf("header %s = %q, want %q", k, got.header.Get(k), v)
				}
			}
			if tc.body == "" {
				if len(got.body) != 0 {
					t.Fatalf("body %q, want none", got.body)
				}
				return
			}
			var gotBody, wantBody any
			if err := json.Unmarshal(got.body, &gotBody); err != nil {
				t.Fatalf("body %q: %v", got.body, err)
			}
			_ = json.Unmarshal([]byte(tc.body), &wantBody)
			if !reflect.DeepEqual(gotBody, wantBody) {
				t.Fatalf("body = %s\nwant   %s", got.body, tc.body)
			}
		})
	}
}

func TestReadShapes(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f)
	ctx := testCtx(t)
	rec := store.Record{Key: "kv:a", Kind: store.KindKV, Version: 3, Value: json.RawMessage(`1`)}
	read := internalapi.ReadInfo{Consistency: internalapi.ReadInfoConsistencyLinearizable}
	f.setAnswer(func(w http.ResponseWriter, r *http.Request, _ int) {
		switch r.URL.Path {
		case "/internal/data/kv:a":
			writeJSON(w, http.StatusOK, internalapi.RecordResponse{Record: rec, Read: read, RingVersion: 1})
		case "/internal/resources/s7/closures/a-1":
			writeJSON(w, http.StatusOK, internalapi.ClosureResponse{Closure: store.Closure{ResourceKey: "res:s7", AttemptID: "a-1"}, Read: read, RingVersion: 1})
		case "/internal/operations/op-1":
			writeJSON(w, http.StatusOK, internalapi.OperationResponse{Result: okResult, Read: read, RingVersion: 1})
		case "/internal/outbox":
			writeJSON(w, http.StatusOK, internalapi.OutboxPage{Shard: 2, Events: []store.OutboxEvent{{EventID: "e1"}}, Read: read, RingVersion: 1})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
		}
	})
	if got, err := c.ReadRecord(ctx, "kv:a"); err != nil || got.Version != 3 {
		t.Fatalf("ReadRecord = %+v, %v", got, err)
	}
	if got, err := c.ReadClosure(ctx, "res:s7", "a-1"); err != nil || got.AttemptID != "a-1" {
		t.Fatalf("ReadClosure = %+v, %v", got, err)
	}
	if got, err := c.Operation(ctx, "kv:a", "op-1"); err != nil || got != okResult {
		t.Fatalf("Operation = %+v, %v", got, err)
	}
	if got, err := c.PendingOutbox(ctx, store.OutboxQuery{Shard: 2, Limit: 50}); err != nil || len(got) != 1 || got[0].EventID != "e1" {
		t.Fatalf("PendingOutbox = %+v, %v", got, err)
	}
	want := []string{"/internal/data/kv:a?consistency=linearizable", "/internal/resources/s7/closures/a-1?consistency=linearizable", "/internal/operations/op-1?consistency=linearizable&key=kv%3Aa", "/internal/outbox?consistency=linearizable&limit=50&shard=2"}
	for i, r := range f.requests() {
		if r.method != http.MethodGet || r.path+"?"+r.query != want[i] {
			t.Errorf("read %d = %s %s?%s, want GET %s", i, r.method, r.path, r.query, want[i])
		}
	}
	// A key that is not a resource key cannot name a closure route.
	if _, err := c.ReadClosure(ctx, "req:q1", "a-1"); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ReadClosure(req:) = %v, want a non-NotFound error", err)
	}
}

func TestScanShapeAndPartial(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f)
	failed := []int{1}
	f.setAnswer(func(w http.ResponseWriter, _ *http.Request, _ int) {
		writeJSON(w, http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStrong,
			Records: []store.Record{{Key: "res:a"}}, Partial: true, FailedShards: &failed, Positions: []store.ShardPosition{{Shard: 0, AppliedIndex: 3, HasMore: true}, {Shard: 2, AppliedIndex: 4}}})
	})
	got, err := c.Scan(testCtx(t), store.ScanQuery{Kind: store.KindResource, Type: store.TypeWater, Statuses: []string{"AVAILABLE", "OFFLINE"}, GeohashPrefixes: []string{"tt n", "ttp"}, Limit: 20})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !got.Partial || !reflect.DeepEqual(got.FailedShards, []int{1}) || len(got.Records) != 1 || len(got.Positions) != 2 ||
		!got.Positions[0].HasMore || got.Positions[1].HasMore {
		t.Fatalf("scan = %+v, want the partial result (and hasMore) unchanged", got)
	}
	r := f.requests()[0]
	if r.path != "/internal/scan" || r.query != "consistency=linearizable&geohashPrefixes=tt+n&geohashPrefixes=ttp&kind=resource&limit=20&ringVersion=1&statuses=AVAILABLE&statuses=OFFLINE&type=WATER" {
		t.Fatalf("scan sent %s?%s", r.path, r.query)
	}

	// Limit must be 1 to 1000: 0 is invalid, never "no limit", and nothing
	// is sent.
	for _, limit := range []int{0, -1, 1001} {
		_, err := c.Scan(testCtx(t), store.ScanQuery{Kind: store.KindKV, Limit: limit})
		var ce *coordclient.Error
		if !errors.As(err, &ce) || ce.Code != internalapi.INVALID || ce.Retryable || errors.Is(err, store.ErrUnknownOutcome) {
			t.Fatalf("limit %d: err = %v, want a non-retryable INVALID", limit, err)
		}
	}
	if n := len(f.requests()); n != 1 {
		t.Fatalf("%d scans sent, want only the valid one", n)
	}

	// An answer that drops a shard without listing it is partial anyway.
	f.setAnswer(answerWith(http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStrong,
		Records: []store.Record{}, Positions: []store.ShardPosition{{Shard: 0}, {Shard: 1}}}))
	got, err = c.Scan(testCtx(t), store.ScanQuery{Kind: store.KindKV, Limit: 5})
	if err != nil || !got.Partial || !reflect.DeepEqual(got.FailedShards, []int{2}) {
		t.Fatalf("scan missing shard 2 = %+v, %v; want partial [2]", got, err)
	}
}

type outcome int

const (
	committed   outcome = iota // Result, nil
	unknown                    // error matching ErrUnknownOutcome
	notProposed                // retryable error, not ErrUnknownOutcome
	refused                    // non-retryable error, not ErrUnknownOutcome
)

func answerWith(status int, body any) func(http.ResponseWriter, *http.Request, int) {
	return func(w http.ResponseWriter, _ *http.Request, _ int) { writeJSON(w, status, body) }
}

func errBody(code internalapi.ErrorCode, outcome store.Outcome) internalapi.Error {
	e := internalapi.Error{Code: code, Message: string(code)}
	if outcome != "" {
		e.Result = &store.Result{Outcome: outcome, OperationID: "op-1", Key: "kv:a", Version: 3, Shard: 1, AppliedIndex: 9, Reason: "why"}
	}
	return e
}

// contracts/README.md, from a mutation's side. A committed outcome is never
// an error, an unknown outcome is never a rejection, and nothing proposed
// is not unknown.
func TestMutationOutcomeMapping(t *testing.T) {
	cases := []struct {
		name   string
		answer func(http.ResponseWriter, *http.Request, int)
		want   outcome
		result store.Outcome
	}{
		{"200 OK", nil, committed, store.OutcomeOK},
		{"200 SUCCESS", answerWith(200, internalapi.MutationResponse{RingVersion: 1, Result: store.Result{Outcome: store.ClaimSuccess}}), committed, store.ClaimSuccess},
		{"200 CONFLICT_CLOSED", answerWith(200, internalapi.MutationResponse{RingVersion: 1, Result: store.Result{Outcome: store.ClaimConflictClosed}}), committed, store.ClaimConflictClosed},
		{"400 INVALID with result", answerWith(400, errBody(internalapi.INVALID, store.OutcomeInvalid)), committed, store.OutcomeInvalid},
		{"404 NOT_FOUND with result", answerWith(404, errBody(internalapi.NOTFOUND, store.OutcomeNotFound)), committed, store.OutcomeNotFound},
		{"409 DOMAIN_CONFLICT", answerWith(409, errBody(internalapi.DOMAINCONFLICT, store.OutcomeDomainConflict)), committed, store.OutcomeDomainConflict},
		{"409 OPERATION_ID_REUSED", answerWith(409, errBody(internalapi.OPERATIONIDREUSED, store.OutcomeOperationIDReuse)), committed, store.OutcomeOperationIDReuse},
		{"409 VERSION_CONFLICT", answerWith(409, errBody(internalapi.VERSIONCONFLICT, store.OutcomeVersionConflict)), committed, store.OutcomeVersionConflict},
		{"412 VERSION_CONFLICT", answerWith(412, errBody(internalapi.VERSIONCONFLICT, store.OutcomeVersionConflict)), committed, store.OutcomeVersionConflict},
		{"412 EXISTS", answerWith(412, errBody(internalapi.EXISTS, store.OutcomeExists)), committed, store.OutcomeExists},
		{"503 OUTCOME_UNKNOWN", answerWith(503, internalapi.Error{Code: internalapi.OUTCOMEUNKNOWN, Retryable: true, OutcomeUnknown: true}), unknown, ""},
		{"503 NOT_LEADER", answerWith(503, internalapi.Error{Code: internalapi.NOTLEADER, Retryable: true}), notProposed, ""},
		{"503 NO_LEADER", answerWith(503, internalapi.Error{Code: internalapi.NOLEADER, Retryable: true}), notProposed, ""},
		{"503 STORAGE_UNAVAILABLE", answerWith(503, internalapi.Error{Code: internalapi.STORAGEUNAVAILABLE, Retryable: true}), notProposed, ""},
		{"503 without a body", func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(503) }, unknown, ""},
		{"500", func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(500) }, unknown, ""},
		{"502 from a proxy", func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(502) }, unknown, ""},
		{"200 garbled", func(w http.ResponseWriter, _ *http.Request, _ int) { _, _ = w.Write([]byte("{")) }, unknown, ""},
		{"connection dropped", func(w http.ResponseWriter, _ *http.Request, _ int) {
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				_ = conn.Close()
			}
		}, unknown, ""},
		{"400 without result (never proposed)", answerWith(400, internalapi.Error{Code: internalapi.INVALID, Message: "bad body"}), refused, ""},
		{"401", answerWith(401, internalapi.Error{Code: internalapi.UNAUTHORIZED}), refused, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			c := newClient(t, f)
			f.setAnswer(tc.answer)
			res, err := c.TryClaim(testCtx(t), store.TryClaim{Meta: meta("op-1"), ResourceKey: "res:s7", RequestID: "q1",
				Attempt: store.Attempt{AttemptID: "a", ResourceID: "s7", Fingerprint: "fp"}, Type: store.TypeWater})
			var ce *coordclient.Error
			switch tc.want {
			case committed:
				if err != nil || res.Outcome != tc.result {
					t.Fatalf("= %+v, %v; want committed %s with a nil error", res, err, tc.result)
				}
			case unknown:
				if !errors.Is(err, store.ErrUnknownOutcome) {
					t.Fatalf("err = %v, want ErrUnknownOutcome", err)
				}
			case notProposed:
				if err == nil || errors.Is(err, store.ErrUnknownOutcome) || !errors.As(err, &ce) || !ce.Retryable {
					t.Fatalf("err = %v, want a retryable error that is not ErrUnknownOutcome", err)
				}
			case refused:
				if err == nil || errors.Is(err, store.ErrUnknownOutcome) || errors.As(err, &ce) && ce.Retryable {
					t.Fatalf("err = %v, want a non-retryable error that is not ErrUnknownOutcome", err)
				}
			}
			if tc.want != committed && err != nil && errors.Is(err, store.ErrNotFound) {
				t.Fatalf("mutation error %v matches ErrNotFound", err)
			}
			if tc.want == committed && res.Reason == "" && tc.answer != nil && tc.result != store.ClaimSuccess && tc.result != store.ClaimConflictClosed {
				t.Fatalf("rejection lost its reason: %+v", res)
			}
		})
	}
}

// A mutation whose deadline passes after sending is an unknown outcome.
func TestMutationDeadlineIsUnknown(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f, coordclient.WithTimeout(100*time.Millisecond))
	release := make(chan struct{})
	defer close(release)
	f.setAnswer(func(w http.ResponseWriter, r *http.Request, _ int) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	start := time.Now()
	_, err := c.CompareAndSet(context.Background(), store.CompareAndSet{Meta: meta("op-1"), Key: "kv:a", ExpectedVersion: 1, Value: json.RawMessage(`1`)})
	if !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("err = %v, want ErrUnknownOutcome", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %s with a 100ms cap", d)
	}
}

// Reads: 404 is ErrNotFound; a failed read is an error, never ErrNotFound
// and never ErrUnknownOutcome (a read changes nothing). A dropped
// connection, a deadline, a 5xx (even OUTCOME_UNKNOWN) or a garbled 200 is
// retryable with OutcomeUnknown=false.
func TestReadErrorMapping(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	for name, tc := range map[string]struct {
		answer    func(http.ResponseWriter, *http.Request, int)
		notFound  bool
		retryable bool
	}{
		"404":                 {answerWith(404, internalapi.Error{Code: internalapi.NOTFOUND}), true, false},
		"404 from a bad path": {func(w http.ResponseWriter, _ *http.Request, _ int) { http.NotFound(w, nil) }, false, false},
		"503 NO_LEADER":       {answerWith(503, internalapi.Error{Code: internalapi.NOLEADER, Retryable: true}), false, true},
		"503 STORAGE":         {answerWith(503, internalapi.Error{Code: internalapi.STORAGEUNAVAILABLE, Retryable: true}), false, true},
		"503 OUTCOME_UNKNOWN": {answerWith(503, internalapi.Error{Code: internalapi.OUTCOMEUNKNOWN, Retryable: true, OutcomeUnknown: true}), false, true},
		"500":                 {func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(500) }, false, true},
		"502 from a proxy":    {func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(502) }, false, true},
		"200 garbled":         {func(w http.ResponseWriter, _ *http.Request, _ int) { _, _ = w.Write([]byte("{")) }, false, true},
		"connection dropped": {func(w http.ResponseWriter, _ *http.Request, _ int) {
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				_ = conn.Close()
			}
		}, false, true},
		"deadline": {func(w http.ResponseWriter, r *http.Request, _ int) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}, false, true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			c := newClient(t, f, coordclient.WithTimeout(200*time.Millisecond))
			f.setAnswer(tc.answer)
			check := func(what string, err error) {
				t.Helper()
				var ce *coordclient.Error
				if err == nil || errors.Is(err, store.ErrUnknownOutcome) || !errors.As(err, &ce) || ce.OutcomeUnknown || ce.Retryable != tc.retryable {
					t.Fatalf("%s err = %v; want retryable=%v and never unknown", what, err, tc.retryable)
				}
			}
			_, err := c.ReadRecord(testCtx(t), "kv:a")
			check("ReadRecord", err)
			if errors.Is(err, store.ErrNotFound) != tc.notFound {
				t.Fatalf("ReadRecord err = %v; want notFound=%v", err, tc.notFound)
			}
			_, err = c.ReadClosure(testCtx(t), "res:s7", "a-1")
			check("ReadClosure", err)
			_, err = c.Operation(testCtx(t), "kv:a", "op-1")
			check("Operation", err)
			if tc.notFound {
				return // the rest have no NOT_FOUND
			}
			_, err = c.Scan(testCtx(t), store.ScanQuery{Kind: store.KindKV, Limit: 1})
			check("Scan", err) // a failed scan is an error, never an empty result
			_, err = c.PendingOutbox(testCtx(t), store.OutboxQuery{Shard: 1, Limit: 10})
			check("PendingOutbox", err)
		})
	}
}

// RING_VERSION_MISMATCH: refresh the ring and, if it changed, retry once
// with the same operationId and payload and the new ringVersion. If the
// ring did not change, it is a retryable error, not unknown, not rejected.
func TestRingVersionMismatchRefreshesAndRetriesOnce(t *testing.T) {
	two := uint64(2)
	mismatch := internalapi.Error{Code: internalapi.RINGVERSIONMISMATCH, Retryable: true, RingVersion: &two}

	f := newFake(t)
	c := newClient(t, f)
	f.mu.Lock()
	f.ring.RingVersion = 2
	f.mu.Unlock()
	f.setAnswer(func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			writeJSON(w, http.StatusConflict, mismatch)
			return
		}
		writeJSON(w, http.StatusOK, internalapi.MutationResponse{RingVersion: 2, Result: okResult})
	})
	res, err := c.CreateEntity(testCtx(t), store.CreateEntity{Meta: meta("op-1"), Key: "kv:a", Value: json.RawMessage(`1`)})
	if err != nil || res.Outcome != store.OutcomeOK {
		t.Fatalf("after refresh = %+v, %v", res, err)
	}
	reqs := f.requests()
	if len(reqs) != 2 {
		t.Fatalf("%d attempts, want 2", len(reqs))
	}
	var first, second map[string]any
	_ = json.Unmarshal(reqs[0].body, &first)
	_ = json.Unmarshal(reqs[1].body, &second)
	if first["ringVersion"] != float64(1) || second["ringVersion"] != float64(2) {
		t.Fatalf("ringVersion %v then %v, want 1 then 2", first["ringVersion"], second["ringVersion"])
	}
	delete(first, "ringVersion")
	delete(second, "ringVersion")
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("retry changed the command: %s vs %s", reqs[0].body, reqs[1].body)
	}

	// The coordinator still disagrees after a refresh: give up, retryable.
	f2 := newFake(t)
	c2 := newClient(t, f2)
	f2.setAnswer(answerWith(http.StatusConflict, mismatch))
	_, err = c2.CreateEntity(testCtx(t), store.CreateEntity{Meta: meta("op-2"), Key: "kv:a", Value: json.RawMessage(`1`)})
	var ce *coordclient.Error
	if err == nil || errors.Is(err, store.ErrUnknownOutcome) || !errors.As(err, &ce) || ce.Code != internalapi.RINGVERSIONMISMATCH || !ce.Retryable {
		t.Fatalf("persistent mismatch err = %v", err)
	}
	if n := len(f2.requests()); n > 2 {
		t.Fatalf("%d attempts on a persistent mismatch, want at most 2", n)
	}
}

// The client's timeout caps the whole call, ring refresh and retry
// included, not each request inside it.
func TestTimeoutCapsTheWholeCall(t *testing.T) {
	two := uint64(2)
	mismatch := internalapi.Error{Code: internalapi.RINGVERSIONMISMATCH, Retryable: true, RingVersion: &two}
	for name, call := range map[string]func(*coordclient.Client) error{
		"mutation": func(c *coordclient.Client) error {
			_, err := c.CreateEntity(context.Background(), store.CreateEntity{Meta: meta("op-1"), Key: "kv:a", Value: json.RawMessage(`1`)})
			if !errors.Is(err, store.ErrUnknownOutcome) {
				return fmt.Errorf("err = %w, want ErrUnknownOutcome (the retry was sent)", err)
			}
			return nil
		},
		"read": func(c *coordclient.Client) error {
			_, err := c.Scan(context.Background(), store.ScanQuery{Kind: store.KindResource, Limit: 10})
			var ce *coordclient.Error
			if err == nil || errors.Is(err, store.ErrUnknownOutcome) || !errors.As(err, &ce) || !ce.Retryable {
				return fmt.Errorf("err = %w, want a retryable read error", err)
			}
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFake(t)
			c := newClient(t, f, coordclient.WithTimeout(400*time.Millisecond))
			f.mu.Lock()
			f.ring.RingVersion = 2
			f.mu.Unlock()
			f.setAnswer(func(w http.ResponseWriter, r *http.Request, n int) {
				if n == 1 {
					time.Sleep(300 * time.Millisecond)
					writeJSON(w, http.StatusConflict, mismatch)
					return
				}
				<-r.Context().Done() // the retry hangs
			})
			start := time.Now()
			if err := call(c); err != nil {
				t.Fatal(err)
			}
			if d := time.Since(start); d > 600*time.Millisecond {
				t.Fatalf("call took %s with a 400ms timeout", d)
			}
		})
	}
}

// S1.6: the cursor is sent as is and nextCursor returned. Shards the cursor
// no longer lists are exhausted, not missing. A shard the answer drops
// without saying so is partial and stays in NextCursor at the position it
// had. A malformed cursor is never sent.
func TestScanCursor(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f)
	q := store.ScanQuery{Kind: store.KindKV, Limit: 5}
	in := store.EncodeScanCursor(store.ScanCursor{RingVersion: 1, Query: store.ScanQueryHash(q), After: map[int]string{1: "kv:b", 2: "kv:c"}})
	out := store.EncodeScanCursor(store.ScanCursor{RingVersion: 1, Query: store.ScanQueryHash(q), After: map[int]string{1: "kv:f"}})
	f.setAnswer(answerWith(http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStrong,
		Records: []store.Record{{Key: "kv:f"}}, Positions: []store.ShardPosition{{Shard: 1, HasMore: true}}, NextCursor: &out}))
	q.Cursor = in
	got, err := c.Scan(testCtx(t), q)
	if err != nil || !got.Partial || !reflect.DeepEqual(got.FailedShards, []int{2}) {
		t.Fatalf("scan dropping shard 2 = %+v, %v; want partial [2] (shard 0 is exhausted, not missing)", got, err)
	}
	next, err := store.DecodeScanCursor(got.NextCursor)
	if err != nil || !reflect.DeepEqual(next.After, map[int]string{1: "kv:f", 2: "kv:c"}) {
		t.Fatalf("NextCursor = %+v, %v; want shard 1 after kv:f and shard 2 still after kv:c", next, err)
	}
	if r := f.requests()[0]; !strings.Contains(r.query, "cursor="+in) {
		t.Fatalf("scan sent %s, want cursor=%s", r.query, in)
	}

	// Every listed shard answered and none has more: the walk is over.
	f.setAnswer(answerWith(http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStrong,
		Records: []store.Record{}, Positions: []store.ShardPosition{{Shard: 1}, {Shard: 2}}}))
	if got, err := c.Scan(testCtx(t), q); err != nil || got.Partial || got.NextCursor != "" {
		t.Fatalf("last page = %+v, %v; want complete with no NextCursor", got, err)
	}

	sent := len(f.requests())
	q.Cursor = "garbage"
	var ce *coordclient.Error
	if _, err := c.Scan(testCtx(t), q); !errors.As(err, &ce) || ce.Code != internalapi.INVALID || ce.Retryable {
		t.Fatalf("malformed cursor: err = %v, want a non-retryable INVALID", err)
	}
	if n := len(f.requests()); n != sent {
		t.Fatal("a malformed cursor was sent")
	}
}
