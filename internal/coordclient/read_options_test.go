package coordclient_test

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// sentQuery is the query of the i-th non-ring request.
func sentQuery(t *testing.T, f *fakeCoordinator, i int) url.Values {
	t.Helper()
	reqs := f.requests()
	if len(reqs) <= i {
		t.Fatalf("%d requests sent, want at least %d", len(reqs), i+1)
	}
	q, err := url.ParseQuery(reqs[i].query)
	if err != nil {
		t.Fatalf("request %d query %q: %v", i, reqs[i].query, err)
	}
	return q
}

func scanAnswer(c internalapi.ScanResponseConsistency, recs ...store.Record) func(http.ResponseWriter, *http.Request, int) {
	if recs == nil {
		recs = []store.Record{}
	}
	return answerWith(http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: c, Records: recs,
		Positions: []store.ShardPosition{{Shard: 0}, {Shard: 1}, {Shard: 2}}})
}

// #41: ScanQuery.Consistency goes on the wire; the zero value is sent as an
// explicit strong, never left for the server to default. An unknown mode is
// INVALID and nothing is sent.
func TestScanSendsConsistency(t *testing.T) {
	for _, tc := range []struct {
		in   store.Consistency
		want string
	}{
		{"", "strong"},
		{store.ConsistencyStrong, "strong"},
		{store.ConsistencyStale, "stale"},
	} {
		f := newFake(t)
		c := newClient(t, f)
		f.setAnswer(scanAnswer(internalapi.ScanResponseConsistency(tc.want)))
		if _, err := c.Scan(testCtx(t), store.ScanQuery{Kind: store.KindResource, Limit: 10, Consistency: tc.in}); err != nil {
			t.Fatalf("Scan(%q): %v", tc.in, err)
		}
		if got := sentQuery(t, f, 0)["consistency"]; len(got) != 1 || got[0] != tc.want {
			t.Errorf("Scan(%q) sent consistency=%v, want %s", tc.in, got, tc.want)
		}
	}

	f := newFake(t)
	c := newClient(t, f)
	_, err := c.Scan(testCtx(t), store.ScanQuery{Kind: store.KindResource, Limit: 10, Consistency: "eventual"})
	var ce *coordclient.Error
	if !errors.As(err, &ce) || ce.Code != internalapi.INVALID || ce.Retryable {
		t.Fatalf("consistency eventual: err = %v, want a non-retryable INVALID", err)
	}
	if n := len(f.requests()); n != 0 {
		t.Fatalf("%d requests sent for an invalid consistency, want 0", n)
	}
}

// A1.2's shape: a stale candidate scan that finds nothing is only a hint, so
// the caller falls back to a fresh strong walk before NO_CANDIDATE. The
// fallback goes out as strong with no cursor, and an answer that says it
// was served stale is not accepted as the authoritative miss.
func TestStrongFallbackAfterStaleMiss(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f)
	want := store.Record{Key: "res:s7", Kind: store.KindResource}
	f.setAnswer(func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			scanAnswer(internalapi.ScanResponseConsistencyStale)(w, r, n)
			return
		}
		scanAnswer(internalapi.ScanResponseConsistencyStrong, want)(w, r, n)
	})
	q := store.ScanQuery{Kind: store.KindResource, Type: store.TypeWater, Limit: 10, Consistency: store.ConsistencyStale}
	hint, err := c.Scan(testCtx(t), q)
	if err != nil || len(hint.Records) != 0 {
		t.Fatalf("stale scan = %+v, %v; want an empty hint", hint, err)
	}
	q.Consistency = ""
	got, err := c.Scan(testCtx(t), q)
	if err != nil || len(got.Records) != 1 || got.Records[0].Key != want.Key {
		t.Fatalf("strong fallback = %+v, %v; want %s", got, err, want.Key)
	}
	if s := sentQuery(t, f, 0).Get("consistency"); s != "stale" {
		t.Errorf("first scan consistency=%q, want stale", s)
	}
	fallback := sentQuery(t, f, 1)
	if s := fallback.Get("consistency"); s != "strong" {
		t.Errorf("fallback consistency=%q, want strong", s)
	}
	if fallback.Has("cursor") {
		t.Errorf("fallback sent cursor %q, want a fresh walk", fallback.Get("cursor"))
	}

	// Asked strong, answered stale: a retryable error, never an empty result.
	f2 := newFake(t)
	c2 := newClient(t, f2)
	f2.setAnswer(scanAnswer(internalapi.ScanResponseConsistencyStale))
	res, err := c2.Scan(testCtx(t), store.ScanQuery{Kind: store.KindResource, Limit: 10})
	var ce *coordclient.Error
	if !errors.As(err, &ce) || !ce.Retryable || errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("strong scan answered stale = %+v, %v; want a retryable read error", res, err)
	}
}

// Cancelling the caller's context ends the whole read: the first answer, the
// ring refresh and the retry all fit inside it, and the retry keeps the
// read options of the original call.
func TestCallerCancelCapsTheWholeRead(t *testing.T) {
	two := uint64(2)
	mismatch := internalapi.Error{Code: internalapi.RINGVERSIONMISMATCH, Retryable: true, RingVersion: &two}
	f := newFake(t)
	c := newClient(t, f) // DefaultTimeout: only the caller's cancel can end this early
	f.mu.Lock()
	f.ring.RingVersion = 2
	f.mu.Unlock()
	f.setAnswer(func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			writeJSON(w, http.StatusConflict, mismatch)
			return
		}
		<-r.Context().Done() // the retry hangs
	})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	_, err := c.Scan(ctx, store.ScanQuery{Kind: store.KindResource, Limit: 10, Consistency: store.ConsistencyStale})
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Scan took %s after a 200ms cancel", d)
	}
	var ce *coordclient.Error
	if !errors.As(err, &ce) || !ce.Retryable || errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("cancelled scan err = %v, want a retryable read error", err)
	}
	if n := len(f.requests()); n != 2 {
		t.Fatalf("%d attempts, want the first and one retry", n)
	}
	retry := sentQuery(t, f, 1)
	if retry.Get("consistency") != "stale" || retry.Get("ringVersion") != "2" {
		t.Fatalf("retry sent %v, want consistency=stale under ringVersion 2", retry)
	}
}

// A1.4's relay call: GET /internal/outbox?shard=N&wait=1&consistency=strong.
// Wait is whole seconds, 0 to 1 (the contract's maxOutboxWait); anything
// else is INVALID and nothing is sent. Zero values send strong and no wait.
func TestPendingOutboxLongPoll(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f)
	f.setAnswer(answerWith(http.StatusOK, internalapi.OutboxPage{Shard: 2, Events: []store.OutboxEvent{{EventID: "e1"}}, RingVersion: 1}))
	ctx := testCtx(t)
	if _, err := c.PendingOutbox(ctx, store.OutboxQuery{Shard: 2, Limit: 50, Wait: time.Second, Consistency: store.ConsistencyStrong}); err != nil {
		t.Fatalf("long poll: %v", err)
	}
	if _, err := c.PendingOutbox(ctx, store.OutboxQuery{Shard: 2, Limit: 50}); err != nil {
		t.Fatalf("zero options: %v", err)
	}
	for i, want := range []string{"consistency=strong&limit=50&shard=2&wait=1", "consistency=strong&limit=50&shard=2"} {
		if got := f.requests()[i].query; got != want {
			t.Errorf("outbox %d sent %s, want %s", i, got, want)
		}
	}
	sent := len(f.requests())
	for _, wait := range []time.Duration{-time.Second, 500 * time.Millisecond, 2 * time.Second} {
		_, err := c.PendingOutbox(ctx, store.OutboxQuery{Shard: 2, Limit: 50, Wait: wait})
		var ce *coordclient.Error
		if !errors.As(err, &ce) || ce.Code != internalapi.INVALID || ce.Retryable {
			t.Errorf("wait %s: err = %v, want a non-retryable INVALID", wait, err)
		}
	}
	if n := len(f.requests()); n != sent {
		t.Fatalf("%d invalid waits were sent", n-sent)
	}
}

// The long poll fits the client's single budget and the caller's context:
// a coordinator that holds the poll open past either does not hold the
// caller.
func TestPendingOutboxWaitIsBounded(t *testing.T) {
	hold := func(w http.ResponseWriter, r *http.Request, _ int) { <-r.Context().Done() }
	q := store.OutboxQuery{Shard: 1, Limit: 10, Wait: time.Second, Consistency: store.ConsistencyStrong}

	f := newFake(t)
	c := newClient(t, f, coordclient.WithTimeout(400*time.Millisecond))
	f.setAnswer(hold)
	start := time.Now()
	if _, err := c.PendingOutbox(context.Background(), q); err == nil {
		t.Fatal("held long poll succeeded")
	}
	if d := time.Since(start); d > 700*time.Millisecond {
		t.Fatalf("long poll took %s with a 400ms client timeout", d)
	}

	f2 := newFake(t)
	c2 := newClient(t, f2)
	f2.setAnswer(hold)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	start = time.Now()
	_, err := c2.PendingOutbox(ctx, q)
	var ce *coordclient.Error
	if !errors.As(err, &ce) || !ce.Retryable || errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("cancelled long poll err = %v, want a retryable read error", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("long poll took %s after a 200ms cancel", d)
	}
}
