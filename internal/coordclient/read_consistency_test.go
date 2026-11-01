package coordclient_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// readAnswer serves every point read, scan and outbox page as served (#72).
func readAnswer(served internalapi.ReadInfoConsistency) func(http.ResponseWriter, *http.Request, int) {
	info := internalapi.ReadInfo{Consistency: served, ServedBy: "storage-1-0", AppliedIndex: 77}
	return func(w http.ResponseWriter, r *http.Request, n int) {
		switch r.URL.Path {
		case "/internal/data/kv:a":
			writeJSON(w, http.StatusOK, internalapi.RecordResponse{Record: store.Record{Key: "kv:a", Kind: store.KindKV, Version: 3}, Read: info, RingVersion: 1})
		case "/internal/resources/s7/closures/a-1":
			writeJSON(w, http.StatusOK, internalapi.ClosureResponse{Closure: store.Closure{ResourceKey: "res:s7", AttemptID: "a-1"}, Read: info, RingVersion: 1})
		case "/internal/operations/op-1":
			writeJSON(w, http.StatusOK, internalapi.OperationResponse{Result: okResult, Read: info, RingVersion: 1})
		case "/internal/outbox":
			writeJSON(w, http.StatusOK, internalapi.OutboxPage{Shard: 2, Events: []store.OutboxEvent{}, Read: info, RingVersion: 1})
		case "/internal/scan":
			scanAnswer(internalapi.ScanResponseConsistency(served))(w, r, n)
		default:
			http.Error(w, "unexpected "+r.URL.Path, http.StatusTeapot)
		}
	}
}

// readAll makes one of each read and returns their errors by name.
func readAll(t *testing.T, c *coordclient.Client, scanMode store.Consistency) map[string]error {
	t.Helper()
	ctx := testCtx(t)
	errs := map[string]error{}
	_, errs["ReadRecord"] = c.ReadRecord(ctx, "kv:a")
	_, errs["ReadClosure"] = c.ReadClosure(ctx, "res:s7", "a-1")
	_, errs["Operation"] = c.Operation(ctx, "kv:a", "op-1")
	_, errs["Scan"] = c.Scan(ctx, store.ScanQuery{Kind: store.KindResource, Limit: 10, Consistency: scanMode})
	_, errs["PendingOutbox"] = c.PendingOutbox(ctx, store.OutboxQuery{Shard: 2, Limit: 10, Consistency: scanMode})
	return errs
}

// #72: linearizable is a read mode the client sends (the S4.1 fence on the
// storage side), on scans and outbox pages as on every other read.
func TestLinearizableScanAndOutbox(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f)
	f.setAnswer(readAnswer(internalapi.ReadInfoConsistencyLinearizable))
	ctx := testCtx(t)
	if _, err := c.Scan(ctx, store.ScanQuery{Kind: store.KindResource, Limit: 10, Consistency: store.ConsistencyLinearizable}); err != nil {
		t.Fatalf("linearizable Scan: %v", err)
	}
	if _, err := c.PendingOutbox(ctx, store.OutboxQuery{Shard: 2, Limit: 10, Consistency: store.ConsistencyLinearizable}); err != nil {
		t.Fatalf("linearizable PendingOutbox: %v", err)
	}
	for i := range 2 {
		if got := sentQuery(t, f, i).Get("consistency"); got != "linearizable" {
			t.Errorf("read %d sent consistency=%q, want linearizable", i, got)
		}
	}
}

// WithReadConsistency sets how every read is made: point reads carry it on
// the wire, and a scan or outbox page that names no mode uses it. A query's
// own mode still wins.
func TestWithReadConsistencySendsTheMode(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f, coordclient.WithReadConsistency(store.ConsistencyLinearizable))
	f.setAnswer(readAnswer(internalapi.ReadInfoConsistencyLinearizable))
	for name, err := range readAll(t, c, "") {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for i, r := range f.requests() {
		if got := sentQuery(t, f, i).Get("consistency"); got != "linearizable" {
			t.Errorf("%s %s sent consistency=%q, want linearizable", r.method, r.path, got)
		}
	}
	if got := sentQuery(t, f, 2).Get("key"); got != "kv:a" {
		t.Errorf("Operation lost its key query: key=%q", got)
	}

	n := len(f.requests())
	f.setAnswer(readAnswer(internalapi.ReadInfoConsistencyStrong))
	if _, err := c.Scan(testCtx(t), store.ScanQuery{Kind: store.KindResource, Limit: 10, Consistency: store.ConsistencyStrong}); err != nil {
		t.Fatalf("Scan with its own mode: %v", err)
	}
	if got := sentQuery(t, f, n).Get("consistency"); got != "strong" {
		t.Errorf("a query's own mode: sent consistency=%q, want strong", got)
	}
}

// An answer served weaker than asked is a retryable read error, never a
// result: a stale miss or old version must not stand in for a fenced read.
// strong is accepted for linearizable (both are linearizable; strong goes
// through the log). With a mode set, an answer that does not say how it was
// served proves nothing either.
func TestReadServedWeakerThanAskedIsRefused(t *testing.T) {
	for _, tc := range []struct {
		ask    store.Consistency
		served internalapi.ReadInfoConsistency
		ok     bool
	}{
		{store.ConsistencyLinearizable, internalapi.ReadInfoConsistencyLinearizable, true},
		{store.ConsistencyLinearizable, internalapi.ReadInfoConsistencyStrong, true},
		{store.ConsistencyLinearizable, internalapi.ReadInfoConsistencyStale, false},
		{store.ConsistencyLinearizable, "", false},
		{store.ConsistencyStrong, internalapi.ReadInfoConsistencyStrong, true},
		{store.ConsistencyStrong, internalapi.ReadInfoConsistencyStale, false},
		{store.ConsistencyStrong, "", false},
	} {
		f := newFake(t)
		c := newClient(t, f, coordclient.WithReadConsistency(tc.ask))
		f.setAnswer(readAnswer(tc.served))
		for name, err := range readAll(t, c, "") {
			var ce *coordclient.Error
			switch {
			case tc.ok && err != nil:
				t.Errorf("ask %s, served %q: %s: %v", tc.ask, tc.served, name, err)
			case !tc.ok && (!errors.As(err, &ce) || !ce.Retryable || errors.Is(err, store.ErrUnknownOutcome) || errors.Is(err, store.ErrNotFound)):
				t.Errorf("ask %s, served %q: %s err = %v, want a retryable read error", tc.ask, tc.served, name, err)
			}
		}
	}
}

// With no read mode set, nothing changes (the rule above applies only to an
// explicit WithReadConsistency): point reads send no consistency and accept
// an answer that does not say how it was served, and scans and outbox pages
// send strong and accept an unlabelled answer, exactly as before #72.
func TestNoReadModeIsUnchanged(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f)
	f.setAnswer(readAnswer(""))
	for name, err := range readAll(t, c, "") {
		if err != nil {
			t.Errorf("%s, unlabelled answer: %v", name, err)
		}
	}
	want := []string{"", "", "key=kv%3Aa", "consistency=strong&kind=resource&limit=10&ringVersion=1", "consistency=strong&limit=10&shard=2"}
	for i, r := range f.requests() {
		if r.query != want[i] {
			t.Errorf("%s sent %q, want %q", r.path, r.query, want[i])
		}
	}
}

// A mode the client does not know fails New, before any read is made.
func TestWithReadConsistencyRejectsUnknownMode(t *testing.T) {
	f := newFake(t)
	if _, err := coordclient.New(testCtx(t), f.srv.URL, coordclient.WithReadConsistency("eventual")); err == nil {
		t.Fatal("New with consistency eventual succeeded")
	}
}

// ReadRecordInfo is ReadRecord plus how the read was served: the reader
// workers record it so a chaos report shows the fence (not the barrier
// fallback, not a stale replica) answered.
func TestReadRecordInfo(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f, coordclient.WithReadConsistency(store.ConsistencyLinearizable))
	f.setAnswer(readAnswer(internalapi.ReadInfoConsistencyLinearizable))
	rec, info, err := c.ReadRecordInfo(testCtx(t), "kv:a")
	if err != nil || rec.Version != 3 {
		t.Fatalf("ReadRecordInfo = %+v, %v", rec, err)
	}
	if info.Consistency != internalapi.ReadInfoConsistencyLinearizable || info.ServedBy != "storage-1-0" || info.AppliedIndex != 77 {
		t.Fatalf("info = %+v", info)
	}
	f.setAnswer(func(w http.ResponseWriter, _ *http.Request, _ int) {
		writeJSON(w, http.StatusNotFound, internalapi.Error{Code: internalapi.NOTFOUND, Message: "no such key"})
	})
	if _, _, err := c.ReadRecordInfo(testCtx(t), "kv:a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing key: err = %v, want store.ErrNotFound", err)
	}
}
