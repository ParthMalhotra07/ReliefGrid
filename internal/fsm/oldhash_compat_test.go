package fsm_test

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// The fixtures in testdata/oldhash were written by the FSM as it was before
// the trace-free dedupe hash (main at c5fe7ca). They are frozen: regenerate
// them only with that code checked out (-update-oldhash), never with the
// code under test.
var updateOldHash = flag.Bool("update-oldhash", false, "rewrite testdata/oldhash from the current FSM (only on pre-change code)")

const oldHashDir = "testdata/oldhash"

// traceOnlyRetries lists log entries (0-based) that retry an earlier
// operationId with only an envelope traceparent changed. The pre-change code
// answered OPERATION_ID_REUSED; the new code replays. Neither writes state.
var traceOnlyRetries = map[int]bool{3: true}

// oldHashLog builds the frozen log: outbox envelopes with traceparents on
// every command kind that carries them, plus one trace-only retry.
func oldHashLog(t *testing.T) [][]byte {
	a1 := attempt("a1", "r1")
	bodies := []any{
		store.CreateEntity{Meta: meta("c-r1"), Key: store.ResourceKey("r1"), Resource: resource("r1", store.TypeWater, store.ResourceAvailable)},
		store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater),
			Outbox: []store.OutboxEvent{envelopeEvent("ev-c-q1", "q1", traceA)}},
		store.CreateEntity{Meta: meta("c-q2"), Key: store.RequestKey("q2"), Request: request("q2", store.TypeWater),
			Outbox: []store.OutboxEvent{envelopeEvent("ev-c-q2", "q2", traceA)}},
		store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater),
			Outbox: []store.OutboxEvent{envelopeEvent("ev-c-q1", "q1", traceB)}}, // trace-only retry
		store.TransitionRequest{Meta: meta("t-q1"), Key: store.RequestKey("q1"), ExpectedVersion: 1,
			ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a1,
			Outbox: []store.OutboxEvent{envelopeEvent("ev-t-q1", "q1", traceA)}},
		store.TransitionRequest{Meta: meta("t-q1-stale"), Key: store.RequestKey("q1"), ExpectedVersion: 1,
			ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: t0 + 30_000,
			Outbox: []store.OutboxEvent{envelopeEvent("ev-stale", "q1", traceA)}}, // VERSION_CONFLICT, stored
		store.TransitionRequest{Meta: meta("t-q2"), Key: store.RequestKey("q2"), ExpectedVersion: 1,
			ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: t0 + 30_000},
		store.RetryWorkflow{Meta: meta("retry-q2"), Key: store.RequestKey("q2"), ExpectedVersion: 2, NextAttemptAt: t0 + 60_000,
			Outbox: []store.OutboxEvent{envelopeEvent("ev-retry-q2", "q2", traceA)}},
		store.CreateEntity{Meta: meta("c-r2"), Key: store.ResourceKey("r2"), Resource: resource("r2", store.TypeWater, store.ResourceAvailable)},
		store.TransitionResource{Meta: meta("tr-r2"), Key: store.ResourceKey("r2"), ExpectedVersion: 1, Status: store.ResourceOffline,
			Outbox: []store.OutboxEvent{envelopeEvent("ev-tr-r2", "r2", traceA)}},
		store.CompareAndSet{Meta: meta("kv-1"), Key: "kv:demo", Value: json.RawMessage(`1`)},
		store.MarkOutboxPublished{Meta: meta("mark-1"), EventIDs: []string{"ev-c-q1"}},
	}
	log := make([][]byte, len(bodies))
	for i, b := range bodies {
		log[i] = mustEncode(t, b)
	}
	return log
}

type oldHashFixture struct {
	Log     []string            `json:"log"` // exact entry bytes
	Results []store.Result      `json:"results"`
	Dump    map[string][]string `json:"dump"`
}

// Verify (S1.x): a log written under the old hash replays to identical state
// (every table, payload_hash included) under the new code. Results are
// identical except the trace-only retries, which replay instead of REUSED.
func TestOldHashLogReplaysToIdenticalState(t *testing.T) {
	ctx := context.Background()
	if *updateOldHash {
		writeOldHashFixtures(t)
	}
	raw, err := os.ReadFile(filepath.Join(oldHashDir, "fixture.json"))
	if err != nil {
		t.Fatalf("read fixture (generate on pre-change code with -update-oldhash): %v", err)
	}
	var fx oldHashFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	h := newHarness(t)
	for i, entry := range fx.Log {
		got := h.applyEntry([]byte(entry))
		want := fx.Results[i]
		if traceOnlyRetries[i] {
			if want.Outcome != store.OutcomeOperationIDReuse {
				t.Fatalf("fixture entry %d: old outcome %s, want the pre-change OPERATION_ID_REUSED", i, want.Outcome)
			}
			orig := fx.Results[1] // the c-q1 create it retries
			if got.Outcome != orig.Outcome || !got.Replayed || got.Version != orig.Version || got.AppliedIndex != orig.AppliedIndex {
				t.Errorf("entry %d (trace-only retry) = %+v (%s), want replay of %+v", i, got, got.Reason, orig)
			}
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("entry %d result = %+v, want %+v", i, got, want)
		}
	}
	dump, err := h.f.Dump(ctx)
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	if !reflect.DeepEqual(dump, fx.Dump) {
		t.Fatalf("state after replaying the old log differs:\n got %v\nwant %v", dump, fx.Dump)
	}
}

// Verify (S1.x): a snapshot image taken by the old code still dedupes under
// the new code. Raft restores the FSM from its latest snapshot, so rows whose
// payload_hash the old code wrote must keep working: an identical retry
// replays, a trace-only retry replays (except of a rejection), any other
// change is REUSED.
func TestOldHashSnapshotDedupesUnderNewCode(t *testing.T) {
	ctx := context.Background()
	src, err := os.ReadFile(filepath.Join(oldHashDir, "snapshot.db"))
	if err != nil {
		t.Fatalf("read snapshot image (generate with -update-oldhash): %v", err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.db")
	if err := os.WriteFile(path, src, 0o600); err != nil {
		t.Fatal(err)
	}
	img, err := fsm.OpenImage(ctx, path)
	if err != nil {
		t.Fatalf("OpenImage: %v", err)
	}
	t.Cleanup(func() { _ = img.Close() })
	idx := img.AppliedIndex()
	apply := func(body any) store.Result {
		t.Helper()
		idx++
		res, err := img.Apply(idx, mustEncode(t, body))
		if err != nil {
			t.Fatalf("Apply(%d): %v", idx, err)
		}
		return res
	}
	q1 := func(trace string) store.CreateEntity {
		return store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater),
			Outbox: []store.OutboxEvent{envelopeEvent("ev-c-q1", "q1", trace)}}
	}
	if res := apply(q1(traceA)); res.Outcome != store.OutcomeOK || !res.Replayed {
		t.Fatalf("identical retry on old image = %+v (%s), want replay", res, res.Reason)
	}
	if res := apply(q1(traceB)); res.Outcome != store.OutcomeOK || !res.Replayed {
		t.Fatalf("trace-only retry on old image = %+v (%s), want replay", res, res.Reason)
	}
	stale := store.TransitionRequest{Meta: meta("t-q1-stale"), Key: store.RequestKey("q1"), ExpectedVersion: 1,
		ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: t0 + 30_000,
		Outbox: []store.OutboxEvent{envelopeEvent("ev-stale", "q1", traceB)}}
	// Limitation: a rejection has no committed outbox rows to take the
	// trace from, so a trace-only retry of it stays REUSED.
	expectOutcome(t, "trace-only retry of an old stored rejection (limitation)", apply(stale), store.OutcomeOperationIDReuse)
	changed := q1(traceB)
	changed.Request.PeopleAffected = 9
	expectOutcome(t, "changed retry on old image", apply(changed), store.OutcomeOperationIDReuse)
}

func writeOldHashFixtures(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	h := newHarness(t)
	fx := oldHashFixture{}
	for _, entry := range oldHashLog(t) {
		fx.Log = append(fx.Log, string(entry))
		fx.Results = append(fx.Results, h.applyEntry(entry))
	}
	dump, err := h.f.Dump(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fx.Dump = dump
	body, err := json.MarshalIndent(fx, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(oldHashDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldHashDir, "fixture.json"), append(body, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(oldHashDir, "snapshot.db")
	_ = os.Remove(snap)
	if err := h.f.VacuumInto(ctx, snap); err != nil {
		t.Fatal(err)
	}
}
