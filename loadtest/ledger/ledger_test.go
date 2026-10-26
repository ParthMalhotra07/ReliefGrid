package ledger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

const t0 = int64(1_792_000_000_000)

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newStore(t *testing.T) *memstore.Store {
	t.Helper()
	s, err := memstore.New(context.Background())
	if err != nil {
		t.Fatalf("memstore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func drive(t *testing.T, s store.Store, workers, keys, ops int) *ledger.Ledger {
	t.Helper()
	l, err := ledger.Driver{Store: s, Prefix: "ld", Workers: workers, Keys: keys, Ops: ops, Retries: 2, At: t0}.Run(testCtx(t))
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	return l
}

func verify(t *testing.T, l *ledger.Ledger, s store.Store) ledger.Verdict {
	t.Helper()
	v, err := l.Verify(testCtx(t), s)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	return v
}

// A clean run: every write is acknowledged, the ledger holds each with its
// result, and Verify finds every one.
func TestDriverCleanRunVerifies(t *testing.T) {
	s := newStore(t)
	l := drive(t, s, 4, 3, 60)
	if want := 4 * (3 + 60); len(l.Acked) != want || len(l.Unknown) != 0 || len(l.Rejected) != 0 {
		t.Fatalf("acked %d unknown %d rejected %d, want %d acked", len(l.Acked), len(l.Unknown), len(l.Rejected), want)
	}
	creates, updates, ops := 0, 0, map[string]bool{}
	for _, e := range l.Acked {
		if e.Result == nil || e.Result.Outcome != store.OutcomeOK || e.Result.OperationID != e.OperationID || e.Result.Key != e.Key || len(e.PayloadHash) != 64 {
			t.Fatalf("entry %+v", e)
		}
		if ops[e.OperationID] {
			t.Fatalf("operation ID %s used twice", e.OperationID)
		}
		ops[e.OperationID] = true
		switch e.Op {
		case ledger.OpCreate:
			creates++
		case ledger.OpUpdate:
			updates++
		}
	}
	if creates != 4*(3+30) || updates != 4*30 {
		t.Fatalf("%d creates and %d updates, want %d and %d", creates, updates, 4*(3+30), 4*30)
	}
	v := verify(t, l, s)
	if !v.OK() || v.Checked != len(l.Acked) || v.Keys != creates || v.Unknown != 0 {
		t.Fatalf("verdict %+v, want OK over %d writes and %d keys", v, len(l.Acked), creates)
	}
}

// Updates of one key carry consecutive sequence numbers and versions.
func TestDriverUpdateSequences(t *testing.T) {
	l := drive(t, newStore(t), 2, 2, 40)
	updated := map[string]bool{}
	for _, e := range l.Acked {
		updated[e.Key] = updated[e.Key] || e.Op == ledger.OpUpdate
	}
	byKey := map[string][]ledger.Entry{}
	for _, e := range l.Acked {
		if updated[e.Key] {
			byKey[e.Key] = append(byKey[e.Key], e)
		}
	}
	if len(byKey) != 4 {
		t.Fatalf("%d update keys, want 4", len(byKey))
	}
	for k, es := range byKey {
		slices.SortFunc(es, func(a, b ledger.Entry) int { return a.Seq - b.Seq })
		for i, e := range es {
			if e.Seq != i || e.Result.Version != uint64(i+1) {
				t.Fatalf("%s write %d = seq %d version %d", k, i, e.Seq, e.Result.Version)
			}
		}
	}
}

// lossy acknowledges one chosen write with a fabricated OK result and never
// applies it: an acknowledged write the system then loses.
type lossy struct {
	store.Store
	drop func(method, operationID string) bool

	mu      sync.Mutex
	dropped []string
}

func (s *lossy) dropping(method, op string) bool {
	if !s.drop(method, op) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped = append(s.dropped, op)
	return true
}

func (s *lossy) CreateEntity(ctx context.Context, cmd store.CreateEntity) (store.Result, error) {
	if s.dropping("CreateEntity", cmd.OperationID) {
		return store.Result{Outcome: store.OutcomeOK, OperationID: cmd.OperationID, Key: cmd.Key, Version: 1}, nil
	}
	return s.Store.CreateEntity(ctx, cmd)
}

func (s *lossy) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	if s.dropping("CompareAndSet", cmd.OperationID) {
		return store.Result{Outcome: store.OutcomeOK, OperationID: cmd.OperationID, Key: cmd.Key, Version: cmd.ExpectedVersion + 1}, nil
	}
	return s.Store.CompareAndSet(ctx, cmd)
}

func nth(method string, n int64) func(string, string) bool {
	var calls atomic.Int64
	return func(m, _ string) bool { return m == method && calls.Add(1) == n }
}

func lostOps(v ledger.Verdict) []string {
	var out []string
	for _, l := range v.Lost {
		out = append(out, l.Entry.OperationID)
	}
	return out
}

// Verify (S2.2): the ledger finds a deliberately dropped acknowledged write,
// and names it.
func TestVerifyFindsADroppedAcknowledgedWrite(t *testing.T) {
	t.Run("create acknowledged, never applied", func(t *testing.T) {
		s := newStore(t)
		ls := &lossy{Store: s, drop: nth("CreateEntity", 20)}
		l := drive(t, ls, 2, 2, 30)
		if len(ls.dropped) != 1 {
			t.Fatalf("dropped %v, want one write", ls.dropped)
		}
		v := verify(t, l, s)
		if v.OK() || !slices.Equal(lostOps(v), ls.dropped) || v.Lost[0].Reason == "" {
			t.Fatalf("lost %+v, want exactly %v with a reason", v.Lost, ls.dropped)
		}
	})

	t.Run("update acknowledged, never applied", func(t *testing.T) {
		s := newStore(t)
		ls := &lossy{Store: s, drop: nth("CompareAndSet", 7)}
		l := drive(t, ls, 2, 2, 30)
		if len(ls.dropped) != 1 {
			t.Fatalf("dropped %v, want one write", ls.dropped)
		}
		v := verify(t, l, s)
		if v.OK() || !slices.Contains(lostOps(v), ls.dropped[0]) {
			t.Fatalf("lost %+v, want %s among them", v.Lost, ls.dropped[0])
		}
	})

	// The last acknowledged update of a key is replaced by an older value at
	// a higher version: the version alone would hide the loss.
	t.Run("last update overwritten by an older value", func(t *testing.T) {
		s := newStore(t)
		l := drive(t, s, 2, 2, 30)
		lastByKey := map[string]ledger.Entry{}
		for _, e := range l.Acked {
			if e.Op == ledger.OpUpdate && e.Seq > lastByKey[e.Key].Seq {
				lastByKey[e.Key] = e
			}
		}
		var last ledger.Entry
		for _, e := range lastByKey {
			if e.Seq > last.Seq {
				last = e
			}
		}
		if last.Seq < 2 {
			t.Fatalf("no key with two updates: %+v", lastByKey)
		}
		old, err := s.ReadRecord(testCtx(t), last.Key)
		if err != nil {
			t.Fatal(err)
		}
		stale := json.RawMessage(`{"stale":true}`)
		if res, err := s.CompareAndSet(testCtx(t), store.CompareAndSet{Meta: store.Meta{OperationID: "rollback", At: t0}, Key: last.Key,
			ExpectedVersion: old.Version, Value: stale}); err != nil || res.Outcome != store.OutcomeOK {
			t.Fatalf("overwrite: %+v, %v", res, err)
		}
		v := verify(t, l, s)
		if v.OK() || !slices.Equal(lostOps(v), []string{last.OperationID}) {
			t.Fatalf("lost %+v, want exactly %s", v.Lost, last.OperationID)
		}
	})
}

// faultFor picks a fault from the operation ID alone, so every retry of an
// operation meets the same fault.
func faultFor(op string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(op))
	switch h.Sum32() % 11 {
	case 0:
		return "response" // applied, answer lost
	case 1:
		return "before" // never applied
	}
	return ""
}

// Unknown outcomes stay out of the acknowledged set until Resolve looks them
// up: an applied one becomes acknowledged, one that never committed is not
// applied, and the run then verifies.
func TestUnknownOutcomesResolvedByOperationLookup(t *testing.T) {
	s := newStore(t)
	var mu sync.Mutex
	seen := map[string]string{}
	faulty := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
		if c.OperationID == "" {
			return memstore.Fault{}
		}
		f := faultFor(c.OperationID)
		mu.Lock()
		if f != "" {
			seen[c.OperationID] = f
		}
		mu.Unlock()
		return memstore.Fault{DropResponse: f == "response", DropBefore: f == "before"}
	})
	l := drive(t, faulty, 4, 3, 60)
	applied, never := 0, 0
	for _, f := range seen {
		if f == "response" {
			applied++
		} else {
			never++
		}
	}
	if applied == 0 || never == 0 || len(l.Unknown) != applied+never {
		t.Fatalf("unknown %d, faults %d applied + %d never; want both kinds, all unknown", len(l.Unknown), applied, never)
	}
	for _, e := range l.Acked {
		if seen[e.OperationID] != "" {
			t.Fatalf("%s acknowledged despite fault %s", e.OperationID, seen[e.OperationID])
		}
	}
	acked := len(l.Acked)

	res, err := l.Resolve(testCtx(t), s)
	if err != nil || res.Resolved != applied || res.NotApplied != never || res.StillUnknown != 0 {
		t.Fatalf("Resolve = %+v, %v; want %d resolved, %d not applied", res, err, applied, never)
	}
	if len(l.Unknown) != 0 || len(l.Acked) != acked+applied || len(l.NotApplied) != never {
		t.Fatalf("after Resolve: acked %d unknown %d not applied %d", len(l.Acked), len(l.Unknown), len(l.NotApplied))
	}
	if v := verify(t, l, s); !v.OK() || v.Unknown != 0 {
		t.Fatalf("verdict %+v, want OK", v)
	}
}

// A ledger survives Save and Load and still verifies.
func TestLedgerSaveLoad(t *testing.T) {
	s := newStore(t)
	l := drive(t, s, 2, 2, 20)
	if len(l.Acked) == 0 {
		t.Fatal("the driver acknowledged nothing")
	}
	var buf bytes.Buffer
	if err := l.Save(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := ledger.Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Acked) != len(l.Acked) || !slices.EqualFunc(got.Acked, l.Acked, func(a, b ledger.Entry) bool {
		return a.OperationID == b.OperationID && a.Key == b.Key && a.Seq == b.Seq && a.PayloadHash == b.PayloadHash && *a.Result == *b.Result
	}) {
		t.Fatalf("loaded %d entries, saved %d, or they differ", len(got.Acked), len(l.Acked))
	}
	if v := verify(t, got, s); !v.OK() || v.Checked != len(l.Acked) {
		t.Fatalf("verdict of the loaded ledger %+v", v)
	}
}

// A read that fails is no verdict at all, never "nothing lost".
func TestVerifyReadErrorIsNoVerdict(t *testing.T) {
	s := newStore(t)
	l := drive(t, s, 1, 1, 4)
	broken := errors.New("broken on purpose")
	failing := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
		if c.Method == "ReadRecord" {
			return memstore.Fault{Before: func(context.Context) error { return broken }}
		}
		return memstore.Fault{}
	})
	if v, err := l.Verify(testCtx(t), failing); !errors.Is(err, broken) {
		t.Fatalf("Verify = %+v, %v; want the read error", v, err)
	}
}

// The payload hash ignores whitespace, so it matches the stored value.
func TestPayloadHash(t *testing.T) {
	a := ledger.PayloadHash(json.RawMessage(`{"w":1, "seq": 2}`))
	if len(a) != 64 || a != ledger.PayloadHash(json.RawMessage(`{"w":1,"seq":2}`)) || a == ledger.PayloadHash(json.RawMessage(`{"w":1,"seq":3}`)) {
		t.Fatalf("hash %q", a)
	}
}
