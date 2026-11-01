package ledger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// The read check (#72, S4.3): a read that began after a write was
// acknowledged must see that write or a later one. Both instants are
// nanoseconds on one monotonic clock (the driver's, since the run began). One writer owns each key,
// so its acknowledged versions are a sequence and "later" is a higher
// version. A write found only by Resolve was never acknowledged while the
// readers ran, so it sets no bound.
func TestCheckReads(t *testing.T) {
	const k = "rd-w0-k0"
	ack := func(seq int, v uint64, ackNs int64, hash string) ledger.Entry {
		return ledger.Entry{OperationID: "op-" + hash, Key: k, Op: ledger.OpUpdate, Seq: seq, PayloadHash: hash, At: t0, AckNs: ackNs,
			Result: &store.Result{Outcome: store.OutcomeOK, OperationID: "op-" + hash, Key: k, Version: v}}
	}
	resolved := ack(2, 3, 300, "hc")
	resolved.Resolved = true
	l := &ledger.Ledger{Acked: []ledger.Entry{ack(0, 1, 100, "ha"), ack(1, 2, 200, "hb"), resolved}}
	read := func(start int64, v uint64, hash string) ledger.Read {
		return ledger.Read{Key: k, StartNs: start, EndNs: start + 5, Version: v, PayloadHash: hash, Served: "linearizable"}
	}
	for _, tc := range []struct {
		name  string
		r     ledger.Read
		stale string // "" if the read is fine; else a word its reason must contain
		count string // checked, unchecked or failed
	}{
		{"after the create, sees it", read(150, 1, "ha"), "", "checked"},
		{"same instant as the update ack: bound is the create", read(200, 1, "ha"), "", "checked"},
		{"after the update, sees the create", read(201, 1, "ha"), "v1", "checked"},
		{"after the update, right version, wrong value", read(201, 2, "hx"), "payload", "checked"},
		{"after the create, key missing", ledger.Read{Key: k, StartNs: 201, EndNs: 210, NotFound: true, Served: "linearizable"}, "missing", "checked"},
		{"before any ack, key missing", ledger.Read{Key: k, StartNs: 50, EndNs: 55, NotFound: true, Served: "linearizable"}, "", "unchecked"},
		{"after the resolved write, sees the update", read(400, 2, "hb"), "", "checked"},
		{"sees a later write", read(250, 3, "hc"), "", "checked"},
		{"read failed", ledger.Read{Key: k, StartNs: 201, EndNs: 260, Error: "503 NOT_LEADER"}, "", "failed"},
		{"unknown key", ledger.Read{Key: "rd-w9-k9", StartNs: 500, EndNs: 505, Version: 1, PayloadHash: "hz", Served: "linearizable"}, "", "unchecked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			one := &ledger.Ledger{Acked: l.Acked, Reads: []ledger.Read{tc.r}}
			st, stale := one.CheckReads()
			if st.Total != 1 {
				t.Fatalf("total %d, want 1", st.Total)
			}
			if got := map[string]int{"checked": st.Checked, "unchecked": st.Unchecked, "failed": st.Failed}[tc.count]; got != 1 {
				t.Fatalf("stats %+v, want the read counted as %s", st, tc.count)
			}
			switch {
			case tc.stale == "" && len(stale) != 0:
				t.Fatalf("stale reads %+v, want none", stale)
			case tc.stale != "" && (len(stale) != 1 || !strings.Contains(stale[0].Reason, tc.stale)):
				t.Fatalf("stale reads %+v, want one whose reason mentions %q", stale, tc.stale)
			case tc.stale != "" && (stale[0].Want.Resolved || stale[0].Want.Seq != 1):
				t.Fatalf("stale read bound %+v, want the update acknowledged at 200", stale[0].Want)
			}
		})
	}
	all := &ledger.Ledger{Acked: l.Acked, Reads: []ledger.Read{read(150, 1, "ha"), read(201, 2, "hb"), {Key: k, StartNs: 201, EndNs: 260, Error: "x"}}}
	if st, _ := all.CheckReads(); st.Served["linearizable"] != 2 || st.Total != 3 {
		t.Fatalf("stats %+v, want 3 reads, 2 served linearizable", st)
	}
}

// memRead reads s the way coordclient's ReadRecordInfo does, reporting the
// mode it was asked for.
func memRead(s store.Store, mode string) ledger.ReadFunc {
	return func(ctx context.Context, key string) (store.Record, string, error) {
		rec, err := s.ReadRecord(ctx, key)
		return rec, mode, err
	}
}

// Readers run beside the writers and stop with them; on a store that is
// linearizable every read they make passes, and Verify reports them.
func TestDriverReadersCleanRun(t *testing.T) {
	s := newStore(t)
	l, err := ledger.Driver{Store: s, Prefix: "rd", Workers: 2, Keys: 2, Ops: 40, Retries: 1, At: t0,
		Readers: 2, ReadInterval: time.Millisecond, Read: memRead(s, "linearizable"), ReadConsistency: "linearizable"}.Run(testCtx(t))
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	if len(l.Reads) == 0 || l.ReadConsistency != "linearizable" {
		t.Fatalf("%d reads, readConsistency %q; want reads made linearizable", len(l.Reads), l.ReadConsistency)
	}
	for _, r := range l.Reads {
		if !strings.HasPrefix(r.Key, "rd-w") || !strings.Contains(r.Key, "-k") || r.EndNs < r.StartNs {
			t.Fatalf("read %+v: want an update key and EndNs >= StartNs", r)
		}
	}
	for _, e := range l.Acked {
		if e.AckNs <= 0 {
			t.Fatalf("acknowledged %+v without a monotonic ack instant", e)
		}
	}
	v := verify(t, l, s)
	if !v.OK() || len(v.StaleReads) != 0 || v.Reads.Total != len(l.Reads) || v.Reads.Checked == 0 {
		t.Fatalf("verdict %+v, want OK with every read counted and some checked", v)
	}
}

// lagging plays a replica one write behind: a read returns the version a
// key had before its latest update.
type lagging struct {
	store.Store
	mu   sync.Mutex
	prev map[string]store.Record
}

func (s *lagging) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	before, err := s.ReadRecord(ctx, cmd.Key)
	res, cerr := s.Store.CompareAndSet(ctx, cmd)
	if err == nil && cerr == nil && res.Outcome == store.OutcomeOK {
		s.mu.Lock()
		s.prev[cmd.Key] = before
		s.mu.Unlock()
	}
	return res, cerr
}

func (s *lagging) read(ctx context.Context, key string) (store.Record, string, error) {
	s.mu.Lock()
	rec, ok := s.prev[key]
	s.mu.Unlock()
	if ok {
		return rec, "stale", nil
	}
	rec, err := s.ReadRecord(ctx, key)
	return rec, "stale", err
}

// The negative control: readers on a lagging replica must be caught. Every
// write is still acknowledged and kept, so only the read check fails.
func TestDriverReadersCatchLaggingReplica(t *testing.T) {
	lag := &lagging{Store: newStore(t), prev: map[string]store.Record{}}
	l, err := ledger.Driver{Store: lag, Prefix: "lag", Workers: 2, Keys: 2, Ops: 40, Retries: 1, At: t0,
		Readers: 2, ReadInterval: time.Millisecond, Read: lag.read, ReadConsistency: "stale"}.Run(testCtx(t))
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	v := verify(t, l, lag.Store)
	if len(v.Lost) != 0 {
		t.Fatalf("lost %+v: the writes themselves are intact", v.Lost)
	}
	if v.OK() || len(v.StaleReads) == 0 || v.Reads.Served["stale"] == 0 {
		t.Fatalf("verdict %+v: a reader one write behind was not caught", v)
	}
}

// Readers need a read function; without one the run is refused, not run
// without reads.
func TestDriverReadersNeedARead(t *testing.T) {
	if _, err := (ledger.Driver{Store: newStore(t), Prefix: "nr", Ops: 1, Readers: 1}).Run(testCtx(t)); err == nil {
		t.Fatal("Readers without Read ran")
	}
}

// Reads survive Save and Load, and a ledger written before reads existed
// still loads.
func TestLedgerReadsRoundTrip(t *testing.T) {
	s := newStore(t)
	l, err := ledger.Driver{Store: s, Prefix: "rt", Workers: 1, Keys: 1, Ops: 10, At: t0,
		Readers: 1, ReadInterval: time.Millisecond, Read: memRead(s, "strong"), ReadConsistency: "strong"}.Run(testCtx(t))
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	var buf bytes.Buffer
	if err := l.Save(&buf); err != nil {
		t.Fatal(err)
	}
	back, err := ledger.Load(&buf)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(back.Reads) != len(l.Reads) || back.ReadConsistency != "strong" {
		t.Fatalf("loaded %d reads (%q), saved %d", len(back.Reads), back.ReadConsistency, len(l.Reads))
	}
	old, _ := json.Marshal(map[string]any{"acked": []any{}, "unknown": []any{}, "rejected": []any{}, "notApplied": []any{}, "errors": []any{}})
	if _, err := ledger.Load(bytes.NewReader(old)); err != nil {
		t.Fatalf("a ledger without reads: %v", err)
	}
}

// A write found committed by Resolve is marked, so the read check can tell
// it from one acknowledged during the run.
func TestResolveMarksResolvedWrites(t *testing.T) {
	s := newStore(t)
	cmd := store.CreateEntity{Meta: store.Meta{OperationID: "res-op", At: t0}, Key: "res-k", Value: json.RawMessage(`1`)}
	if _, err := s.CreateEntity(testCtx(t), cmd); err != nil {
		t.Fatal(err)
	}
	l := &ledger.Ledger{Unknown: []ledger.Entry{{OperationID: "res-op", Key: "res-k", Op: ledger.OpCreate, PayloadHash: ledger.PayloadHash(cmd.Value), At: t0}}}
	if _, err := l.Resolve(testCtx(t), s); err != nil {
		t.Fatal(err)
	}
	if len(l.Acked) != 1 || !l.Acked[0].Resolved {
		t.Fatalf("acked %+v, want the resolved write marked", l.Acked)
	}
}
