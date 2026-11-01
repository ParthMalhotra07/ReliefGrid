package ledger_test

import (
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// shardByName: keys "s<N>-..." belong to shard N.
func shardByName(key string) (int, error) { return int(key[1] - '0'), nil }

const ms = int64(1_000_000) // ns per ms

// Window lines the ledger's monotonic instants up with a harness's fault
// [from, to] (wall ms; StartedAtMs is the run's zero) and counts, per shard:
// acks before, during and after; the writes that started after from+grace
// and were acknowledged before to (committed while the fault fully held);
// reads, and those served while it fully held, through the coordinator and
// by probe; the first ack of a write started after the fault began and
// after it healed; latencies during; and attempt errors during.
func TestWindow(t *testing.T) {
	const run = int64(1_792_000_000_000) // StartedAtMs
	ack := func(key string, startMs, ackMs int64) ledger.Entry {
		return ledger.Entry{OperationID: key, Key: key, StartNs: startMs * ms, AckNs: ackMs * ms, At: run + ackMs,
			Result: &store.Result{Outcome: store.OutcomeOK, Version: 1}}
	}
	read := func(key, target string, startMs, endMs int64, failed bool) ledger.Read {
		r := ledger.Read{Key: key, Target: target, StartNs: startMs * ms, EndNs: endMs * ms, Version: 1, Served: "linearizable"}
		if failed {
			r = ledger.Read{Key: key, Target: target, StartNs: startMs * ms, EndNs: endMs * ms, Error: "503 NOT_LEADER"}
		}
		return r
	}
	l := &ledger.Ledger{StartedAtMs: run,
		Acked: []ledger.Entry{
			ack("s0-a", 100, 110),   // before
			ack("s0-b", 1990, 2050), // started before the fault, acked during: not inside
			ack("s0-c", 5000, 5020), // inside (from+grace = 4000)
			ack("s0-d", 9000, 9500), // started inside, acked after the heal: not inside
			ack("s0-e", 9100, 9150), // after the heal
			ack("s1-a", 3000, 3010), // shard 1, during (inside the window but before from+grace)
			ack("s1-b", 6000, 6005), // shard 1, inside
		},
		Reads: []ledger.Read{
			read("s0-a", "", 2500, 2510, false),         // during, served, before grace ends
			read("s0-a", "", 6000, 6010, false),         // inside, served
			read("s0-a", "", 6100, 6200, true),          // inside, failed
			read("s0-a", "probe-0-0", 7000, 7400, true), // probe, inside, refused
			read("s0-a", "probe-0-0", 7500, 7600, false),
			read("s1-a", "", 8000, 8010, false),
		},
		Errors: []ledger.AttemptError{{At: run + 4500, Key: "s0-x"}, {At: run + 50, Key: "s0-y"}},
	}
	w, err := l.Window(run+2000, run+9000, 2000, shardByName)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Shards) != 2 {
		t.Fatalf("shards %+v, want 0 and 1", w.Shards)
	}
	s0, s1 := w.Shards[0], w.Shards[1]
	if s0.Shard != 0 || s0.AcksBefore != 1 || s0.AcksDuring != 2 || s0.AcksAfter != 2 || s0.AcksInsideFault != 1 {
		t.Errorf("shard 0 acks %+v, want before 1, during 2, after 2, inside 1", s0)
	}
	if s0.ReadsDuring != 5 || s0.ReadsServedDuring != 3 || s0.ReadsServedInsideFault != 2 {
		t.Errorf("shard 0 reads during %d served %d inside %d, want 5, 3, 2", s0.ReadsDuring, s0.ReadsServedDuring, s0.ReadsServedInsideFault)
	}
	if s0.ProbeReadsDuring["probe-0-0"] != 2 || s0.ProbeServedInsideFault["probe-0-0"] != 1 {
		t.Errorf("shard 0 probe reads %v, served inside %v; want 2 and 1", s0.ProbeReadsDuring, s0.ProbeServedInsideFault)
	}
	if s0.FirstAckAfterFaultMs != 3020 || s0.FirstAckAfterHealMs != 150 {
		t.Errorf("shard 0 first ack after the fault %d ms, after the heal %d ms; want 3020 and 150", s0.FirstAckAfterFaultMs, s0.FirstAckAfterHealMs)
	}
	if s0.ErrorsDuring != 1 {
		t.Errorf("shard 0 errors during %d, want 1", s0.ErrorsDuring)
	}
	if s0.WriteLatencyMs.P50 == 0 || s0.ReadLatencyMs.P99 != 400 {
		t.Errorf("shard 0 latency write %+v read %+v, want a write p50 and read p99 400", s0.WriteLatencyMs, s0.ReadLatencyMs)
	}
	if s1.AcksDuring != 2 || s1.AcksInsideFault != 1 || s1.FirstAckAfterHealMs != -1 {
		t.Errorf("shard 1 %+v, want 2 during, 1 inside, no ack after the heal", s1)
	}
}

// Without the run's wall-clock start, the ledger can't be lined up with a
// fault, so Window refuses rather than guessing.
func TestWindowNeedsStartedAt(t *testing.T) {
	l := &ledger.Ledger{Acked: []ledger.Entry{{Key: "s0-a", AckNs: 5}}}
	if _, err := l.Window(1, 2, 0, shardByName); err == nil || !strings.Contains(err.Error(), "startedAtMs") {
		t.Fatalf("err = %v, want a refusal naming startedAtMs", err)
	}
}

// A driven ledger has StartedAtMs, StartNs and AckNs, and probes record
// their target; a probe owning no key is refused.
func TestDriverProbes(t *testing.T) {
	s := newStore(t)
	owns := func(key string) bool { return strings.HasSuffix(key, "-k0") }
	l, err := ledger.Driver{Store: s, Prefix: "pr", Workers: 2, Keys: 2, Ops: 30, At: t0, ReadInterval: 1e6,
		Probes: []ledger.Probe{{Name: "probe-x", Owns: owns, Read: memRead(s, "linearizable")}}, ReadConsistency: "linearizable"}.Run(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if l.StartedAtMs == 0 {
		t.Fatal("StartedAtMs not set")
	}
	n := 0
	for _, r := range l.Reads {
		if r.Target != "probe-x" || !owns(r.Key) {
			t.Fatalf("read %+v: want only probe-x reads of the keys it owns", r)
		}
		n++
	}
	if n == 0 {
		t.Fatal("the probe made no reads")
	}
	for _, e := range l.Acked {
		if e.StartNs <= 0 || e.AckNs < e.StartNs {
			t.Fatalf("entry %+v: want 0 < StartNs <= AckNs", e)
		}
	}
	if v := verify(t, l, s); !v.OK() {
		t.Fatalf("verdict %+v", v)
	}
	_, err = ledger.Driver{Store: s, Prefix: "pr2", Ops: 1, At: t0,
		Probes: []ledger.Probe{{Name: "nobody", Owns: func(string) bool { return false }, Read: memRead(s, "strong")}}}.Run(testCtx(t))
	if err == nil || !strings.Contains(err.Error(), "owns none") {
		t.Fatalf("err = %v, want a probe owning no key refused", err)
	}
}
