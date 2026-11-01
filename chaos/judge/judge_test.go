package judge

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// clean is a passing run of experiment on shard 1, target storage-shard-1-0,
// a 20 s fault with a 2 s grace: every shard acked during and after the
// fault and was written and read after the grace, the faulted shard acked
// none while it fully held (as lose-quorum needs), and the probes (the
// target; all three replicas for lose-quorum) were refused after the grace.
func clean(experiment string) Input {
	probed := []string{"storage-shard-1-0"}
	if experiment == "lose-quorum" {
		probed = []string{"storage-shard-1-0", "storage-shard-1-1", "storage-shard-1-2"}
	}
	shard := func(id int) ledger.ShardWindow {
		w := ledger.ShardWindow{Shard: id, AcksBefore: 10, AcksDuring: 20, AcksAfter: 10, FirstAckAfterFaultMs: 50, FirstAckAfterHealMs: 40,
			WriteAttemptsInsideFault: 18, ReadsInsideFault: 16,
			ProbeReadsDuring: map[string]int{}, ProbeServedInsideFault: map[string]int{}, ProbeReadsInsideFault: map[string]int{}}
		if id == 1 {
			for _, p := range probed {
				w.ProbeReadsDuring[p] = 30
				w.ProbeReadsInsideFault[p] = 25
			}
		}
		return w
	}
	in := Input{Experiment: experiment, Shard: 1, Target: "storage-shard-1-0",
		Bounds: Bounds{ElectionMaxSeconds: 10, RecoveryMaxSeconds: 60},
		Verify: &Verify{OK: true, Ledger: ledger.Stats{Acked: 500, Unknown: 3},
			Resolution: ledger.Resolution{Resolved: 2, NotApplied: 1},
			Verdict: &ledger.Verdict{Checked: 500, Lost: []ledger.Loss{}, StaleReads: []ledger.StaleRead{},
				Reads: ledger.ReadStats{Total: 800, Checked: 700, Failed: 60}},
			Availability: &ledger.WindowStats{FromMs: 1_000_000, ToMs: 1_020_000, GraceMs: 2000, Shards: []ledger.ShardWindow{shard(0), shard(1), shard(2)}}},
		Check: &Check{OK: true, Verdict: "clean", Report: &struct {
			Quiescent  bool              `json:"quiescent"`
			Violations []json.RawMessage `json:"violations"`
		}{Quiescent: true}},
		Harness: Harness{SettleOK: true},
	}
	h := &in.Harness
	switch experiment {
	case "isolate-leader", "delay-isolate-leader":
		h.Isolation = &struct {
			TargetCommitAdvanced   bool `json:"targetCommitAdvanced"`
			MajorityCommitAdvanced bool `json:"majorityCommitAdvanced"`
			IsolatedStillLed       bool `json:"isolatedStillLed"`
		}{MajorityCommitAdvanced: true}
		h.Recovery = &struct {
			Seconds    float64 `json:"seconds"`
			Term       uint64  `json:"term"`
			TermBefore uint64  `json:"termBefore"`
			Leader     string  `json:"leader"`
		}{Seconds: 2.1, Term: 3, TermBefore: 2, Leader: "storage-shard-1-2"}
	case "lose-quorum":
		w := &in.Verify.Availability.Shards[1]
		w.AcksDuring, w.AcksInsideFault = 2, 0 // two acked in the grace before the cut held
	case "delay-follower":
		h.Delay = &struct {
			MaxLag   uint64 `json:"maxLag"`
			FinalLag uint64 `json:"finalLag"`
			Samples  int    `json:"samples"`
		}{MaxLag: 7, Samples: 40}
	case "stop-worker":
		h.Worker = &struct {
			Node string `json:"node"`
			Back bool   `json:"back"`
		}{Node: "reliefgrid-worker2", Back: true}
	case "restart-coordinator":
		h.Coordinator = &struct {
			RingBefore          string            `json:"ringBefore"`
			RingAfter           string            `json:"ringAfter"`
			TermsBefore         map[string]uint64 `json:"termsBefore"`
			TermsAfter          map[string]uint64 `json:"termsAfter"`
			BootstrapJobsBefore int               `json:"bootstrapJobsBefore"`
			BootstrapJobsAfter  int               `json:"bootstrapJobsAfter"`
			Back                bool              `json:"back"`
		}{RingBefore: "1/sha256:670e", RingAfter: "1/sha256:670e", TermsBefore: map[string]uint64{"0": 2, "1": 2, "2": 3},
			TermsAfter: map[string]uint64{"0": 2, "1": 2, "2": 3}, BootstrapJobsBefore: 1, BootstrapJobsAfter: 1, Back: true}
	}
	return in
}

var experiments = []string{"isolate-leader", "lose-quorum", "delay-follower", "delay-isolate-leader", "stop-worker",
	"restart-coordinator", "kill-leader", "kill-follower", "restart-shard", "isolate-replica"}

func TestCleanRunsPass(t *testing.T) {
	for _, e := range experiments {
		r := Judge(clean(e))
		if !r.Pass || r.NoVerdict || len(r.Failures) != 0 {
			t.Errorf("%s: %+v, want a pass", e, r)
		}
		if r.Summary.UnknownRaw != 3 || r.Summary.UnresolvedUnknown != 0 || r.Summary.Resolved != 2 {
			t.Errorf("%s: summary %+v, want the raw unknown count (3) reported beside 0 unresolved", e, r.Summary)
		}
	}
}

func shardOf(in *Input, id int) *ledger.ShardWindow {
	for i := range in.Verify.Availability.Shards {
		if in.Verify.Availability.Shards[i].Shard == id {
			return &in.Verify.Availability.Shards[i]
		}
	}
	return nil
}

// Each criterion, broken alone, fails the run with a reason naming it.
func TestEachCriterionFails(t *testing.T) {
	for _, tc := range []struct {
		experiment string
		breakIt    func(*Input)
		has        string
	}{
		{"kill-leader", func(in *Input) { in.Verify.Verdict.Lost = []ledger.Loss{{Reason: "x"}} }, "1 acknowledged write(s) lost"},
		{"kill-leader", func(in *Input) { in.Verify.Resolution.StillUnknown = 2 }, "2 unknown outcome(s) still unresolved"},
		{"kill-leader", func(in *Input) { in.Verify.Verdict.StaleReads = []ledger.StaleRead{{Reason: "x"}} }, "1 stale read(s)"},
		{"kill-leader", func(in *Input) { in.Verify.Verdict.Reads.Checked = 0 }, "no read was checked"},
		{"kill-leader", func(in *Input) { in.CheckExit = 3; in.Check.Report.Quiescent = false }, "not quiescent"},
		{"kill-leader", func(in *Input) {
			in.CheckExit = 1
			in.Check.Verdict = "violations"
			in.Check.Report.Violations = []json.RawMessage{json.RawMessage(`{}`)}
		}, "invcheck: violations (1 violation(s))"},
		{"kill-leader", func(in *Input) {
			in.Harness.SettleOK, in.Harness.SettleDetail = false, "storage-shard-1-2 not caught up"
		}, "settle: storage-shard-1-2 not caught up"},
		{"kill-leader", func(in *Input) { in.Verify.Availability, in.Verify.AvailabilityError = nil, "no startedAtMs" }, "availability: not lined up with the fault (no startedAtMs)"},
		{"kill-leader", func(in *Input) {
			for i := range in.Verify.Availability.Shards {
				in.Verify.Availability.Shards[i].AcksDuring = 0
			}
		}, "no write was acknowledged during the fault"},

		{"isolate-leader", func(in *Input) { in.Harness.Isolation.TargetCommitAdvanced = true }, "advanced its commit index"},
		{"isolate-leader", func(in *Input) { in.Harness.Isolation.MajorityCommitAdvanced = false }, "majority's commit index did not advance"},
		{"isolate-leader", func(in *Input) { in.Harness.Isolation.IsolatedStillLed = true }, "still led its group"},
		{"isolate-leader", func(in *Input) { in.Harness.Recovery.Seconds = 14 }, "a new leader after 14.0s, over 10s"},
		{"isolate-leader", func(in *Input) { in.Harness.Recovery.Term = 2 }, "not above the faulted leader's term 2"},
		{"isolate-leader", func(in *Input) { in.Harness.Recovery = nil }, "elected no new leader"},
		{"delay-isolate-leader", func(in *Input) { shardOf(in, 1).ProbeServedInsideFault["storage-shard-1-0"] = 4 }, "served 4 linearizable read(s) while cut off"},
		{"isolate-leader", func(in *Input) { dropProbe(in, "storage-shard-1-0") }, "probe: missing probe for storage-shard-1-0"},
		{"isolate-leader", func(in *Input) { shardOf(in, 1).AcksDuring = 0 }, "majority: shard 1 acknowledged no write"},

		{"lose-quorum", func(in *Input) { shardOf(in, 1).AcksInsideFault = 1 }, "acknowledged 1 write(s) with no majority"},
		{"lose-quorum", func(in *Input) { shardOf(in, 1).ReadsServedInsideFault = 3 }, "served 3 linearizable read(s) with no majority"},
		{"lose-quorum", func(in *Input) { shardOf(in, 1).FirstAckAfterHealMs = -1 }, "acknowledged no write after the heal"},
		{"lose-quorum", func(in *Input) { shardOf(in, 1).FirstAckAfterHealMs = 75_000 }, "75.0s after the heal, over 60s"},
		{"lose-quorum", func(in *Input) { shardOf(in, 2).AcksDuring = 0 }, "shard 2 (not faulted) acknowledged no write"},
		{"lose-quorum", func(in *Input) {
			shardOf(in, 1).ProbeReadsDuring, shardOf(in, 1).ProbeReadsInsideFault = map[string]int{}, map[string]int{}
		}, "probe: missing probe for storage-shard-1-0"},

		{"delay-follower", func(in *Input) { in.Harness.Delay.MaxLag = 0 }, "lag never rose"},
		{"delay-follower", func(in *Input) { in.Harness.Delay.FinalLag = 5 }, "still 5 entries behind"},
		{"delay-follower", func(in *Input) { in.Harness.Delay = nil }, "lag was not sampled"},

		{"stop-worker", func(in *Input) { in.Harness.Worker.Back = false }, "did not come back"},
		{"stop-worker", func(in *Input) { shardOf(in, 0).AcksDuring = 0 }, "worker: shard 0 acknowledged no write"},

		{"restart-coordinator", func(in *Input) { in.Harness.Coordinator.RingAfter = "1/sha256:dead" }, "ring \"1/sha256:670e\" before, \"1/sha256:dead\" after"},
		{"restart-coordinator", func(in *Input) { in.Harness.Coordinator.TermsAfter["2"] = 4 }, "shard 2's leader term 3 before, 4 after"},
		{"restart-coordinator", func(in *Input) { in.Harness.Coordinator.BootstrapJobsAfter = 2 }, "1 bootstrap Job(s) before, 2 after"},
		{"restart-coordinator", func(in *Input) { in.Harness.Coordinator.Back = false }, "not Ready again"},
		{"restart-coordinator", func(in *Input) { shardOf(in, 0).FirstAckAfterHealMs = -1 }, "shard 0 acknowledged no write after the restart"},
	} {
		in := clean(tc.experiment)
		tc.breakIt(&in)
		r := Judge(in)
		if r.Pass || !strings.Contains(strings.Join(r.Failures, "\n"), tc.has) {
			t.Errorf("%s, want %q: pass %v, failures %q", tc.experiment, tc.has, r.Pass, r.Failures)
		}
	}
}

// No verdict is not a failure of the system: it is reported apart.
func TestNoVerdict(t *testing.T) {
	for name, breakIt := range map[string]func(*Input){
		"verify exit 2":     func(in *Input) { in.VerifyExit = 2 },
		"verify incomplete": func(in *Input) { in.Verify.Incomplete = "operation lookup failed" },
		"verify missing":    func(in *Input) { in.Verify = nil },
		"check exit 2":      func(in *Input) { in.CheckExit = 2 },
	} {
		in := clean("kill-leader")
		breakIt(&in)
		if r := Judge(in); r.Pass || !r.NoVerdict || !strings.HasPrefix(r.Failures[0], "no verdict") {
			t.Errorf("%s: %+v, want no verdict", name, r)
		}
	}
}

// A negative control passes only if its injected fault was caught.
func TestNegativeControls(t *testing.T) {
	in := clean("kill-follower")
	in.Negative = NegativeLostWrite
	if r := Judge(in); r.Pass {
		t.Errorf("lost-write control with 0 lost passed: %+v", r)
	}
	in.Verify.Verdict.Lost = []ledger.Loss{{Reason: "overwritten"}}
	in.VerifyExit = 1
	if r := Judge(in); !r.Pass {
		t.Errorf("lost-write control that caught the loss failed: %+v", r)
	}

	in = clean("delay-follower")
	in.Negative = NegativeStaleReader
	if r := Judge(in); r.Pass || !strings.Contains(r.Failures[0], "were not caught") {
		t.Errorf("stale-reader control with 0 stale reads passed: %+v", r)
	}
	in.Verify.Verdict.StaleReads = []ledger.StaleRead{{Reason: "read v1"}}
	in.VerifyExit = 1
	if r := Judge(in); !r.Pass {
		t.Errorf("stale-reader control that caught stale reads failed: %+v", r)
	}

	in = clean("kill-leader")
	in.Negative = "bogus"
	if r := Judge(in); r.Pass {
		t.Error("an unknown negative control passed")
	}
}

func TestUnknownExperiment(t *testing.T) {
	if r := Judge(clean("unplug-the-laptop")); r.Pass {
		t.Error("an unknown experiment passed")
	}
}

// dropProbe removes pod's probe from the faulted shard's results.
func dropProbe(in *Input, pod string) {
	w := shardOf(in, 1)
	delete(w.ProbeReadsDuring, pod)
	delete(w.ProbeServedInsideFault, pod)
	delete(w.ProbeReadsInsideFault, pod)
}

// A run proves nothing about a fault the load never touched once it fully
// held: an empty interval after the grace, or one with fewer than
// MinCoverage write attempts or reads on the faulted shard (every shard for
// a node or coordinator fault), fails as no coverage. A probe that ran only
// in the grace was never refused while the fault held.
func TestCoverage(t *testing.T) {
	for _, tc := range []struct {
		name, experiment string
		breakIt          func(*Input)
		has              string
	}{
		{"empty window", "kill-leader", func(in *Input) { in.Verify.Availability.GraceMs = 20_000 },
			"coverage: no observation window after the grace: the fault lasted 20000ms, the grace is 20000ms"},
		{"grace past the heal", "lose-quorum", func(in *Input) { in.Verify.Availability.GraceMs = 20_001 },
			"coverage: no observation window after the grace: the fault lasted 20000ms, the grace is 20001ms"},
		{"no writes after the grace", "kill-leader", func(in *Input) { shardOf(in, 1).WriteAttemptsInsideFault = 0 },
			"coverage: shard 1 had 0 write attempt(s) after the grace, want at least 3"},
		{"too few reads after the grace", "isolate-replica", func(in *Input) { shardOf(in, 1).ReadsInsideFault = MinCoverage - 1 },
			"coverage: shard 1 had 2 read(s) after the grace, want at least 3"},
		{"no writes after the grace, quorum lost", "lose-quorum", func(in *Input) { shardOf(in, 1).WriteAttemptsInsideFault = 0 },
			"coverage: shard 1 had 0 write attempt(s) after the grace"},
		{"another shard, node stopped", "stop-worker", func(in *Input) { shardOf(in, 0).ReadsInsideFault = 0 },
			"coverage: shard 0 had 0 read(s) after the grace"},
		{"probes only in the grace", "lose-quorum", func(in *Input) { shardOf(in, 1).ProbeReadsInsideFault = map[string]int{} },
			"probe: storage-shard-1-0 had 0 direct read(s) after the grace, want at least 3"},
		{"probe only in the grace", "isolate-leader", func(in *Input) { shardOf(in, 1).ProbeReadsInsideFault["storage-shard-1-0"] = 2 },
			"probe: storage-shard-1-0 had 2 direct read(s) after the grace, want at least 3"},
	} {
		in := clean(tc.experiment)
		tc.breakIt(&in)
		r := Judge(in)
		if r.Pass || !strings.Contains(strings.Join(r.Failures, "\n"), tc.has) {
			t.Errorf("%s (%s), want %q: pass %v, failures %q", tc.name, tc.experiment, tc.has, r.Pass, r.Failures)
		}
	}
}

// Every replica an experiment cuts off must have been probed: for
// lose-quorum all three of the shard's, for an isolated leader the target.
// A missing one, or a probe of the wrong pod, is reported by name.
func TestReplicaProbes(t *testing.T) {
	for _, tc := range []struct {
		name, experiment string
		breakIt          func(*Input)
		has              []string
	}{
		{"one missing", "lose-quorum", func(in *Input) { dropProbe(in, "storage-shard-1-2") },
			[]string{"probe: missing probe for storage-shard-1-2"}},
		{"two missing", "lose-quorum", func(in *Input) { dropProbe(in, "storage-shard-1-1"); dropProbe(in, "storage-shard-1-2") },
			[]string{"probe: missing probe for storage-shard-1-1", "probe: missing probe for storage-shard-1-2"}},
		{"wrong target, quorum lost", "lose-quorum", func(in *Input) {
			dropProbe(in, "storage-shard-1-2")
			w := shardOf(in, 1)
			w.ProbeReadsDuring["storage-shard-2-2"], w.ProbeReadsInsideFault["storage-shard-2-2"] = 30, 25
		}, []string{"probe: missing probe for storage-shard-1-2",
			"probe: unexpected probe storage-shard-2-2 on shard 1 (want storage-shard-1-0, storage-shard-1-1, storage-shard-1-2)"}},
		{"wrong target, leader isolated", "delay-isolate-leader", func(in *Input) {
			dropProbe(in, "storage-shard-1-0")
			w := shardOf(in, 1)
			w.ProbeReadsDuring["storage-shard-1-1"], w.ProbeReadsInsideFault["storage-shard-1-1"] = 30, 25
		}, []string{"probe: missing probe for storage-shard-1-0", "probe: unexpected probe storage-shard-1-1 on shard 1 (want storage-shard-1-0)"}},
		{"target on another shard", "isolate-leader", func(in *Input) { in.Target = "storage-shard-2-0" },
			[]string{"probe: target storage-shard-2-0 is not a replica of shard 1"}},
	} {
		in := clean(tc.experiment)
		tc.breakIt(&in)
		r := Judge(in)
		got := strings.Join(r.Failures, "\n")
		for _, h := range tc.has {
			if r.Pass || !strings.Contains(got, h) {
				t.Errorf("%s (%s), want %q: pass %v, failures %q", tc.name, tc.experiment, h, r.Pass, r.Failures)
			}
		}
	}
}

// A coordinator outage stalls every shard's traffic alike, so its write
// attempts after the grace are counted across the shards (at least
// MinCoverage per shard in total); its reads are still held per shard.
func TestCoordinatorCoverage(t *testing.T) {
	writes := func(in *Input, per [3]int) {
		for id, n := range per {
			shardOf(in, id).WriteAttemptsInsideFault = n
		}
	}
	in := clean("restart-coordinator")
	writes(&in, [3]int{0, 0, 9}) // all on one shard
	if r := Judge(in); !r.Pass {
		t.Errorf("9 write attempts, all on shard 2: failures %q, want a pass", r.Failures)
	}
	in = clean("restart-coordinator")
	writes(&in, [3]int{2, 3, 3})
	if r := Judge(in); r.Pass || !strings.Contains(strings.Join(r.Failures, "\n"),
		"coverage: 8 write attempt(s) after the grace across all shards, want at least 9") {
		t.Errorf("8 write attempts in total: pass %v, failures %q, want a coverage failure", r.Pass, r.Failures)
	}
	in = clean("restart-coordinator")
	writes(&in, [3]int{0, 0, 41})
	shardOf(&in, 1).ReadsInsideFault = 2
	if r := Judge(in); r.Pass || !strings.Contains(strings.Join(r.Failures, "\n"), "coverage: shard 1 had 2 read(s) after the grace, want at least 3") {
		t.Errorf("shard 1 with 2 reads: pass %v, failures %q, want a coverage failure", r.Pass, r.Failures)
	}
	// Other experiments keep the per-shard write minimum.
	in = clean("stop-worker")
	writes(&in, [3]int{0, 0, 41})
	if r := Judge(in); r.Pass || !strings.Contains(strings.Join(r.Failures, "\n"), "coverage: shard 0 had 0 write attempt(s) after the grace") {
		t.Errorf("stop-worker with writes on one shard: pass %v, failures %q, want a per-shard failure", r.Pass, r.Failures)
	}
}
