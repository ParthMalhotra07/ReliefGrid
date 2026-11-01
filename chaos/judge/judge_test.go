package judge

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// clean is a passing run of experiment on shard 1, target storage-shard-1-0:
// every shard acked during and after the fault, the faulted shard none while
// it fully held (as lose-quorum needs), and the probe was refused.
func clean(experiment string) Input {
	shard := func(id int) ledger.ShardWindow {
		w := ledger.ShardWindow{Shard: id, AcksBefore: 10, AcksDuring: 20, AcksAfter: 10, FirstAckAfterFaultMs: 50, FirstAckAfterHealMs: 40,
			ProbeReadsDuring: map[string]int{}, ProbeServedInsideFault: map[string]int{}}
		if id == 1 {
			w.ProbeReadsDuring["storage-shard-1-0"] = 30
			w.ProbeServedInsideFault["storage-shard-1-0"] = 0
		}
		return w
	}
	in := Input{Experiment: experiment, Shard: 1, Target: "storage-shard-1-0",
		Bounds: Bounds{ElectionMaxSeconds: 10, RecoveryMaxSeconds: 60},
		Verify: &Verify{OK: true, Ledger: ledger.Stats{Acked: 500, Unknown: 3},
			Resolution: ledger.Resolution{Resolved: 2, NotApplied: 1},
			Verdict: &ledger.Verdict{Checked: 500, Lost: []ledger.Loss{}, StaleReads: []ledger.StaleRead{},
				Reads: ledger.ReadStats{Total: 800, Checked: 700, Failed: 60}},
			Availability: &ledger.WindowStats{FromMs: 1, ToMs: 2, GraceMs: 2000, Shards: []ledger.ShardWindow{shard(0), shard(1), shard(2)}}},
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
		{"isolate-leader", func(in *Input) { delete(shardOf(in, 1).ProbeReadsDuring, "storage-shard-1-0") }, "no direct read of storage-shard-1-0"},
		{"isolate-leader", func(in *Input) { shardOf(in, 1).AcksDuring = 0 }, "majority: shard 1 acknowledged no write"},

		{"lose-quorum", func(in *Input) { shardOf(in, 1).AcksInsideFault = 1 }, "acknowledged 1 write(s) with no majority"},
		{"lose-quorum", func(in *Input) { shardOf(in, 1).ReadsServedInsideFault = 3 }, "served 3 linearizable read(s) with no majority"},
		{"lose-quorum", func(in *Input) { shardOf(in, 1).FirstAckAfterHealMs = -1 }, "acknowledged no write after the heal"},
		{"lose-quorum", func(in *Input) { shardOf(in, 1).FirstAckAfterHealMs = 75_000 }, "75.0s after the heal, over 60s"},
		{"lose-quorum", func(in *Input) { shardOf(in, 2).AcksDuring = 0 }, "shard 2 (not faulted) acknowledged no write"},
		{"lose-quorum", func(in *Input) { shardOf(in, 1).ProbeReadsDuring = map[string]int{} }, "no direct read of shard 1's replicas"},

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
