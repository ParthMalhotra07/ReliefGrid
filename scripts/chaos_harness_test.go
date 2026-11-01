package scripts

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// The S4.3 harness's verdict path, without a cluster: chaos/scripts/lib.sh is
// sourced with its cluster calls stubbed, run_judge runs the real judge
// (a fake docker execs a freshly built invcheck), and apply_judge plus
// write_report produce the report a real run would.

var (
	buildOnce sync.Once
	invcheck  string
	buildErr  error
)

func invcheckBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "invcheck-bin")
		if err != nil {
			buildErr = err
			return
		}
		invcheck = filepath.Join(dir, "invcheck")
		out, err := exec.CommandContext(context.Background(), "go", "build", "-o", invcheck, "../cmd/invcheck").CombinedOutput()
		if err != nil {
			buildErr = err
			t.Logf("%s", out)
		}
	})
	if buildErr != nil {
		t.Fatalf("build invcheck: %v", buildErr)
	}
	return invcheck
}

// verifyFixture is invcheck verify --json for a run on faulted shard 1 with a
// probe at storage-shard-1-0: all shards acked during and after, the faulted
// shard none while the fault fully held.
func verifyFixture(edit func(w []ledger.ShardWindow, v *ledger.Verdict)) string {
	shards := []ledger.ShardWindow{}
	for id := range 3 {
		w := ledger.ShardWindow{Shard: id, AcksBefore: 9, AcksDuring: 12, AcksAfter: 9, FirstAckAfterFaultMs: 40, FirstAckAfterHealMs: 30,
			ProbeReadsDuring: map[string]int{}, ProbeServedInsideFault: map[string]int{}}
		if id == 1 {
			w.AcksDuring, w.ProbeReadsDuring["storage-shard-1-0"] = 2, 25
		}
		shards = append(shards, w)
	}
	v := ledger.Verdict{Checked: 300, Keys: 40, Lost: []ledger.Loss{}, StaleReads: []ledger.StaleRead{},
		Reads: ledger.ReadStats{Total: 400, Checked: 350, Failed: 30, Served: map[string]int{"linearizable": 370}}}
	if edit != nil {
		edit(shards, &v)
	}
	b, _ := json.Marshal(map[string]any{"ok": true, "ledger": ledger.Stats{Acked: 300, Unknown: 4, AckedInWindow: 30, Errors: []ledger.AttemptError{}},
		"resolution": ledger.Resolution{Resolved: 3, NotApplied: 1}, "verdict": v,
		"availability": ledger.WindowStats{FromMs: 1000, ToMs: 21000, GraceMs: 2000, Shards: shards}})
	return string(b)
}

const checkFixture = `{"ok":true,"verdict":"clean","report":{"requests":0,"resources":0,"quiescent":true,"violations":[]}}`

type harnessRun struct {
	experiment string
	args       string // extra lib.sh flags
	verify     string
	verifyExit int
	vars       string // extra bash assignments (harness facts)
}

// runHarness runs the verdict path and returns the report and the exit code.
func runHarness(t *testing.T, h harnessRun) (map[string]any, int, string) {
	t.Helper()
	dir := t.TempDir()
	docker := "#!/usr/bin/env bash\n# docker run --rm -i IMAGE <invcheck args>\nshift\nwhile [[ \"$1\" == -* ]]; do shift; done\nshift\nexec " + invcheckBinary(t) + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(docker), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	out := filepath.Join(dir, "report.json")
	script := `set -u
EXPERIMENT=` + h.experiment + ` DESCRIPTION=test
. ../chaos/scripts/lib.sh
k() { echo "cluster call: k $*" >&2; return 1; }
kn() { echo "cluster call: kn $*" >&2; return 1; }
parse_args --seed 3 --shard 1 --out "` + out + `" ` + h.args + `
make_plan
work="$(mktemp -d)"; : >"$work/notes"; failures=(); run_id=test-run
load_job=chaos-load-test verify_job=chaos-verify-test check_job=chaos-check-test ledger_path=/ledger/test.json
target_pod=storage-shard-1-0 target_id=storage-shard-1-0/abc target_term=2 fault_kind=test fault_mechanism=test
fault_started_at=1000 fault_healed_at=21000 settle_ok=true settle_detail="shard 1 is one caught-up group" settle_seconds=3.0
settle_started_at=22000 settled_at=25000 load_started_at=500 load_finished_at=30000 load_exit=0
recovery_ms=3100 recovery_term=3 recovery_leader=storage-shard-1-2 injected="" isolation=""
harness_isolation="" harness_delay="" harness_worker="" harness_coordinator="" judge_json="" control_json=""
read_mode=` + map[bool]string{true: "stale", false: "linearizable"}[strings.Contains(h.args, "--stale-readers")] + `
probe_args=(--probe "1=http://storage-shard-1-0.storage-shard-1.reliefgrid.svc.cluster.local:8080")
verify_json="$(cat "` + write("verify.json", h.verify) + `")" verify_exit=` + string(rune('0'+h.verifyExit)) + `
check_json="$(cat "` + write("check.json", checkFixture) + `")" check_exit=0
` + h.vars + `
apply_judge
if [ "${#failures[@]}" = 0 ]; then write_report true; else write_report false; fi
`
	cmd := exec.CommandContext(t.Context(), "bash", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "TOOLS_IMAGE=reliefgrid/tools:test")
	stderr, _ := cmd.CombinedOutput()
	code := cmd.ProcessState.ExitCode()
	if strings.Contains(string(stderr), "cluster call:") {
		t.Fatalf("the verdict path touched the cluster:\n%s", stderr)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("no report (exit %d):\n%s", code, stderr)
	}
	var r map[string]any
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatalf("report is not JSON: %v\n%s", err, b)
	}
	return r, code, string(stderr)
}

func failuresOf(r map[string]any) string {
	var parts []string
	for _, f := range r["failures"].([]any) {
		parts = append(parts, f.(string))
	}
	return strings.Join(parts, "\n")
}

func TestHarnessVerdicts(t *testing.T) {
	if testing.Short() {
		t.Skip("builds invcheck")
	}
	delay := `harness_delay='{"maxLag":6,"finalLag":0,"samples":40}'`

	// A passing lose-quorum run: the judge passes it and the report carries
	// its verdict, the availability, the reads and the harness facts.
	r, code, log := runHarness(t, harnessRun{experiment: "lose-quorum", verify: verifyFixture(nil)})
	if code != 0 || r["pass"] != true || r["judge"].(map[string]any)["pass"] != true || failuresOf(r) != "" {
		t.Fatalf("passing run: exit %d, pass %v, failures %q\n%s", code, r["pass"], failuresOf(r), log)
	}
	if n := len(r["availability"].(map[string]any)["shards"].([]any)); n != 3 {
		t.Errorf("availability for %d shards, want 3", n)
	}
	reads := r["reads"].(map[string]any)
	if reads["consistency"] != "linearizable" || len(reads["probes"].([]any)) != 1 || reads["verdict"].(map[string]any)["checked"] != float64(350) {
		t.Errorf("reads %v", reads)
	}
	if s := r["judge"].(map[string]any)["summary"].(map[string]any); s["unknownRaw"] != float64(4) || s["unresolvedUnknown"] != float64(0) {
		t.Errorf("summary %v: want the raw unknown count (4) beside 0 unresolved", s)
	}
	if r["ledger"].(map[string]any)["checked"] != float64(300) || r["invcheck"].(map[string]any)["verdict"] != "clean" {
		t.Errorf("S2.3 fields missing: ledger %v invcheck %v", r["ledger"], r["invcheck"])
	}

	// The faulted shard acknowledged a write with no majority: the run fails, saying so.
	r, _, _ = runHarness(t, harnessRun{experiment: "lose-quorum", verify: verifyFixture(func(w []ledger.ShardWindow, _ *ledger.Verdict) {
		w[1].AcksInsideFault = 1
	})})
	if r["pass"] != false || strings.HasPrefix(failuresOf(r), "no verdict") || !strings.Contains(failuresOf(r), "acknowledged 1 write(s) with no majority") {
		t.Errorf("quorum violation: pass %v, failures %q", r["pass"], failuresOf(r))
	}

	// The stale-reader control: stale reads fail the run as stale reads (not
	// as lost writes), and the control records that they were caught.
	r, _, _ = runHarness(t, harnessRun{experiment: "delay-follower", args: "--stale-readers", verifyExit: 1, vars: delay,
		verify: verifyFixture(func(_ []ledger.ShardWindow, v *ledger.Verdict) {
			v.StaleReads = []ledger.StaleRead{{Reason: "read v3"}}
		})})
	f := failuresOf(r)
	ctl, _ := r["control"].(map[string]any)
	if r["pass"] != false || !strings.Contains(f, "1 stale read(s)") || strings.Contains(f, "lost") || ctl == nil || ctl["pass"] != true || ctl["negative"] != "stale-reader" {
		t.Errorf("stale-reader control: pass %v, failures %q, control %v", r["pass"], f, ctl)
	}
	if r["reads"].(map[string]any)["consistency"] != "stale" {
		t.Errorf("stale-reader control read %v, want stale", r["reads"].(map[string]any)["consistency"])
	}

	// The lost-write control (S2.3's --inject): the failure names the ledger
	// and the loss, as verify-S2.3.sh expects, and the control is caught.
	r, _, _ = runHarness(t, harnessRun{experiment: "kill-follower", args: "--inject lost-write", verifyExit: 1,
		verify: verifyFixture(func(_ []ledger.ShardWindow, v *ledger.Verdict) { v.Lost = []ledger.Loss{{Reason: "overwritten"}} })})
	ctl, _ = r["control"].(map[string]any)
	if r["pass"] != false || !strings.Contains(failuresOf(r), "ledger: 1 acknowledged write(s) lost") || ctl == nil || ctl["pass"] != true {
		t.Errorf("lost-write control: pass %v, failures %q, control %v", r["pass"], failuresOf(r), ctl)
	}

	// No verdict from verify: exit 2, reported as no verdict.
	r, code, _ = runHarness(t, harnessRun{experiment: "kill-leader", verify: `{}`, verifyExit: 2})
	if code != 2 || r["pass"] != false || !strings.HasPrefix(failuresOf(r), "no verdict") {
		t.Errorf("no verdict: exit %d, pass %v, failures %q", code, r["pass"], failuresOf(r))
	}
}

// Every experiment script exists, prints a deterministic plan without
// touching anything, and --stale-readers is refused outside delay-follower.
func TestChaosExperimentPlans(t *testing.T) {
	for _, e := range []string{"kill-leader", "kill-follower", "restart-shard", "isolate-replica", "isolate-leader",
		"lose-quorum", "delay-follower", "delay-isolate-leader", "stop-worker", "restart-coordinator"} {
		plan := func() string {
			out, err := exec.CommandContext(t.Context(), "bash", "../chaos/scripts/"+e+".sh", "--seed", "7", "--plan").Output()
			if err != nil {
				t.Fatalf("%s --plan: %v", e, err)
			}
			return string(out)
		}
		a, b := plan(), plan()
		var p map[string]any
		if a != b || json.Unmarshal([]byte(a), &p) != nil || p["experiment"] != e {
			t.Errorf("%s: plan %q then %q", e, a, b)
		}
		cmd := exec.CommandContext(t.Context(), "bash", "../chaos/scripts/"+e+".sh", "--seed", "7", "--stale-readers", "--plan")
		_ = cmd.Run()
		if want := map[bool]int{true: 0, false: 2}[e == "delay-follower"]; cmd.ProcessState.ExitCode() != want {
			t.Errorf("%s --stale-readers: exit %d, want %d", e, cmd.ProcessState.ExitCode(), want)
		}
	}
}

// suite.sh --summarize builds the suite's verdict from its reports: every
// experiment must pass, and each control must have caught its fault with no
// other failure; a missing report is a failure, never a skip.
func TestSuiteSummary(t *testing.T) {
	report := func(experiment string, pass bool, failures []string, control map[string]any) map[string]any {
		r := map[string]any{"experiment": experiment, "seed": 431, "pass": pass, "failures": failures,
			"recovery": map[string]any{"seconds": 2.4}, "settle": map[string]any{"seconds": 3.0},
			"target": map[string]any{"shard": 1},
			"fault":  map[string]any{"startedAt": 1000, "healedAt": 21000},
			"judge":  map[string]any{"summary": map[string]any{"acked": 300, "unknownRaw": 4, "unresolvedUnknown": 0, "lost": 0, "staleReads": 0}},
			"availability": map[string]any{"shards": []any{map[string]any{"shard": 1, "acksDuring": 2, "acksInsideFault": 0,
				"probeServedInsideFault": map[string]any{"storage-shard-1-0": 0}, "firstAckAfterHealMs": 310}}}}
		if control != nil {
			r["control"] = control
		}
		return r
	}
	write := func(dir string, reports map[string]map[string]any) {
		if err := os.MkdirAll(filepath.Join(dir, "reports"), 0o755); err != nil {
			t.Fatal(err)
		}
		for name, r := range reports {
			b, _ := json.Marshal(r)
			if err := os.WriteFile(filepath.Join(dir, "reports", name+".json"), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	summarize := func(dir string) (int, string) {
		cmd := exec.CommandContext(t.Context(), "bash", "../chaos/scripts/suite.sh", "--summarize", dir)
		cmd.Env = append(os.Environ(), "ONLY=lose-quorum,control-lost-write,control-stale-reader")
		out, _ := cmd.CombinedOutput()
		return cmd.ProcessState.ExitCode(), string(out)
	}
	good := map[string]map[string]any{
		"lose-quorum": report("lose-quorum", true, []string{}, nil),
		"control-lost-write": report("kill-follower", false, []string{"ledger: 1 acknowledged write(s) lost"},
			map[string]any{"negative": "lost-write", "pass": true}),
		"control-stale-reader": report("delay-follower", false, []string{"reads: 3 stale read(s): a read missed a write acknowledged before it began"},
			map[string]any{"negative": "stale-reader", "pass": true}),
	}
	dir := t.TempDir()
	write(dir, good)
	code, out := summarize(dir)
	if code != 0 || !strings.Contains(out, "PASSED (3/3 runs)") || !strings.Contains(out, "lose-quorum") {
		t.Fatalf("all good: exit %d\n%s", code, out)
	}

	// A control that was caught but also broke something else is not a pass.
	bad := map[string]map[string]any{}
	for k, v := range good {
		bad[k] = v
	}
	bad["control-stale-reader"] = report("delay-follower", false,
		[]string{"reads: 3 stale read(s): x", "invcheck: violations (1 violation(s))"}, map[string]any{"negative": "stale-reader", "pass": true})
	dir = t.TempDir()
	write(dir, bad)
	if code, out := summarize(dir); code != 1 || !strings.Contains(out, "control-stale-reader") || !strings.Contains(out, "FAILED (2/3 runs)") {
		t.Fatalf("control with another failure: exit %d\n%s", code, out)
	}

	// A missing report fails the suite.
	dir = t.TempDir()
	delete(good, "lose-quorum")
	write(dir, good)
	if code, out := summarize(dir); code != 1 || !strings.Contains(out, "lose-quorum: no report") {
		t.Fatalf("missing report: exit %d\n%s", code, out)
	}
}
