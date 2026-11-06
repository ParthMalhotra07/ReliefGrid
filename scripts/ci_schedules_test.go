package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// schedules (A2.3, #110): the schedules-tagged tests in tests/workflow, including
// TestKillRejectsExtraRestartBeforeReady (exact restartCount == before+1), are built only
// with -tags schedules, so make check never ran them. The schedules job runs every one of
// them on the M8 backend against a disposable pinned broker (scripts/ci-schedules.sh), in
// its own job so check's runtime does not change. It runs for ready PRs whose diff touches
// tests/workflow, the packages it builds, or how it is run (ci-changes.sh schedules=true).
func TestCISchedulesJob(t *testing.T) {
	w := loadCI(t)
	j := mustJob(t, w, "schedules")
	for _, cond := range []string{
		"github.event_name == 'pull_request'",
		"!github.event.pull_request.draft",
		"needs.changes.outputs.schedules == 'true'",
	} {
		if !strings.Contains(j.If, cond) {
			t.Errorf("schedules if: %q does not contain %q", j.If, cond)
		}
	}
	if !slices.Contains(j.needs(), "changes") {
		t.Errorf("schedules needs %v; it must need changes", j.needs())
	}
	if j.Timeout == 0 || j.Timeout > 20 {
		t.Errorf("schedules timeout-minutes = %d, want 1..20", j.Timeout)
	}
	if !slices.ContainsFunc(j.Steps, func(s step) bool { return strings.TrimSpace(s.Run) == "make ci-schedules" }) {
		t.Error("schedules does not run make ci-schedules")
	}
	for i, s := range j.Steps {
		if s.If != "" && !strings.HasPrefix(s.If, "failure()") {
			t.Errorf("schedules step %d (%s%s) is conditional (if: %q); the tests must always run", i, s.Name, s.Uses, s.If)
		}
		if strings.Contains(s.Run, "continue-on-error") {
			t.Errorf("schedules step %d may not continue on error", i)
		}
	}
	if got := mustJob(t, w, "changes").Outputs["schedules"]; got != "${{ steps.diff.outputs.schedules }}" {
		t.Errorf("changes outputs schedules = %q, want it from scripts/ci-changes.sh", got)
	}
	out := makeDryRun(t, "ci-schedules")
	if !strings.Contains(out, "scripts/ci-schedules.sh") {
		t.Errorf("make -n ci-schedules does not run scripts/ci-schedules.sh:\n%s", out)
	}
	// check stays as it is: the schedules build and its broker are not in make check.
	check := makeDryRun(t, "check")
	for _, slow := range []string{"-tags schedules", "scripts/ci-schedules.sh"} {
		if strings.Contains(check, slow) {
			t.Errorf("make check runs %q; that belongs in the schedules job", slow)
		}
	}
	if strings.Contains(mustJob(t, w, "check").text(), "schedules") {
		t.Error("the check job mentions schedules; it must stay unchanged")
	}
}

// The script runs the whole schedules-tagged package (no -run filter, no -short) on M8 with
// the race detector, and takes its broker image from the app chart's digest pin.
func TestCISchedulesScriptRunsWholePackage(t *testing.T) {
	b, err := os.ReadFile("ci-schedules.sh")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		"-tags schedules", "-race", "-count=1", "SCHEDULE_BACKEND=m8", "./tests/workflow",
		"TestKillRejectsExtraRestartBeforeReady", "deploy/helm/reliefgrid/values.yaml", "@sha256:",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("ci-schedules.sh has no %q", want)
		}
	}
	for _, bad := range []string{" -run ", "-short", "-skip"} {
		if strings.Contains(s, bad) {
			t.Errorf("ci-schedules.sh uses %q; it must run every schedules-tagged test", strings.TrimSpace(bad))
		}
	}
}

// ci-changes.sh prints schedules=true when the diff touches tests/workflow, a package it
// builds, or how the job runs it; schedules=false otherwise.
func TestCIChangesSchedules(t *testing.T) {
	for _, tc := range []struct {
		files []string
		want  string
	}{
		{[]string{"tests/workflow/kill_restart_test.go"}, "schedules=true"},
		{[]string{"tests/workflow/schedules.md"}, "schedules=true"}, // TestFixturesMatchSchedulesMarkdown reads it
		{[]string{"internal/workflow/matcher.go"}, "schedules=true"},
		{[]string{"internal/fsm/memstore/memstore.go"}, "schedules=true"},
		{[]string{"internal/api/gen/api.go"}, "schedules=true"},
		{[]string{"contracts/openapi.yaml"}, "schedules=true"},             // internal/api/gen and internalapi come from it
		{[]string{"deploy/helm/reliefgrid/values.yaml"}, "schedules=true"}, // the broker image pin
		{[]string{"scripts/ci-schedules.sh"}, "schedules=true"},
		{[]string{"scripts/ci-changes.sh"}, "schedules=true"},
		{[]string{".github/workflows/ci.yml"}, "schedules=true"},
		{[]string{"go.mod"}, "schedules=true"},
		{[]string{"go.sum"}, "schedules=true"},
		{[]string{"Makefile"}, "schedules=true"},
		{[]string{"tools.mk"}, "schedules=true"},
		{[]string{"docs/a.md", "tests/workflow/x_test.go"}, "schedules=true"},
		{[]string{"docs/a.md"}, "schedules=false"},
		{[]string{"tests/seams/kind_test.go"}, "schedules=false"},
		{[]string{"tests/workflowx/a.go"}, "schedules=false"}, // the directory only
		{[]string{"internal/raftstore/node.go"}, "schedules=false"},
		{[]string{"internal/reconcile/reconcile.go"}, "schedules=false"},
		{[]string{"cmd/api/main.go"}, "schedules=false"},
		{[]string{"deploy/helm/storage/values.yaml"}, "schedules=false"},
		{[]string{"ui/app.js"}, "schedules=false"},
	} {
		t.Run(strings.Join(tc.files, ","), func(t *testing.T) {
			lines := ciChanges(t, tc.files)
			if !slices.Contains(lines, tc.want) {
				t.Errorf("got %q, want a line %q", lines, tc.want)
			}
		})
	}
}

// The filter is a fixed list, so a new import in tests/workflow could silently fall outside
// it. Every module package the schedules-tagged tests build must count as a change.
func TestCIChangesSchedulesCoversDeps(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "go", "list", "-deps", "-test", "-tags", "schedules",
		"-f", "{{if not .Standard}}{{.Dir}}{{end}}", "./tests/workflow")
	cmd.Dir = ".."
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for _, d := range strings.Fields(string(out)) {
		rel, err := filepath.Rel(root, d)
		if err != nil || strings.HasPrefix(rel, "..") || slices.Contains(dirs, rel) {
			continue // outside the module (the module cache) or already listed
		}
		dirs = append(dirs, rel)
	}
	if !slices.Contains(dirs, filepath.Join("tests", "workflow")) || !slices.Contains(dirs, filepath.Join("internal", "workflow")) {
		t.Fatalf("go list found %v; want at least tests/workflow and internal/workflow", dirs)
	}
	for _, d := range dirs {
		f := filepath.ToSlash(filepath.Join(d, "dep_probe.go"))
		if lines := ciChanges(t, []string{f}); !slices.Contains(lines, "schedules=true") {
			t.Errorf("%s is built by the schedules tests but ci-changes.sh prints %q", d, lines)
		}
	}
}

// --- scripts/ci-schedules.sh with fake go, docker and curl --------------------------------

const fakeScheduleTests = "TestCASProposalBarrierProbe\nTestSeededWorkflowSchedules\nTestKillRejectsExtraRestartBeforeReady\n"

// runCISchedules runs the script with go, docker and curl replaced. listed is what
// `go test -list` prints; run is the verbose output of the test run and code its exit code.
// It returns the script's output, the fake tools' call log and the script's exit error.
func runCISchedules(t *testing.T, listed, run string, code int) (string, string, error) {
	t.Helper()
	bin := t.TempDir()
	calls := filepath.Join(bin, "calls")
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/usr/bin/env bash\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "listed"), []byte(listed), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "run"), []byte(run), 0o644); err != nil {
		t.Fatal(err)
	}
	write("go", `echo "go $* SCHEDULE_BACKEND=${SCHEDULE_BACKEND:-} MQ=${MQ_AMQP_URL:-}" >> "$FAKE_CALLS"
case " $* " in
*" -list "*) cat "$FAKE_DIR/listed"; echo "ok  	x	0.01s";;
*) cat "$FAKE_DIR/run"; exit "$FAKE_CODE";;
esac
`)
	write("docker", `echo "docker $*" >> "$FAKE_CALLS"
case "$1" in run) echo fakebroker;; port) echo 127.0.0.1:4242;; esac
`)
	write("curl", `echo "curl $*" >> "$FAKE_CALLS"`)
	script, err := filepath.Abs("ci-schedules.sh")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_CALLS="+calls, "FAKE_DIR="+bin,
		"FAKE_CODE="+strconv.Itoa(code), "SCHEDULES_LOG_DIR="+t.TempDir())
	out, err := cmd.CombinedOutput()
	c, _ := os.ReadFile(calls)
	return string(out), string(c), err
}

func passLines(names ...string) string {
	var b strings.Builder
	for _, n := range names {
		b.WriteString("=== RUN   " + n + "\n--- PASS: " + n + " (0.10s)\n")
	}
	return b.String() + "PASS\nok  	github.com/ParthMalhotra07/ReliefGrid/tests/workflow	1.0s\n"
}

func TestCISchedulesAllPass(t *testing.T) {
	out, calls, err := runCISchedules(t, fakeScheduleTests,
		passLines("TestCASProposalBarrierProbe", "TestSeededWorkflowSchedules", "TestKillRejectsExtraRestartBeforeReady"), 0)
	if err != nil {
		t.Fatalf("all tests passed but the script failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "3/3") {
		t.Errorf("output does not report 3/3 tests passed:\n%s", out)
	}
	if !strings.Contains(calls, "SCHEDULE_BACKEND=m8 MQ=amqp://") {
		t.Errorf("the test run did not get the M8 backend and the broker URL:\n%s", calls)
	}
	if !strings.Contains(calls, "docker rm -f fakebroker") {
		t.Errorf("the broker was not removed:\n%s", calls)
	}
}

func TestCISchedulesFailsOnSkip(t *testing.T) {
	run := strings.Replace(passLines("TestCASProposalBarrierProbe", "TestSeededWorkflowSchedules", "TestKillRejectsExtraRestartBeforeReady"),
		"--- PASS: TestSeededWorkflowSchedules", "    --- SKIP: TestSeededWorkflowSchedules/seed-1/SCH15 (0.00s)\n--- PASS: TestSeededWorkflowSchedules", 1)
	out, calls, err := runCISchedules(t, fakeScheduleTests, run, 0)
	if err == nil {
		t.Fatalf("a skipped subtest passed the job:\n%s", out)
	}
	if !strings.Contains(out, "SKIP") || !strings.Contains(calls, "docker rm -f fakebroker") {
		t.Errorf("want the skip reported and the broker removed:\n%s\n%s", out, calls)
	}
}

func TestCISchedulesFailsWhenATestDoesNotPass(t *testing.T) {
	// go test exits 0 but one listed test never reports PASS (e.g. it was not run).
	out, _, err := runCISchedules(t, fakeScheduleTests, passLines("TestCASProposalBarrierProbe", "TestSeededWorkflowSchedules"), 0)
	if err == nil || !strings.Contains(out, "TestKillRejectsExtraRestartBeforeReady") {
		t.Fatalf("a listed test without PASS passed the job (err %v):\n%s", err, out)
	}
}

func TestCISchedulesFailsWithoutKillRestartTest(t *testing.T) {
	// The exact-restart regression must be among the listed tests, so renaming it or
	// changing its build tag cannot drop it.
	listed := "TestCASProposalBarrierProbe\nTestSeededWorkflowSchedules\n"
	out, _, err := runCISchedules(t, listed, passLines("TestCASProposalBarrierProbe", "TestSeededWorkflowSchedules"), 0)
	if err == nil || !strings.Contains(out, "TestKillRejectsExtraRestartBeforeReady") {
		t.Fatalf("no kill-restart test, yet the job passed (err %v):\n%s", err, out)
	}
}

func TestCISchedulesFailsWhenGoTestFails(t *testing.T) {
	run := "=== RUN   TestKillRejectsExtraRestartBeforeReady\n--- FAIL: TestKillRejectsExtraRestartBeforeReady (0.10s)\nFAIL\n"
	out, calls, err := runCISchedules(t, fakeScheduleTests, run, 1)
	if err == nil {
		t.Fatalf("go test failed but the job passed:\n%s", out)
	}
	if !strings.Contains(out, "--- FAIL: TestKillRejectsExtraRestartBeforeReady") || !strings.Contains(calls, "docker rm -f fakebroker") {
		t.Errorf("want the failure shown and the broker removed:\n%s\n%s", out, calls)
	}
}
