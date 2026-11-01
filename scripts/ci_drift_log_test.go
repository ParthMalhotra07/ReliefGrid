package scripts

import (
	"archive/zip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// verify-S4.6 reads the drift probe's run log archive (gh api .../logs)
// through scripts/ci-drift-log.sh. GitHub's archive has changed layout:
// it now holds one file per job (2_check.txt), where it used to hold one
// file per step (check/5_<step name>.txt).

const (
	ts        = "2026-10-27T19:40:11.0000000Z "
	stepsHead = ts + "##[group]Run actions/checkout@v4\n" + ts + "checked out\n" + ts + "##[endgroup]\n"
	driftStep = ts + "##[group]Run make gen-check\n" + ts + "make gen-check\n" + ts + "##[endgroup]\n" +
		ts + "git diff --exit-code -- internal/api/gen internal/internalapi\n" +
		ts + "diff --git a/internal/internalapi/types.gen.go b/internal/internalapi/types.gen.go\n" +
		ts + "index 1111111..2222222 100644\n" + ts + "-\tOld string\n" + ts + "+\tNew string\n" +
		ts + "##[error]Process completed with exit code 2.\n"
	cleanStep = ts + "##[group]Run make gen-check\n" + ts + "make gen-check\n" + ts + "##[endgroup]\n" +
		ts + "git diff --exit-code -- internal/api/gen internal/internalapi\n"
	otherDrift = ts + "##[group]Run make gen-check\n" + ts + "##[endgroup]\n" +
		ts + "diff --git a/internal/api/gen/types.gen.go b/internal/api/gen/types.gen.go\n" +
		ts + "##[error]Process completed with exit code 2.\n"
	laterStep = ts + "##[group]Run make check\n" + ts + "##[endgroup]\n" +
		ts + "diff --git a/internal/internalapi/types.gen.go b/internal/internalapi/types.gen.go\n" +
		ts + "##[error]Process completed with exit code 2.\n"
)

func writeZip(t *testing.T, files map[string]string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "logs.zip")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	w := zip.NewWriter(f)
	for name, body := range files {
		e, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCIDriftLog(t *testing.T) {
	for _, c := range []struct {
		name  string
		files map[string]string
		want  int
	}{
		{"job-level archive (current layout)", map[string]string{
			"2_check.txt": stepsHead + driftStep, "check/system.txt": "runner", "3_commit-lint.txt": stepsHead}, 0},
		{"step-level archive (former layout)", map[string]string{
			"check/1_Set up job.txt": stepsHead,
			"check/5_contract drift (contracts regenerated must match the committed code).txt": driftStep,
			"commit-lint/1_Set up job.txt": stepsHead}, 0},
		{"gen-check passed", map[string]string{"2_check.txt": stepsHead + cleanStep}, 1},
		{"drift in another generated file", map[string]string{"2_check.txt": stepsHead + otherDrift}, 1},
		{"types.gen.go diff only in a later step", map[string]string{"2_check.txt": stepsHead + cleanStep + laterStep}, 1},
		{"no check job log", map[string]string{"3_commit-lint.txt": stepsHead + driftStep}, 1},
		{"empty archive", map[string]string{}, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), "bash", "ci-drift-log.sh", writeZip(t, c.files))
			out, _ := cmd.CombinedOutput()
			if got := cmd.ProcessState.ExitCode(); got != c.want {
				t.Fatalf("exit %d, want %d\n%s", got, c.want, out)
			}
			if c.want == 0 && !strings.Contains(string(out), "diff --git a/internal/internalapi/types.gen.go") {
				t.Fatalf("drift not printed:\n%s", out)
			}
		})
	}
	cmd := exec.CommandContext(t.Context(), "bash", "ci-drift-log.sh", filepath.Join(t.TempDir(), "missing.zip"))
	if out, _ := cmd.CombinedOutput(); cmd.ProcessState.ExitCode() != 2 {
		t.Fatalf("unreadable archive: exit %d, want 2\n%s", cmd.ProcessState.ExitCode(), out)
	}
}
