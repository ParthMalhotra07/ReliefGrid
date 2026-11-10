package scripts

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// loadtest/run.sh confines the kind nodes to KIND_CPUS and sets the power
// profile for a run; its EXIT trap, restore(), must put both back whatever
// ended the run. This runs restore() alone, under set -e with an ERR trap,
// with fake docker and powerprofilesctl, after the sampler has already
// exited (kill fails): the case that once left the laptop in performance mode.
func TestLoadRunRestoreAlwaysCompletes(t *testing.T) {
	script := `set -euo pipefail
eval "$(sed -n '/^restore() {/,/^}/p' ../loadtest/run.sh)"
log() { echo "log: $*"; }
exec 3>&1
docker() { echo "docker $*" >&3; return 1; }   # fd 3: restore() discards stdout; a failing docker must not stop it
powerprofilesctl() { if [ "$1" = get ]; then echo performance; else echo "powerprofilesctl $*"; fi; }
nproc() { echo 8; }
NODES=(n1 n2); declare -A CPUSET_BEFORE=([n1]="" [n2]="2-3"); POWER_BEFORE=balanced
sleep 0 & sampler_pid=$!; wait "$sampler_pid"   # already exited
trap 'echo "ERR trap fired"' ERR
safety_failure=false
restore
echo "safety_failure=$safety_failure"
echo "restore returned"
`
	out, err := exec.CommandContext(t.Context(), "bash", "-c", script).CombinedOutput()
	got := string(out)
	if err != nil {
		t.Fatalf("restore() ended the shell: %v\n%s", err, got)
	}
	for _, want := range []string{
		"docker update --cpuset-cpus 0-7 n1", // empty cpuset = every CPU, spelled out
		"docker update --cpuset-cpus 2-3 n2",
		"powerprofilesctl set balanced",
		"restore returned",
		"safety_failure=true",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "ERR trap fired") {
		t.Errorf("restore() tripped the ERR trap:\n%s", got)
	}
}

func TestLoadRunOutcomeSidecar(t *testing.T) {
	for _, c := range []struct {
		name               string
		runner, k6, report string
		safety             bool
		want               int
	}{
		{"runner failure", "1", "null", "null", false, 1},
		{"threshold crossing", "0", "99", "0", false, 0},
		{"inconclusive", "3", "99", "3", false, 3},
		{"safety", "0", "0", "0", true, 4},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "run")
			script := fmt.Sprintf(`set -euo pipefail
exec 3>&1 4>&2
eval "$(sed -n '/^finish() {/,/^}/p' ../loadtest/run.sh)"
DIR="$1"; k6_exit=%s; report_exit=%s; safety_failure=%t; log_pid=123
exec {status_fd}>"$DIR.runner-status.json"
wait() { return 0; }
trap finish EXIT
exit %s
`, c.k6, c.report, c.safety, c.runner)
			cmd := exec.CommandContext(t.Context(), "bash", "-c", script, "test", dir)
			out, err := cmd.CombinedOutput()
			if err == nil && c.want != 0 {
				t.Fatalf("expected failure: %s", out)
			}
			if err != nil {
				var ee *exec.ExitError
				if !errors.As(err, &ee) || ee.ExitCode() != c.want {
					t.Fatalf("%v: %s", err, out)
				}
			}
			b, err := os.ReadFile(dir + ".runner-status.json")
			if err != nil {
				t.Fatal(err)
			}
			var status map[string]any
			if err := json.Unmarshal(b, &status); err != nil {
				t.Fatal(err)
			}
			if status["runnerExit"] != float64(c.want) || status["safetyFailure"] != c.safety {
				t.Fatalf("%s", b)
			}
			if c.k6 == "null" && status["k6Exit"] != nil {
				t.Fatalf("%s", b)
			}
			if c.k6 == "99" && status["k6Exit"] != float64(99) {
				t.Fatalf("%s", b)
			}
		})
	}
}

func TestLoadRunPreflightPreservesDiagnostics(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "explicit run")
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", "../loadtest/run.sh", "smoke")
	cmd.Env = append(os.Environ(), "RUN_DIR="+dir, "K6_VERSION=v2.3.0", "PATH="+bin+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("preflight unexpectedly passed: %s", out)
	}
	for _, file := range []string{dir + ".runner-status.json", dir + ".runner.log"} {
		if _, err := os.Stat(file); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(dir + ".runner-status.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"k6Exit":null`) || !strings.Contains(string(b), `"runnerExit":1`) {
		t.Fatalf("%s", b)
	}
	if !strings.Contains(string(out), "RUN_DIR="+dir) {
		t.Fatalf("%s", out)
	}
}

func TestLoadClusterRejectsCustomNameBeforeMutation(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "bash", "../loadtest/cluster.sh")
	cmd.Env = append(os.Environ(), "CLUSTER=not-the-fixed-config-name")
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 2 || !strings.Contains(string(out), "only supports") {
		t.Fatalf("%v: %s", err, out)
	}
}

func TestLoadRunRejectsExistingDirectory(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "existing run")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(dir, "sentinel")
	if err := os.WriteFile(sentinel, []byte("preserve me"), 0o640); err != nil {
		t.Fatal(err)
	}
	sidecar := dir + ".runner.log"
	if err := os.WriteFile(sidecar, []byte("old diagnostics"), 0o640); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	called := filepath.Join(root, "unexpected-command")
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte("#!/bin/sh\ntouch \"$CALLED\"\nexit 97\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", "../loadtest/run.sh", "smoke")
	cmd.Env = append(os.Environ(), "RUN_DIR="+dir, "PATH="+bin+":"+os.Getenv("PATH"), "CALLED="+called)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 2 || !strings.Contains(string(out), "already exists") {
		t.Fatalf("%v: %s", err, out)
	}
	for name, want := range map[string]string{sentinel: "preserve me", sidecar: "old diagnostics"} {
		b, err := os.ReadFile(name)
		if err != nil || string(b) != want {
			t.Fatalf("changed %s: %q %v", name, b, err)
		}
	}
	for _, name := range []string{called, dir + ".runner-status.json"} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("unexpected file %s: %v", name, err)
		}
	}
}

func TestLoadClusterPreservesA15(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("deployFailure=", fail), func(t *testing.T) {
			root := t.TempDir()
			evidence := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, "loadtest"), 0o755); err != nil {
				t.Fatal(err)
			}
			script, err := os.ReadFile("../loadtest/cluster.sh")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "loadtest/cluster.sh"), script, 0o755); err != nil {
				t.Fatal(err)
			}
			a15 := filepath.Join(root, "docs/evidence/A1.5")
			if err := os.MkdirAll(a15, 0o755); err != nil {
				t.Fatal(err)
			}
			for name, content := range map[string]string{"tracked.txt": "tracked evidence", "untracked.txt": "untracked evidence", "secret.ignored": "ignored evidence"} {
				if err := os.WriteFile(filepath.Join(a15, name), []byte(content), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.ignored\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			git := func(args ...string) string {
				t.Helper()
				cmd := exec.CommandContext(t.Context(), "git", args...)
				cmd.Dir = root
				b, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v %s", args, err, b)
				}
				return string(b)
			}
			git("init", "-q")
			git("add", ".gitignore", "docs/evidence/A1.5/tracked.txt")
			before := git("status", "--porcelain", "--ignored", "--untracked-files=all")
			bin := t.TempDir()
			mocks := map[string]string{
				"kind": "exit 0\n",
				"helm": "exit 0\n",
				"make": `printf '%s\n' "$A15_EVIDENCE_DIR" > "$A15_RECORD"
case " $* " in
 *" app-deploy "*) mkdir -p "$A15_EVIDENCE_DIR"; echo diagnostic > "$A15_EVIDENCE_DIR/deploy.txt"; if [ "$FAIL_DEPLOY" = true ]; then exit 1; fi ;;
esac
exit 0
`,
				"kubectl": `case " $* " in
 *" get pods "*) echo '{"items":[{"status":{"qosClass":"Guaranteed"}}]}' ;;
 *" wait "*) exit 0 ;;
 *) exit 97 ;;
esac
`,
			}
			for name, body := range mocks {
				if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			record := filepath.Join(evidence, "a15-path")
			cmd := exec.CommandContext(t.Context(), "bash", filepath.Join(root, "loadtest/cluster.sh"))
			cmd.Env = append(os.Environ(), "CLUSTER=reliefgrid", "PATH="+bin+":"+os.Getenv("PATH"), "TMPDIR="+evidence, "A15_EVIDENCE_DIR=", "A15_RECORD="+record, "FAIL_DEPLOY="+fmt.Sprint(fail))
			out, err := cmd.CombinedOutput()
			if fail {
				var ee *exec.ExitError
				if !errors.As(err, &ee) || ee.ExitCode() != 1 {
					t.Fatalf("%v: %s", err, out)
				}
			} else if err != nil {
				t.Fatalf("%v: %s", err, out)
			}
			for name, want := range map[string]string{"tracked.txt": "tracked evidence", "untracked.txt": "untracked evidence", "secret.ignored": "ignored evidence"} {
				path := filepath.Join(a15, name)
				b, err := os.ReadFile(path)
				if err != nil || string(b) != want {
					t.Fatalf("changed %s: %q %v", path, b, err)
				}
				info, err := os.Stat(path)
				if err != nil || info.Mode().Perm() != 0o640 {
					t.Fatalf("changed mode %s", path)
				}
			}
			if after := git("status", "--porcelain", "--ignored", "--untracked-files=all"); after != before {
				t.Fatalf("git state changed:\nbefore %s\nafter %s", before, after)
			}
			entries, err := os.ReadDir(a15)
			if err != nil || len(entries) != 3 {
				t.Fatalf("unexpected A1.5 files: %v %v", entries, err)
			}
			b, err := os.ReadFile(record)
			if err != nil {
				t.Fatal(err)
			}
			output := strings.TrimSpace(string(b))
			if !strings.HasPrefix(output, evidence+string(os.PathSeparator)) {
				t.Fatalf("evidence not redirected: %s", output)
			}
			if _, err := os.Stat(filepath.Join(output, "deploy.txt")); err != nil {
				t.Fatalf("diagnostics not preserved: %v", err)
			}
		})
	}
}

func TestLoadSupervisorOffline(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "python3", "-I", "load-supervision-test.py")
	cmd.Env = append(os.Environ(), "RG_TEST_SYSTEMD=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

func TestLoadPilotEntrypointOffline(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "python3", "-I", "load-pilot-entry-test.py")
	cmd.Env = append(os.Environ(), "RG_TEST_SYSTEMD=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}
