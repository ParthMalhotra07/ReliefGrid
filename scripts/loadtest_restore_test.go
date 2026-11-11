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

// runFuncs evaluates the named functions of loadtest/run.sh, then script,
// under set -euo pipefail, and returns the combined output.
func runFuncs(t *testing.T, funcs []string, script string, env ...string) (string, error) {
	t.Helper()
	var pick strings.Builder
	for _, f := range funcs {
		fmt.Fprintf(&pick, "/^%s() {/,/^}/p;", f)
	}
	full := "set -euo pipefail\neval \"$(sed -n '" + pick.String() + "' ../loadtest/run.sh)\"\n" + script
	cmd := exec.CommandContext(t.Context(), "bash", "-c", full)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// The audit stops retry admission (the matcher) as demo 5 does; restore()
// must put the matcher back on every exit, and a matcher it cannot put back
// is a safety failure, like a cpuset or power profile left changed.
func TestLoadRunRestoreResumesMatcher(t *testing.T) {
	for _, c := range []struct {
		name, replicas, scaleOK string
		wantSafety              string
		want, notWant           []string
	}{
		{"resumed", "2", "1", "false", []string{"kn scale deploy/r-matcher --replicas=2", "forward gone"}, nil},
		{"cannot resume", "2", "0", "true", []string{"kn scale deploy/r-matcher --replicas=2", "could not restore the matcher"}, nil},
		{"never read", "", "1", "false", []string{"forward gone"}, []string{"kn "}},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, err := runFuncs(t, []string{"restore", "resume_matcher", "stop_forwards"}, `
log() { echo "log: $*"; }
exec 3>&1
docker() { if [ "$1" = inspect ]; then echo 0-5; fi; }
powerprofilesctl() { echo balanced; }
REPLICAS=0
kn() {
	echo "kn $*" >&3
	case "$*" in
	"get deploy/r-matcher -o jsonpath={.spec.replicas}") echo "$REPLICAS" ;;
	"scale deploy/r-matcher --replicas=2") [ "$SCALE_OK" = 1 ] || return 1; REPLICAS=2 ;;
	"rollout status deploy/r-matcher --timeout=180s") [ "$REPLICAS" = 2 ] ;;
	"get deploy/r-matcher -o jsonpath={.status.readyReplicas}") echo "$REPLICAS" ;;
	*) return 1 ;;
	esac
}
MATCHER=deploy/r-matcher MATCHER_REPLICAS="$WANT"
NODES=(n1); declare -A CPUSET_BEFORE=([n1]=0-5); POWER_BEFORE=balanced; sampler_pid=""
sleep 30 >/dev/null 2>&1 & BG_PFS=("$!")
trap 'echo "ERR trap fired"' ERR
safety_failure=false
restore
kill -0 "${BG_PFS[0]:-$!}" 2>/dev/null && echo "forward alive" || echo "forward gone"
echo "safety_failure=$safety_failure"
`, "WANT="+c.replicas, "SCALE_OK="+c.scaleOK)
			if err != nil {
				t.Fatalf("restore() ended the shell: %v\n%s", err, out)
			}
			for _, w := range append(c.want, "safety_failure="+c.wantSafety) {
				if !strings.Contains(out, w) {
					t.Errorf("missing %q in:\n%s", w, out)
				}
			}
			for _, w := range append(c.notWant, "ERR trap fired") {
				if strings.Contains(out, w) {
					t.Errorf("unexpected %q in:\n%s", w, out)
				}
			}
		})
	}
}

func TestLoadRunPauseMatcher(t *testing.T) {
	for _, c := range []struct {
		name, pods string
		ok         bool
	}{{"stopped", "", true}, {"pods outlive the scale", "pod/r-matcher-x", false}} {
		t.Run(c.name, func(t *testing.T) {
			out, err := runFuncs(t, []string{"pause_matcher"}, `
exec 3>&2 # pause_matcher discards kubectl's output; record calls on fd 3
kn() {
	echo "kn $*" >&3
	case "$*" in
	"get pods -l "*" -o name") printf '%s' "$PODS" ;;
	esac
}
MATCHER=deploy/r-matcher MATCHER_SELECTOR=app.kubernetes.io/instance=r,app.kubernetes.io/component=matcher
pause_matcher && echo paused || echo "not paused"
`, "PODS="+c.pods)
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			if !strings.Contains(out, "kn scale deploy/r-matcher --replicas=0") ||
				!strings.Contains(out, "kn wait --for=delete pod -l app.kubernetes.io/instance=r,app.kubernetes.io/component=matcher") {
				t.Errorf("did not scale to zero and wait:\n%s", out)
			}
			if got := strings.Contains(out, "\npaused"); got != c.ok {
				t.Errorf("paused=%v, want %v:\n%s", got, c.ok, out)
			}
		})
	}
}

// The drain reuses the demo's relay-only drainer with the broker's
// credentials in its environment only: never on a command line, and
// redacted (raw and URL-encoded) from the evidence it leaves.
func TestLoadRunDrainHandoffs(t *testing.T) {
	for _, c := range []struct {
		name string
		exit int
	}{{"drained", 0}, {"failed", 1}} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "drain-outbox")
			if err := os.WriteFile(bin, []byte(`#!/bin/sh
printf '%s\n%s\n' "$COORDINATOR_URL" "$AMQP_URL" > "$SEEN"
echo "dial $AMQP_URL: refused (p@ss:w/rd)" >&2
echo '{"Read":3,"Published":3,"Marked":3}'
exit $DRAIN_EXIT
`), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(dir, "run", "raw"), 0o750); err != nil {
				t.Fatal(err)
			}
			out, err := runFuncs(t, []string{"drain_handoffs"}, `
kn() {
	echo "$*" >> "$ARGV"
	case "$*" in
	"get secret r-rabbitmq -o jsonpath={.data.username}") printf rg-user | base64 ;;
	"get secret r-rabbitmq -o jsonpath={.data.password}") printf 'p@ss:w/rd' | base64 ;;
	*) return 1 ;;
	esac
}
RELEASE=r DIR="$RUN" DRAIN_BIN="$BIN"
drain_handoffs http://127.0.0.1:5801 127.0.0.1:5999 && echo "drain ok" || echo "drain failed"
`, "RUN="+filepath.Join(dir, "run"), "BIN="+bin, "SEEN="+filepath.Join(dir, "seen"), "ARGV="+filepath.Join(dir, "argv"),
				fmt.Sprintf("DRAIN_EXIT=%d", c.exit))
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			if want := map[bool]string{true: "drain ok", false: "drain failed"}[c.exit == 0]; !strings.Contains(out, want) {
				t.Fatalf("want %q:\n%s", want, out)
			}
			seen, _ := os.ReadFile(filepath.Join(dir, "seen"))
			if string(seen) != "http://127.0.0.1:5801\namqp://rg-user:p%40ss%3Aw%2Frd@127.0.0.1:5999/\n" {
				t.Errorf("drainer environment:\n%s", seen)
			}
			logb, err := os.ReadFile(filepath.Join(dir, "run", "raw", "handoff-drain.log"))
			if err != nil {
				t.Fatal(err)
			}
			argv, _ := os.ReadFile(filepath.Join(dir, "argv"))
			for name, b := range map[string][]byte{"log": logb, "kubectl argv": argv, "output": []byte(out)} {
				if strings.Contains(string(b), "p@ss:w/rd") || strings.Contains(string(b), "p%40ss%3Aw%2Frd") {
					t.Errorf("password in %s:\n%s", name, b)
				}
			}
			if !strings.Contains(string(logb), "[redacted]") {
				t.Errorf("log not redacted:\n%s", logb)
			}
			got, err := os.ReadFile(filepath.Join(dir, "run", "raw", "handoff-drain.json"))
			if c.exit == 0 && (err != nil || strings.TrimSpace(string(got)) != `{"read":3,"published":3,"marked":3}`) {
				t.Errorf("drain report %q (%v)", got, err)
			}
			if leftovers, _ := filepath.Glob(filepath.Join(dir, "run", "raw", "handoff-drain.*.tmp")); len(leftovers) > 0 {
				t.Errorf("unredacted temporary files left: %v", leftovers)
			}
		})
	}
}

// Fix for the pilot: every stage had the supervisor's 1800 s default, which a
// 1800 s soak can never meet. A run stage gets its k6 duration plus 900 s.
func TestLoadStageTimeout(t *testing.T) {
	script := `set -euo pipefail
eval "$(sed -n '/^stage_timeout() {/,/^}/p' ../loadtest/pilot-supervision.sh)"
for d in 60 960 600 1800; do printf '%s ' "$(stage_timeout "$d")"; done
stage_timeout x && echo accepted || echo refused
`
	out, err := exec.CommandContext(t.Context(), "bash", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(string(out), "960 1860 1500 2700 ") || !strings.Contains(string(out), "refused") {
		t.Fatalf("%s", out)
	}
}

// supervise() hands the stage's deadline to the supervisor, and refuses a
// stage without one rather than fall back to the 1800 s default.
func TestLoadSuperviseStageDeadline(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "loadtest"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "loadtest", "supervise.py"), []byte(`import json, pathlib, sys
ev = pathlib.Path(sys.argv[sys.argv.index("--evidence-dir") + 1]); ev.mkdir(parents=True)
(ev / "argv.json").write_text(json.dumps(sys.argv[1:]))
(ev / "result.json").write_text(json.dumps({"exitConfirmed": True, "supervisorExit": 0}))
`), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := filepath.Abs("../loadtest/pilot-supervision.sh")
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	script := `set -euo pipefail
ROOT="$1" OUT="$2"
source "$3"
supervise smoke "$(stage_timeout 60)" /bin/true
echo "service_rc=$service_rc"
if supervise step /bin/true; then echo "no deadline accepted"; else echo "no deadline refused"; fi
`
	b, err := exec.CommandContext(t.Context(), "bash", "-c", script, "test", root, out, src).CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	if !strings.Contains(string(b), "service_rc=0") || !strings.Contains(string(b), "no deadline refused") {
		t.Fatalf("%s", b)
	}
	argv, err := os.ReadFile(filepath.Join(out, "smoke.supervisor", "argv.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(argv), `"--timeout-seconds", "960", "--", "/bin/true"`) {
		t.Fatalf("supervisor argv %s", argv)
	}
	if _, err := os.Stat(filepath.Join(out, "step.supervisor")); err == nil {
		t.Fatal("a stage without a deadline was launched")
	}
}

func TestLoadBackgroundSampler(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "python3", "-I", "load-background-test.py").CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}
