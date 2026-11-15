package scripts

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// scripts/auth-log-checks.sh holds the pod-log checks of the internal-auth gate (#183).
// These tests run it against a fake kubectl: a check passes only after every listed pod's
// logs were read, and a collection failure is INCOMPLETE (exit 2), never PASS.

const plantedToken = "planted-sender-token-Qm9ndXNUb2tlbkZvclRlc3RzT25seQ"

// logFake is one fake cluster: the pod list (or its failure) and each pod's logs.
type logFake struct {
	pods     string            // stdout of get pods: "<pod>\t<container>=<restarts> ...\n" lines
	previous map[string]string // "<pod>/<container>" -> stdout of logs --previous; absent: unavailable
	listErr  string            // non-empty: get pods prints this to stderr and exits 1
	logs     map[string]string // pod -> stdout of logs
	logErr   map[string]string // pod -> stderr; the logs call exits 1
	logSleep map[string]bool   // pod -> the logs call hangs
	grepFail string            // a grep whose arguments contain this exits 2
}

type logRun struct {
	dir, summary  string
	code          int
	stdout, all   string
	kubectlCalled bool
}

func (f logFake) setup(t *testing.T) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	fk := filepath.Join(dir, "fake")
	for _, d := range []string{bin, fk, filepath.Join(fk, "logs"), filepath.Join(dir, "tmp")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, body string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(fk, "pods"), f.pods, 0o644)
	if f.listErr != "" {
		write(filepath.Join(fk, "list.err"), f.listErr, 0o644)
	}
	for p, l := range f.logs {
		write(filepath.Join(fk, "logs", p), l, 0o644)
	}
	for p, e := range f.logErr {
		write(filepath.Join(fk, "logs", p+".err"), e, 0o644)
	}
	for pc, l := range f.previous {
		write(filepath.Join(fk, "logs", strings.ReplaceAll(pc, "/", ".prev.")), l, 0o644)
	}
	for p := range f.logSleep {
		write(filepath.Join(fk, "logs", p+".sleep"), "", 0o644)
	}
	// The fake reads what the old (#181) and new helpers send: get pods ... and
	// logs [flags] <pod> [-c <container>] [--previous]; the pod is the argument after
	// "logs" that is no flag or flag value.
	write(filepath.Join(bin, "kubectl"), `#!/bin/sh
echo "kubectl $*" >> "$ARGV"
verb=""; pod=""; container=""; previous=false; prev=""
for a in "$@"; do
	if [ "$prev" = -c ]; then container="$a"; prev=""; continue; fi
	prev="$a"
	case "$a" in
	get|logs) [ -z "$verb" ] && verb="$a" ;;
	--previous) previous=true ;;
	-*) ;;
	*) [ "$verb" = logs ] && [ -z "$pod" ] && pod="$a" ;;
	esac
done
if [ "$verb" = logs ] && $previous; then
	f="$FAKE/logs/$pod.prev.$container"
	[ -f "$f" ] || { echo "Error from server (BadRequest): previous terminated container \"$container\" in pod \"$pod\" not found" >&2; exit 1; }
	cat "$f"; exit 0
fi
case "$verb" in
get)
	if [ -f "$FAKE/list.err" ]; then cat "$FAKE/list.err" >&2; exit 1; fi
	cat "$FAKE/pods" ;;
logs)
	if [ -f "$FAKE/logs/$pod.sleep" ]; then exec sleep 30; fi
	if [ -f "$FAKE/logs/$pod.err" ]; then cat "$FAKE/logs/$pod.err" >&2; exit 1; fi
	[ -f "$FAKE/logs/$pod" ] || { echo "Error from server (NotFound): pods \"$pod\" not found" >&2; exit 1; }
	cat "$FAKE/logs/$pod" ;;
*) exit 1 ;;
esac
`, 0o755)
	realGrep, err := exec.LookPath("grep")
	if err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(bin, "grep"), `#!/bin/sh
if [ -n "$GREP_FAIL" ]; then
	case "$*" in *"$GREP_FAIL"*) exit 2 ;; esac
fi
exec `+realGrep+` "$@"
`, 0o755)
	env := append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE="+fk,
		"ARGV="+filepath.Join(dir, "argv"), "TMPDIR="+filepath.Join(dir, "tmp"),
		"GREP_FAIL="+f.grepFail, "KUBE_CONTEXT=kind-test", "NAMESPACE=test", "AUTH_LOG_TIMEOUT=1",
		"K=kubectl --context kind-test -n test")
	return dir, env
}

// tokenFile writes a token file in dir and returns its path.
func tokenFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// argsFn returns the check name and its arguments, creating any input files in dir.
type argsFn func(t *testing.T, dir string) []string

func runLogCheck(t *testing.T, f logFake, args argsFn) logRun {
	t.Helper()
	dir, env := f.setup(t)
	summary := filepath.Join(dir, "summary.txt")
	given := args(t, dir)
	a := append([]string{"auth-log-checks.sh", given[0], "--summary", summary}, given[1:]...)
	cmd := exec.CommandContext(t.Context(), "bash", a...)
	cmd.Env = env
	var so strings.Builder
	all := &lockedBuf{}
	cmd.Stdout = &multi{&so, all}
	cmd.Stderr = all
	err := cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	_, statErr := os.Stat(filepath.Join(dir, "argv"))
	return logRun{dir: dir, summary: summary, code: code, stdout: so.String(), all: all.String(), kubectlCalled: statErr == nil}
}

// lockedBuf collects stdout and stderr, which exec copies from two goroutines.
type lockedBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

type multi struct {
	a *strings.Builder
	b *lockedBuf
}

func (m *multi) Write(p []byte) (int, error) { m.a.Write(p); return m.b.Write(p) }

func (r logRun) summaryText(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(r.summary)
	if err != nil {
		t.Fatalf("no completeness summary: %v\n%s", err, r.all)
	}
	return string(b)
}

func (r logRun) expect(t *testing.T, code int, result string, summary ...string) {
	t.Helper()
	if r.code != code {
		t.Fatalf("exit %d, want %d\n%s", r.code, code, r.all)
	}
	if !strings.HasPrefix(r.stdout, result+"  ") {
		t.Fatalf("stdout %q, want result %s", r.stdout, result)
	}
	s := r.summaryText(t)
	for _, want := range append([]string{"result=" + result + "\n"}, summary...) {
		if !strings.Contains(s, want) {
			t.Fatalf("summary lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(r.all, plantedToken) || strings.Contains(s, plantedToken) {
		t.Fatalf("a token was printed in diagnostics:\n%s", r.all)
	}
}

const (
	refusalLoopback = `{"time":"t","level":"WARN","msg":"internal auth refused","credential":"app","method":"GET","route":"/internal/ring","reason":"missing","peer":"127.0.0.1:41234"}`
	refusalCluster  = `{"time":"t","level":"WARN","msg":"internal auth refused","credential":"node","method":"GET","route":"/internal/scan","reason":"missing","peer":"10.244.1.7:52110"}`
)

func twoPods(logA, logB string) logFake {
	return logFake{pods: "storage-coordinator-abc\nstorage-shard-0-0\n",
		logs: map[string]string{"storage-coordinator-abc": logA, "storage-shard-0-0": logB}}
}

func noToken(t *testing.T, dir string) []string {
	return []string{"no-token", tokenFile(t, dir, "app", plantedToken+"\n"), tokenFile(t, dir, "node", "node-token-xyz\n")}
}

// Each failure mode, for all three checks: never PASS.
func TestAuthLogChecksFailClosed(t *testing.T) {
	refusals := func(t *testing.T, dir string) []string {
		return []string{"refusals", "--since", "2026-11-15T00:00:00Z", "--out", filepath.Join(dir, "refusals.txt")}
	}
	inCluster := func(t *testing.T, dir string) []string {
		return []string{"in-cluster-refusals", "--since", "2026-11-15T00:00:00Z", "--out", filepath.Join(dir, "in-cluster.txt")}
	}
	forbidden := "Error from server (Forbidden): pods is forbidden: User \"gate\" cannot list resource \"pods\""
	cases := []struct {
		name    string
		fake    logFake
		summary []string
	}{
		{"list denied", logFake{listErr: forbidden}, []string{"pods_listed=0\n", "logs_read=0\n", "failure=pod list: forbidden (kubectl exit 1)\n"}},
		{"log denied", func() logFake {
			f := twoPods("ok\n", "")
			f.logErr = map[string]string{"storage-shard-0-0": "Error from server (Forbidden): pods \"storage-shard-0-0\" is forbidden: cannot get resource \"pods/log\""}
			return f
		}(), []string{"pods_listed=2\n", "logs_read=1\n", "log_failures=1\n", "failure=pod storage-shard-0-0: logs forbidden (kubectl exit 1)\n"}},
		{"log timeout", func() logFake {
			f := twoPods("ok\n", "ok\n")
			f.logSleep = map[string]bool{"storage-coordinator-abc": true}
			return f
		}(), []string{"logs_read=1\n", "log_failures=1\n", "failure=pod storage-coordinator-abc: logs timed out after 1s\n"}},
		{"pod gone", logFake{pods: "storage-shard-0-0\n"}, []string{"failure=pod storage-shard-0-0: logs not found (kubectl exit 1)\n"}},
		{"empty pod list", logFake{pods: ""}, []string{"pods_listed=0\n", "failure=pod list: no pods\n"}},
		{"non-name in pod list", logFake{pods: "error: You must be logged in to the server\n"}, []string{"failure=pod list: an entry is not a pod name\n"}},
		{"duplicate pods", logFake{pods: "storage-shard-0-0\nstorage-shard-0-0\n", logs: map[string]string{"storage-shard-0-0": "ok\n"}}, []string{"failure=pod list: duplicate names\n"}},
		{"grep error", func() logFake { f := twoPods("ok\n", "ok\n"); f.grepFail = "-a -F"; return f }(), []string{"logs_read=2\n", "scan failed (grep exit 2)"}},
	}
	for _, c := range cases {
		for _, check := range []struct {
			name string
			args func(*testing.T, string) []string
		}{{"no-token", noToken}, {"refusals", refusals}, {"in-cluster-refusals", inCluster}} {
			t.Run(c.name+"/"+check.name, func(t *testing.T) {
				r := runLogCheck(t, c.fake, check.args)
				r.expect(t, 2, "INCOMPLETE", append([]string{"check=" + check.name + "\n"}, c.summary...)...)
				for _, out := range []string{"refusals.txt", "in-cluster.txt"} {
					if _, err := os.Stat(filepath.Join(r.dir, out)); err == nil {
						t.Fatalf("%s written after an incomplete collection", out)
					}
				}
			})
		}
	}
	t.Run("unexpected pod count", func(t *testing.T) {
		r := runLogCheck(t, twoPods("ok\n", "ok\n"), func(t *testing.T, dir string) []string {
			return append([]string{"refusals", "--expect-pods", "10"}, refusals(t, dir)[1:]...)
		})
		r.expect(t, 2, "INCOMPLETE", "pods_expected=10\n", "pods_listed=2\n", "failure=pod list: 2 pods, expected 10\n")
	})
	t.Run("no context", func(t *testing.T) {
		f := twoPods("ok\n", "ok\n")
		dir, env := f.setup(t)
		cmd := exec.CommandContext(t.Context(), "bash", "auth-log-checks.sh", "no-token", "--summary", filepath.Join(dir, "s"), tokenFile(t, dir, "a", "x\n"))
		cmd.Env = append(env, "KUBE_CONTEXT=")
		if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "INCOMPLETE") {
			t.Fatalf("ran without a context: %v\n%s", err, out)
		}
	})
}

// Unusable credential inputs stop the check before any kubectl call.
func TestAuthLogChecksTokenInputs(t *testing.T) {
	for _, c := range []struct {
		name, want string
		args       argsFn
	}{
		{"missing", "missing or unreadable", func(t *testing.T, dir string) []string {
			return []string{"no-token", filepath.Join(dir, "absent")}
		}},
		{"empty", ": empty", func(t *testing.T, dir string) []string { return []string{"no-token", tokenFile(t, dir, "e", "")} }},
		{"blank line", "has a blank line", func(t *testing.T, dir string) []string {
			return []string{"no-token", tokenFile(t, dir, "b", plantedToken+"\n\n")}
		}},
		{"one good one missing", "missing or unreadable", func(t *testing.T, dir string) []string {
			return []string{"no-token", tokenFile(t, dir, "g", plantedToken+"\n"), filepath.Join(dir, "absent")}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := runLogCheck(t, twoPods("ok\n", "ok\n"), c.args)
			r.expect(t, 2, "INCOMPLETE", c.want, "pods_listed=0\n", "logs_read=0\n")
			if r.kubectlCalled {
				t.Fatal("kubectl was called with unusable token inputs")
			}
		})
	}
	if os.Geteuid() != 0 {
		t.Run("unreadable", func(t *testing.T) {
			r := runLogCheck(t, twoPods("ok\n", "ok\n"), func(t *testing.T, dir string) []string {
				p := tokenFile(t, dir, "u", plantedToken+"\n")
				if err := os.Chmod(p, 0); err != nil {
					t.Fatal(err)
				}
				return []string{"no-token", p}
			})
			r.expect(t, 2, "INCOMPLETE", "missing or unreadable")
		})
	}
}

func TestAuthLogChecksResults(t *testing.T) {
	since := "2026-11-15T00:00:00Z"
	t.Run("clean logs pass", func(t *testing.T) {
		r := runLogCheck(t, twoPods("started\n", "{\"msg\":\"leader\"}\n"), noToken)
		r.expect(t, 0, "PASS", "pods_listed=2\n", "logs_read=2\n", "log_failures=0\n", "token_files=2\n", "tokens_found_in=0\n")
	})
	t.Run("planted token fails without printing it", func(t *testing.T) {
		r := runLogCheck(t, twoPods("ok\n", "x-token: "+plantedToken+" leaked\n"), noToken)
		r.expect(t, 1, "FAIL", "tokens_found_in=1\n", "failure=pod storage-shard-0-0: logs: a token from ")
	})
	t.Run("token found beats an unread pod", func(t *testing.T) {
		f := twoPods(plantedToken+"\n", "")
		f.logErr = map[string]string{"storage-shard-0-0": "Forbidden"}
		r := runLogCheck(t, f, noToken)
		r.expect(t, 1, "FAIL", "tokens_found_in=1\n", "log_failures=1\n")
	})
	t.Run("refusals collected from every pod", func(t *testing.T) {
		r := runLogCheck(t, twoPods(refusalLoopback+"\nother\n", "noise\n"+refusalCluster+"\n"), func(t *testing.T, dir string) []string {
			return []string{"refusals", "--since", since, "--out", filepath.Join(dir, "r.txt")}
		})
		r.expect(t, 0, "PASS", "logs_read=2\n")
		got, err := os.ReadFile(filepath.Join(r.dir, "r.txt"))
		if err != nil {
			t.Fatal(err)
		}
		want := "storage-coordinator-abc " + refusalLoopback + "\nstorage-shard-0-0 " + refusalCluster + "\n"
		if string(got) != want {
			t.Fatalf("refusals\n%s\nwant\n%s", got, want)
		}
		argv, _ := os.ReadFile(filepath.Join(r.dir, "argv"))
		for _, w := range []string{"--since-time=" + since, "-l app.kubernetes.io/component in (coordinator,storagenode)", "--context kind-test -n test"} {
			if !strings.Contains(string(argv), w) {
				t.Fatalf("kubectl calls lack %q:\n%s", w, argv)
			}
		}
	})
	t.Run("no refusals is a complete empty result", func(t *testing.T) {
		r := runLogCheck(t, twoPods("ok\n", "ok\n"), func(t *testing.T, dir string) []string {
			return []string{"refusals", "--since", since, "--out", filepath.Join(dir, "r.txt")}
		})
		r.expect(t, 0, "PASS", "logs_read=2\n")
		if b, err := os.ReadFile(filepath.Join(r.dir, "r.txt")); err != nil || len(b) != 0 {
			t.Fatalf("want an empty refusal file, got %q, %v", b, err)
		}
	})
	inCluster := func(t *testing.T, dir string) []string {
		return []string{"in-cluster-refusals", "--since", since, "--out", filepath.Join(dir, "ic.txt")}
	}
	t.Run("loopback refusals only pass", func(t *testing.T) {
		r := runLogCheck(t, twoPods(refusalLoopback+"\n", "ok\n"), inCluster)
		r.expect(t, 0, "PASS", "logs_read=2\n")
	})
	t.Run("whole current logs without --since", func(t *testing.T) {
		r := runLogCheck(t, twoPods(refusalLoopback+"\n", "ok\n"), func(*testing.T, string) []string {
			return []string{"in-cluster-refusals", "--expect-pods", "2"}
		})
		r.expect(t, 0, "PASS", "since=(all)\n", "pods_expected=2\n", "logs_read=2\n")
		if argv, _ := os.ReadFile(filepath.Join(r.dir, "argv")); strings.Contains(string(argv), "--since-time") {
			t.Fatalf("--since-time passed without --since:\n%s", argv)
		}
	})
	t.Run("in-cluster refusal fails", func(t *testing.T) {
		r := runLogCheck(t, twoPods(refusalLoopback+"\n", refusalCluster+"\n"), inCluster)
		r.expect(t, 1, "FAIL", "logs_read=2\n")
		b, _ := os.ReadFile(filepath.Join(r.dir, "ic.txt"))
		if string(b) != "storage-shard-0-0 "+refusalCluster+"\n" {
			t.Fatalf("in-cluster refusals %q", b)
		}
	})
}

// A container that restarted has its previous instance's log read too (--previous).
func TestAuthLogChecksRestartedContainers(t *testing.T) {
	restartedPods := func(prev map[string]string) logFake {
		return logFake{
			pods: "storage-coordinator-abc\tcoordinator=0 \nstorage-shard-0-0\tinit-perms=0 storagenode=2 \n",
			logs: map[string]string{"storage-coordinator-abc": "ok\n", "storage-shard-0-0": "ok\n"}, previous: prev,
		}
	}
	t.Run("restarted pod clean", func(t *testing.T) {
		r := runLogCheck(t, restartedPods(map[string]string{"storage-shard-0-0/storagenode": "before the restart\n"}), noToken)
		r.expect(t, 0, "PASS", "logs_read=2\n", "containers_restarted=1\n", "previous_logs_read=1\n", "previous_logs_failed=0\n")
		argv, _ := os.ReadFile(filepath.Join(r.dir, "argv"))
		if !strings.Contains(string(argv), "logs storage-shard-0-0 -c storagenode --previous") {
			t.Fatalf("no previous-log read of the restarted container:\n%s", argv)
		}
		if strings.Contains(string(argv), "-c coordinator") || strings.Contains(string(argv), "-c init-perms") {
			t.Fatalf("previous logs read for a container that never restarted:\n%s", argv)
		}
	})
	t.Run("planted token in the previous log fails", func(t *testing.T) {
		r := runLogCheck(t, restartedPods(map[string]string{"storage-shard-0-0/storagenode": "leak " + plantedToken + "\n"}), noToken)
		r.expect(t, 1, "FAIL", "tokens_found_in=1\n", "previous_logs_read=1\n",
			"failure=pod storage-shard-0-0 container storagenode: previous logs: a token from ")
	})
	for _, check := range []struct {
		name string
		args argsFn
	}{{"no-token", noToken}, {"refusals", func(t *testing.T, dir string) []string {
		return []string{"refusals", "--since", "2026-11-15T00:00:00Z", "--out", filepath.Join(dir, "refusals.txt")}
	}}, {"in-cluster-refusals", func(t *testing.T, dir string) []string {
		return []string{"in-cluster-refusals", "--out", filepath.Join(dir, "in-cluster.txt")}
	}}} {
		t.Run("previous log unavailable/"+check.name, func(t *testing.T) {
			r := runLogCheck(t, restartedPods(nil), check.args)
			r.expect(t, 2, "INCOMPLETE", "logs_read=2\n", "containers_restarted=1\n", "previous_logs_read=0\n", "previous_logs_failed=1\n",
				"failure=pod storage-shard-0-0 container storagenode: previous logs not found (kubectl exit 1)\n")
			for _, out := range []string{"refusals.txt", "in-cluster.txt"} {
				if _, err := os.Stat(filepath.Join(r.dir, out)); err == nil {
					t.Fatalf("%s written after an incomplete collection", out)
				}
			}
		})
	}
	t.Run("previous refusals are collected", func(t *testing.T) {
		r := runLogCheck(t, restartedPods(map[string]string{"storage-shard-0-0/storagenode": refusalCluster + "\n"}),
			func(t *testing.T, dir string) []string {
				return []string{"in-cluster-refusals", "--out", filepath.Join(dir, "ic.txt")}
			})
		r.expect(t, 1, "FAIL", "previous_logs_read=1\n")
		b, _ := os.ReadFile(filepath.Join(r.dir, "ic.txt"))
		if string(b) != "storage-shard-0-0/storagenode:previous "+refusalCluster+"\n" {
			t.Fatalf("in-cluster refusals %q", b)
		}
	})
	t.Run("unreadable container status", func(t *testing.T) {
		r := runLogCheck(t, logFake{pods: "storage-shard-0-0\tstoragenode=<nil> \n"}, noToken)
		r.expect(t, 2, "INCOMPLETE", "failure=pod list: pod storage-shard-0-0 has an unreadable container status\n")
	})
}

// Parth's #183 reproducer: #181's no_token_in_logs, run unchanged with its quiet_grep,
// accepts unreadable logs as token-free; the new check reports them INCOMPLETE.
func TestAuthLogChecksIssue183Reproducer(t *testing.T) {
	f := logFake{pods: "pod-one\n", logErr: map[string]string{"pod-one": "Error from server (Forbidden): pods \"pod-one\" is forbidden"}}
	dir, env := f.setup(t)
	tok := tokenFile(t, dir, "token", "dummy-token-value\n")

	old := exec.CommandContext(t.Context(), "bash", "-c", `set -euo pipefail; . "$1"; no_token_in_logs "$2"; echo "no_token_in_logs returned $?"`,
		"old", "testdata/auth-log-checks/old-181-helper.sh", tok)
	old.Env = env
	out, err := old.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "no_token_in_logs returned 0") {
		t.Fatalf("the #181 helper no longer reproduces #183 (want return 0 on a denied log read): %v\n%s", err, out)
	}

	r := runLogCheck(t, f, func(*testing.T, string) []string { return []string{"no-token", tok} })
	r.expect(t, 2, "INCOMPLETE", "pods_listed=1\n", "logs_read=0\n", "log_failures=1\n", "failure=pod pod-one: logs forbidden (kubectl exit 1)\n")
}

// The vendored #181 helper is the function at #181's head, unchanged.
func TestAuthLogChecksOldHelperIsVerbatim(t *testing.T) {
	const ref = "68265a2dfeeb8183e207f4e1061fc7d0f69e2e26:docs/evidence/S-29-auth-enable/official-20261114/stages/lib.sh"
	src, err := exec.CommandContext(t.Context(), "git", "show", ref).Output()
	if err != nil {
		t.Skipf("#181 head not in this clone (%v); the fixture records its source", err)
	}
	fixture, err := os.ReadFile("testdata/auth-log-checks/old-181-helper.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(fixture), "\n") {
		if line == "" || (strings.HasPrefix(line, "# ") && !strings.HasPrefix(line, "# no_token_in_logs")) {
			continue
		}
		if !strings.Contains(string(src), line+"\n") {
			t.Fatalf("fixture line not in #181's lib.sh: %q", line)
		}
	}
}
