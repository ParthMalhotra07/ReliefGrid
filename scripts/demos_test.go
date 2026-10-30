package scripts

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The demo gate (S3.2): scripts/demos/lib.sh gives every demo the same
// result vocabulary and evidence file, and scripts/demos/gate.sh runs all
// nine from a clean deployment twice. These tests run the scripts with fake
// demos and a fake reset; none of them touches a cluster.

// Exit codes of a demo, as lib.sh defines them.
const (
	exitPass           = 0
	exitFail           = 1
	exitNotImplemented = 2
	exitPending        = 3
)

// runScript runs a bash script with env added to the environment and
// returns its combined output and exit code.
func runScript(t *testing.T, stdin string, env []string, script string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "bash", append([]string{script}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("run %s: %v\n%s", script, err, out)
		}
		code = ee.ExitCode()
	}
	return string(out), code
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("evidence: %v", err)
	}
	return string(b)
}

func libPath(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs("demos/lib.sh")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// fakeDemo writes a demo script that sources lib.sh and runs body.
func fakeDemo(t *testing.T, dir string, n int, body string) string {
	t.Helper()
	p := filepath.Join(dir, fmt.Sprintf("demo-%d.sh", n))
	src := fmt.Sprintf("#!/usr/bin/env bash\n. %q\ndemo_begin %d \"fake demo %d\"\n%s\n", libPath(t), n, n, body)
	if err := os.WriteFile(p, []byte(src), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// Every demo ends with one explicit result, written with its evidence.
func TestDemoLibResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		code   int
		result string   // the evidence's result line
		has    []string // more evidence lines
	}{
		{
			name: "pass",
			body: `demo_reset "fresh keys under seed 7"
run "say hello" echo hello
check "true holds" true
pass "by hand"
demo_end`,
			code: exitPass, result: "result:     PASS (2/2 checks)",
			has: []string{"seed/reset: fresh keys under seed 7", "PASS  true holds", "PASS  by hand",
				"$ echo hello", "hello", "[exit 0"},
		},
		{
			name: "a failed check",
			body: `demo_reset "none"
check "true holds" true
fail "false holds" "line one
line two"
demo_end`,
			code: exitFail, result: "result:     FAIL (1 of 2 checks failed)",
			has: []string{"FAIL  false holds", "      line one", "      line two"},
		},
		{
			name: "a failed command is recorded, not fatal",
			body: `demo_reset "none"
run "fails" sh -c 'echo boom; exit 5' || fail "the command worked"
demo_end`,
			code: exitFail, result: "result:     FAIL (1 of 1 checks failed)",
			has: []string{"$ sh -c echo boom; exit 5", "boom", "[exit 5"},
		},
		{
			name: "no checks is not a pass",
			body: `demo_reset "none"
demo_end`,
			code: exitFail, result: "result:     FAIL (no checks ran)",
		},
		{
			name: "not implemented",
			body: `demo_not_implemented "owner A, goal A3.1"
echo unreachable >&2
exit 0`,
			code: exitNotImplemented, result: "result:     NOT IMPLEMENTED (owner A, goal A3.1)",
		},
		{
			name: "pending",
			body: `demo_reset "none"
check "contract loaded" true
demo_pending "the public API (A1.1) is not deployed"`,
			code: exitPending, result: "result:     PENDING (the public API (A1.1) is not deployed)",
			has: []string{"PASS  contract loaded"},
		},
		{
			name: "dying before demo_end is a failure",
			body: `demo_reset "none"
check "true holds" true
exit 7`,
			code: exitFail, result: "result:     FAIL (exited with status 7 before demo_end)",
			has: []string{"PASS  true holds"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			script := fakeDemo(t, t.TempDir(), 4, tc.body)
			out, code := runScript(t, "", []string{"EVIDENCE_ROOT=" + root, "DEMO_RUN_ID=run-1", "KUBE_CONTEXT=no-such-context"}, script)
			if code != tc.code {
				t.Fatalf("exit %d, want %d\n%s", code, tc.code, out)
			}
			if strings.Contains(out, "unreachable") {
				t.Fatalf("the demo kept running after its result\n%s", out)
			}
			ev := readFile(t, filepath.Join(root, "run-1", "demo-4.txt"))
			for _, want := range append([]string{
				"# demo-4: fake demo 4", tc.result, "runId:      run-1",
				"started:    ", "finished:   ", "duration:   ", "code tree:  ", "code dirty: ", "## versions", "go: ",
			}, tc.has...) {
				if !strings.Contains(ev, want) {
					t.Errorf("evidence lacks %q\n%s", want, ev)
				}
			}
			if !regexp.MustCompile(`(?m)^code tree:  [0-9a-f]{12} `).MatchString(ev) {
				t.Errorf("code tree is not a 12-digit fingerprint\n%s", ev)
			}
			if !strings.Contains(out, strings.TrimSpace(strings.TrimPrefix(tc.result, "result:"))) {
				t.Errorf("the result is not printed\n%s", out)
			}
		})
	}
}

// A run ID defaults to one per run, so two runs never overwrite each other.
func TestDemoRunIDDefault(t *testing.T) {
	root := t.TempDir()
	script := fakeDemo(t, t.TempDir(), 2, "demo_not_implemented x")
	if _, code := runScript(t, "", []string{"EVIDENCE_ROOT=" + root, "DEMO_RUN_ID=", "KUBE_CONTEXT=no-such-context"}, script); code != exitNotImplemented {
		t.Fatalf("exit %d", code)
	}
	ev, _ := filepath.Glob(filepath.Join(root, "*", "demo-2.txt"))
	if len(ev) != 1 || !regexp.MustCompile(`/\d{8}T\d{6}Z-[^/]+/demo-2\.txt$`).MatchString(ev[0]) {
		t.Fatalf("evidence at %v, want <UTC timestamp>-<host>/demo-2.txt", ev)
	}
}

// gateFixture is a demos dir of fake demos with the given exit codes (a
// missing entry has no script) and a reset that logs each call.
func gateFixture(t *testing.T, codes map[int]int, resetCode string) (dir, calls string) {
	t.Helper()
	dir = t.TempDir()
	calls = filepath.Join(dir, "calls")
	for n, code := range codes {
		body := fmt.Sprintf("#!/usr/bin/env bash\necho \"demo-%d $DEMO_RUN_ID\" >>%q\nexit %d\n", n, calls, code)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("demo-%d.sh", n)), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	reset := fmt.Sprintf("#!/usr/bin/env bash\necho \"reset RESET_CONFIRM=$RESET_CONFIRM\" >>%q\nexit %s\n", calls, resetCode)
	if err := os.WriteFile(filepath.Join(dir, "reset.sh"), []byte(reset), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir, calls
}

func allNine(code int) map[int]int {
	m := map[int]int{}
	for n := 1; n <= 9; n++ {
		m[n] = code
	}
	return m
}

// fakeKubectl puts a kubectl on PATH that logs its arguments to <dir>/kubectl.log and answers
// the gate's node queries from files: <dir>/nodes.json for "get nodes -o json" (default: no
// node cordoned) and <dir>/labelled for "get nodes -l <demo cordon label> -o name". Every
// "uncordon <node>" also clears that node from both files, as the real one would.
func fakeKubectl(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	src := `#!/usr/bin/env bash
d=` + fmt.Sprintf("%q", dir) + `
echo "$*" >>"$d/kubectl.log"
args=" $* "
case "$args" in
*" uncordon "*)
	n="${args##* uncordon }"; n="${n%% *}"
	[ -f "$d/labelled" ] && grep -vx "node/$n" "$d/labelled" >"$d/l.tmp"; mv -f "$d/l.tmp" "$d/labelled" 2>/dev/null
	[ -f "$d/nodes.json" ] && jq --arg n "$n" '.items |= map(if .metadata.name == $n then .spec.unschedulable = false else . end)' "$d/nodes.json" >"$d/n.tmp" && mv "$d/n.tmp" "$d/nodes.json"
	;;
*" get nodes -l "*) cat "$d/labelled" 2>/dev/null ;;
*" get nodes -o json "*) cat "$d/nodes.json" 2>/dev/null || echo '{"items":[]}' ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(src), 0o755); err != nil {
		t.Fatal(err)
	}
	return "PATH=" + bin + ":" + os.Getenv("PATH")
}

func gateEnv(t *testing.T, dir, root string) []string {
	// PF_REPO_ROOT: an empty tree, so no real port-forward on this machine counts as a leftover.
	return []string{"DEMOS_DIR=" + dir, "GATE_RESET=" + filepath.Join(dir, "reset.sh"), "EVIDENCE_ROOT=" + root, "GATE_RUN_ID=gate-1",
		fakeKubectl(t, dir), "PF_REPO_ROOT=" + t.TempDir()}
}

// cordonedNodes is a "kubectl get nodes -o json" answer with these nodes unschedulable.
func cordonedNodes(names ...string) string {
	var items []string
	for _, n := range names {
		items = append(items, fmt.Sprintf(`{"metadata":{"name":%q},"spec":{"unschedulable":true}}`, n))
	}
	return `{"items":[` + strings.Join(items, ",") + `]}`
}

// The gate resets before each round, runs all nine demos per round, prints
// one row per demo, and passes only with 18 PASS. Every other result counts
// as a failure.
func TestGateSummary(t *testing.T) {
	gate, _ := filepath.Abs("demos/gate.sh")
	codes := allNine(exitPass)
	codes[1] = exitPending
	codes[2] = exitNotImplemented
	codes[3] = exitFail
	codes[5] = 9 // anything else: an error
	delete(codes, 8)
	dir, calls := gateFixture(t, codes, "0")
	root := t.TempDir()
	out, code := runScript(t, "reliefgrid\n", gateEnv(t, dir, root), gate)
	if code != 1 {
		t.Fatalf("gate exit %d, want 1\n%s", code, out)
	}

	var want []string
	for r := 1; r <= 2; r++ {
		want = append(want, "reset RESET_CONFIRM=reliefgrid")
		for n := 1; n <= 9; n++ {
			if n != 8 {
				want = append(want, fmt.Sprintf("demo-%d gate-1/round-%d", n, r))
			}
		}
	}
	if got := strings.Split(strings.TrimSpace(readFile(t, calls)), "\n"); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("calls:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	summary := readFile(t, filepath.Join(root, "gate-1", "gate.txt"))
	for _, row := range []string{
		`(?m)^demo-1 +PENDING +PENDING$`,
		`(?m)^demo-2 +NOT IMPLEMENTED +NOT IMPLEMENTED$`,
		`(?m)^demo-3 +FAIL +FAIL$`,
		`(?m)^demo-4 +PASS +PASS$`,
		`(?m)^demo-5 +ERROR \(exit 9\) +ERROR \(exit 9\)$`,
		`(?m)^demo-8 +MISSING +MISSING$`,
		`(?m)^demo-9 +PASS +PASS$`,
		`(?m)^reset +OK \(\d+ s\) +OK \(\d+ s\)$`,
		`(?m)^cordons +none +none$`,
		`(?m)^result: FAIL \(8 of 18 passed\)$`,
		`(?m)^evidence: .*gate-1/round-1/ .*gate-1/round-2/$`,
	} {
		if !regexp.MustCompile(row).MatchString(summary) {
			t.Errorf("summary lacks %s\n%s", row, summary)
		}
	}
	if !strings.Contains(out, "result: FAIL (8 of 18 passed)") {
		t.Errorf("the summary is not printed\n%s", out)
	}
}

func TestGateAllPass(t *testing.T) {
	gate, _ := filepath.Abs("demos/gate.sh")
	dir, _ := gateFixture(t, allNine(exitPass), "0")
	root := t.TempDir()
	out, code := runScript(t, "reliefgrid\n", gateEnv(t, dir, root), gate)
	if code != 0 || !strings.Contains(out, "result: PASS (18/18 passed)") {
		t.Fatalf("gate exit %d, want 0 with 18/18\n%s", code, out)
	}
}

// A failed reset runs no demo in that round; they all count as failures.
func TestGateResetFailure(t *testing.T) {
	gate, _ := filepath.Abs("demos/gate.sh")
	dir, calls := gateFixture(t, allNine(exitPass), "1")
	root := t.TempDir()
	out, code := runScript(t, "reliefgrid\n", gateEnv(t, dir, root), gate)
	if code != 1 {
		t.Fatalf("gate exit %d, want 1\n%s", code, out)
	}
	if strings.Contains(readFile(t, calls), "demo-") {
		t.Fatalf("demos ran after a failed reset:\n%s", readFile(t, calls))
	}
	summary := readFile(t, filepath.Join(root, "gate-1", "gate.txt"))
	for _, row := range []string{`(?m)^demo-4 +NOT RUN +NOT RUN$`, `(?m)^reset +FAILED \(\d+ s\) +FAILED \(\d+ s\)$`, `(?m)^result: FAIL \(0 of 18 passed\)$`} {
		if !regexp.MustCompile(row).MatchString(summary) {
			t.Errorf("summary lacks %s\n%s", row, summary)
		}
	}
}

// A clean deployment deletes the kind cluster, so the gate asks first; with
// no confirmation nothing is reset and no demo runs.
func TestGateRefusesWithoutConfirmation(t *testing.T) {
	gate, _ := filepath.Abs("demos/gate.sh")
	dir, calls := gateFixture(t, allNine(exitPass), "0")
	for _, stdin := range []string{"", "yes\n"} {
		out, code := runScript(t, stdin, gateEnv(t, dir, t.TempDir()), gate)
		if code == 0 || !strings.Contains(out, "not confirmed") {
			t.Fatalf("stdin %q: gate exit %d, want a refusal\n%s", stdin, code, out)
		}
		if _, err := os.Stat(calls); err == nil {
			t.Fatalf("stdin %q: something ran:\n%s", stdin, readFile(t, calls))
		}
	}
}

// reset.sh refuses without confirmation before calling any tool.
func TestResetRefusesWithoutConfirmation(t *testing.T) {
	reset, _ := filepath.Abs("demos/reset.sh")
	bin := t.TempDir()
	log := filepath.Join(bin, "log")
	for _, tool := range []string{"kind", "kubectl", "helm", "docker", "make"} {
		if err := os.WriteFile(filepath.Join(bin, tool), []byte("#!/bin/sh\necho "+tool+" >>"+log+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	out, code := runScript(t, "", []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "RESET_CONFIRM="}, reset)
	if code == 0 || !strings.Contains(out, "not confirmed") {
		t.Fatalf("reset exit %d, want a refusal\n%s", code, out)
	}
	if _, err := os.Stat(log); err == nil {
		t.Fatalf("reset called tools before confirmation:\n%s", readFile(t, log))
	}
}

// Parth's demos (A3.1) are stubs that say so: NOT IMPLEMENTED, with the §28
// acceptance condition in the header for whoever fills in the body.
func TestDemoStubs(t *testing.T) {
	for n, accept := range map[int]string{
		2: "Resource persists with validated state and location",
		3: "reciprocal assignment; no double claim",
		5: "after bounded quiescence, outbox and invariants pass",
		8: "Kubernetes creates replacement; progress continues; interrupted calls counted/retried",
		9: "New pods become ready and receive requests through the Service",
	} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			script, _ := filepath.Abs(fmt.Sprintf("demos/demo-%d.sh", n))
			src := readFile(t, script)
			if !strings.Contains(src, accept) || !strings.Contains(src, "§28") {
				t.Errorf("demo-%d.sh header lacks the §28 acceptance %q", n, accept)
			}
			root := t.TempDir()
			out, code := runScript(t, "", []string{"EVIDENCE_ROOT=" + root, "DEMO_RUN_ID=r", "KUBE_CONTEXT=no-such-context"}, script)
			if code != exitNotImplemented {
				t.Fatalf("exit %d, want NOT IMPLEMENTED (%d)\n%s", code, exitNotImplemented, out)
			}
			if ev := readFile(t, filepath.Join(root, "r", fmt.Sprintf("demo-%d.txt", n))); !strings.Contains(ev, "result:     NOT IMPLEMENTED") {
				t.Fatalf("evidence:\n%s", ev)
			}
		})
	}
}

// Demo 1 goes through the public API; until A1.1 is deployed it is PENDING,
// decided before it touches the cluster.
func TestDemo1PendingWithoutAPI(t *testing.T) {
	script, _ := filepath.Abs("demos/demo-1.sh")
	root := t.TempDir()
	out, code := runScript(t, "", []string{"EVIDENCE_ROOT=" + root, "DEMO_RUN_ID=r", "API_URL=http://127.0.0.1:9/api/v1", "KUBE_CONTEXT=no-such-context"}, script)
	if code != exitPending {
		t.Fatalf("exit %d, want PENDING (%d)\n%s", code, exitPending, out)
	}
	if ev := readFile(t, filepath.Join(root, "r", "demo-1.txt")); !strings.Contains(ev, "result:     PENDING (public API not reachable at http://127.0.0.1:9/api/v1") {
		t.Fatalf("evidence:\n%s", ev)
	}
}

// Demos 4, 6 and 7 need a cluster: without one they FAIL with evidence,
// never PASS by default.
func TestClusterDemosFailWithoutACluster(t *testing.T) {
	for _, n := range []int{4, 6, 7} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			script, _ := filepath.Abs(fmt.Sprintf("demos/demo-%d.sh", n))
			root := t.TempDir()
			out, code := runScript(t, "", []string{"EVIDENCE_ROOT=" + root, "DEMO_RUN_ID=r", "KUBE_CONTEXT=no-such-context"}, script)
			if code != exitFail {
				t.Fatalf("exit %d, want FAIL (%d)\n%s", code, exitFail, out)
			}
			if ev := readFile(t, filepath.Join(root, "r", fmt.Sprintf("demo-%d.txt", n))); !strings.Contains(ev, "FAIL  preflight:") {
				t.Fatalf("evidence lacks a preflight failure:\n%s", ev)
			}
		})
	}
}

// make demo-N and make gate run these scripts.
func TestMakeDemoTargets(t *testing.T) {
	for n := 1; n <= 9; n++ {
		out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-n", fmt.Sprintf("demo-%d", n)).CombinedOutput()
		if err != nil || !strings.Contains(string(out), fmt.Sprintf("scripts/demos/demo-%d.sh", n)) {
			t.Errorf("make -n demo-%d: %v\n%s", n, err, out)
		}
	}
	out, err := exec.CommandContext(t.Context(), "make", "-C", "..", "-n", "gate").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "scripts/demos/gate.sh") {
		t.Errorf("make -n gate: %v\n%s", err, out)
	}
}

// No worker may stay cordoned: the gate restores the demos' cordons and checks the nodes at its
// start and after every round. A worker cordoned before the gate by anyone else stops it before
// anything runs; a demo cordon left at the end of a round fails the gate and is restored.
func TestGateCordonChecks(t *testing.T) {
	gate, _ := filepath.Abs("demos/gate.sh")

	t.Run("cordoned before the gate", func(t *testing.T) {
		dir, calls := gateFixture(t, allNine(exitPass), "0")
		if err := os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(cordonedNodes("reliefgrid-worker")), 0o644); err != nil {
			t.Fatal(err)
		}
		out, code := runScript(t, "reliefgrid\n", gateEnv(t, dir, t.TempDir()), gate)
		if code == 0 || !strings.Contains(out, "reliefgrid-worker is cordoned") {
			t.Fatalf("gate exit %d, want a refusal naming the worker\n%s", code, out)
		}
		if _, err := os.Stat(calls); err == nil {
			t.Fatalf("something ran:\n%s", readFile(t, calls))
		}
	})

	t.Run("a demo's cordon left at the start is restored", func(t *testing.T) {
		dir, _ := gateFixture(t, allNine(exitPass), "0")
		_ = os.WriteFile(filepath.Join(dir, "nodes.json"), []byte(cordonedNodes("reliefgrid-worker2")), 0o644)
		_ = os.WriteFile(filepath.Join(dir, "labelled"), []byte("node/reliefgrid-worker2\n"), 0o644)
		out, code := runScript(t, "reliefgrid\n", gateEnv(t, dir, t.TempDir()), gate)
		if code != 0 || !strings.Contains(readFile(t, filepath.Join(dir, "kubectl.log")), "uncordon reliefgrid-worker2") {
			t.Fatalf("gate exit %d, want it to restore the demo cordon and pass\n%s", code, out)
		}
	})

	t.Run("leaked by a round", func(t *testing.T) {
		dir, _ := gateFixture(t, allNine(exitPass), "0")
		// demo-7 leaves its worker cordoned (with the demos' label) in every round.
		leak := fmt.Sprintf("#!/usr/bin/env bash\necho %q >%q\necho node/reliefgrid-worker3 >%q\nexit 0\n",
			cordonedNodes("reliefgrid-worker3"), filepath.Join(dir, "nodes.json"), filepath.Join(dir, "labelled"))
		if err := os.WriteFile(filepath.Join(dir, "demo-7.sh"), []byte(leak), 0o755); err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		out, code := runScript(t, "reliefgrid\n", gateEnv(t, dir, root), gate)
		if code != 1 {
			t.Fatalf("gate exit %d, want 1\n%s", code, out)
		}
		summary := readFile(t, filepath.Join(root, "gate-1", "gate.txt"))
		for _, row := range []string{`(?m)^cordons +LEAKED: reliefgrid-worker3 +LEAKED: reliefgrid-worker3$`, `(?m)^result: FAIL \(18 of 18 passed; cordons leaked\)$`} {
			if !regexp.MustCompile(row).MatchString(summary) {
				t.Errorf("summary lacks %s\n%s", row, summary)
			}
		}
		if strings.Count(readFile(t, filepath.Join(dir, "kubectl.log")), "uncordon reliefgrid-worker3") < 2 {
			t.Errorf("the leaked cordon was not restored after each round\n%s", readFile(t, filepath.Join(dir, "kubectl.log")))
		}
	})

	t.Run("interrupted", func(t *testing.T) {
		dir, _ := gateFixture(t, allNine(exitPass), "0")
		started := filepath.Join(dir, "started")
		slow := fmt.Sprintf("#!/usr/bin/env bash\necho node/reliefgrid-worker >%q\ntouch %q\nsleep 30\n", filepath.Join(dir, "labelled"), started)
		if err := os.WriteFile(filepath.Join(dir, "demo-6.sh"), []byte(slow), 0o755); err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), "bash", gate)
		cmd.Env = append(os.Environ(), gateEnv(t, dir, t.TempDir())...)
		cmd.Stdin = strings.NewReader("reliefgrid\n")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			if _, err := os.Stat(started); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) // the process group, as Ctrl-C would
		if err := cmd.Wait(); err == nil {
			t.Fatal("an interrupted gate exited 0")
		}
		if !strings.Contains(readFile(t, filepath.Join(dir, "kubectl.log")), "uncordon reliefgrid-worker") {
			t.Fatalf("the interrupted gate did not restore the demo cordon\n%s", readFile(t, filepath.Join(dir, "kubectl.log")))
		}
	})
}

// An interrupted demo still runs its cleanups and writes its evidence, as a FAIL.
func TestDemoInterrupted(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	cleaned := filepath.Join(dir, "cleaned")
	started := filepath.Join(dir, "started")
	script := fakeDemo(t, dir, 6, fmt.Sprintf(`demo_reset "none"
demo_on_exit touch %q
check "true holds" true
touch %q
sleep 30
demo_end`, cleaned, started))
	cmd := exec.CommandContext(t.Context(), "bash", script)
	cmd.Env = append(os.Environ(), "EVIDENCE_ROOT="+root, "DEMO_RUN_ID=r", "KUBE_CONTEXT=no-such-context")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(started); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) // the process group, as Ctrl-C would
	_ = cmd.Wait()
	if _, err := os.Stat(cleaned); err != nil {
		t.Fatal("the cleanup did not run")
	}
	if ev := readFile(t, filepath.Join(root, "r", "demo-6.txt")); !strings.Contains(ev, "result:     FAIL (exited with status 143 before demo_end)") {
		t.Fatalf("evidence:\n%s", ev)
	}
}
