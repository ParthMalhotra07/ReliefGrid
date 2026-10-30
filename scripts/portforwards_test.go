package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A leaked kubectl port-forward outlives the script that started it (#50):
// scripts/port-forwards.sh finds the ones started from this repository, so
// the Verify can fail on them and the gate can refuse to start.

// startFake starts a process that looks like "kubectl ... port-forward ..."
// (or, with portForward false, another kubectl command) with its working
// directory at dir, and stops it when the test ends.
func startFake(t *testing.T, dir string, portForward bool) int {
	t.Helper()
	bin := t.TempDir()
	kubectl := filepath.Join(bin, "kubectl")
	if err := os.WriteFile(kubectl, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	args := []string{"--context", "kind-reliefgrid", "-n", "reliefgrid", "get", "pods", "--watch"}
	if portForward {
		args = []string{"--context", "kind-reliefgrid", "-n", "reliefgrid", "port-forward", "pod/storage-shard-1-0", "40000:8080"}
	}
	cmd := exec.CommandContext(t.Context(), kubectl, args...)
	cmd.Dir = dir
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	for range 50 { // until /proc shows the exec'd command line
		b, _ := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/cmdline")
		if strings.Contains(string(b), "kubectl") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cmd.Process.Pid
}

func listForwards(t *testing.T, root string) string {
	t.Helper()
	lib, _ := filepath.Abs("port-forwards.sh")
	out, code := runScript(t, "", []string{"PF_REPO_ROOT=" + root}, "-c", ". "+lib+"; repo_port_forwards")
	if code != 0 {
		t.Fatalf("repo_port_forwards exit %d\n%s", code, out)
	}
	return out
}

func TestRepoPortForwards(t *testing.T) {
	repo, elsewhere := t.TempDir(), t.TempDir()
	sub := filepath.Join(repo, "scripts")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	inRepo := startFake(t, repo, true)
	inSub := startFake(t, sub, true)
	outside := startFake(t, elsewhere, true)
	notPF := startFake(t, repo, false)

	got := listForwards(t, repo)
	for _, pid := range []int{inRepo, inSub} {
		if !strings.Contains(got, strconv.Itoa(pid)+" ") || !strings.Contains(got, "port-forward pod/storage-shard-1-0 40000:8080") {
			t.Errorf("pid %d (a port-forward started in the repo) is not listed:\n%s", pid, got)
		}
	}
	for _, pid := range []int{outside, notPF} {
		if strings.Contains(got, strconv.Itoa(pid)+" ") {
			t.Errorf("pid %d is listed but is not a port-forward from the repo:\n%s", pid, got)
		}
	}
	if got := listForwards(t, t.TempDir()); strings.TrimSpace(got) != "" {
		t.Errorf("an empty repo lists %q", got)
	}
}

// The gate refuses to start while a port-forward from the repo is running.
func TestGateRefusesWithLeftoverPortForward(t *testing.T) {
	gate, _ := filepath.Abs("demos/gate.sh")
	dir, calls := gateFixture(t, allNine(exitPass), "0")
	repo := t.TempDir()
	pid := startFake(t, repo, true)
	env := append(gateEnv(t, dir, t.TempDir()), "PF_REPO_ROOT="+repo)
	out, code := runScript(t, "reliefgrid\n", env, gate)
	if code == 0 || !strings.Contains(out, "port-forward") || !strings.Contains(out, strconv.Itoa(pid)) {
		t.Fatalf("gate exit %d, want a refusal naming pid %d\n%s", code, pid, out)
	}
	if _, err := os.Stat(calls); err == nil {
		t.Fatalf("something ran:\n%s", readFile(t, calls))
	}
}

// Every port-forward in the Verify script and the demos backgrounds kubectl
// itself, so $! is the process to kill; none is started inside $(...),
// where its PID would be lost.
func TestPortForwardsAreTracked(t *testing.T) {
	for _, f := range []string{"verify-storage-k8s.sh", "demos/kube.sh", "demos/reset.sh"} {
		src := readFile(t, f)
		for i, line := range strings.Split(src, "\n") {
			code := strings.TrimSpace(line)
			if strings.HasPrefix(code, "#") {
				continue
			}
			if strings.Contains(code, "port-forward ") && strings.HasSuffix(code, "&") && !strings.HasPrefix(code, "kubectl ") {
				t.Errorf("%s:%d backgrounds a wrapper, not kubectl (its $! is a subshell): %s", f, i+1, code)
			}
			// write_key and read_key port-forward too.
			if strings.Contains(code, "$(port_forward") || strings.Contains(code, "$(forward") || strings.Contains(code, "$(coord_forward") ||
				strings.Contains(code, "$(write_key") || strings.Contains(code, "$(read_key") {
				t.Errorf("%s:%d starts a port-forward inside $(...): %s", f, i+1, code)
			}
		}
	}
}
