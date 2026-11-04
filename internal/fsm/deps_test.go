package fsm_test

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Invariant 2 (deterministic apply): nothing trace-related enters the FSM.
// Tracing reads the wall clock and random IDs, so the FSM and everything it
// links must not import OpenTelemetry. raft.apply is timed by raftstore
// around the proposal, outside Apply (S4.4).
func TestFSMDoesNotImportTracing(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, goBin, "list", "-deps", "./...").Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Fatalf("go list: %v\n%s", err, ee.Stderr)
		}
		t.Fatalf("go list: %v", err)
	}
	pkgs := strings.Fields(string(out))
	if len(pkgs) == 0 {
		t.Fatal("go list -deps listed nothing")
	}
	for _, p := range pkgs {
		if strings.HasPrefix(p, "go.opentelemetry.io/") {
			t.Errorf("internal/fsm depends on %s", p)
		}
	}
}
