//go:build seams

package raftstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// The seams build arms boundaries on a running node with no Config hooks:
// this is what the kind runner reaches through /mgmt/test/seams.

func TestArmSeamPersistAtRuntimeThenRestartDisarms(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	last := writeKeys(t, l, "a", 8)
	s, err := l.ArmSeam(SeamSpec{Kind: testseam.SnapshotPersist, Timeout: waitFor})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := l.Snapshot(context.Background()); done <- err }()
	p, err := s.Hold.WaitReached(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if p.ImageIndex != last.AppliedIndex || p.Bytes <= 0 || p.Bytes >= p.Size {
		t.Fatalf("paused at %+v", p)
	}
	s.Hold.Release()
	if err := <-done; err != nil {
		t.Fatalf("released snapshot: %v", err)
	}

	if _, err := l.ArmSeam(SeamSpec{Kind: testseam.SnapshotPersist, Timeout: waitFor}); err != nil {
		t.Fatal(err)
	}
	c.restart(li)
	// A restarted node has nothing to snapshot until it applies a command.
	_, nl := c.waitLeader()
	more := writeKeys(t, nl, "b", 2)
	if err := c.nodes[li].WaitApplied(testCtx(t), more.AppliedIndex); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.nodes[li].Snapshot(ctx); err != nil {
		t.Fatalf("snapshot after restart: %v (a restarted node must come up disarmed)", err)
	}
}

func TestArmSeamLogBarrierAtRuntime(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	mustApply(t, l, cas("op-create", "kv:x", 0, 0))
	s, err := l.ArmSeam(SeamSpec{Kind: testseam.LogBarrier, OperationIDs: []string{"op-a", "op-b"}, Timeout: waitFor})
	if err != nil {
		t.Fatal(err)
	}
	if s.Barrier == nil {
		t.Fatal("log barrier seam without its Barrier")
	}
	a := goApply(l, cas("op-a", "kv:x", 1, 1))
	if _, err := s.Barrier.WaitAppended(testCtx(t), "op-a"); err != nil {
		t.Fatal(err)
	}
	b := goApply(l, cas("op-b", "kv:x", 1, 2))
	if _, err := s.Hold.WaitReached(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	s.Hold.Release()
	if r := <-a; r.err != nil || r.res.Version != 2 {
		t.Fatalf("A: %+v, %v", r.res, r.err)
	}
	if r := <-b; r.err != nil || r.res.Outcome != "VERSION_CONFLICT" {
		t.Fatalf("B: %+v, %v", r.res, r.err)
	}
}

func TestArmSeamRejectsBadSpecs(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	for name, spec := range map[string]SeamSpec{
		"unknown kind":           {Kind: "nope", Timeout: time.Second},
		"barrier with one op":    {Kind: testseam.LogBarrier, OperationIDs: []string{"a"}, Timeout: time.Second},
		"barrier duplicate op":   {Kind: testseam.LogBarrier, OperationIDs: []string{"a", "a"}, Timeout: time.Second},
		"stream with ops":        {Kind: testseam.RestoreStage, OperationIDs: []string{"a"}, Timeout: time.Second},
		"no timeout":             {Kind: testseam.RestoreTransfer},
		"timeout over 2 minutes": {Kind: testseam.RestoreTransfer, Timeout: 3 * time.Minute},
	} {
		if _, err := l.ArmSeam(spec); err == nil {
			t.Errorf("%s: armed", name)
		}
	}
	if _, err := l.ArmSeam(SeamSpec{Kind: testseam.RestoreStage, Timeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ArmSeam(SeamSpec{Kind: testseam.RestoreStage, Timeout: time.Second}); !errors.Is(err, ErrSeamArmed) {
		t.Fatalf("second restore-stage seam: %v, want ErrSeamArmed", err)
	}
}
