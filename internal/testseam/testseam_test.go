package testseam_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

func ctxFor(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// reach runs h.Reach(p) on its own goroutine, as the paused code would.
func reach(h *testseam.Hold, p testseam.Point) <-chan error {
	done := make(chan error, 1)
	go func() { done <- h.Reach(p) }()
	return done
}

func TestHoldReachThenRelease(t *testing.T) {
	h := testseam.NewHold(testseam.SnapshotPersist, time.Minute)
	if s := h.Status(); s.Kind != testseam.SnapshotPersist || s.State != testseam.Armed || s.Point != nil {
		t.Fatalf("new hold %+v, want armed snapshot-persist with no point", s)
	}
	done := reach(h, testseam.Point{ImageIndex: 7, Bytes: 10, Size: 20})
	p, err := h.WaitReached(ctxFor(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if p.ImageIndex != 7 || p.Bytes != 10 || p.Size != 20 {
		t.Fatalf("point %+v", p)
	}
	select {
	case err := <-done:
		t.Fatalf("Reach returned (%v) before Release", err)
	case <-time.After(50 * time.Millisecond):
	}
	if s := h.Status(); s.State != testseam.Reached || s.Point == nil {
		t.Fatalf("status %+v, want reached with its point", s)
	}
	h.Release()
	if err := <-done; err != nil {
		t.Fatalf("Reach after Release: %v", err)
	}
	if s := h.Status(); s.State != testseam.Released {
		t.Fatalf("status %+v, want released", s)
	}
	// One-shot: a later pass through the boundary does not pause.
	if err := h.Reach(testseam.Point{}); err != nil {
		t.Fatalf("second Reach: %v", err)
	}
}

func TestHoldAbortFailsThePausedCode(t *testing.T) {
	h := testseam.NewHold(testseam.RestoreStage, time.Minute)
	done := reach(h, testseam.Point{Bytes: 1, Size: 2})
	if _, err := h.WaitReached(ctxFor(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	h.Abort()
	if err := <-done; !errors.Is(err, testseam.ErrAborted) {
		t.Fatalf("Reach after Abort: %v, want ErrAborted", err)
	}
	if s := h.Status(); s.State != testseam.Aborted {
		t.Fatalf("status %+v, want aborted", s)
	}
}

// A forgotten seam lets the node go on, and says so.
func TestHoldExpiresWithoutRelease(t *testing.T) {
	h := testseam.NewHold(testseam.SnapshotPersist, 100*time.Millisecond)
	done := reach(h, testseam.Point{})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expired Reach: %v, want nil (the paused code goes on)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hold never expired")
	}
	if s := h.Status(); s.State != testseam.Expired {
		t.Fatalf("status %+v, want expired", s)
	}
}

// #88: the harness must fail when the boundary is never reached.
func TestWaitReachedNeverReachedIsAnError(t *testing.T) {
	h := testseam.NewHold(testseam.RestoreTransfer, time.Minute)
	start := time.Now()
	_, err := h.WaitReached(ctxFor(t, 100*time.Millisecond))
	if !errors.Is(err, testseam.ErrNotReached) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v, want ErrNotReached wrapping the deadline", err)
	}
	if time.Since(start) < 90*time.Millisecond {
		t.Fatal("WaitReached gave up before its context ended")
	}
	if s := h.Status(); s.State != testseam.Armed {
		t.Fatalf("status %+v, want still armed", s)
	}
}

// fakeNode is a log and FSM for the barrier: entries are appended, then
// committed, then applied in order through BeforeApply.
type fakeNode struct{ committed, applied atomic.Uint64 }

// SCH01: A and B are both appended and committed while A is held before
// apply; nothing applies until Release, then A applies first.
func TestBarrierHoldsFirstUntilAllAppendedAndCommitted(t *testing.T) {
	var n fakeNode
	n.applied.Store(4)
	n.committed.Store(4)
	b := testseam.NewBarrier(time.Minute, n.committed.Load, n.applied.Load, "op-a", "op-b")

	b.Appended(5, "op-a")
	if i, err := b.WaitAppended(ctxFor(t, 5*time.Second), "op-a"); err != nil || i != 5 {
		t.Fatalf("WaitAppended(op-a) = %d, %v; want 5", i, err)
	}
	n.committed.Store(5)
	applyA := make(chan struct{})
	go func() { b.BeforeApply(5); close(applyA) }()

	b.Appended(6, "op-other") // not listed: ignored
	if got := b.Indexes(); len(got) != 1 || got["op-a"] != 5 {
		t.Fatalf("Indexes() = %v while armed, want op-a 5 only", got)
	}
	if _, err := b.Hold().WaitReached(ctxFor(t, 100*time.Millisecond)); !errors.Is(err, testseam.ErrNotReached) {
		t.Fatalf("reached before op-b was appended: %v", err)
	}
	b.Appended(7, "op-b")
	if _, err := b.Hold().WaitReached(ctxFor(t, 100*time.Millisecond)); !errors.Is(err, testseam.ErrNotReached) {
		t.Fatalf("reached before op-b was committed: %v", err)
	}
	n.committed.Store(7)
	p, err := b.Hold().WaitReached(ctxFor(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if p.Indexes["op-a"] != 5 || p.Indexes["op-b"] != 7 || len(p.Indexes) != 2 {
		t.Fatalf("indexes %v, want op-a 5 and op-b 7 only", p.Indexes)
	}
	if p.CommitIndex < 7 || p.AppliedIndex >= 5 {
		t.Fatalf("point %+v: want commit >= 7 and applied < 5", p)
	}
	select {
	case <-applyA:
		t.Fatal("A applied before Release")
	case <-time.After(50 * time.Millisecond):
	}
	b.Hold().Release()
	select {
	case <-applyA:
	case <-time.After(5 * time.Second):
		t.Fatal("A still held after Release")
	}
	b.BeforeApply(7) // B never waits
}

// The barrier is reached only while no listed command has applied.
func TestBarrierNotReachedOnceACommandApplied(t *testing.T) {
	var n fakeNode
	b := testseam.NewBarrier(time.Minute, n.committed.Load, n.applied.Load, "op-a", "op-b")
	b.Appended(1, "op-a")
	b.Appended(2, "op-b")
	n.committed.Store(2)
	n.applied.Store(1) // A applied before the barrier saw both
	if _, err := b.Hold().WaitReached(ctxFor(t, 100*time.Millisecond)); !errors.Is(err, testseam.ErrNotReached) {
		t.Fatalf("reached after op-a applied: %v", err)
	}
}

func TestBarrierExpires(t *testing.T) {
	var n fakeNode
	b := testseam.NewBarrier(100*time.Millisecond, n.committed.Load, n.applied.Load, "op-a", "op-b")
	b.Appended(1, "op-a")
	done := make(chan struct{})
	go func() { b.BeforeApply(1); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("barrier held past its timeout")
	}
	if s := b.Hold().Status(); s.State != testseam.Expired {
		t.Fatalf("status %+v, want expired", s)
	}
}
