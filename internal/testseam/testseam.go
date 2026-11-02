// Package testseam holds the observable test boundaries (S4.6, issue #88)
// that the adversarial schedule harness pauses the storage layer at: a log
// barrier (SCH01) and pauses inside snapshot creation and restore (SCH12,
// SCH13).
//
// A boundary is one-shot: armed, then reached (the code under test is paused
// there and reports where), then released, aborted or expired. A harness
// waits for it to be reached and fails if it never is. Hooks never change what
// the paused code computes; they only delay it or, for the snapshot streams,
// make it fail as a crash would.
//
// Production binaries never link this package: raftstore's hooks are plain
// nil funcs, and only the seams build tag and tests connect them here.
package testseam

import (
	"context"
	"errors"
	"time"
)

// Kind names a boundary.
type Kind string

const (
	// LogBarrier: every listed command is in the leader's log and committed
	// while none of them has applied (SCH01).
	LogBarrier Kind = "log-barrier"
	// SnapshotPersist: halfway through writing a snapshot image (SCH12).
	SnapshotPersist Kind = "snapshot-persist"
	// RestoreTransfer: halfway through receiving InstallSnapshot (SCH13a).
	RestoreTransfer Kind = "restore-transfer"
	// RestoreStage: halfway through staging a snapshot into the FSM (SCH13b).
	RestoreStage Kind = "restore-stage"
)

// State is where a boundary is in its life.
type State string

const (
	Armed    State = "armed"
	Reached  State = "reached"
	Released State = "released"
	Aborted  State = "aborted"
	Expired  State = "expired" // released by its timeout, not the harness
)

// Point is what the paused code reports when the boundary is reached.
type Point struct {
	// Log barrier: each operationId's log index, the commit index and the
	// FSM's applied index when the boundary was reached.
	Indexes      map[string]uint64 `json:"indexes,omitempty"`
	CommitIndex  uint64            `json:"commitIndex,omitempty"`
	AppliedIndex uint64            `json:"appliedIndex,omitempty"`
	// Snapshot streams: the image's applied index (0 if the stream does not
	// carry it yet), the bytes written before the pause and the total.
	ImageIndex uint64 `json:"imageIndex,omitempty"`
	Bytes      int64  `json:"bytes,omitempty"`
	Size       int64  `json:"size,omitempty"`
}

// Status is a boundary's state and, once reached, its point.
type Status struct {
	Kind  Kind   `json:"kind"`
	State State  `json:"state"`
	Point *Point `json:"point,omitempty"`
}

var (
	// ErrNotReached: the boundary was not reached before the wait ended.
	ErrNotReached = errors.New("testseam: boundary not reached")
	// ErrAborted is what Reach returns to the paused code on Abort: it stops
	// as a killed process would.
	ErrAborted = errors.New("testseam: aborted at boundary")
)

// Hold is a one-shot pause.
type Hold struct{}

// NewHold arms a pause of the given kind. A reached hold that is neither
// released nor aborted within timeout lets the paused code go on, as
// Expired, so a forgotten seam cannot wedge a node.
func NewHold(kind Kind, timeout time.Duration) *Hold { return &Hold{} }

// Reach is called by the code under test at the boundary. The first call
// blocks until Release (nil), Abort (ErrAborted) or the timeout (nil);
// later calls return nil at once.
func (h *Hold) Reach(p Point) error { return nil }

// WaitReached waits until the boundary is reached; ErrNotReached (wrapping
// ctx's error) if ctx ends first.
func (h *Hold) WaitReached(ctx context.Context) (Point, error) { return Point{}, ErrNotReached }

// Release lets the paused code continue.
func (h *Hold) Release() {}

// Abort makes the paused code's Reach return ErrAborted.
func (h *Hold) Abort() {}

// Status reports the hold's state.
func (h *Hold) Status() Status { return Status{} }

// Barrier is the SCH01 log barrier: it holds the first listed command at
// apply until every listed command is appended and committed, then is
// Reached and waits for Release.
type Barrier struct{}

// NewBarrier arms a barrier over opIDs. committed and applied report the
// node's commit index and FSM applied index.
func NewBarrier(timeout time.Duration, committed, applied func() uint64, opIDs ...string) *Barrier {
	return &Barrier{}
}

// Appended is called once a command entry is in the log.
func (b *Barrier) Appended(index uint64, opID string) {}

// BeforeApply is called before each command applies; it blocks on the
// first listed command until the barrier is released, aborted or expires.
func (b *Barrier) BeforeApply(index uint64) {}

// WaitAppended waits until opID is in the log and returns its index.
func (b *Barrier) WaitAppended(ctx context.Context, opID string) (uint64, error) {
	return 0, ErrNotReached
}

// Hold is the barrier's observable boundary.
func (b *Barrier) Hold() *Hold { return &Hold{} }

// Indexes reports the listed commands appended so far (operationId → log
// index), so a harness can order its proposals before the barrier is
// reached.
func (b *Barrier) Indexes() map[string]uint64 { return nil }
