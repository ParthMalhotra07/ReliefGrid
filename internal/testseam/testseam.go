// Package testseam holds the observable test boundaries (S4.6b, issue #88)
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
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
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
	// Snapshot streams: the snapshot's last Raft index (InstallSnapshot
	// only), the image's applied index (0 if the stream does not carry it
	// yet), the bytes through before the pause and the total.
	RaftIndex  uint64 `json:"raftIndex,omitempty"`
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
type Hold struct {
	kind    Kind
	timeout time.Duration

	mu      sync.Mutex
	state   State
	point   *Point
	taken   bool          // the boundary has been passed once
	reached chan struct{} // closed when Reached
	done    chan struct{} // closed when Released, Aborted or Expired
}

// NewHold arms a pause of the given kind. A reached hold that is neither
// released nor aborted within timeout lets the paused code go on, as
// Expired, so a forgotten seam cannot wedge a node.
func NewHold(kind Kind, timeout time.Duration) *Hold {
	return &Hold{kind: kind, timeout: timeout, state: Armed, reached: make(chan struct{}), done: make(chan struct{})}
}

// Reach is called by the code under test at the boundary. The first call
// blocks until Release (nil), Abort (ErrAborted) or the timeout (nil);
// later calls return nil at once. A hold aborted before it is reached
// fails the first pass; one released before it is reached never pauses.
func (h *Hold) Reach(p Point) error {
	if !h.markReached(p) {
		h.mu.Lock()
		defer h.mu.Unlock()
		if !h.taken && h.state == Aborted {
			h.taken = true
			return ErrAborted
		}
		return nil
	}
	return h.wait(time.Now().Add(h.timeout))
}

// markReached records p and reports whether this call reached the hold.
func (h *Hold) markReached(p Point) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.taken || h.state != Armed {
		return false
	}
	h.taken, h.state = true, Reached
	h.point = &p
	close(h.reached)
	return true
}

// wait blocks until the hold finishes or deadline passes (then Expired).
func (h *Hold) wait(deadline time.Time) error {
	t := time.NewTimer(time.Until(deadline))
	defer t.Stop()
	select {
	case <-h.done:
	case <-t.C:
		h.finish(Expired)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state == Aborted {
		return ErrAborted
	}
	return nil
}

// finish moves an armed or reached hold to s; it reports whether it did.
func (h *Hold) finish(s State) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != Armed && h.state != Reached {
		return false
	}
	h.state = s
	close(h.done)
	return true
}

// WaitReached waits until the boundary is reached; ErrNotReached (wrapping
// ctx's error) if ctx ends first.
func (h *Hold) WaitReached(ctx context.Context) (Point, error) {
	select {
	case <-h.reached:
		h.mu.Lock()
		defer h.mu.Unlock()
		return clonePoint(*h.point), nil
	case <-ctx.Done():
		return Point{}, fmt.Errorf("%w: %w", ErrNotReached, ctx.Err())
	}
}

// Release lets the paused code continue.
func (h *Hold) Release() { h.finish(Released) }

// Abort makes the paused code's Reach return ErrAborted.
func (h *Hold) Abort() { h.finish(Aborted) }

// Status reports the hold's state.
func (h *Hold) Status() Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := Status{Kind: h.kind, State: h.state}
	if h.point != nil {
		p := clonePoint(*h.point)
		st.Point = &p
	}
	return st
}

func clonePoint(p Point) Point {
	p.Indexes = maps.Clone(p.Indexes)
	return p
}

// Barrier is the SCH01 log barrier: it holds the first listed command at
// apply until every listed command is appended and committed, then is
// Reached and waits for Release. Its timeout runs from arming: past it the
// held command applies and the barrier is Expired.
type Barrier struct {
	hold      *Hold
	ops       []string
	committed func() uint64
	applied   func() uint64
	deadline  time.Time

	mu      sync.Mutex
	idx     map[string]uint64
	changed chan struct{} // closed and replaced whenever idx grows
}

// barrierPoll is how often the barrier rechecks the commit and applied
// indexes, which move without telling it.
const barrierPoll = 2 * time.Millisecond

// NewBarrier arms a barrier over opIDs. committed and applied report the
// node's commit index and FSM applied index.
func NewBarrier(timeout time.Duration, committed, applied func() uint64, opIDs ...string) *Barrier {
	b := &Barrier{
		hold: NewHold(LogBarrier, timeout), ops: slices.Clone(opIDs), committed: committed, applied: applied,
		deadline: time.Now().Add(timeout), idx: map[string]uint64{}, changed: make(chan struct{}),
	}
	go b.watch()
	return b
}

// watch reaches the barrier once its condition holds, or expires it.
func (b *Barrier) watch() {
	t := time.NewTicker(barrierPoll)
	defer t.Stop()
	for {
		if time.Now().After(b.deadline) {
			b.hold.finish(Expired)
			return
		}
		if p, ok := b.condition(); ok {
			b.hold.markReached(p)
			return
		}
		select {
		case <-b.hold.done:
			return
		case <-t.C:
		}
	}
}

// condition: every listed command is in the log, all are committed, and
// none has applied.
func (b *Barrier) condition() (Point, bool) {
	b.mu.Lock()
	if len(b.idx) < len(b.ops) {
		b.mu.Unlock()
		return Point{}, false
	}
	idx := maps.Clone(b.idx)
	b.mu.Unlock()
	lo, hi := uint64(math.MaxUint64), uint64(0)
	for _, i := range idx {
		lo, hi = min(lo, i), max(hi, i)
	}
	c, a := b.committed(), b.applied()
	if c < hi || a >= lo {
		return Point{}, false
	}
	return Point{Indexes: idx, CommitIndex: c, AppliedIndex: a}, true
}

// Appended is called once a command entry is in the log.
func (b *Barrier) Appended(index uint64, opID string) {
	if !slices.Contains(b.ops, opID) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.idx[opID]; ok {
		return // a retried append of the same command keeps its first index
	}
	b.idx[opID] = index
	close(b.changed)
	b.changed = make(chan struct{})
}

// BeforeApply is called before each command applies; it blocks on the
// first listed command until the barrier is released, aborted or expires.
func (b *Barrier) BeforeApply(index uint64) {
	if len(b.ops) == 0 {
		return
	}
	b.mu.Lock()
	first, ok := b.idx[b.ops[0]]
	b.mu.Unlock()
	if !ok || index != first {
		return
	}
	_ = b.hold.wait(b.deadline)
}

// WaitAppended waits until opID is in the log and returns its index.
func (b *Barrier) WaitAppended(ctx context.Context, opID string) (uint64, error) {
	for {
		b.mu.Lock()
		i, ok := b.idx[opID]
		ch := b.changed
		b.mu.Unlock()
		if ok {
			return i, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, fmt.Errorf("%w: %s not appended: %w", ErrNotReached, opID, ctx.Err())
		}
	}
}

// Hold is the barrier's observable boundary.
func (b *Barrier) Hold() *Hold { return b.hold }

// Indexes reports the listed commands appended so far (operationId → log
// index), so a harness can order its proposals before the barrier is
// reached.
func (b *Barrier) Indexes() map[string]uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return maps.Clone(b.idx)
}
