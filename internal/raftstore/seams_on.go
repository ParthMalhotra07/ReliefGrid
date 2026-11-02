//go:build seams

package raftstore

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// seamsBuild reports whether this binary was built with the seams tag.
const seamsBuild = true

// MaxSeamTimeout bounds how long an armed boundary may hold a node.
const MaxSeamTimeout = 2 * time.Minute

// ErrSeamArmed: a boundary of that kind is already armed and not finished.
var ErrSeamArmed = errors.New("raftstore: seam already armed")

// SeamSpec is a boundary to arm at runtime (seams build only).
type SeamSpec struct {
	Kind         testseam.Kind
	OperationIDs []string      // LogBarrier: at least two, distinct; the first is held
	Timeout      time.Duration // after which a held boundary expires; at most MaxSeamTimeout
}

// Seam is an armed boundary. Barrier is set for a LogBarrier only.
type Seam struct {
	Hold    *testseam.Hold
	Barrier *testseam.Barrier
}

// armedSeams are a node's runtime boundaries, by kind (Node.armed). A
// restarted node is a new Node, so it starts disarmed.
type armedSeams struct {
	mu    sync.Mutex
	kinds map[testseam.Kind]*Seam
}

func (spec SeamSpec) validate() error {
	if spec.Timeout <= 0 || spec.Timeout > MaxSeamTimeout {
		return fmt.Errorf("raftstore: seam timeout %v: must be in (0, %v]", spec.Timeout, MaxSeamTimeout)
	}
	switch spec.Kind {
	case testseam.LogBarrier:
		if len(spec.OperationIDs) < 2 || len(slices.Compact(slices.Sorted(slices.Values(spec.OperationIDs)))) != len(spec.OperationIDs) {
			return fmt.Errorf("raftstore: log barrier needs two or more distinct operationIds, got %v", spec.OperationIDs)
		}
	case testseam.SnapshotPersist, testseam.RestoreTransfer, testseam.RestoreStage:
		if len(spec.OperationIDs) != 0 {
			return fmt.Errorf("raftstore: %s takes no operationIds", spec.Kind)
		}
	default:
		return fmt.Errorf("raftstore: unknown seam kind %q", spec.Kind)
	}
	return nil
}

// ArmSeam arms a boundary on this running node. Arming lives in memory, so
// a restarted node is always disarmed. One boundary of each kind at a time:
// arming a kind whose last boundary is still armed or reached fails with
// ErrSeamArmed.
func (n *Node) ArmSeam(spec SeamSpec) (*Seam, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	a := &n.armed
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.kinds == nil {
		a.kinds = map[testseam.Kind]*Seam{}
	}
	if prev := a.kinds[spec.Kind]; prev != nil {
		if st := prev.Hold.Status().State; st == testseam.Armed || st == testseam.Reached {
			return nil, fmt.Errorf("%w: %s is %s", ErrSeamArmed, spec.Kind, st)
		}
	}
	var s *Seam
	if spec.Kind == testseam.LogBarrier {
		b := testseam.NewBarrier(spec.Timeout, n.raft.CommitIndex, n.sm.appliedIndex, spec.OperationIDs...)
		s = &Seam{Hold: b.Hold(), Barrier: b}
		n.sm.setBeforeApply(b.BeforeApply)
	} else {
		s = &Seam{Hold: testseam.NewHold(spec.Kind, spec.Timeout)}
	}
	a.kinds[spec.Kind] = s
	n.seams.hooks.Store(a.hooks())
	return s, nil
}

// hooks builds the hooks for the armed boundaries. A finished hold passes
// straight through (Reach is one-shot), so finished seams need no removal.
func (a *armedSeams) hooks() *seamHooks {
	h := &seamHooks{}
	if s := a.kinds[testseam.LogBarrier]; s != nil {
		b := s.Barrier
		h.afterAppend = func(e appendedEntry) { b.Appended(e.Index, e.OperationID) }
	}
	pause := func(k testseam.Kind) func(streamPoint) error {
		s := a.kinds[k]
		if s == nil {
			return nil
		}
		return func(p streamPoint) error {
			return s.Hold.Reach(testseam.Point{RaftIndex: p.RaftIndex, ImageIndex: p.ImageIndex, Bytes: p.Bytes, Size: p.Size})
		}
	}
	h.persistPause = pause(testseam.SnapshotPersist)
	h.installPause = pause(testseam.RestoreTransfer)
	h.stagePause = pause(testseam.RestoreStage)
	return h
}
