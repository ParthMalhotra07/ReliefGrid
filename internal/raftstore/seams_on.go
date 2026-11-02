//go:build seams

package raftstore

import (
	"errors"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// seamsBuild reports whether this binary was built with the seams tag.
const seamsBuild = true

// SeamSpec is a boundary to arm at runtime (seams build only).
type SeamSpec struct {
	Kind         testseam.Kind
	OperationIDs []string      // LogBarrier: at least two; the first is held
	Timeout      time.Duration // after which a held boundary expires
}

// Seam is an armed boundary. Barrier is set for a LogBarrier only.
type Seam struct {
	Hold    *testseam.Hold
	Barrier *testseam.Barrier
}

// ArmSeam arms a boundary on this running node. Arming lives in memory, so
// a restarted node is always disarmed. One boundary of each kind at a time.
func (n *Node) ArmSeam(spec SeamSpec) (*Seam, error) {
	return nil, errors.New("raftstore: ArmSeam not implemented")
}

// ErrSeamArmed: a boundary of that kind is already armed and not finished.
var ErrSeamArmed = errors.New("raftstore: seam already armed")
