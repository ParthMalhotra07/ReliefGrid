package raftstore

import (
	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
)

// Test seams (S4.6, #88). The hooks are nil in production: only tests (via
// Config.seams) and the seams build (seams_on.go, at runtime) set them. They
// receive copies of positions and IDs, never a log entry or the FSM, and
// cannot change what Apply computes: the apply hold only delays, and only
// the snapshot stream pauses may fail, through the error paths a crash or
// I/O error already takes.

// seamHooks are the boundaries; any may be nil.
type seamHooks struct {
	// afterAppend runs once a command entry is in the local log.
	afterAppend func(e appendedEntry)
	// persistPause runs halfway through writing a snapshot image; an error
	// fails the snapshot as a crash would.
	persistPause func(p streamPoint) error
	// installPause runs halfway through receiving InstallSnapshot.
	installPause func(p streamPoint) error
	// stagePause runs halfway through staging a snapshot into the FSM.
	stagePause func(p streamPoint) error
}

// appendedEntry is a command entry just stored in the log.
type appendedEntry struct {
	Index       uint64
	Op          fsm.Op
	OperationID string
}

// streamPoint is where a snapshot stream paused.
type streamPoint struct {
	RaftIndex  uint64 // the snapshot's last included Raft index, if known
	ImageIndex uint64 // the image's applied index, if known
	Bytes      int64  // bytes through the stream before the pause
	Size       int64  // the stream's total
}

// seamLogStore reports appended command entries to afterAppend. Open uses
// it only when seams are configured or built in.
type seamLogStore struct {
	raft.LogStore
}
