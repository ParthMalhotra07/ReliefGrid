package raftstore

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
)

// Test seams (S4.6b, #88). The hooks are nil in production: only tests (via
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
	seams *seamState
}

// seamState is the hooks currently in force, shared by the log store, the
// state machine and the transport. It is nil (every check a nil check)
// unless seams are configured (tests) or built in (the seams tag).
type seamState struct {
	hooks atomic.Pointer[seamHooks]
}

// load returns the hooks in force; nil without seams.
func (s *seamState) load() *seamHooks {
	if s == nil {
		return nil
	}
	return s.hooks.Load()
}

func (w *seamLogStore) StoreLog(l *raft.Log) error { return w.StoreLogs([]*raft.Log{l}) }

// StoreLogs stores the entries, then reports each command entry. Only the
// index and the decoded op and operationId leave here, never the entry.
func (w *seamLogStore) StoreLogs(logs []*raft.Log) error {
	if err := w.LogStore.StoreLogs(logs); err != nil {
		return err
	}
	h := w.seams.load()
	if h == nil || h.afterAppend == nil {
		return nil
	}
	for _, l := range logs {
		if l.Type != raft.LogCommand {
			continue
		}
		op, opID, err := fsm.Describe(l.Data)
		if err != nil {
			continue // not ours to judge; Apply reports bad entries
		}
		h.afterAppend(appendedEntry{Index: l.Index, Op: op, OperationID: opID})
	}
	return nil
}

// pausingReader reads an InstallSnapshot stream and calls pause once half
// of it has been read; an error from pause fails the read.
type pausingReader struct {
	r         io.Reader
	n, size   int64
	raftIndex uint64
	pause     func(streamPoint) error
	paused    bool
}

func (p *pausingReader) Read(b []byte) (int, error) {
	half := p.size / 2
	if !p.paused && p.n >= half {
		p.paused = true
		if err := p.pause(streamPoint{RaftIndex: p.raftIndex, Bytes: p.n, Size: p.size}); err != nil {
			return 0, fmt.Errorf("raftstore: install snapshot: %w", err)
		}
	}
	if !p.paused && p.n+int64(len(b)) > half {
		b = b[:half-p.n]
	}
	k, err := p.r.Read(b)
	p.n += int64(k)
	return k, err
}

// copyWithPause copies size bytes from r to w, calling pause (if set)
// once half of them are through.
func copyWithPause(w io.Writer, r io.Reader, size int64, pause func(streamPoint) error, at streamPoint) (int64, error) {
	if pause == nil {
		return io.Copy(w, io.LimitReader(r, size))
	}
	n, err := io.CopyN(w, r, size/2)
	if err != nil {
		return n, err
	}
	at.Bytes, at.Size = n, size
	if err := pause(at); err != nil {
		return n, err
	}
	m, err := io.Copy(w, io.LimitReader(r, size-n))
	return n + m, err
}
