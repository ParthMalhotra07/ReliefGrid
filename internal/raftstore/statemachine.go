package raftstore

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Reader is the read surface of one shard's FSM. *fsm.FSM implements it.
type Reader interface {
	ReadRecord(ctx context.Context, key string) (store.Record, error)
	ReadClosure(ctx context.Context, resourceKey, attemptID string) (store.Closure, error)
	Scan(ctx context.Context, q store.ScanQuery, after string) ([]store.Record, error)
	PendingOutbox(ctx context.Context, limit int) ([]store.OutboxEvent, error)
	Operation(ctx context.Context, operationID string) (store.Result, error)
	Dump(ctx context.Context) (map[string][]string, error)
	Counts(ctx context.Context) (fsm.Counts, error)
}

var _ Reader = (*fsm.FSM)(nil)

const (
	snapshotFormat = 1
	maxHeaderBytes = 4096

	// File name prefixes the state machine owns in its directory.
	dbPrefix      = "fsm-"      // fsm-<gen>.db and its -wal/-shm
	snapPrefix    = "snapshot-" // VACUUM INTO output, removed by Release
	restorePrefix = "restore-"  // incoming snapshot being staged
)

// stateMachine adapts internal/fsm to raft.FSM.
//
// Raft calls Apply, Snapshot and Restore on its single FSM goroutine, so they
// never overlap. Reads come from other goroutines and hold mu for reading;
// Restore holds it for writing while it swaps in the new database, so no read
// sees a closed handle or a half-installed image (v3.1 §16).
//
// Apply returns a store.Result. An FSM infrastructure error (SQLite I/O)
// panics: continuing would let this replica diverge, so it fails stop and is
// rebuilt from the snapshot and log on restart.
//
// The open database is dir/fsm-<gen>.db; each Restore installs the next
// generation and removes the previous one with its -wal and -shm files.
//
// Snapshot runs VACUUM INTO a new file in dir and hands that file to Persist,
// which streams it to the sink behind a header carrying the group, shard,
// applied index, size and SHA-256. Restore stages the stream into a new file
// in dir, checks the header, checksum and schema, then swaps it in.
type stateMachine struct {
	dir   string
	group string
	shard int

	mu  sync.RWMutex
	f   *fsm.FSM
	gen uint64 // generation of the open database file in dir

	applied   atomic.Uint64 // copy of f.AppliedIndex() readable without mu
	restored  atomic.Uint64 // applied index of the last snapshot restored (0: none)
	appliedMu sync.Mutex
	appliedCh chan struct{} // closed and replaced whenever the applied index moves

	// closing is closed when the node starts to close: the applied index
	// will not move again, so waitApplied stops waiting.
	closing     chan struct{}
	closingOnce sync.Once

	snapSeq     atomic.Uint64
	beforeApply atomic.Pointer[func(index uint64)] // tests only: runs before each command
	beforeSwap  func()                             // tests only: runs in Restore before the swap; set before Raft starts
	seams       *seamState                         // tests and the seams build only; nil otherwise (seams.go)
}

var _ raft.FSM = (*stateMachine)(nil)

// snapshotHeader precedes the database image in every snapshot.
type snapshotHeader struct {
	Format       int    `json:"format"`
	GroupID      string `json:"groupId"`
	Shard        int    `json:"shard"`
	AppliedIndex uint64 `json:"appliedIndex"`
	Size         int64  `json:"size"`
	SHA256       string `json:"sha256"`
}

// newStateMachine removes every FSM file in dir and opens a fresh database
// there with WAL and synchronous=OFF (M3).
func newStateMachine(ctx context.Context, dir, group string, shard int) (*stateMachine, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("raftstore: create fsm dir: %w", err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("raftstore: read fsm dir: %w", err)
	}
	for _, e := range ents {
		name := e.Name()
		if strings.HasPrefix(name, dbPrefix) || strings.HasPrefix(name, snapPrefix) || strings.HasPrefix(name, restorePrefix) {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, fmt.Errorf("raftstore: remove derived fsm file: %w", err)
			}
		}
	}
	s := &stateMachine{dir: dir, group: group, shard: shard, appliedCh: make(chan struct{}), closing: make(chan struct{})}
	f, err := fsm.Open(ctx, "file:"+s.dbPath(0))
	if err != nil {
		return nil, err
	}
	s.f = f
	return s, nil
}

func (s *stateMachine) dbPath(gen uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s%d.db", dbPrefix, gen))
}

func (s *stateMachine) Apply(l *raft.Log) any {
	if fn := s.beforeApply.Load(); fn != nil && *fn != nil {
		(*fn)(l.Index)
	}
	// Only Restore replaces s.f, and it runs on this goroutine.
	res, err := s.f.Apply(l.Index, l.Data)
	if err != nil {
		panic(fmt.Sprintf("raftstore: FSM apply failed, stopping to avoid divergence: %v", err))
	}
	res.Shard = s.shard
	s.setApplied(l.Index)
	return res
}

func (s *stateMachine) setApplied(index uint64) {
	s.applied.Store(index)
	s.appliedMu.Lock()
	close(s.appliedCh)
	s.appliedCh = make(chan struct{})
	s.appliedMu.Unlock()
}

func (s *stateMachine) Snapshot() (raft.FSMSnapshot, error) {
	path := filepath.Join(s.dir, fmt.Sprintf("%s%d.db", snapPrefix, s.snapSeq.Add(1)))
	if err := s.f.VacuumInto(context.Background(), path); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	sn := &fsmSnapshot{path: path, header: snapshotHeader{
		Format: snapshotFormat, GroupID: s.group, Shard: s.shard, AppliedIndex: s.f.AppliedIndex(),
	}}
	if h := s.seams.load(); h != nil {
		sn.pause = h.persistPause
	}
	return sn, nil
}

// fsmSnapshot owns one immutable VACUUM INTO file; Persist may run while
// later commands apply.
type fsmSnapshot struct {
	path   string
	header snapshotHeader
	pause  func(streamPoint) error // test seam: halfway through the image; nil otherwise
}

func (sn *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := sn.persist(sink); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (sn *fsmSnapshot) persist(w io.Writer) error {
	f, err := os.Open(sn.path)
	if err != nil {
		return fmt.Errorf("raftstore: open snapshot image: %w", err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return fmt.Errorf("raftstore: hash snapshot image: %w", err)
	}
	header := sn.header
	header.Size, header.SHA256 = size, hex.EncodeToString(h.Sum(nil))
	b, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("raftstore: encode snapshot header: %w", err)
	}
	if _, err := w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("raftstore: write snapshot header: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("raftstore: rewind snapshot image: %w", err)
	}
	var n int64
	if sn.pause == nil {
		n, err = io.Copy(w, f)
	} else {
		n, err = copyWithPause(w, f, size, sn.pause, streamPoint{ImageIndex: header.AppliedIndex})
	}
	if err != nil {
		return fmt.Errorf("raftstore: write snapshot image (%d of %d bytes): %w", n, size, err)
	}
	if n != size {
		return fmt.Errorf("raftstore: snapshot image changed while writing: %d of %d bytes", n, size)
	}
	return nil
}

func (sn *fsmSnapshot) Release() { _ = os.Remove(sn.path) }

// readSnapshotHeader reads the header line at the start of a snapshot stream.
func readSnapshotHeader(br *bufio.Reader) (snapshotHeader, error) {
	line, err := br.ReadSlice('\n')
	if err != nil {
		return snapshotHeader{}, fmt.Errorf("raftstore: read snapshot header: %w", err)
	}
	var h snapshotHeader
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&h); err != nil {
		return snapshotHeader{}, fmt.Errorf("raftstore: decode snapshot header: %w", err)
	}
	if h.Format != snapshotFormat {
		return snapshotHeader{}, fmt.Errorf("raftstore: snapshot format %d, want %d", h.Format, snapshotFormat)
	}
	return h, nil
}

func (s *stateMachine) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	br := bufio.NewReaderSize(rc, maxHeaderBytes)
	h, err := readSnapshotHeader(br)
	if err != nil {
		return err
	}
	if h.GroupID != s.group || h.Shard != s.shard {
		return fmt.Errorf("raftstore: snapshot of %s shard %d, this node is %s shard %d", h.GroupID, h.Shard, s.group, s.shard)
	}

	gen := s.gen + 1
	staged := filepath.Join(s.dir, fmt.Sprintf("%s%d.db", restorePrefix, gen))
	final := s.dbPath(gen)
	next, err := s.stage(br, h, staged, final)
	if err != nil {
		_ = os.Remove(staged)
		removeDB(final)
		return err
	}

	if s.beforeSwap != nil {
		s.beforeSwap()
	}
	s.mu.Lock() // waits for in-flight reads on the old database
	old, oldGen := s.f, s.gen
	s.f, s.gen = next, gen
	s.mu.Unlock()
	s.restored.Store(h.AppliedIndex)
	s.setApplied(next.AppliedIndex())
	slog.Info("raftstore: restored snapshot", "group", s.group, "shard", s.shard, "appliedIndex", h.AppliedIndex, "gen", gen)

	// The new database is live; failing Restore now would make Raft treat
	// the install as failed while reads already see it.
	if err := old.Close(); err != nil {
		slog.Warn("raftstore: close replaced fsm", "gen", oldGen, "err", err)
	}
	removeDB(s.dbPath(oldGen))
	return nil
}

// stage copies the image into staged while checking its size and checksum,
// renames it to final and opens it.
func (s *stateMachine) stage(r io.Reader, h snapshotHeader, staged, final string) (*fsm.FSM, error) {
	out, err := os.OpenFile(staged, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("raftstore: create staged snapshot: %w", err)
	}
	sum := sha256.New()
	var pause func(streamPoint) error
	if hs := s.seams.load(); hs != nil {
		pause = hs.stagePause
	}
	n, cerr := copyWithPause(io.MultiWriter(out, sum), r, h.Size, pause, streamPoint{ImageIndex: h.AppliedIndex})
	if err := errors.Join(cerr, out.Close()); err != nil {
		return nil, fmt.Errorf("raftstore: stage snapshot: %w", err)
	}
	if n != h.Size {
		return nil, fmt.Errorf("raftstore: snapshot image truncated: %d of %d bytes", n, h.Size)
	}
	var extra [1]byte
	if k, _ := r.Read(extra[:]); k != 0 {
		return nil, errors.New("raftstore: trailing data after snapshot image")
	}
	if got := hex.EncodeToString(sum.Sum(nil)); got != h.SHA256 {
		return nil, fmt.Errorf("raftstore: snapshot checksum %s, header says %s", got, h.SHA256)
	}
	if err := os.Rename(staged, final); err != nil {
		return nil, fmt.Errorf("raftstore: install staged snapshot: %w", err)
	}
	f, err := fsm.OpenImage(context.Background(), final)
	if err != nil {
		return nil, err
	}
	if f.AppliedIndex() != h.AppliedIndex {
		_ = f.Close()
		return nil, fmt.Errorf("raftstore: snapshot image at index %d, header says %d", f.AppliedIndex(), h.AppliedIndex)
	}
	return f, nil
}

func removeDB(path string) {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		_ = os.Remove(p)
	}
}

// restoredIndex is the applied index of the last snapshot restored since the
// state machine was created; 0 if none.
func (s *stateMachine) restoredIndex() uint64 { return s.restored.Load() }

// appliedIndex is the index of the last command SQLite has committed.
func (s *stateMachine) appliedIndex() uint64 { return s.applied.Load() }

// appliedChanged is closed the next time the applied index moves.
func (s *stateMachine) appliedChanged() <-chan struct{} {
	s.appliedMu.Lock()
	defer s.appliedMu.Unlock()
	return s.appliedCh
}

// errClosing is waitApplied's answer once the node has begun to close.
var errClosing = errors.New("raftstore: node closing")

// waitApplied blocks until appliedIndex() >= index, ctx ends or the node
// begins to close.
func (s *stateMachine) waitApplied(ctx context.Context, index uint64) error {
	for {
		ch := s.appliedChanged()
		if s.appliedIndex() >= index {
			return nil
		}
		select {
		case <-ch:
		case <-s.closing:
			return fmt.Errorf("raftstore: fsm at %d, waiting for %d: %w", s.appliedIndex(), index, errClosing)
		case <-ctx.Done():
			return fmt.Errorf("raftstore: fsm at %d, waiting for %d: %w", s.appliedIndex(), index, ctx.Err())
		}
	}
}

// beginClose wakes every waitApplied; the node is closing. Idempotent.
func (s *stateMachine) beginClose() { s.closingOnce.Do(func() { close(s.closing) }) }

// read runs fn against the current database, holding off Restore until fn returns.
func (s *stateMachine) read(fn func(f *fsm.FSM) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fn(s.f)
}

func (s *stateMachine) setBeforeApply(fn func(index uint64)) { s.beforeApply.Store(&fn) }

func (s *stateMachine) close() error {
	s.beginClose()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
