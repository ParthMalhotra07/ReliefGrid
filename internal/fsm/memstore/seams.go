package memstore

import (
	"bufio"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// Test seams for the adversarial schedules (S4.6b, #88). M8 has no Raft: a
// shard's "log" is the sequence of entries handed to its FSM, its
// "snapshot" a checkpoint file (VACUUM INTO plus a header) in the directory
// given by WithCheckpointDir, and a crash drops the shard's FSM so that
// Restart rebuilds it from the last complete checkpoint and the retained log.
//
// This models raftstore's snapshot and restore with the same FSM image
// code; it is not Raft's snapshot store, which the raftstore tests and the
// kind runner exercise.

// ErrShardDown is returned for a shard between Crash and a completed Restart.
var ErrShardDown = errors.New("memstore: shard down")

// WithCheckpointDir enables Checkpoint, Crash and Restart, keeping
// checkpoint files in dir and every shard's log since its last checkpoint.
func WithCheckpointDir(dir string) Option { return func(c *config) { c.checkpointDir = dir } }

// CheckpointInfo describes a complete checkpoint.
type CheckpointInfo struct{ AppliedIndex uint64 }

// RestartInfo is what Restart rebuilt: the checkpoint it restored (0: none)
// and the applied index after replaying the log.
type RestartInfo struct{ RestoredIndex, AppliedIndex uint64 }

// Checkpoint file names in the checkpoint directory. A complete checkpoint
// is renamed into place; a ".part" file is one a crash interrupted.
const (
	ckptPrefix = "checkpoint-"
	ckptSuffix = ".ckpt"
	partSuffix = ".part"
)

// ckptHeader precedes the image in a checkpoint file.
type ckptHeader struct {
	Shard        int    `json:"shard"`
	AppliedIndex uint64 `json:"appliedIndex"`
	Size         int64  `json:"size"`
	SHA256       string `json:"sha256"`
}

func (s *Store) shardArg(shard int) (*shard, error) {
	if shard < 0 || shard >= len(s.shards) {
		return nil, fmt.Errorf("memstore: no shard %d", shard)
	}
	if s.checkpointDir == "" {
		return nil, errors.New("memstore: seams need WithCheckpointDir")
	}
	return s.shards[shard], nil
}

// ArmLogBarrier holds the first of opIDs before it applies on shard until
// every listed command is in the shard's log (SCH01). In M8 an appended
// entry is committed.
func (s *Store) ArmLogBarrier(shard int, timeout time.Duration, opIDs ...string) (*testseam.Barrier, error) {
	if shard < 0 || shard >= len(s.shards) {
		return nil, fmt.Errorf("memstore: no shard %d", shard)
	}
	if len(opIDs) < 2 || len(slices.Compact(slices.Sorted(slices.Values(opIDs)))) != len(opIDs) {
		return nil, fmt.Errorf("memstore: log barrier needs two or more distinct operationIds, got %v", opIDs)
	}
	sh := s.shards[shard]
	position := func(f func() uint64) func() uint64 {
		return func() uint64 {
			sh.mu.RLock()
			defer sh.mu.RUnlock()
			return f()
		}
	}
	b := testseam.NewBarrier(timeout, position(func() uint64 { return sh.index }), position(func() uint64 { return sh.applied }), opIDs...)
	sh.mu.Lock()
	sh.barrier = b
	sh.mu.Unlock()
	return b, nil
}

// ArmCheckpointPause pauses the next Checkpoint of shard halfway through
// writing its image (SCH12).
func (s *Store) ArmCheckpointPause(shard int, timeout time.Duration) (*testseam.Hold, error) {
	sh, err := s.shardArg(shard)
	if err != nil {
		return nil, err
	}
	h := testseam.NewHold(testseam.SnapshotPersist, timeout)
	sh.mu.Lock()
	sh.ckptPause = h
	sh.mu.Unlock()
	return h, nil
}

// ArmRestorePause pauses the next Restart of shard halfway through staging
// its checkpoint (SCH13b).
func (s *Store) ArmRestorePause(shard int, timeout time.Duration) (*testseam.Hold, error) {
	sh, err := s.shardArg(shard)
	if err != nil {
		return nil, err
	}
	h := testseam.NewHold(testseam.RestoreStage, timeout)
	sh.mu.Lock()
	sh.restorePause = h
	sh.mu.Unlock()
	return h, nil
}

// Checkpoint writes a complete checkpoint of shard: the image at its
// applied index, behind a header, renamed into place once whole. The log
// up to that index is then dropped.
func (s *Store) Checkpoint(ctx context.Context, shard int) (CheckpointInfo, error) {
	sh, err := s.shardArg(shard)
	if err != nil {
		return CheckpointInfo{}, err
	}
	sh.mu.Lock()
	if sh.down {
		sh.mu.Unlock()
		return CheckpointInfo{}, fmt.Errorf("memstore: shard %d: %w", shard, ErrShardDown)
	}
	sh.ckptSeq++
	scratch := filepath.Join(s.workDir, fmt.Sprintf("vacuum-%d-%d.db", shard, sh.ckptSeq))
	// Under the write lock no command is applying: the image is exactly
	// the state at sh.applied.
	applied, gen := sh.applied, sh.gen
	err = sh.f.VacuumInto(ctx, scratch)
	pause := sh.ckptPause
	sh.ckptPause = nil
	if pause != nil {
		sh.paused = append(sh.paused, pause)
	}
	sh.mu.Unlock()
	if err != nil {
		_ = os.Remove(scratch)
		return CheckpointInfo{}, err
	}

	final := filepath.Join(s.checkpointDir, fmt.Sprintf("%s%d-%020d%s", ckptPrefix, shard, applied, ckptSuffix))
	part := final + partSuffix
	if err := writeCheckpoint(scratch, part, ckptHeader{Shard: shard, AppliedIndex: applied}, pause); err != nil {
		if !errors.Is(err, testseam.ErrAborted) { // a crash leaves its files
			_ = os.Remove(part)
			_ = os.Remove(scratch)
		}
		return CheckpointInfo{}, err
	}
	_ = os.Remove(scratch)

	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.gen != gen {
		return CheckpointInfo{}, fmt.Errorf("memstore: shard %d crashed during the checkpoint", shard)
	}
	if err := os.Rename(part, final); err != nil {
		return CheckpointInfo{}, fmt.Errorf("memstore: install checkpoint: %w", err)
	}
	old, err := checkpoints(s.checkpointDir, shard)
	if err != nil {
		return CheckpointInfo{}, err
	}
	for _, c := range old {
		if c.path != final {
			_ = os.Remove(c.path)
		}
	}
	keep := sh.log[:0]
	for _, e := range sh.log {
		if e.index > applied {
			keep = append(keep, e)
		}
	}
	sh.log = keep
	return CheckpointInfo{AppliedIndex: applied}, nil
}

// writeCheckpoint writes header and the image at scratch to path, pausing
// at pause (if set) halfway through the image.
func writeCheckpoint(scratch, path string, h ckptHeader, pause *testseam.Hold) error {
	img, err := os.ReadFile(scratch)
	if err != nil {
		return fmt.Errorf("memstore: read checkpoint image: %w", err)
	}
	sum := sha256.Sum256(img)
	h.Size, h.SHA256 = int64(len(img)), hex.EncodeToString(sum[:])
	head, err := json.Marshal(h)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("memstore: create checkpoint: %w", err)
	}
	half := len(img) / 2
	if _, err := out.Write(append(head, '\n')); err != nil {
		_ = out.Close()
		return err
	}
	if _, err := out.Write(img[:half]); err != nil {
		_ = out.Close()
		return err
	}
	if pause != nil {
		if err := pause.Reach(testseam.Point{ImageIndex: h.AppliedIndex, Bytes: int64(half), Size: h.Size}); err != nil {
			_ = out.Close()
			return fmt.Errorf("memstore: checkpoint: %w", err)
		}
	}
	if _, err := out.Write(img[half:]); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

type ckptFile struct {
	path    string
	applied uint64
}

// checkpoints lists shard's complete checkpoints, oldest first.
func checkpoints(dir string, shard int) ([]ckptFile, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("memstore: read checkpoint dir: %w", err)
	}
	prefix := fmt.Sprintf("%s%d-", ckptPrefix, shard)
	var out []ckptFile
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ckptSuffix) {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, prefix), ckptSuffix), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, ckptFile{path: filepath.Join(dir, name), applied: n})
	}
	slices.SortFunc(out, func(a, b ckptFile) int { return cmp.Compare(a.applied, b.applied) })
	return out, nil
}

// Crash kills shard: its FSM is dropped, a checkpoint or restart paused at a
// seam stops where it is (leaving its partial file, as a dying process
// would), and calls fail with ErrShardDown until Restart. Commands appended
// but not yet applied stay in the log; their callers see an unknown outcome.
func (s *Store) Crash(shard int) error {
	sh, err := s.shardArg(shard)
	if err != nil {
		return err
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.f != nil {
		_ = sh.f.Close()
		sh.f = nil
	}
	sh.down = true
	sh.gen++
	sh.turn.Broadcast()
	for _, h := range sh.paused {
		h.Abort()
	}
	sh.paused = nil
	return nil
}

// Restart rebuilds a crashed shard from its last complete checkpoint and
// replays the log after it. Like a node's Open, it first removes what a
// crash left: partial checkpoints and staged databases.
func (s *Store) Restart(ctx context.Context, shard int) (RestartInfo, error) {
	sh, err := s.shardArg(shard)
	if err != nil {
		return RestartInfo{}, err
	}
	sh.mu.Lock()
	if !sh.down {
		sh.mu.Unlock()
		return RestartInfo{}, fmt.Errorf("memstore: shard %d is running", shard)
	}
	gen := sh.gen
	pause := sh.restorePause
	sh.restorePause = nil
	if pause != nil {
		sh.paused = append(sh.paused, pause)
	}
	sh.mu.Unlock()

	if err := s.removeCrashLeftovers(shard); err != nil {
		return RestartInfo{}, err
	}
	all, err := checkpoints(s.checkpointDir, shard)
	if err != nil {
		return RestartInfo{}, err
	}
	var (
		f        *fsm.FSM
		restored uint64
	)
	if len(all) == 0 {
		f, err = fsm.Open(ctx, fmt.Sprintf("file:memstore-%d-shard%d-gen%d?mode=memory", s.id, shard, gen))
	} else {
		latest := all[len(all)-1]
		f, err = s.stage(ctx, shard, gen, latest, pause)
		restored = latest.applied
	}
	if err != nil {
		return RestartInfo{}, err
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.gen != gen {
		_ = f.Close()
		return RestartInfo{}, fmt.Errorf("memstore: shard %d crashed during restart", shard)
	}
	for _, e := range sh.log {
		if e.index <= restored {
			continue
		}
		if _, err := f.Apply(e.index, e.entry); err != nil {
			_ = f.Close()
			return RestartInfo{}, fmt.Errorf("memstore: replay index %d: %w", e.index, err)
		}
	}
	sh.f, sh.down = f, false
	sh.applied = f.AppliedIndex()
	sh.index = max(sh.index, sh.applied)
	sh.paused = nil
	sh.turn.Broadcast()
	return RestartInfo{RestoredIndex: restored, AppliedIndex: sh.applied}, nil
}

// removeCrashLeftovers removes shard's partial checkpoints and every staged
// or scratch database of the shard.
func (s *Store) removeCrashLeftovers(shard int) error {
	for _, d := range []struct{ dir, prefix, suffix string }{
		{s.checkpointDir, fmt.Sprintf("%s%d-", ckptPrefix, shard), partSuffix},
		{s.workDir, fmt.Sprintf("restore-%d-", shard), ""},
		{s.workDir, fmt.Sprintf("vacuum-%d-", shard), ""},
	} {
		ents, err := os.ReadDir(d.dir)
		if err != nil {
			return fmt.Errorf("memstore: read %s: %w", d.dir, err)
		}
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), d.prefix) && strings.HasSuffix(e.Name(), d.suffix) {
				if err := os.Remove(filepath.Join(d.dir, e.Name())); err != nil {
					return fmt.Errorf("memstore: remove crash leftover: %w", err)
				}
			}
		}
	}
	return nil
}

// stage copies c's image into a new database in the work directory,
// checking its size and checksum, pausing at pause (if set) halfway, and
// opens it.
func (s *Store) stage(ctx context.Context, shard int, gen uint64, c ckptFile, pause *testseam.Hold) (*fsm.FSM, error) {
	in, err := os.Open(c.path)
	if err != nil {
		return nil, fmt.Errorf("memstore: open checkpoint: %w", err)
	}
	defer func() { _ = in.Close() }()
	br := bufio.NewReader(in)
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("memstore: checkpoint header: %w", err)
	}
	var h ckptHeader
	if err := json.Unmarshal(line, &h); err != nil {
		return nil, fmt.Errorf("memstore: checkpoint header: %w", err)
	}
	if h.Shard != shard || h.AppliedIndex != c.applied {
		return nil, fmt.Errorf("memstore: checkpoint %s has shard %d at %d", c.path, h.Shard, h.AppliedIndex)
	}
	staged := filepath.Join(s.workDir, fmt.Sprintf("restore-%d-%d.db", shard, gen))
	out, err := os.OpenFile(staged, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("memstore: create staged checkpoint: %w", err)
	}
	sum := sha256.New()
	w := io.MultiWriter(out, sum)
	half := h.Size / 2
	n, err := io.CopyN(w, br, half)
	if err == nil && pause != nil {
		err = pause.Reach(testseam.Point{ImageIndex: h.AppliedIndex, Bytes: n, Size: h.Size})
	}
	if err == nil {
		var m int64
		m, err = io.Copy(w, io.LimitReader(br, h.Size-half))
		n += m
	}
	if err = errors.Join(err, out.Close()); err != nil {
		return nil, fmt.Errorf("memstore: stage checkpoint: %w", err) // a crash leaves the staged file
	}
	if n != h.Size || hex.EncodeToString(sum.Sum(nil)) != h.SHA256 {
		return nil, fmt.Errorf("memstore: checkpoint %s is damaged (%d of %d bytes)", c.path, n, h.Size)
	}
	f, err := fsm.OpenImage(ctx, staged)
	if err != nil {
		return nil, err
	}
	if f.AppliedIndex() != h.AppliedIndex {
		_ = f.Close()
		return nil, fmt.Errorf("memstore: checkpoint image at %d, header says %d", f.AppliedIndex(), h.AppliedIndex)
	}
	return f, nil
}

// Dump returns shard's rows (fsm.FSM.Dump).
func (s *Store) Dump(ctx context.Context, shard int) (map[string][]string, error) {
	if shard < 0 || shard >= len(s.shards) {
		return nil, fmt.Errorf("memstore: no shard %d", shard)
	}
	var out map[string][]string
	err := s.read(shard, func(f *fsm.FSM) (err error) { out, err = f.Dump(ctx); return err })
	return out, err
}
