package memstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store/storetest"
	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// newSeamStore is a one-shard store with checkpoints in a fresh directory.
func newSeamStore(t *testing.T) (*memstore.Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := memstore.New(context.Background(), memstore.WithShards(1), memstore.WithCheckpointDir(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func casOn(t *testing.T, s store.Store, op, key string, expected uint64, value int) store.Result {
	t.Helper()
	res, err := s.CompareAndSet(testCtx(t), store.CompareAndSet{Meta: store.Meta{OperationID: op, At: t0}, Key: key, ExpectedVersion: expected, Value: json.RawMessage(fmt.Sprint(value))})
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return res
}

func writeKeys(t *testing.T, s store.Store, prefix string, n int) store.Result {
	t.Helper()
	var last store.Result
	for i := range n {
		if last = casOn(t, s, fmt.Sprintf("op-%s%d", prefix, i), fmt.Sprintf("kv:%s%d", prefix, i), 0, i); last.Outcome != store.OutcomeOK {
			t.Fatalf("%s%d: %+v", prefix, i, last)
		}
	}
	return last
}

func dump(t *testing.T, s *memstore.Store) map[string][]string {
	t.Helper()
	d, err := s.Dump(testCtx(t), 0)
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	return d
}

type casResult struct {
	res store.Result
	err error
}

func goCAS(s store.Store, op string, expected uint64, value int) <-chan casResult {
	done := make(chan casResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := s.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: op, At: t0}, Key: "kv:x", ExpectedVersion: expected, Value: json.RawMessage(fmt.Sprint(value))})
		done <- casResult{res, err}
	}()
	return done
}

// SCH01 on M8: A is proposed first; A and B are both in the shard's log
// before either applies; on Release A wins and B is a VERSION_CONFLICT.
func TestLogBarrierBothAppendedBeforeEitherApplies(t *testing.T) {
	s, _ := newSeamStore(t)
	if r := casOn(t, s, "create", "kv:x", 0, 0); r.Outcome != store.OutcomeOK {
		t.Fatalf("create: %+v", r)
	}
	b, err := s.ArmLogBarrier(0, 10*time.Second, "op-a", "op-b")
	if err != nil {
		t.Fatal(err)
	}
	a := goCAS(s, "op-a", 1, 1)
	ia, err := b.WaitAppended(testCtx(t), "op-a")
	if err != nil {
		t.Fatal(err)
	}
	bb := goCAS(s, "op-b", 1, 2)
	p, err := b.Hold().WaitReached(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if p.Indexes["op-a"] != ia || p.Indexes["op-b"] <= ia {
		t.Fatalf("indexes %v: want op-a at %d before op-b", p.Indexes, ia)
	}
	if p.AppliedIndex >= ia || p.CommitIndex < p.Indexes["op-b"] {
		t.Fatalf("point %+v: want both committed and neither applied", p)
	}
	b.Hold().Release()
	ra, rb := <-a, <-bb
	if ra.err != nil || ra.res.Outcome != store.OutcomeOK || ra.res.Version != 2 {
		t.Fatalf("A: %+v, %v; want OK at version 2", ra.res, ra.err)
	}
	if rb.err != nil || rb.res.Outcome != store.OutcomeVersionConflict {
		t.Fatalf("B: %+v, %v; want VERSION_CONFLICT", rb.res, rb.err)
	}
}

// SCH12 on M8: a checkpoint dies halfway through writing its image. Restart
// discards the partial file, restores the last complete checkpoint and
// replays the log after it; the rows are those before the crash.
func TestCheckpointCrashMidWriteRestartsFromLastComplete(t *testing.T) {
	s, dir := newSeamStore(t)
	writeKeys(t, s, "a", 4)
	prior, err := s.Checkpoint(testCtx(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	complete := entries(t, dir)
	last := writeKeys(t, s, "b", 4) // the tail Restart must replay
	before := dump(t, s)

	h, err := s.ArmCheckpointPause(0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ckpt := make(chan error, 1)
	go func() { _, err := s.Checkpoint(context.Background(), 0); ckpt <- err }()
	p, err := h.WaitReached(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if p.ImageIndex != last.AppliedIndex || p.Bytes <= 0 || p.Bytes >= p.Size {
		t.Fatalf("paused at %+v: want a partial image at applied index %d", p, last.AppliedIndex)
	}
	partial := entries(t, dir)
	if err := s.Crash(0); err != nil {
		t.Fatal(err)
	}
	if err := <-ckpt; err == nil {
		t.Fatal("the checkpoint that died mid-write reported success")
	}
	if !reflect.DeepEqual(entries(t, dir), partial) {
		t.Fatal("Crash cleaned up after the dying checkpoint; a crash leaves its files")
	}
	if _, err := s.ReadRecord(testCtx(t), "kv:a0"); !errors.Is(err, memstore.ErrShardDown) {
		t.Fatalf("read of a crashed shard: %v, want ErrShardDown", err)
	}

	info, err := s.Restart(testCtx(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.RestoredIndex != prior.AppliedIndex || info.AppliedIndex != last.AppliedIndex {
		t.Fatalf("restart %+v: want restored %d, applied %d", info, prior.AppliedIndex, last.AppliedIndex)
	}
	if got := dump(t, s); !reflect.DeepEqual(got, before) {
		t.Fatal("rows after restart differ from those before the crash")
	}
	if len(partial) <= len(complete) {
		t.Fatalf("checkpoint dir %v at the pause: no partial file was written", partial)
	}
	if got := entries(t, dir); !reflect.DeepEqual(got, complete) {
		t.Fatalf("checkpoint dir %v after restart, want only %v: the partial checkpoint survived", got, complete)
	}
	if r := casOn(t, s, "op-after", "kv:a0", 1, 9); r.Outcome != store.OutcomeOK {
		t.Fatalf("write after restart: %+v", r)
	}
}

// SCH13b on M8: a restart dies halfway through staging its checkpoint; the
// next restart installs the complete checkpoint and replays the log.
func TestRestoreCrashMidStageRestartsComplete(t *testing.T) {
	s, _ := newSeamStore(t)
	writeKeys(t, s, "a", 4)
	prior, err := s.Checkpoint(testCtx(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	last := writeKeys(t, s, "b", 4)
	before := dump(t, s)

	h, err := s.ArmRestorePause(0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Crash(0); err != nil {
		t.Fatal(err)
	}
	restart := make(chan error, 1)
	go func() { _, err := s.Restart(context.Background(), 0); restart <- err }()
	p, err := h.WaitReached(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if p.ImageIndex != prior.AppliedIndex || p.Bytes <= 0 || p.Bytes >= p.Size {
		t.Fatalf("paused at %+v: want a partial stage of the checkpoint at %d", p, prior.AppliedIndex)
	}
	if err := s.Crash(0); err != nil {
		t.Fatal(err)
	}
	if err := <-restart; err == nil {
		t.Fatal("the restart that died mid-stage reported success")
	}
	if _, err := s.ReadRecord(testCtx(t), "kv:a0"); !errors.Is(err, memstore.ErrShardDown) {
		t.Fatalf("read after the dying restart: %v, want ErrShardDown", err)
	}

	info, err := s.Restart(testCtx(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.RestoredIndex != prior.AppliedIndex || info.AppliedIndex != last.AppliedIndex {
		t.Fatalf("restart %+v: want restored %d, applied %d", info, prior.AppliedIndex, last.AppliedIndex)
	}
	if got := dump(t, s); !reflect.DeepEqual(got, before) {
		t.Fatal("rows after the second restart differ from those before the crash")
	}
}

// A boundary nobody reaches is a failure, not a pass (#88).
func TestCheckpointPauseNeverReachedIsAnError(t *testing.T) {
	s, _ := newSeamStore(t)
	h, err := s.ArmCheckpointPause(0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := h.WaitReached(ctx); !errors.Is(err, testseam.ErrNotReached) {
		t.Fatalf("WaitReached with no checkpoint: %v, want ErrNotReached", err)
	}
}

// Seams that are enabled but not armed change nothing: the store contract
// holds with checkpoints enabled.
func TestStoreContractWithCheckpointsEnabled(t *testing.T) {
	s, err := memstore.New(context.Background(), memstore.WithCheckpointDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Checkpoint(testCtx(t), 0); err != nil {
		t.Fatalf("Checkpoint of an empty shard: %v", err)
	}
	storetest.Run(t, func(*testing.T) store.Store { return s })
}

func entries(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}
