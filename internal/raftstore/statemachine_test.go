package raftstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const t0 = int64(1_792_000_000_000)

func cas(op, key string, expected uint64, value int) store.CompareAndSet {
	return store.CompareAndSet{Meta: store.Meta{OperationID: op, At: t0}, Key: key, ExpectedVersion: expected,
		Value: json.RawMessage(fmt.Sprint(value))}
}

func newTestStateMachine(t *testing.T, group string, shard int) *stateMachine {
	t.Helper()
	sm, err := newStateMachine(context.Background(), t.TempDir(), group, shard)
	if err != nil {
		t.Fatalf("newStateMachine: %v", err)
	}
	t.Cleanup(func() { _ = sm.close() })
	return sm
}

func smApply(t *testing.T, sm *stateMachine, index uint64, body any) store.Result {
	t.Helper()
	entry, err := fsm.Encode(body)
	if err != nil {
		t.Fatal(err)
	}
	res, ok := sm.Apply(&raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: entry}).(store.Result)
	if !ok {
		t.Fatalf("Apply(%d) did not return a store.Result", index)
	}
	return res
}

func smDump(t *testing.T, sm *stateMachine) map[string][]string {
	t.Helper()
	var out map[string][]string
	if err := sm.read(func(f *fsm.FSM) error {
		var err error
		out, err = f.Dump(context.Background())
		return err
	}); err != nil {
		t.Fatalf("Dump: %v", err)
	}
	return out
}

// memSink is an in-memory raft.SnapshotSink.
type memSink struct {
	bytes.Buffer
	closed, cancelled bool
}

func (s *memSink) ID() string    { return "mem" }
func (s *memSink) Close() error  { s.closed = true; return nil }
func (s *memSink) Cancel() error { s.cancelled = true; return nil }

func takeSnapshot(t *testing.T, sm *stateMachine) []byte {
	t.Helper()
	snap, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release()
	if !sink.closed || sink.cancelled {
		t.Fatalf("sink closed=%v cancelled=%v, want closed and not cancelled", sink.closed, sink.cancelled)
	}
	return sink.Bytes()
}

// dbFiles lists the file names in dir.
func dbFiles(t *testing.T, dir string) []string {
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

func TestApplyReturnsResultWithShard(t *testing.T) {
	sm := newTestStateMachine(t, "shard-2", 2)
	res := smApply(t, sm, 1, cas("op-1", "kv:a", 0, 1))
	if res.Outcome != store.OutcomeOK || res.Shard != 2 || res.AppliedIndex != 1 {
		t.Fatalf("result = %+v, want OK on shard 2 at index 1", res)
	}
	if sm.appliedIndex() != 1 {
		t.Fatalf("appliedIndex = %d, want 1", sm.appliedIndex())
	}
}

// Snapshot (VACUUM INTO) then Restore into another replica gives the same
// rows and applied index; the restored replica keeps applying, and the
// snapshot's temporary file is gone after Release.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	src := newTestStateMachine(t, "shard-1", 1)
	for i := 1; i <= 20; i++ {
		smApply(t, src, uint64(i), cas(fmt.Sprintf("op-%d", i), fmt.Sprintf("kv:%02d", i%7), uint64((i-1)/7), i))
	}
	image := takeSnapshot(t, src)
	if left := dbFiles(t, src.dir); len(left) > 3 { // the live db, -wal, -shm
		t.Fatalf("snapshot left files behind in %s: %v", src.dir, left)
	}

	dst := newTestStateMachine(t, "shard-1", 1)
	smApply(t, dst, 1, cas("other", "kv:zz", 0, 99)) // state the restore must replace
	if err := dst.Restore(io.NopCloser(bytes.NewReader(image))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !reflect.DeepEqual(smDump(t, src), smDump(t, dst)) {
		t.Fatal("restored rows differ from the source")
	}
	if dst.appliedIndex() != 20 {
		t.Fatalf("restored applied index = %d, want 20", dst.appliedIndex())
	}
	res := smApply(t, dst, 21, cas("op-21", "kv:00", 2, 21))
	if res.Outcome != store.OutcomeOK {
		t.Fatalf("apply after restore = %+v", res)
	}
}

// A corrupted image is refused and the replica keeps serving its old state.
func TestRestoreRejectsCorruptImage(t *testing.T) {
	src := newTestStateMachine(t, "shard-1", 1)
	for i := 1; i <= 5; i++ {
		smApply(t, src, uint64(i), cas(fmt.Sprintf("op-%d", i), fmt.Sprintf("kv:%d", i), 0, i))
	}
	image := takeSnapshot(t, src)

	dst := newTestStateMachine(t, "shard-1", 1)
	smApply(t, dst, 1, cas("keep", "kv:keep", 0, 7))
	before := smDump(t, dst)
	filesBefore := dbFiles(t, dst.dir)

	corrupt := bytes.Clone(image)
	corrupt[len(corrupt)-100] ^= 0xff
	if err := dst.Restore(io.NopCloser(bytes.NewReader(corrupt))); err == nil {
		t.Fatal("Restore accepted a corrupted image")
	}
	truncated := image[:len(image)/2]
	if err := dst.Restore(io.NopCloser(bytes.NewReader(truncated))); err == nil {
		t.Fatal("Restore accepted a truncated image")
	}
	if !reflect.DeepEqual(before, smDump(t, dst)) || dst.appliedIndex() != 1 {
		t.Fatal("a refused restore changed the live state")
	}
	if after := dbFiles(t, dst.dir); !reflect.DeepEqual(filesBefore, after) {
		t.Fatalf("a refused restore left staged files: before %v, after %v", filesBefore, after)
	}
}

func TestRestoreRejectsOtherGroup(t *testing.T) {
	src := newTestStateMachine(t, "shard-0", 0)
	smApply(t, src, 1, cas("op-1", "kv:a", 0, 1))
	image := takeSnapshot(t, src)
	dst := newTestStateMachine(t, "shard-1", 1)
	if err := dst.Restore(io.NopCloser(bytes.NewReader(image))); err == nil {
		t.Fatal("Restore accepted a snapshot of another group")
	}
}

// Restore waits for an in-flight read instead of closing its database.
func TestRestoreWaitsForReaders(t *testing.T) {
	src := newTestStateMachine(t, "shard-1", 1)
	smApply(t, src, 1, cas("op-1", "kv:a", 0, 1))
	image := takeSnapshot(t, src)

	dst := newTestStateMachine(t, "shard-1", 1)
	inRead, release := make(chan struct{}), make(chan struct{})
	readErr := make(chan error, 1)
	go func() {
		readErr <- dst.read(func(f *fsm.FSM) error {
			close(inRead)
			<-release
			_, err := f.Dump(context.Background()) // the handle must still be open
			return err
		})
	}()
	<-inRead
	restored := make(chan error, 1)
	go func() { restored <- dst.Restore(io.NopCloser(bytes.NewReader(image))) }()
	select {
	case err := <-restored:
		t.Fatalf("Restore finished while a read was in flight (err %v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-readErr; err != nil {
		t.Fatalf("in-flight read: %v", err)
	}
	if err := <-restored; err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if dst.appliedIndex() != 1 {
		t.Fatalf("applied index = %d after restore, want 1", dst.appliedIndex())
	}
}

// The FSM directory is derived: a fresh state machine wipes whatever an
// earlier run left there and opens the new database in WAL mode.
func TestNewStateMachineWipesDirAndUsesWAL(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "fsm-7.db")
	if err := os.WriteFile(stale, []byte("not a database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale+"-wal", []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	sm, err := newStateMachine(context.Background(), dir, "shard-1", 1)
	if err != nil {
		t.Fatalf("newStateMachine over stale files: %v", err)
	}
	t.Cleanup(func() { _ = sm.close() })
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale FSM file survived (stat err %v)", err)
	}
	if sm.appliedIndex() != 0 {
		t.Fatalf("fresh FSM applied index = %d", sm.appliedIndex())
	}
	var mode string
	if err := sm.read(func(f *fsm.FSM) error {
		var err error
		mode, err = f.JournalMode(context.Background())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
}

func TestWaitAppliedWaitsForCommit(t *testing.T) {
	sm := newTestStateMachine(t, "shard-1", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := sm.waitApplied(ctx, 1); err == nil {
		t.Fatal("waitApplied(1) returned before anything was applied")
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- sm.waitApplied(ctx, 2)
	}()
	smApply(t, sm, 1, cas("op-1", "kv:a", 0, 1))
	smApply(t, sm, 2, cas("op-2", "kv:a", 1, 2))
	if err := <-done; err != nil {
		t.Fatalf("waitApplied(2): %v", err)
	}
}
