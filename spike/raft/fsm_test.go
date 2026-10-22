package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func apply(t *testing.T, f *fsm, index uint64, key string, expected int64, value string) casResult {
	t.Helper()
	cmd, err := json.Marshal(casCommand{Key: key, ExpectedVersion: expected, Value: value})
	if err != nil {
		t.Fatal(err)
	}
	res, ok := f.Apply(&raft.Log{Index: index, Type: raft.LogCommand, Data: cmd}).(casResult)
	if !ok {
		t.Fatalf("Apply returned %T, want casResult", res)
	}
	if res.Err != "" {
		t.Fatalf("Apply(%s, %d, %s): %s", key, expected, value, res.Err)
	}
	return res
}

func TestCAS(t *testing.T) {
	f, err := openFSM(filepath.Join(t.TempDir(), "fsm.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	if got := apply(t, f, 1, "k", 0, "alpha"); got.Conflict || got.Version != 1 || got.Value != "alpha" {
		t.Fatalf("first CAS = %+v, want alpha@1", got)
	}
	if got := apply(t, f, 2, "k", 0, "stale"); !got.Conflict || got.Version != 1 || got.Value != "alpha" {
		t.Fatalf("stale CAS = %+v, want a conflict reporting alpha@1", got)
	}
	if got := apply(t, f, 3, "k", 1, "beta"); got.Conflict || got.Version != 2 {
		t.Fatalf("second CAS = %+v, want beta@2", got)
	}
	if got := apply(t, f, 4, "other", 5, "x"); !got.Conflict || got.Version != 0 {
		t.Fatalf("CAS on a missing key with version 5 = %+v, want a conflict at version 0", got)
	}
	value, version, err := f.Get("k")
	if err != nil || value != "beta" || version != 2 {
		t.Fatalf("Get = %q@%d, %v; want beta@2", value, version, err)
	}
}

type bufferSink struct{ bytes.Buffer }

func (*bufferSink) ID() string    { return "test" }
func (*bufferSink) Cancel() error { return nil }
func (*bufferSink) Close() error  { return nil }

func TestSnapshotRestore(t *testing.T) {
	dir := t.TempDir()
	src, err := openFSM(filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	apply(t, src, 1, "a", 0, "one")
	apply(t, src, 2, "b", 0, "two")
	apply(t, src, 3, "a", 1, "uno")

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var sink bufferSink
	if err := snap.Persist(&sink); err != nil {
		t.Fatal(err)
	}
	snap.Release()

	dst, err := openFSM(filepath.Join(dir, "dst.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	apply(t, dst, 1, "stale", 0, "gone") // Restore replaces, not merges
	if err := dst.Restore(readCloser{&sink.Buffer}); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]struct {
		value   string
		version int64
	}{"a": {"uno", 2}, "b": {"two", 1}, "stale": {"", 0}} {
		value, version, err := dst.Get(key)
		if err != nil || value != want.value || version != want.version {
			t.Errorf("after restore Get(%s) = %q@%d, %v; want %q@%d", key, value, version, err, want.value, want.version)
		}
	}
}

// The SQLite file is derived (M3): opening it again starts empty, and the state
// comes back only from the Raft snapshot and log.
func TestOpenDiscardsOldFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fsm.db")
	f, err := openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	apply(t, f, 1, "k", 0, "alpha")
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if value, version, err := f.Get("k"); err != nil || version != 0 || value != "" {
		t.Fatalf("reopened Get = %q@%d, %v; want an empty store", value, version, err)
	}
}

type readCloser struct{ *bytes.Buffer }

func (readCloser) Close() error { return nil }
