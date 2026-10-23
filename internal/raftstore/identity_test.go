package raftstore

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestIdentityCreatedOnceOnNewDisk(t *testing.T) {
	dir := t.TempDir()
	want := Identity{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1}

	first, err := loadOrCreateIdentity(dir, want, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !uuidRE.MatchString(first.Incarnation) {
		t.Fatalf("incarnation %q is not a UUID", first.Incarnation)
	}
	if first.PodName != want.PodName || first.GroupID != want.GroupID || first.Shard != want.Shard {
		t.Fatalf("identity = %+v, want %+v plus an incarnation", first, want)
	}
	if got := string(first.ServerID()); got != "storage-1-0/"+first.Incarnation {
		t.Fatalf("ServerID = %q, want podName/incarnation", got)
	}
	if _, err := os.Stat(filepath.Join(dir, identityFile)); err != nil {
		t.Fatalf("identity file: %v", err)
	}

	// An ordinary restart (now with Raft state) keeps the incarnation.
	again, err := loadOrCreateIdentity(dir, want, true)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if again != first {
		t.Fatalf("reload = %+v, want %+v", again, first)
	}

	// A wiped disk is a new incarnation.
	other, err := loadOrCreateIdentity(t.TempDir(), want, false)
	if err != nil {
		t.Fatal(err)
	}
	if other.Incarnation == first.Incarnation {
		t.Fatal("a new disk reused the old incarnation")
	}
}

func TestIdentityMismatchRefused(t *testing.T) {
	dir := t.TempDir()
	if _, err := loadOrCreateIdentity(dir, Identity{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1}, false); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]Identity{
		"pod":   {PodName: "storage-1-1", GroupID: "shard-1", Shard: 1},
		"group": {PodName: "storage-1-0", GroupID: "shard-2", Shard: 1},
		"shard": {PodName: "storage-1-0", GroupID: "shard-1", Shard: 2},
	} {
		if _, err := loadOrCreateIdentity(dir, want, true); !errors.Is(err, ErrIdentityMismatch) {
			t.Errorf("%s mismatch: err = %v, want ErrIdentityMismatch", name, err)
		}
	}
}

// Raft state without an identity file is never adopted by a new incarnation.
func TestIdentityMissingWithRaftStateRefused(t *testing.T) {
	dir := t.TempDir()
	_, err := loadOrCreateIdentity(dir, Identity{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1}, true)
	if !errors.Is(err, ErrIdentityMissing) {
		t.Fatalf("err = %v, want ErrIdentityMissing", err)
	}
	if _, err := os.Stat(filepath.Join(dir, identityFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an identity file was written anyway (stat err %v)", err)
	}
}
