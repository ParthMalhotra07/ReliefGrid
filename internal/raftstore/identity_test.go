package raftstore

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/hashicorp/raft"
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestIdentityCreatedOnceOnNewDisk(t *testing.T) {
	dir, stable := t.TempDir(), raft.NewInmemStore()
	want := Identity{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1}

	first, err := loadOrCreateIdentity(dir, want, stable, false)
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
	if got, err := stable.Get(incarnationKey); err != nil || string(got) != first.Incarnation {
		t.Fatalf("stable store incarnation = %q (err %v), want %q", got, err, first.Incarnation)
	}

	// A restart before bootstrap (no Raft state yet) keeps the incarnation.
	notYet, err := loadOrCreateIdentity(dir, want, stable, false)
	if err != nil || notYet != first {
		t.Fatalf("restart before bootstrap = %+v (err %v), want %+v", notYet, err, first)
	}

	// An ordinary restart (now with Raft state) keeps the incarnation.
	again, err := loadOrCreateIdentity(dir, want, stable, true)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if again != first {
		t.Fatalf("reload = %+v, want %+v", again, first)
	}

	// A wiped disk is a new incarnation.
	other, err := loadOrCreateIdentity(t.TempDir(), want, raft.NewInmemStore(), false)
	if err != nil {
		t.Fatal(err)
	}
	if other.Incarnation == first.Incarnation {
		t.Fatal("a new disk reused the old incarnation")
	}
}

func TestIdentityMismatchRefused(t *testing.T) {
	dir, stable := t.TempDir(), raft.NewInmemStore()
	if _, err := loadOrCreateIdentity(dir, Identity{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1}, stable, false); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]Identity{
		"pod":   {PodName: "storage-1-1", GroupID: "shard-1", Shard: 1},
		"group": {PodName: "storage-1-0", GroupID: "shard-2", Shard: 1},
		"shard": {PodName: "storage-1-0", GroupID: "shard-1", Shard: 2},
	} {
		if _, err := loadOrCreateIdentity(dir, want, stable, true); !errors.Is(err, ErrIdentityMismatch) {
			t.Errorf("%s mismatch: err = %v, want ErrIdentityMismatch", name, err)
		}
	}
}

// Raft state without an identity file is never adopted by a new incarnation.
func TestIdentityMissingWithRaftStateRefused(t *testing.T) {
	dir := t.TempDir()
	_, err := loadOrCreateIdentity(dir, Identity{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1}, raft.NewInmemStore(), true)
	if !errors.Is(err, ErrIdentityMissing) {
		t.Fatalf("err = %v, want ErrIdentityMissing", err)
	}
	if _, err := os.Stat(filepath.Join(dir, identityFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an identity file was written anyway (stat err %v)", err)
	}
}

// identity.json and the stable store must name the same incarnation: a
// stable store that lacks it (raft/ wiped or replaced) or names another one
// is refused, so a new log is never adopted under an old identity.
func TestIdentityStableStoreDisagreementRefused(t *testing.T) {
	want := Identity{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1}
	dir := t.TempDir()
	if _, err := loadOrCreateIdentity(dir, want, raft.NewInmemStore(), false); err != nil {
		t.Fatal(err)
	}
	for _, hasState := range []bool{false, true} {
		if _, err := loadOrCreateIdentity(dir, want, raft.NewInmemStore(), hasState); !errors.Is(err, ErrIdentityMismatch) {
			t.Errorf("stable store lacks the incarnation (hasState %v): err = %v, want ErrIdentityMismatch", hasState, err)
		}
		other := raft.NewInmemStore()
		if err := other.Set(incarnationKey, []byte("0b7c2a1e-5d4f-4e3a-9b8c-7d6e5f4a3b2c")); err != nil {
			t.Fatal(err)
		}
		if _, err := loadOrCreateIdentity(dir, want, other, hasState); !errors.Is(err, ErrIdentityMismatch) {
			t.Errorf("stable store names another incarnation (hasState %v): err = %v, want ErrIdentityMismatch", hasState, err)
		}
	}
}

// The stable store is written before identity.json, so a crash between the
// two leaves the incarnation only in the stable store. With no Raft state
// that is an interrupted creation, finished with the same incarnation; with
// Raft state it is a missing identity file.
func TestIdentityInterruptedCreationResumes(t *testing.T) {
	want := Identity{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1}
	const inc = "0b7c2a1e-5d4f-4e3a-9b8c-7d6e5f4a3b2c"
	stable := raft.NewInmemStore()
	if err := stable.Set(incarnationKey, []byte(inc)); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateIdentity(t.TempDir(), want, stable, true); !errors.Is(err, ErrIdentityMissing) {
		t.Fatalf("with raft state: err = %v, want ErrIdentityMissing", err)
	}
	dir := t.TempDir()
	id, err := loadOrCreateIdentity(dir, want, stable, false)
	if err != nil || id.Incarnation != inc {
		t.Fatalf("resume = %+v (err %v), want incarnation %s", id, err, inc)
	}
	again, err := loadOrCreateIdentity(dir, want, stable, true)
	if err != nil || again != id {
		t.Fatalf("reload = %+v (err %v), want %+v", again, err, id)
	}
}
