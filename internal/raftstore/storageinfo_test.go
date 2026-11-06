package raftstore

import "testing"

// StorageInfo (S5.2 metrics, #113): the PVC's Raft log and snapshot sizes and
// the newest snapshot's index, for log growth and snapshot frequency (#33).
func TestStorageInfo(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	before, err := l.StorageInfo()
	if err != nil {
		t.Fatal(err)
	}
	if before.LogBytes <= 0 || before.SnapshotBytes != 0 || before.LastSnapshotIndex != 0 {
		t.Fatalf("before any snapshot: %+v, want a non-empty log and no snapshot", before)
	}
	writeKeys(t, l, "s", 3)
	snap, err := l.Snapshot(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	after, err := l.StorageInfo()
	if err != nil {
		t.Fatal(err)
	}
	if after.SnapshotBytes <= 0 || after.LastSnapshotIndex != snap.RaftIndex || after.LogBytes < before.LogBytes {
		t.Fatalf("after snapshot %+v: %+v, want its Raft index %d and a snapshot on disk", snap, after, snap.RaftIndex)
	}
}
