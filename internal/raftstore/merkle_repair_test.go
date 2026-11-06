package raftstore

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
)

// merkleAt is n's digest at command index at, read like /mgmt/merkle.
func merkleAt(t *testing.T, n *Node, at uint64) fsm.MerkleReport {
	t.Helper()
	var rep fsm.MerkleReport
	if _, err := n.Read(testCtx(t), ReadStale, func(ctx context.Context, rd Reader) error {
		var err error
		rep, err = rd.Merkle(ctx, at)
		return err
	}); err != nil {
		t.Fatalf("%s: Merkle(%d): %v", n.cfg.PodName, at, err)
	}
	return rep
}

// corruptFSMFile changes a row in n's FSM database file from outside the
// node, as a disk fault or a bug outside Raft would: nothing goes through
// the log.
func corruptFSMFile(t *testing.T, n *Node, stmt string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+n.sm.dbPath(n.sm.gen))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	res, err := db.ExecContext(testCtx(t), stmt, args...)
	if err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if k, _ := res.RowsAffected(); k != 1 {
		t.Fatalf("corrupt: %d rows changed, want 1", k)
	}
}

func waitAll(t *testing.T, c *testCluster, index uint64) {
	t.Helper()
	for _, n := range c.nodes {
		if err := n.WaitApplied(testCtx(t), index); err != nil {
			t.Fatal(err)
		}
	}
}

// The repair for a divergent replica is Raft, not row copying (v3.1 §26):
// its FSM is derived (M3), so restarting it rebuilds the database from its
// snapshot and log, and its root equals the others' again.
func TestDivergentFollowerIsRepairedByRebuild(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	fi, _ := c.follower(li)
	last := writeKeys(t, l, "a", 20)
	at := last.AppliedIndex
	waitAll(t, c, at)
	want := merkleAt(t, l, at)
	for _, n := range c.nodes {
		if got := merkleAt(t, n, at); got.Digest.Root != want.Digest.Root {
			t.Fatalf("%s: root %s before any fault, leader %s", n.cfg.PodName, got.Digest.Root, want.Digest.Root)
		}
	}

	corruptFSMFile(t, c.nodes[fi], `UPDATE records SET body = replace(body, '"value":7', '"value":8') WHERE key = ?`, "kv:a7")
	bad := merkleAt(t, c.nodes[fi], at)
	if bad.Digest.Root == want.Digest.Root {
		t.Fatal("the corrupted follower still has the leader's root")
	}
	diffs, err := merkle.DiffBuckets(map[string]merkle.Digest{"leader": want.Digest, "follower": bad.Digest})
	if err != nil || len(diffs) != 1 || diffs[0].Table != "records" || len(diffs[0].Buckets) != 1 {
		t.Fatalf("bucket diff = %+v, %v; want one records bucket", diffs, err)
	}

	c.restart(fi)
	if err := c.nodes[fi].WaitApplied(testCtx(t), at); err != nil {
		t.Fatal(err)
	}
	if got := merkleAt(t, c.nodes[fi], at); got.Digest.Root != want.Digest.Root || got.Summary != want.Summary {
		t.Fatalf("after rebuild: root %s, want %s", got.Digest.Root, want.Digest.Root)
	}
}

// A follower restarted behind compaction installs the leader's snapshot (a
// VACUUM INTO image) and then replays the log: at the same index its root
// equals the leader's, so the digest compares logical content, not files.
func TestSnapshotInstalledFollowerHasLeaderRoot(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	fi, _ := c.follower(li)
	waitAll(t, c, writeKeys(t, l, "a", 5).AppliedIndex)
	c.kill(fi)
	writeKeys(t, l, "c", 5*int(testProfile.TrailingLogs))
	snap, err := l.Snapshot(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	c.open(fi)
	if err := c.nodes[fi].WaitApplied(testCtx(t), snap.AppliedIndex); err != nil {
		t.Fatal(err)
	}
	if got := c.nodes[fi].Status().RestoredIndex; got != snap.AppliedIndex {
		t.Fatalf("restored %d, want the leader's snapshot at %d", got, snap.AppliedIndex)
	}
	last := writeKeys(t, l, "d", 3)
	waitAll(t, c, last.AppliedIndex)
	want := merkleAt(t, l, last.AppliedIndex)
	for _, n := range c.nodes {
		if got := merkleAt(t, n, last.AppliedIndex); got.Digest.Root != want.Digest.Root || got.Summary != want.Summary {
			t.Fatalf("%s: root %s, leader %s", n.cfg.PodName, got.Digest.Root, want.Digest.Root)
		}
	}
	if fmt.Sprint(want.Digest.Tables[0].Rows) != fmt.Sprint(5+5*int(testProfile.TrailingLogs)+3) {
		t.Fatalf("records = %d", want.Digest.Tables[0].Rows)
	}
}
