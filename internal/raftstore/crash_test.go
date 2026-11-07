package raftstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Crash tests (v3.1 §16): a crash during snapshot creation or install must
// leave either a valid prior snapshot or a retryable incomplete file, never
// an incomplete image published as current. A crash is the disk state at
// the moment the process dies: the node is closed and the files a dying
// process leaves are put in place, then the node is opened again on them.

// writeKeys applies n fresh keys prefix0..prefixN-1 through the leader.
func writeKeys(t *testing.T, l *Node, prefix string, n int) store.Result {
	t.Helper()
	var last store.Result
	for i := range n {
		last = mustApply(t, l, cas(fmt.Sprintf("op-%s%d", prefix, i), fmt.Sprintf("kv:%s%d", prefix, i), 0, i))
		if last.Outcome != store.OutcomeOK {
			t.Fatalf("%s%d: %+v", prefix, i, last)
		}
	}
	return last
}

// writeKeys applies n fresh keys prefix0..prefixN-1 through whichever node
// leads at each write, and returns the command index that node had applied
// once the last write committed: the index every replica must reach.
func (c *testCluster) writeKeys(t *testing.T, prefix string, n int) uint64 {
	t.Helper()
	var at uint64
	for i := range n {
		var res store.Result
		res, at = c.applyOnLeader(t, cas(fmt.Sprintf("op-%s%d", prefix, i), fmt.Sprintf("kv:%s%d", prefix, i), 0, i))
		if res.Outcome != store.OutcomeOK {
			t.Fatalf("%s%d: %+v", prefix, i, res)
		}
	}
	return at
}

// applyOnLeader proposes cmd through the node that leads now. A leader
// found before a fault may have stepped down since (issue #122), so the
// leader is looked up again for every attempt. ErrNotLeader means the
// command was never appended; ErrUnknownOutcome means it may or may not
// commit (a leader can lose its lease between appending and committing).
// Either way the same command, with the same operationId, is proposed
// again, as the Store contract asks (invariant 5): the FSM applies it once
// and answers a repeat with the stored result (Replayed). Any other error,
// or the deadline, fails the test; callers check the outcome.
//
// It returns the committed result and the command index the answering node
// had applied by then. A replayed result carries the index the command
// first committed at, but the repeat is a later command on the log, so
// only the second index says how far every replica must get.
func (c *testCluster) applyOnLeader(t *testing.T, cmd any) (store.Result, uint64) {
	t.Helper()
	ctx := testCtx(t)
	for {
		_, l := c.waitLeader()
		res, err := l.Apply(ctx, cmd)
		if err == nil {
			// The FSM records its applied index before Apply answers.
			return res, max(res.AppliedIndex, l.Status().AppliedIndex)
		}
		retryable := errors.Is(err, ErrNotLeader) || errors.Is(err, store.ErrUnknownOutcome)
		if !retryable || ctx.Err() != nil {
			t.Fatalf("Apply(%T): %v", cmd, err)
		}
		t.Logf("Apply(%T) on %s: %v; proposing the same command again through the current leader", cmd, l.Identity().ServerID(), err)
	}
}

// compactLiveNodes snapshots every live node once it has applied the
// command at applied, and checks that each one's log now starts after
// next, the entry a stopped follower needs next. Whichever node leads when
// that follower rejoins must then send it a snapshot (InstallSnapshot): a
// node with the entries still in its log would replicate them instead
// (issue #122). It returns the snapshot, which is the same command index on
// every node.
func (c *testCluster) compactLiveNodes(t *testing.T, applied, next uint64) SnapshotInfo {
	t.Helper()
	var snap SnapshotInfo
	for i, n := range c.nodes {
		if n == nil {
			continue
		}
		if err := n.WaitApplied(testCtx(t), applied); err != nil {
			t.Fatal(err)
		}
		s, err := n.Snapshot(testCtx(t))
		if err != nil {
			t.Fatalf("snapshot node %d: %v", i, err)
		}
		if s.AppliedIndex != applied {
			t.Fatalf("node %d snapshot %+v, want one at applied index %d", i, s, applied)
		}
		if first, err := n.logs.FirstIndex(); err != nil || first <= next {
			t.Fatalf("node %d log first index %d (err %v): the follower's next entry %d was not compacted", i, first, err, next)
		}
		snap = s
	}
	return snap
}

// derivedFiles lists the FSM directory's files other than n's open database
// (fsm-<gen>.db and its -wal/-shm).
func derivedFiles(t *testing.T, n *Node, dir string) []string {
	t.Helper()
	db := filepath.Base(n.sm.dbPath(n.sm.gen))
	var out []string
	for _, name := range dbFiles(t, dir) {
		if name != db && name != db+"-wal" && name != db+"-shm" {
			out = append(out, name)
		}
	}
	return out
}

// tmpSnapshots lists incomplete snapshot directories (Raft's "<id>.tmp").
func tmpSnapshots(t *testing.T, dataDir string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(dataDir, "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			out = append(out, e.Name())
		}
	}
	return out
}

// catchUpAndCompare waits until n has applied the leader's last command and
// checks that their rows are equal at that index.
func catchUpAndCompare(t *testing.T, l, n *Node) {
	t.Helper()
	final := mustApply(t, l, finalWrite(l))
	compareAt(t, l, n, final.AppliedIndex)
}

// catchUpAndCompare commits one more command through whichever node leads
// now, waits until n and the current leader have applied it, and checks
// that their rows are equal at that index.
func (c *testCluster) catchUpAndCompare(t *testing.T, n *Node) {
	t.Helper()
	_, l := c.waitLeader()
	final, at := c.applyOnLeader(t, finalWrite(l))
	if final.Outcome != store.OutcomeOK {
		t.Fatalf("final write: %+v", final)
	}
	_, l = c.waitLeader()
	compareAt(t, l, n, at)
}

func finalWrite(l *Node) store.CompareAndSet {
	at := l.Status().CommitIndex
	return cas(fmt.Sprintf("op-final-%d", at), fmt.Sprintf("kv:final-%d", at), 0, 1)
}

// compareAt waits until l and n have applied the command at index and checks
// that their rows are equal there.
func compareAt(t *testing.T, l, n *Node, index uint64) {
	t.Helper()
	for _, x := range []*Node{l, n} {
		if err := x.WaitApplied(testCtx(t), index); err != nil {
			t.Fatal(err)
		}
	}
	dl, dn := dumpOf(t, l), dumpOf(t, n)
	if !reflect.DeepEqual(dl["meta"], dn["meta"]) { // meta holds applied_index: compare only at equal indexes
		t.Fatalf("dumps at different applied indexes: %v vs %v", dl["meta"], dn["meta"])
	}
	if !reflect.DeepEqual(dl, dn) {
		t.Fatal("rows differ from the leader's at the same applied index")
	}
}

// A replica dies while taking a snapshot: VACUUM INTO had half-written its
// temporary file in the FSM directory, and Persist had half-written the
// image into Raft's "<id>.tmp" sink. On restart the incomplete snapshot is
// neither restored nor left behind, the prior snapshot is restored and the
// log replayed, and the next snapshot succeeds.
func TestCrashDuringSnapshotKeepsPriorSnapshot(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, _ := c.waitLeader()
	fi, f := c.follower(li)
	last := c.writeKeys(t, "a", 8)
	if err := f.WaitApplied(testCtx(t), last); err != nil {
		t.Fatal(err)
	}
	prior, err := f.Snapshot(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if prior.AppliedIndex != last {
		t.Fatalf("prior snapshot %+v, want a new one at applied index %d", prior, last)
	}
	last = c.writeKeys(t, "b", 8) // the tail the restart must replay
	if err := f.WaitApplied(testCtx(t), last); err != nil {
		t.Fatal(err)
	}
	crashIndex, crashTerm := f.Status().LastLogIndex, f.Status().Term
	c.kill(fi)

	// What the dying snapshot left: a truncated image in a sink that was
	// never closed (Raft's own FileSnapshotStore, as Persist uses it), and a
	// truncated VACUUM INTO output.
	cfg := c.cfgs[fi]
	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, retainSnapshots, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	meta, rc, err := snaps.Open(prior.ID)
	if err != nil {
		t.Fatal(err)
	}
	image, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	_, trans := raft.NewInmemTransport("") // Create encodes the peers through a transport
	sink, err := snaps.Create(raft.SnapshotVersionMax, crashIndex, crashTerm, meta.Configuration, meta.ConfigurationIndex, trans)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write(image[:len(image)/2]); err != nil {
		t.Fatal(err)
	}
	// No Close or Cancel: the process died here.
	if tmp := tmpSnapshots(t, cfg.DataDir); len(tmp) != 1 {
		t.Fatalf("incomplete snapshots %v, want the one the crash left", tmp)
	}
	if err := os.WriteFile(filepath.Join(cfg.FSMDir, "snapshot-9.db"), image[len(image)/4:len(image)/2], 0o600); err != nil {
		t.Fatal(err)
	}

	c.open(fi)
	f = c.nodes[fi]
	if got := f.Status().RestoredIndex; got != prior.AppliedIndex {
		t.Fatalf("restored the snapshot at applied index %d, want the prior one (%d)", got, prior.AppliedIndex)
	}
	if tmp := tmpSnapshots(t, cfg.DataDir); len(tmp) != 0 {
		t.Fatalf("incomplete snapshots %v left on the PVC after restart", tmp)
	}
	if extra := derivedFiles(t, f, cfg.FSMDir); len(extra) != 0 {
		t.Fatalf("FSM directory kept %v after restart", extra)
	}
	c.catchUpAndCompare(t, f) // the tail replayed from the log

	next, err := f.Snapshot(testCtx(t))
	if err != nil {
		t.Fatalf("snapshot after the crash: %v", err)
	}
	if next.RaftIndex <= prior.RaftIndex || next.AppliedIndex <= prior.AppliedIndex {
		t.Fatalf("next snapshot %+v, want a new one after %+v", next, prior)
	}
	c.restart(fi)
	if got := c.nodes[fi].Status().RestoredIndex; got != next.AppliedIndex {
		t.Fatalf("second restart restored applied index %d, want the new snapshot's %d", got, next.AppliedIndex)
	}
}

// A replica dies while installing a snapshot from the leader: the image was
// staged and renamed to the next database generation, but not yet swapped
// in (and a later stage was half-written). On restart the FSM directory is
// rebuilt from scratch: the staged generation is never opened, the node
// restores the installed snapshot from its own store, and its rows equal the
// leader's at the same index. Both live nodes compact their logs while the
// replica is down, so the snapshot comes whichever of them leads.
func TestCrashDuringRestoreRebuildsCleanly(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, _ := c.waitLeader()
	fi, f := c.follower(li)
	last := c.writeKeys(t, "a", 3)
	if err := f.WaitApplied(testCtx(t), last); err != nil {
		t.Fatal(err)
	}
	followerLast := f.Status().LastLogIndex
	c.kill(fi)
	last = c.writeKeys(t, "b", 5*int(testProfile.TrailingLogs))
	snap := c.compactLiveNodes(t, last, followerLast+1)

	// At the swap, copy the FSM directory: that is what the crash leaves.
	cfg := c.cfgs[fi]
	crashDir := t.TempDir()
	var copied atomic.Int32
	c.cfgs[fi].beforeRestoreSwap = func() {
		if copied.Add(1) == 1 {
			copyDir(t, cfg.FSMDir, crashDir)
		}
	}
	c.open(fi)
	f = c.nodes[fi]
	if err := f.WaitApplied(testCtx(t), snap.AppliedIndex); err != nil {
		t.Fatal(err)
	}
	c.kill(fi)
	if copied.Load() == 0 {
		t.Fatal("the follower never reached the restore swap")
	}
	staged := dbFiles(t, crashDir)
	if !containsName(staged, "fsm-1.db") || !containsName(staged, "fsm-0.db") {
		t.Fatalf("crash image %v: want the old generation and the staged one side by side", staged)
	}
	if err := os.RemoveAll(cfg.FSMDir); err != nil {
		t.Fatal(err)
	}
	copyDir(t, crashDir, cfg.FSMDir)
	if err := os.WriteFile(filepath.Join(cfg.FSMDir, "restore-2.db"), []byte("SQLite format 3\x00 truncated"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Hold the staged file open across the restart: an unlinked inode stays
	// allocated while open, so the filesystem cannot hand its number to the
	// new fsm-1.db (ext4 reuses freed inode numbers at once; tmpfs never does).
	stagedFile, err := os.Open(filepath.Join(cfg.FSMDir, "fsm-1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stagedFile.Close() }()
	stagedBefore, err := stagedFile.Stat()
	if err != nil {
		t.Fatal(err)
	}

	c.cfgs[fi].beforeRestoreSwap = nil
	c.open(fi)
	f = c.nodes[fi]
	// Open returns after Raft restored the newest local snapshot: the one
	// InstallSnapshot persisted before the crash.
	st := f.Status()
	if st.RestoredIndex != snap.AppliedIndex || st.AppliedIndex != snap.AppliedIndex {
		t.Fatalf("after restart: restored %d, applied %d; want the installed snapshot at %d", st.RestoredIndex, st.AppliedIndex, snap.AppliedIndex)
	}
	if extra := derivedFiles(t, f, cfg.FSMDir); len(extra) != 0 {
		t.Fatalf("FSM directory kept %v: a staged or half-swapped database survived the restart", extra)
	}
	// Open wiped the directory, created generation 0 and restored the
	// snapshot into a new generation 1: the same name as the staged file the
	// crash left, but never that file.
	if f.sm.gen != 1 {
		t.Fatalf("restarted on database generation %d, want 1 (fresh 0, then the restore)", f.sm.gen)
	}
	if now, err := os.Stat(f.sm.dbPath(f.sm.gen)); err != nil || os.SameFile(now, stagedBefore) {
		t.Fatalf("the restarted node opened the staged file the crash left (err %v)", err)
	}
	c.catchUpAndCompare(t, f)
}

// A replica restarts behind a compacted log: it restores its own older
// snapshot, replays its log, finds the entries it needs next compacted away
// on both live nodes, and installs the leader's snapshot, whichever of them
// leads. Its rows then equal the leader's at the same index.
func TestRestartBehindCompactedGapInstallsSnapshot(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, _ := c.waitLeader()
	fi, f := c.follower(li)
	last := c.writeKeys(t, "a", 6)
	if err := f.WaitApplied(testCtx(t), last); err != nil {
		t.Fatal(err)
	}
	own, err := f.Snapshot(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	last = c.writeKeys(t, "b", 3) // in the follower's log, after its snapshot
	if err := f.WaitApplied(testCtx(t), last); err != nil {
		t.Fatal(err)
	}
	followerLast := f.Status().LastLogIndex
	c.kill(fi)

	last = c.writeKeys(t, "c", 5*int(testProfile.TrailingLogs))
	snap := c.compactLiveNodes(t, last, followerLast+1)

	c.open(fi)
	f = c.nodes[fi]
	if got := f.Status().RestoredIndex; got != own.AppliedIndex {
		t.Fatalf("at open: restored %d, want its own snapshot at %d", got, own.AppliedIndex)
	}
	if err := f.WaitApplied(testCtx(t), snap.AppliedIndex); err != nil {
		t.Fatal(err)
	}
	if got := f.Status().RestoredIndex; got != snap.AppliedIndex {
		t.Fatalf("after catching up: restored %d, want the snapshot at %d (InstallSnapshot)", got, snap.AppliedIndex)
	}
	c.catchUpAndCompare(t, f)
}

// Snapshot reports the snapshot it took. raft v1.8.0 takes a new snapshot on
// every request once anything was applied, even at an unchanged index
// (fsm.go:218 answers ErrNothingNewToSnapshot only before the first
// command); a node with nothing applied and no snapshot is an error.
func TestSnapshotReportsIndexes(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	last := writeKeys(t, l, "a", 3)
	first, err := l.Snapshot(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || first.AppliedIndex != last.AppliedIndex || first.RaftIndex < last.AppliedIndex {
		t.Fatalf("snapshot %+v, want a new one at applied index %d", first, last.AppliedIndex)
	}
	again, err := l.Snapshot(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if again.ID == first.ID || again.RaftIndex != first.RaftIndex || again.AppliedIndex != first.AppliedIndex {
		t.Fatalf("second snapshot with nothing applied in between: %+v, want a new one at the same indexes as %+v", again, first)
	}
	fresh := newTestCluster(t, 1, testProfile).nodes[0] // never bootstrapped
	if info, err := fresh.Snapshot(testCtx(t)); err == nil {
		t.Fatalf("snapshot of an empty node: %+v, want an error", info)
	}
}

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	if err := os.MkdirAll(to, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(to, os.DirFS(from)); err != nil {
		t.Fatalf("copy %s to %s: %v", from, to, err)
	}
}

func containsName(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// Open removes only what Raft's FileSnapshotStore names an incomplete
// snapshot (a "<term>-<index>-<ms>.tmp" directory); complete snapshots and
// anything else in the directory stay.
func TestRemoveIncompleteSnapshotsOnlyRaftTmpDirs(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"2-18-1792999593833.tmp", "2-18-1792999593833", "notes.tmp", "2-x-1.tmp"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "3-20-1792999593900.tmp"), nil, 0o600); err != nil { // a file, not a sink
		t.Fatal(err)
	}
	if err := removeIncompleteSnapshots(dir); err != nil {
		t.Fatal(err)
	}
	if got, want := dbFiles(t, dir), []string{"2-18-1792999593833", "2-x-1.tmp", "3-20-1792999593900.tmp", "notes.tmp"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("left %v, want %v", got, want)
	}
	if err := removeIncompleteSnapshots(filepath.Join(dir, "missing")); err != nil {
		t.Fatalf("no snapshot dir yet (a fresh PVC): %v", err)
	}
}
