package raftstore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// S4.6 seams in-process (#88). A held hook cannot stand in for a kill here:
// Node.Close waits for Raft's FSM and snapshot goroutines (raft v1.8.0
// state.go:153), so a hook that never returns deadlocks it. As in
// crash_test.go, a crash is the disk state at the moment of death: the hook
// copies the node's PVC and FSM directory, then fails the stream as the
// dying process would; the test closes the node, puts the copy back and
// opens it again. Real SIGKILLs are the kind runner's.

// errSeamCrash is what a hook returns to stop the paused stream.
var errSeamCrash = errors.New("seam test: process died here")

// seamSwitch lets a test arm one node's hooks after Open; Config.seams is
// read at Open, so the node holds these dispatching funcs from the start.
type seamSwitch struct {
	afterAppend                atomic.Pointer[func(appendedEntry)]
	persist, install, stageArm atomic.Pointer[func(streamPoint) error]
}

func (s *seamSwitch) hooks() *seamHooks {
	pause := func(p *atomic.Pointer[func(streamPoint) error]) func(streamPoint) error {
		return func(sp streamPoint) error {
			if fn := p.Load(); fn != nil {
				return (*fn)(sp)
			}
			return nil
		}
	}
	return &seamHooks{
		afterAppend: func(e appendedEntry) {
			if fn := s.afterAppend.Load(); fn != nil {
				(*fn)(e)
			}
		},
		persistPause: pause(&s.persist),
		installPause: pause(&s.install),
		stagePause:   pause(&s.stageArm),
	}
}

func (s *seamSwitch) clear() {
	s.afterAppend.Store(nil)
	s.persist.Store(nil)
	s.install.Store(nil)
	s.stageArm.Store(nil)
}

func arm(p *atomic.Pointer[func(streamPoint) error], fn func(streamPoint) error) { p.Store(&fn) }

// newSeamCluster is a bootstrapped three-node group whose nodes were opened
// with seam hooks, one switch per node.
func newSeamCluster(t *testing.T) (*testCluster, []*seamSwitch) {
	t.Helper()
	c := newTestCluster(t, 3, testProfile)
	sw := make([]*seamSwitch, 3)
	for i := range sw {
		sw[i] = &seamSwitch{}
		c.cfgs[i].seams = sw[i].hooks()
		c.restart(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	if err := c.nodes[0].Bootstrap(ctx, c.servers()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	c.waitLeader()
	return c, sw
}

// crashImage copies a node's PVC and FSM directory as a crash would leave
// them. It runs inside a hook, off the test goroutine, so it returns errors.
type crashImage struct{ pvc, fsm string }

func takeCrashImage(t *testing.T, cfg Config) (crashImage, error) {
	root := t.TempDir()
	img := crashImage{pvc: filepath.Join(root, "pvc"), fsm: filepath.Join(root, "emptydir")}
	for _, d := range [][2]string{{cfg.DataDir, img.pvc}, {cfg.FSMDir, img.fsm}} {
		if err := os.MkdirAll(d[1], 0o750); err != nil {
			return img, err
		}
		if err := os.CopyFS(d[1], os.DirFS(d[0])); err != nil {
			return img, err
		}
	}
	return img, nil
}

// restoreCrashImage replaces a closed node's directories with img.
func restoreCrashImage(t *testing.T, cfg Config, img crashImage) {
	t.Helper()
	for _, d := range [][2]string{{img.pvc, cfg.DataDir}, {img.fsm, cfg.FSMDir}} {
		if err := os.RemoveAll(d[1]); err != nil {
			t.Fatal(err)
		}
		copyDir(t, d[0], d[1])
	}
}

type pausedAt struct {
	p   streamPoint
	img crashImage
	err error
}

// crashAt arms p so that the first pass through the boundary takes a crash
// image and reports it; every pass fails the stream until the switch is
// cleared, so a retry cannot finish before the test kills the node.
func crashAt(t *testing.T, p *atomic.Pointer[func(streamPoint) error], cfg Config) <-chan pausedAt {
	reached := make(chan pausedAt, 1)
	var once atomic.Bool
	arm(p, func(sp streamPoint) error {
		if once.CompareAndSwap(false, true) {
			img, err := takeCrashImage(t, cfg)
			reached <- pausedAt{sp, img, err}
		}
		return errSeamCrash
	})
	return reached
}

func waitPaused(t *testing.T, reached <-chan pausedAt) pausedAt {
	t.Helper()
	select {
	case r := <-reached:
		if r.err != nil {
			t.Fatalf("crash image: %v", r.err)
		}
		return r
	case <-time.After(waitFor):
		t.Fatal("the boundary was never reached")
		return pausedAt{}
	}
}

type applied struct {
	res store.Result
	err error
}

func goApply(n *Node, cmd any) <-chan applied {
	done := make(chan applied, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitFor)
		defer cancel()
		res, err := n.Apply(ctx, cmd)
		done <- applied{res, err}
	}()
	return done
}

// SCH01: with A held before apply on the leader, A and then B are appended
// and committed; neither has applied. On release A wins and B is a
// VERSION_CONFLICT. A follower that applied without the hold has the same
// rows at the same index (invariant 2).
func TestLogBarrierBothCommittedBeforeEitherApplies(t *testing.T) {
	c, sw := newSeamCluster(t)
	li, l := c.waitLeader()
	mustApply(t, l, cas("op-create", "kv:x", 0, 0))

	b := testseam.NewBarrier(waitFor, func() uint64 { return l.Status().CommitIndex }, func() uint64 { return l.Status().AppliedIndex }, "op-a", "op-b")
	onAppend := func(e appendedEntry) { b.Appended(e.Index, e.OperationID) }
	sw[li].afterAppend.Store(&onAppend)
	l.sm.setBeforeApply(b.BeforeApply)
	t.Cleanup(b.Hold().Release)

	a := goApply(l, cas("op-a", "kv:x", 1, 1))
	ia, err := b.WaitAppended(testCtx(t), "op-a")
	if err != nil {
		t.Fatal(err)
	}
	bb := goApply(l, cas("op-b", "kv:x", 1, 2))
	p, err := b.Hold().WaitReached(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	ib := p.Indexes["op-b"]
	if p.Indexes["op-a"] != ia || ib <= ia {
		t.Fatalf("indexes %v: want op-a at %d, then op-b", p.Indexes, ia)
	}
	// Raft's own view while A is held, not only the barrier's.
	if st := l.Status(); st.CommitIndex < ib || st.AppliedIndex >= ia {
		t.Fatalf("status %+v: want commit >= %d and applied < %d", st, ib, ia)
	}
	b.Hold().Release()
	ra, rb := <-a, <-bb
	if ra.err != nil || ra.res.Outcome != store.OutcomeOK || ra.res.Version != 2 {
		t.Fatalf("A: %+v, %v; want OK at version 2", ra.res, ra.err)
	}
	if rb.err != nil || rb.res.Outcome != store.OutcomeVersionConflict {
		t.Fatalf("B: %+v, %v; want VERSION_CONFLICT", rb.res, rb.err)
	}
	_, f := c.follower(li)
	catchUpAndCompare(t, l, f)
}

// afterAppend sees only command entries: Raft's no-op, configuration and
// barrier entries are not reported.
func TestAfterAppendReportsOnlyCommands(t *testing.T) {
	c, sw := newSeamCluster(t)
	li, l := c.waitLeader()
	got := make(chan appendedEntry, 16)
	onAppend := func(e appendedEntry) { got <- e }
	sw[li].afterAppend.Store(&onAppend)
	res := mustApply(t, l, cas("op-1", "kv:y", 0, 1))
	if err := l.barrier(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	sw[li].clear()
	close(got)
	var seen []appendedEntry
	for e := range got {
		seen = append(seen, e)
	}
	if len(seen) != 1 || seen[0].OperationID != "op-1" || seen[0].Op != fsm.OpCompareAndSet || seen[0].Index != res.AppliedIndex {
		t.Fatalf("appended %+v, want only op-1 CompareAndSet at %d", seen, res.AppliedIndex)
	}
}

// Without seams Raft gets the bolt store itself: production adds nothing to
// the log path.
func TestLogStoreUnwrappedWithoutSeams(t *testing.T) {
	c := newTestCluster(t, 1, testProfile)
	_, wrapped := c.nodes[0].raftLogs.(*seamLogStore)
	if wrapped != seamsBuild {
		t.Fatalf("log store wrapped = %v without Config.seams, want %v (the seams build tag)", wrapped, seamsBuild)
	}
	c2, _ := newSeamCluster(t)
	if _, ok := c2.nodes[0].raftLogs.(*seamLogStore); !ok {
		t.Fatal("Config.seams set but the log store is not wrapped")
	}
}

// SCH12: the leader dies halfway through writing a snapshot, after one
// complete snapshot. On restart the partial snapshot is gone, the complete
// one is restored and the log replayed; the rows equal the new leader's and
// the next snapshot succeeds.
func TestCrashMidPersistRestoresPriorSnapshotAndReplays(t *testing.T) {
	c, sw := newSeamCluster(t)
	li, l := c.waitLeader()
	writeKeys(t, l, "a", 8)
	prior, err := l.Snapshot(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	last := writeKeys(t, l, "b", 8)
	cfg := c.cfgs[li]
	reached := crashAt(t, &sw[li].persist, cfg)
	if _, err := l.Snapshot(testCtx(t)); err == nil {
		t.Fatal("the snapshot that died mid-write reported success")
	}
	r := waitPaused(t, reached)
	if r.p.ImageIndex != last.AppliedIndex || r.p.Bytes <= 0 || r.p.Bytes >= r.p.Size {
		t.Fatalf("paused at %+v: want a partial image at applied index %d", r.p, last.AppliedIndex)
	}
	if tmp := tmpSnapshots(t, r.img.pvc); len(tmp) != 1 {
		t.Fatalf("crash image has incomplete snapshots %v, want the one being written", tmp)
	}

	c.kill(li)
	restoreCrashImage(t, cfg, r.img)
	sw[li].clear()
	c.open(li)
	n := c.nodes[li]
	if got := n.Status().RestoredIndex; got != prior.AppliedIndex {
		t.Fatalf("restored applied index %d, want the prior complete snapshot's %d", got, prior.AppliedIndex)
	}
	if tmp := tmpSnapshots(t, cfg.DataDir); len(tmp) != 0 {
		t.Fatalf("incomplete snapshots %v survived the restart", tmp)
	}
	if extra := derivedFiles(t, n, cfg.FSMDir); len(extra) != 0 {
		t.Fatalf("FSM directory kept %v after restart", extra)
	}
	_, nl := c.waitLeader()
	catchUpAndCompare(t, nl, n)
	if next, err := n.Snapshot(testCtx(t)); err != nil || next.AppliedIndex <= prior.AppliedIndex {
		t.Fatalf("snapshot after the crash: %+v, %v", next, err)
	}
}

// behindCompactedLog kills a follower, writes past the leader's trailing
// logs and snapshots the leader, so the follower must install a snapshot
// when it next opens. It returns the follower's index and the snapshot.
func behindCompactedLog(t *testing.T, c *testCluster, l *Node, li int) (int, SnapshotInfo) {
	t.Helper()
	fi, f := c.follower(li)
	last := writeKeys(t, l, "a", 3)
	if err := f.WaitApplied(testCtx(t), last.AppliedIndex); err != nil {
		t.Fatal(err)
	}
	followerLast := f.Status().LastLogIndex
	c.kill(fi)
	writeKeys(t, l, "b", 5*int(testProfile.TrailingLogs))
	snap, err := l.Snapshot(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if first, err := l.logs.FirstIndex(); err != nil || first <= followerLast+1 {
		t.Fatalf("leader log first index %d (err %v): the follower's next entry %d was not compacted", first, err, followerLast+1)
	}
	return fi, snap
}

// countInstalls counts InstallSnapshot RPCs reaching node i from its next Open.
func countInstalls(c *testCluster, i int) *atomic.Int32 {
	var n atomic.Int32
	c.cfgs[i].beforeRPC = func(rpc raft.RPC) {
		if _, ok := rpc.Command.(*raft.InstallSnapshotRequest); ok {
			n.Add(1)
		}
	}
	return &n
}

// SCH13a: a follower dies halfway through receiving InstallSnapshot. On
// restart the incomplete snapshot is gone and the leader sends it again;
// the follower ends at the leader's rows.
func TestCrashMidInstallTransferRefetchesFromLeader(t *testing.T) {
	c, sw := newSeamCluster(t)
	li, l := c.waitLeader()
	fi, snap := behindCompactedLog(t, c, l, li)
	cfg := c.cfgs[fi]
	reached := crashAt(t, &sw[fi].install, cfg)
	c.open(fi)
	r := waitPaused(t, reached)
	if r.p.RaftIndex != snap.RaftIndex || r.p.Bytes <= 0 || r.p.Bytes >= r.p.Size {
		t.Fatalf("paused at %+v: want a partial transfer of the snapshot at Raft index %d", r.p, snap.RaftIndex)
	}
	if tmp := tmpSnapshots(t, r.img.pvc); len(tmp) != 1 {
		t.Fatalf("crash image has incomplete snapshots %v, want the one being received", tmp)
	}

	c.kill(fi)
	restoreCrashImage(t, cfg, r.img)
	sw[fi].clear()
	installs := countInstalls(c, fi)
	c.open(fi)
	f := c.nodes[fi]
	if tmp := tmpSnapshots(t, cfg.DataDir); len(tmp) != 0 {
		t.Fatalf("incomplete snapshots %v survived the restart", tmp)
	}
	if got := f.Status().RestoredIndex; got != 0 {
		t.Fatalf("restored %d at open: the partial transfer was installed", got)
	}
	if err := f.WaitApplied(testCtx(t), snap.AppliedIndex); err != nil {
		t.Fatal(err)
	}
	if installs.Load() == 0 {
		t.Fatal("the follower caught up without the leader sending the snapshot again")
	}
	if got := f.Status().RestoredIndex; got != snap.AppliedIndex {
		t.Fatalf("restored %d, want the leader's snapshot at %d", got, snap.AppliedIndex)
	}
	catchUpAndCompare(t, l, f)
}

// SCH13b: a follower dies halfway through staging a snapshot it has fully
// received. Raft had already stored it, so on restart the follower restores
// its own copy without asking the leader, and ends at the leader's rows.
func TestCrashMidRestoreStageUsesLocalSnapshot(t *testing.T) {
	c, sw := newSeamCluster(t)
	li, l := c.waitLeader()
	fi, snap := behindCompactedLog(t, c, l, li)
	cfg := c.cfgs[fi]
	reached := crashAt(t, &sw[fi].stageArm, cfg)
	c.open(fi)
	r := waitPaused(t, reached)
	if r.p.ImageIndex != snap.AppliedIndex || r.p.Bytes <= 0 || r.p.Bytes >= r.p.Size {
		t.Fatalf("paused at %+v: want a partial stage of the image at applied index %d", r.p, snap.AppliedIndex)
	}
	local, err := raft.NewFileSnapshotStore(r.img.pvc, retainSnapshots, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	metas, err := local.List()
	if err != nil || len(metas) == 0 || metas[0].Index != snap.RaftIndex {
		t.Fatalf("crash image snapshots %+v (err %v): want the received one at Raft index %d complete", metas, err, snap.RaftIndex)
	}

	c.kill(fi)
	restoreCrashImage(t, cfg, r.img)
	sw[fi].clear()
	installs := countInstalls(c, fi)
	c.open(fi)
	f := c.nodes[fi]
	if st := f.Status(); st.RestoredIndex != snap.AppliedIndex || st.AppliedIndex < snap.AppliedIndex {
		t.Fatalf("at open %+v: want its own copy of the snapshot at %d restored", st, snap.AppliedIndex)
	}
	if extra := derivedFiles(t, f, cfg.FSMDir); len(extra) != 0 {
		t.Fatalf("FSM directory kept %v: the partial stage survived the restart", extra)
	}
	catchUpAndCompare(t, l, f)
	if n := installs.Load(); n != 0 {
		t.Fatalf("leader sent the snapshot %d more times; the local copy was complete", n)
	}
}
