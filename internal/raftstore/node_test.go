package raftstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// testProfile elects fast and never snapshots on its own; tests call
// Node.Snapshot. TrailingLogs is small so a snapshot really compacts the log.
var testProfile = Profile{
	Name:               "test",
	SnapshotThreshold:  1 << 30,
	TrailingLogs:       4,
	SnapshotInterval:   time.Hour,
	HeartbeatTimeout:   200 * time.Millisecond,
	ElectionTimeout:    200 * time.Millisecond,
	LeaderLeaseTimeout: 100 * time.Millisecond,
}

const waitFor = 10 * time.Second

type testCluster struct {
	t     testing.TB
	cfgs  []Config
	nodes []*Node // nil while killed
}

// newTestCluster opens n nodes on loopback, each advertising a name
// (localhost:<port>) that the transport resolves on every dial. It does not
// bootstrap them.
func newTestCluster(t testing.TB, n int, p Profile) *testCluster {
	t.Helper()
	c := &testCluster{t: t, cfgs: make([]Config, n), nodes: make([]*Node, n)}
	for i := range n {
		root := t.TempDir()
		c.cfgs[i] = Config{
			PodName:       fmt.Sprintf("storage-1-%d", i),
			GroupID:       "shard-1",
			Shard:         1,
			DataDir:       filepath.Join(root, "pvc"),
			FSMDir:        filepath.Join(root, "emptydir"),
			BindAddr:      "127.0.0.1:0",
			AdvertiseAddr: "localhost:0",
			Profile:       p,
			Logger:        hclog.New(&hclog.LoggerOptions{Name: fmt.Sprintf("raft-%d", i), Level: hclog.Warn, Output: os.Stderr}),
		}
		c.open(i)
		// Restarts reuse the same port and name, as a pod keeps its DNS name.
		_, port, err := net.SplitHostPort(string(c.nodes[i].Addr()))
		if err != nil {
			t.Fatalf("node %d address %q: %v", i, c.nodes[i].Addr(), err)
		}
		c.cfgs[i].BindAddr = "127.0.0.1:" + port
		c.cfgs[i].AdvertiseAddr = "localhost:" + port
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.kill(i)
		}
	})
	return c
}

// newBootstrappedCluster is a three-node group bootstrapped through node 0.
func newBootstrappedCluster(t testing.TB, p Profile) *testCluster {
	t.Helper()
	c := newTestCluster(t, 3, p)
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	if err := c.nodes[0].Bootstrap(ctx, c.servers()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	c.waitLeader()
	return c
}

func (c *testCluster) open(i int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	n, err := Open(ctx, c.cfgs[i])
	if err != nil {
		c.t.Fatalf("Open node %d: %v", i, err)
	}
	c.nodes[i] = n
}

func (c *testCluster) kill(i int) {
	c.t.Helper()
	if c.nodes[i] == nil {
		return
	}
	if err := c.nodes[i].Close(); err != nil {
		c.t.Errorf("Close node %d: %v", i, err)
	}
	c.nodes[i] = nil
}

func (c *testCluster) restart(i int) {
	c.t.Helper()
	c.kill(i)
	c.open(i)
}

func (c *testCluster) servers() []raft.Server {
	var out []raft.Server
	for _, n := range c.nodes {
		out = append(out, raft.Server{Suffrage: raft.Voter, ID: n.Identity().ServerID(), Address: n.Addr()})
	}
	return out
}

// waitLeader waits until exactly one live node reports Leader and every live
// node agrees on it.
func (c *testCluster) waitLeader() (int, *Node) {
	c.t.Helper()
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		leader, agreed := -1, true
		for i, n := range c.nodes {
			if n == nil {
				continue
			}
			s := n.Status()
			if s.State == raft.Leader.String() {
				if leader >= 0 {
					agreed = false
				}
				leader = i
			}
		}
		if leader >= 0 && agreed {
			want := c.nodes[leader].Identity().ServerID()
			for _, n := range c.nodes {
				if n != nil && n.Status().LeaderID != want {
					agreed = false
				}
			}
			if agreed {
				return leader, c.nodes[leader]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single agreed leader within %s", waitFor)
	return -1, nil
}

func (c *testCluster) follower(leader int) (int, *Node) {
	for i, n := range c.nodes {
		if i != leader && n != nil {
			return i, n
		}
	}
	c.t.Fatal("no live follower")
	return -1, nil
}

func testCtx(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	t.Cleanup(cancel)
	return ctx
}

func mustApply(t testing.TB, n *Node, cmd any) store.Result {
	t.Helper()
	res, err := n.Apply(testCtx(t), cmd)
	if err != nil {
		t.Fatalf("Apply(%T): %v", cmd, err)
	}
	return res
}

// stepDown transfers n's leadership away and waits until n is no longer
// leader. raft v1.8.0 answers LeadershipTransfer with "leadership transfer
// timeout" once one ElectionTimeout passes, even when the target goes on to
// win (raft.go:727-757); a stalled CI runner hits that. So n's state decides,
// not the future's error, and the transfer is retried while n stays leader.
func stepDown(t *testing.T, n *Node) {
	t.Helper()
	const attempts = 5
	var err error
	for range attempts {
		err = n.raft.LeadershipTransfer().Error()
		for deadline := time.Now().Add(waitFor / attempts); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if n.raft.State() != raft.Leader {
				return
			}
		}
	}
	t.Fatalf("still leader after %d leadership transfers (last error: %v)", attempts, err)
}

func readKV(ctx context.Context, n *Node, mode ReadMode, key string) (store.Record, ReadInfo, error) {
	var rec store.Record
	info, err := n.Read(ctx, mode, func(ctx context.Context, r Reader) error {
		var err error
		rec, err = r.ReadRecord(ctx, key)
		return err
	})
	return rec, info, err
}

func expectValue(t *testing.T, rec store.Record, version uint64, value int) {
	t.Helper()
	if rec.Version != version || string(rec.Value) != fmt.Sprint(value) {
		t.Fatalf("record %s = version %d value %s, want version %d value %d", rec.Key, rec.Version, rec.Value, version, value)
	}
}

func dumpOf(t *testing.T, n *Node) map[string][]string {
	t.Helper()
	var out map[string][]string
	if _, err := n.Read(testCtx(t), ReadStale, func(ctx context.Context, r Reader) error {
		var err error
		out, err = r.Dump(ctx)
		return err
	}); err != nil {
		t.Fatalf("Dump: %v", err)
	}
	return out
}

// Pods never self-bootstrap: fresh nodes stay without a configuration or a
// leader, and refuse writes, until Bootstrap is called.
func TestFreshNodesNeverSelfBootstrap(t *testing.T) {
	c := newTestCluster(t, 3, testProfile)
	time.Sleep(10 * testProfile.ElectionTimeout)
	for i, n := range c.nodes {
		s := n.Status()
		if s.State == raft.Leader.String() || s.Term != 0 || s.LastLogIndex != 0 || s.HasState {
			t.Fatalf("node %d without bootstrap: %+v", i, s)
		}
		if _, err := n.Apply(testCtx(t), cas("op-1", "kv:a", 0, 1)); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("node %d Apply before bootstrap: err = %v, want ErrNotLeader", i, err)
		}
	}
}

func TestBootstrapRefusesExistingState(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	leader, _ := c.waitLeader()
	mustApply(t, c.nodes[leader], cas("op-1", "kv:a", 0, 1))

	for i, n := range c.nodes {
		if err := n.Bootstrap(testCtx(t), c.servers()); !errors.Is(err, ErrHasState) {
			t.Fatalf("node %d re-bootstrap: err = %v, want ErrHasState", i, err)
		}
	}
	// Still refused after a restart, which must not bootstrap either.
	servers := c.servers()
	c.restart(0)
	if err := c.nodes[0].Bootstrap(testCtx(t), servers); !errors.Is(err, ErrHasState) {
		t.Fatalf("re-bootstrap after restart: err = %v, want ErrHasState", err)
	}
	_, n := c.waitLeader()
	rec, _, err := readKV(testCtx(t), n, ReadStrong, "kv:a")
	if err != nil {
		t.Fatal(err)
	}
	expectValue(t, rec, 1, 1)
}

func TestBootstrapRequiresSelfInConfiguration(t *testing.T) {
	c := newTestCluster(t, 3, testProfile)
	others := c.servers()[1:]
	err := c.nodes[0].Bootstrap(testCtx(t), others)
	if !errors.Is(err, ErrBadBootstrap) || errors.Is(err, ErrHasState) {
		t.Fatalf("bootstrap without self: err = %v, want ErrBadBootstrap", err)
	}
	moved := c.servers()
	moved[0].Address = "localhost:1"
	if err := c.nodes[0].Bootstrap(testCtx(t), moved); !errors.Is(err, ErrBadBootstrap) {
		t.Fatalf("bootstrap with a wrong advertise address: err = %v, want ErrBadBootstrap", err)
	}
	if c.nodes[0].Status().HasState {
		t.Fatal("a refused bootstrap wrote Raft state")
	}
}

// Verify (S1.3): a write is still readable through the new leader after
// the old leader is killed, and the old leader catches up on restart.
func TestWriteSurvivesLeaderKill(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	oldLeader, l := c.waitLeader()
	res := mustApply(t, l, cas("op-1", "kv:x", 0, 1))
	if res.Outcome != store.OutcomeOK || res.Shard != 1 {
		t.Fatalf("write = %+v", res)
	}
	oldID := l.Identity().ServerID()

	c.kill(oldLeader)
	newLeader, nl := c.waitLeader()
	if nl.Identity().ServerID() == oldID {
		t.Fatal("killed leader is still the leader")
	}
	rec, info, err := readKV(testCtx(t), nl, ReadStrong, "kv:x")
	if err != nil {
		t.Fatalf("strong read through new leader: %v", err)
	}
	expectValue(t, rec, 1, 1)
	if info.Consistency != "strong" || info.StalePossible || info.ServedBy != c.cfgs[newLeader].PodName || info.AppliedIndex < res.AppliedIndex {
		t.Fatalf("read info = %+v, want strong from %s at >= %d", info, c.cfgs[newLeader].PodName, res.AppliedIndex)
	}
	res2 := mustApply(t, nl, cas("op-2", "kv:x", 1, 2))
	if res2.Outcome != store.OutcomeOK {
		t.Fatalf("write through new leader = %+v", res2)
	}

	c.open(oldLeader)
	if err := c.nodes[oldLeader].WaitApplied(testCtx(t), res2.AppliedIndex); err != nil {
		t.Fatalf("restarted old leader did not catch up: %v", err)
	}
	rec, info, err = readKV(testCtx(t), c.nodes[oldLeader], ReadStale, "kv:x")
	if err != nil {
		t.Fatal(err)
	}
	expectValue(t, rec, 2, 2)
	if !info.StalePossible || info.Consistency != "stale" {
		t.Fatalf("stale read info = %+v", info)
	}
}

// A follower refuses writes and strong reads with a hint naming the leader,
// and serves stale reads only when asked, marked stalePossible.
func TestFollowerNotLeaderHintAndStaleRead(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	leader, l := c.waitLeader()
	res := mustApply(t, l, cas("op-1", "kv:a", 0, 1))
	fi, f := c.follower(leader)

	_, err := f.Apply(testCtx(t), cas("op-2", "kv:a", 1, 2))
	var nle *NotLeaderError
	if !errors.As(err, &nle) || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower Apply: err = %v, want *NotLeaderError", err)
	}
	if nle.LeaderID != l.Identity().ServerID() || nle.LeaderAddr != l.Addr() {
		t.Fatalf("hint = %+v, want %s at %s", nle, l.Identity().ServerID(), l.Addr())
	}
	if errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatal("not-leader must be definite, not an unknown outcome")
	}
	if _, _, err := readKV(testCtx(t), f, ReadStrong, "kv:a"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower strong read: err = %v, want ErrNotLeader", err)
	}

	if err := f.WaitApplied(testCtx(t), res.AppliedIndex); err != nil {
		t.Fatal(err)
	}
	rec, info, err := readKV(testCtx(t), f, ReadStale, "kv:a")
	if err != nil {
		t.Fatalf("follower stale read: %v", err)
	}
	expectValue(t, rec, 1, 1)
	if info.Consistency != "stale" || !info.StalePossible || info.ServedBy != c.cfgs[fi].PodName || info.AppliedIndex < res.AppliedIndex {
		t.Fatalf("stale read info = %+v", info)
	}
}

// Spike finding C1: raft.AppliedIndex() runs ahead of the FSM. With the
// leader's SQLite apply paused, Raft reports the entry applied but the FSM
// has not committed it: a strong read and WaitApplied must wait for the FSM,
// and the node must not report itself caught up.
func TestStrongReadWaitsForFSMNotRaftAppliedIndex(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))

	var paused atomic.Bool
	release := make(chan struct{})
	l.sm.setBeforeApply(func(uint64) {
		if paused.Load() {
			<-release
		}
	})
	paused.Store(true)
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	written := make(chan store.Result, 1)
	go func() {
		res, err := l.Apply(context.Background(), cas("op-2", "kv:a", 1, 2))
		if err != nil {
			t.Errorf("paused write: %v", err)
		}
		written <- res
	}()

	// Wait until Raft has handed the entry to the FSM goroutine.
	var s Status
	deadline := time.Now().Add(waitFor)
	for {
		s = l.Status()
		if s.RaftAppliedIndex > s.AppliedIndex {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("raft applied index never ran ahead of the paused FSM: %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s.CaughtUp {
		t.Fatalf("node reports caught up with the FSM behind: %+v", s)
	}
	short, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := l.WaitApplied(short, s.RaftAppliedIndex); err == nil {
		t.Fatal("WaitApplied returned while the FSM was paused")
	}

	type readResult struct {
		rec  store.Record
		info ReadInfo
		err  error
	}
	read := make(chan readResult, 1)
	go func() {
		rec, info, err := readKV(context.Background(), l, ReadStrong, "kv:a")
		read <- readResult{rec, info, err}
	}()
	select {
	case r := <-read:
		t.Fatalf("strong read returned before the FSM applied the write: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}

	paused.Store(false)
	close(release)
	released = true
	res := <-written
	r := <-read
	if r.err != nil {
		t.Fatalf("strong read: %v", r.err)
	}
	expectValue(t, r.rec, 2, 2)
	if r.info.AppliedIndex < res.AppliedIndex {
		t.Fatalf("read observed index %d, before the write at %d", r.info.AppliedIndex, res.AppliedIndex)
	}
}

// Invariant 5: when the leader loses its quorum mid-write, the outcome is
// unknown, not a rejection, and a retry of the same command after recovery
// gets a stored or fresh result, never OPERATION_ID_REUSED.
func TestLostQuorumIsUnknownOutcome(t *testing.T) {
	// A lease long enough that the leader is still leader when the write is
	// proposed, after both followers are gone.
	p := testProfile
	p.HeartbeatTimeout, p.ElectionTimeout, p.LeaderLeaseTimeout = 2*time.Second, 2*time.Second, 2*time.Second
	c := newBootstrappedCluster(t, p)
	leader, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))

	var down []int
	for i := range c.nodes {
		if i != leader {
			c.kill(i)
			down = append(down, i)
		}
	}
	cmd := cas("op-2", "kv:a", 1, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := l.Apply(ctx, cmd)
	if !errors.Is(err, store.ErrUnknownOutcome) || errors.Is(err, ErrNotLeader) {
		t.Fatalf("write without quorum: err = %v, want ErrUnknownOutcome", err)
	}

	for _, i := range down {
		c.open(i)
	}
	_, nl := c.waitLeader()
	res := mustApply(t, nl, cmd)
	if res.Outcome != store.OutcomeOK {
		t.Fatalf("retry of the same command = %+v, want OK (fresh or replayed)", res)
	}
	rec, _, err := readKV(testCtx(t), nl, ReadStrong, "kv:a")
	if err != nil {
		t.Fatal(err)
	}
	expectValue(t, rec, 2, 2)
}

// Verify (S1.3): a full restart rebuilds every FSM from the snapshot plus
// the log, over stale FSM files, and all replicas end with identical rows.
func TestRestartRebuildsFSMFromSnapshotAndLog(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	const keys = 10
	version := make([]uint64, keys)
	write := func(round int) store.Result {
		var last store.Result
		for k := range keys {
			last = mustApply(t, l, cas(fmt.Sprintf("op-%d-%d", round, k), fmt.Sprintf("kv:%d", k), version[k], round))
			if last.Outcome != store.OutcomeOK {
				t.Fatalf("round %d key %d = %+v", round, k, last)
			}
			version[k] = last.Version
		}
		return last
	}
	write(1)
	write(2)
	last := write(3)
	for i, n := range c.nodes {
		if err := n.WaitApplied(testCtx(t), last.AppliedIndex); err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		if _, err := n.Snapshot(testCtx(t)); err != nil {
			t.Fatalf("node %d Snapshot: %v", i, err)
		}
	}
	write(4) // the suffix replayed from the log

	for i := range c.nodes {
		c.kill(i)
	}
	for i, cfg := range c.cfgs {
		if err := os.WriteFile(filepath.Join(cfg.FSMDir, "fsm-1.db"), []byte("stale derived file"), 0o600); err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		c.open(i)
	}

	_, l = c.waitLeader()
	final := mustApply(t, l, cas("op-final", "kv:final", 0, 5))
	for k := range keys {
		rec, _, err := readKV(testCtx(t), l, ReadStrong, fmt.Sprintf("kv:%d", k))
		if err != nil {
			t.Fatalf("key %d: %v", k, err)
		}
		expectValue(t, rec, 4, 4)
	}
	var dumps []map[string][]string
	for i, n := range c.nodes {
		if err := n.WaitApplied(testCtx(t), final.AppliedIndex); err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		if s := n.Status(); s.AppliedIndex != final.AppliedIndex {
			t.Fatalf("node %d applied index %d, want %d", i, s.AppliedIndex, final.AppliedIndex)
		}
		dumps = append(dumps, dumpOf(t, n))
	}
	for i := 1; i < len(dumps); i++ {
		if !reflect.DeepEqual(dumps[0], dumps[i]) {
			t.Fatalf("node %d rows differ from node 0 at the same applied index", i)
		}
	}
}

// Identity survives a restart and is the Raft LocalID; the PVC holds only
// identity, Raft log/stable store and snapshots, and the FSM lives on the
// emptyDir (M3).
func TestIdentityAndDiskLayout(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	before := c.nodes[1].Identity()
	if s := c.nodes[1].Status(); s.ServerID != before.ServerID() || s.Address != c.nodes[1].Addr() {
		t.Fatalf("status id/address = %s / %s, want %s / %s", s.ServerID, s.Address, before.ServerID(), c.nodes[1].Addr())
	}
	c.restart(1)
	if after := c.nodes[1].Identity(); after != before {
		t.Fatalf("identity after restart = %+v, want %+v", after, before)
	}

	ents, err := os.ReadDir(c.cfgs[1].DataDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if want := []string{identityFile, "raft", "snapshots"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("PVC holds %v, want %v", names, want)
	}
	matches, err := filepath.Glob(filepath.Join(c.cfgs[1].FSMDir, "fsm-*.db"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("emptyDir FSM files = %v (err %v), want exactly one", matches, err)
	}
}

func TestNotLeaderErrorIs(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &NotLeaderError{LeaderID: "storage-1-0/x", LeaderAddr: "a:1"})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatal("NotLeaderError does not match ErrNotLeader")
	}
}

// An index inside the compacted range is satisfied once the FSM holds the
// newest snapshot's image; WaitApplied and CaughtUp never error or hang on
// it. The last entry before the snapshot is a barrier (a strong read), so
// Raft's snapshot index is an entry the SQLite FSM never records.
func TestWaitAppliedOnCompactedRange(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	var first, last store.Result
	for i := range 20 {
		last = mustApply(t, l, cas(fmt.Sprintf("op-%d", i), "kv:a", uint64(i), i+1))
		if i == 0 {
			first = last
		}
	}
	if _, _, err := readKV(testCtx(t), l, ReadStrong, "kv:a"); err != nil { // barrier entry
		t.Fatal(err)
	}
	barrierIndex := l.Status().LastLogIndex
	if barrierIndex <= last.AppliedIndex {
		t.Fatalf("last log index %d is not after the last command %d", barrierIndex, last.AppliedIndex)
	}
	if _, err := l.Snapshot(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	logFirst, err := l.logs.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if logFirst <= first.AppliedIndex {
		t.Fatalf("log first index %d: index %d was not compacted (TrailingLogs %d)", logFirst, first.AppliedIndex, testProfile.TrailingLogs)
	}

	check := func(n *Node, what string) {
		t.Helper()
		for _, idx := range []uint64{first.AppliedIndex, logFirst - 1, barrierIndex} {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := n.WaitApplied(ctx, idx)
			cancel()
			if err != nil {
				t.Fatalf("%s: WaitApplied(%d) with log first index %d: %v", what, idx, logFirst, err)
			}
		}
		deadline := time.Now().Add(waitFor)
		for !n.Status().CaughtUp {
			if time.Now().After(deadline) {
				t.Fatalf("%s: never caught up: %+v", what, n.Status())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	check(l, "leader")

	// After a restart the log starts after the compacted range and the FSM
	// comes back from the snapshot alone.
	c.restart(li)
	c.waitLeader()
	check(c.nodes[li], "restarted node")
	rec, _, err := readKV(testCtx(t), c.nodes[li], ReadStale, "kv:a")
	if err != nil {
		t.Fatal(err)
	}
	expectValue(t, rec, 20, 20)
}

// A node restarted after its identity was created but before any bootstrap
// keeps its incarnation, and can still be bootstrapped: the incarnation in
// the stable store is not Raft state.
func TestRestartBeforeBootstrapKeepsIdentity(t *testing.T) {
	c := newTestCluster(t, 1, testProfile)
	before := c.nodes[0].Identity()
	c.restart(0)
	if after := c.nodes[0].Identity(); after != before {
		t.Fatalf("identity after restart = %+v, want %+v", after, before)
	}
	if s := c.nodes[0].Status(); s.HasState {
		t.Fatal("restart before bootstrap reports Raft state")
	}
	if err := c.nodes[0].Bootstrap(testCtx(t), c.servers()); err != nil {
		t.Fatalf("Bootstrap after restart: %v", err)
	}
	c.waitLeader()
}

// Deleting raft/ while identity.json survives must not let the node rejoin
// under its old ServerID with an empty log (it could vote twice in a term
// or ack entries it no longer has). Open refuses it, bootstrapped or not.
func TestWipedRaftDirWithIdentityRefused(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))
	fresh := newTestCluster(t, 1, testProfile)
	for name, tc := range map[string]struct {
		c *testCluster
		i int
	}{"bootstrapped member": {c, 1}, "never bootstrapped": {fresh, 0}} {
		tc.c.kill(tc.i)
		cfg := tc.c.cfgs[tc.i]
		if err := os.RemoveAll(filepath.Join(cfg.DataDir, "raft")); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(cfg.DataDir, identityFile)); err != nil {
			t.Fatalf("%s: identity file: %v", name, err)
		}
		n, err := Open(testCtx(t), cfg)
		if err == nil {
			_ = n.Close()
		}
		if !errors.Is(err, ErrIdentityMismatch) {
			t.Errorf("%s: Open with raft/ wiped = %v, want ErrIdentityMismatch", name, err)
		}
	}
}

// A follower that falls behind the leader's compacted log catches up through
// InstallSnapshot over the network: it restores the leader's image into a
// new database generation, removes the old one, and ends with the leader's
// rows. A read in flight on the old database finishes before the swap.
func TestLaggingFollowerInstallsSnapshotOverNetwork(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	fi, f := c.follower(li)
	var last store.Result
	for i := range 3 {
		last = mustApply(t, l, cas(fmt.Sprintf("op-a%d", i), "kv:a", uint64(i), i+1))
	}
	if err := f.WaitApplied(testCtx(t), last.AppliedIndex); err != nil {
		t.Fatal(err)
	}
	followerLast := f.Status().LastLogIndex
	c.kill(fi)
	if snaps, err := os.ReadDir(filepath.Join(c.cfgs[fi].DataDir, "snapshots")); err != nil || len(snaps) != 0 {
		t.Fatalf("follower has local snapshots %v (err %v); the test needs it to depend on the leader's", snaps, err)
	}

	for i := range 5 * int(testProfile.TrailingLogs) {
		last = mustApply(t, l, cas(fmt.Sprintf("op-b%d", i), fmt.Sprintf("kv:%d", i), 0, i))
	}
	if _, err := l.Snapshot(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if first, err := l.logs.FirstIndex(); err != nil || first <= followerLast+1 {
		t.Fatalf("leader log first index %d (err %v): the follower's next entry %d was not compacted", first, err, followerLast+1)
	}

	// The swap waits until a stale read on the old database is in flight.
	readStarted, swapWaiting := make(chan struct{}), make(chan struct{})
	var swaps atomic.Int32
	c.cfgs[fi].beforeRestoreSwap = func() {
		if swaps.Add(1) == 1 {
			<-readStarted
			close(swapWaiting)
		}
	}
	c.open(fi)
	f = c.nodes[fi]
	type inflight struct {
		gen uint64
		err error
	}
	readDone := make(chan inflight, 1)
	go func() {
		var got inflight
		_, err := f.Read(testCtx(t), ReadStale, func(ctx context.Context, r Reader) error {
			close(readStarted)
			select {
			case <-swapWaiting:
			case <-time.After(waitFor):
				return errors.New("InstallSnapshot never reached the swap")
			}
			time.Sleep(100 * time.Millisecond) // Restore is now blocked on the read lock
			got.gen = f.sm.gen                 // read lock held: Restore cannot write it
			_, err := r.Dump(ctx)              // the old handle must still be open
			return err
		})
		got.err = err
		readDone <- got
	}()
	got := <-readDone
	if got.err != nil {
		t.Fatalf("read in flight during restore: %v", got.err)
	}
	if got.gen != 0 {
		t.Fatalf("read in flight saw generation %d: the swap did not wait for it", got.gen)
	}

	final := mustApply(t, l, cas("op-final", "kv:final", 0, 1))
	for _, n := range []*Node{l, f} {
		if err := n.WaitApplied(testCtx(t), final.AppliedIndex); err != nil {
			t.Fatal(err)
		}
	}
	var gen uint64
	if err := f.sm.read(func(*fsm.FSM) error { gen = f.sm.gen; return nil }); err != nil {
		t.Fatal(err)
	}
	if gen == 0 || swaps.Load() == 0 {
		t.Fatalf("follower generation %d after %d swaps: it did not restore via InstallSnapshot", gen, swaps.Load())
	}
	old, err := filepath.Glob(filepath.Join(c.cfgs[fi].FSMDir, "fsm-0.db*"))
	if err != nil || len(old) != 0 {
		t.Fatalf("old database files %v survived the restore (err %v)", old, err)
	}
	if _, err := os.Stat(f.sm.dbPath(gen)); err != nil {
		t.Fatalf("restored database: %v", err)
	}
	if !reflect.DeepEqual(dumpOf(t, l), dumpOf(t, f)) {
		t.Fatal("follower rows differ from the leader's at the same applied index")
	}
}

// Term is the current term without building a full Status.
func TestTermMatchesStatus(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	if term := l.Term(); term == 0 || term != l.Status().Term {
		t.Fatalf("Term() = %d, Status().Term = %d", term, l.Status().Term)
	}
}

// WaitApplyAfter returns when the FSM applies a later command, when this
// node's leadership changes, or when ctx ends, and never holds the read lock
// (a Restore could otherwise wait behind a long-poll).
func TestWaitApplyAfter(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	first := mustApply(t, l, cas("op-1", "kv:a", 0, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	start := time.Now()
	l.WaitApplyAfter(ctx, first.AppliedIndex)
	cancel()
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("returned after %v with nothing new applied", d)
	}

	done := make(chan struct{})
	go func() {
		l.WaitApplyAfter(testCtx(t), first.AppliedIndex)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	// The wait holds no lock that would stop a Restore's swap.
	l.sm.mu.Lock()
	l.sm.mu.Unlock() //nolint:staticcheck // checks the lock is free
	mustApply(t, l, cas("op-2", "kv:a", 1, 2))
	select {
	case <-done:
	case <-time.After(waitFor):
		t.Fatal("a commit did not end the wait")
	}

	idle := l.sm.appliedIndex()
	done = make(chan struct{})
	go func() {
		l.WaitApplyAfter(testCtx(t), idle)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	stepDown(t, l)
	select {
	case <-done:
	case <-time.After(waitFor):
		t.Fatalf("losing leadership (node %d) did not end the wait", li)
	}
}
