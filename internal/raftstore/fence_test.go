package raftstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// S4.1: the read fence (v3.1 §13) and its §14 tests. A ReadLinearizable
// read on the leader needs an entry of the leader's own term committed and
// applied, a quorum of acknowledgements to RPCs sent after the read began,
// and the SQLite FSM to have completed every command up to the captured
// commit index. It never writes to the log.

// slowLease keeps a leader that has lost its followers in the Leader state
// for about a second (raft v1.8.0 steps down when LeaderLeaseTimeout passes
// without a quorum, raft.go:1037-1080), so a fenced read can be started on
// it while it still believes it leads. Heartbeats go out every 100-200 ms
// (HeartbeatTimeout/10, replication.go:402).
var slowLease = Profile{
	Name:               "slow-lease",
	SnapshotThreshold:  1 << 30,
	TrailingLogs:       4,
	SnapshotInterval:   time.Hour,
	HeartbeatTimeout:   time.Second,
	ElectionTimeout:    time.Second,
	LeaderLeaseTimeout: time.Second,
}

// testNet blocks outgoing RPCs between chosen addresses and lets a test run
// a hook after an RPC succeeds but before Raft sees its response.
type testNet struct {
	mu      sync.Mutex
	blocked map[[2]raft.ServerAddress]bool // from, to
	after   atomic.Pointer[func(from, to raft.ServerAddress, req any)]
}

func (n *testNet) block(from, to raft.ServerAddress) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[[2]raft.ServerAddress{from, to}] = true
}

// isolate cuts every RPC to and from a.
func (n *testNet) isolate(a raft.ServerAddress, others ...raft.ServerAddress) {
	for _, o := range others {
		n.block(a, o)
		n.block(o, a)
	}
}

func (n *testNet) dropped(from, to raft.ServerAddress) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.blocked[[2]raft.ServerAddress{from, to}]
}

// newPartitionableCluster is newBootstrappedCluster with every node's
// outgoing RPCs routed through net. Nodes are reopened with the hooks
// before the group is bootstrapped.
func newPartitionableCluster(t testing.TB, p Profile, fence ReadFence) (*testCluster, *testNet) {
	t.Helper()
	net := &testNet{blocked: map[[2]raft.ServerAddress]bool{}}
	c := newTestCluster(t, 3, p)
	for i := range c.cfgs {
		self := raft.ServerAddress(c.cfgs[i].AdvertiseAddr)
		c.cfgs[i].ReadFence = fence
		c.cfgs[i].dropSend = func(to raft.ServerAddress, _ any) bool { return net.dropped(self, to) }
		c.cfgs[i].afterSend = func(to raft.ServerAddress, req any) {
			if h := net.after.Load(); h != nil {
				(*h)(self, to, req)
			}
		}
		c.restart(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	if err := c.nodes[0].Bootstrap(ctx, c.servers()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	c.waitLeader()
	return c, net
}

// retryable is how a fenced read may fail: never with data.
func retryable(err error) bool {
	return errors.Is(err, ErrNotLeader) || errors.Is(err, ErrUnavailable)
}

// fencedRead is a ReadLinearizable of key under a fresh deadline.
func fencedRead(n *Node, key string, d time.Duration) (store.Record, ReadInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return readKV(ctx, n, ReadLinearizable, key)
}

// mustFencedRead retries a retryable failure (a leader change) until
// waitFor passes; any other failure is fatal.
func mustFencedRead(t *testing.T, c *testCluster, key string) (store.Record, ReadInfo) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for {
		_, l := c.waitLeader()
		rec, info, err := fencedRead(l, key, time.Second)
		if err == nil {
			return rec, info
		}
		if !retryable(err) || time.Now().After(deadline) {
			t.Fatalf("fenced read of %s: %v", key, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The fence serves the latest committed value, reports itself as
// linearizable and appends nothing to the log. With STRONG_READ_FENCE=barrier
// the same read is a strong read: one barrier entry per read.
func TestFencedReadWritesNoLogEntry(t *testing.T) {
	for _, tc := range []struct {
		fence       ReadFence
		consistency string
		perRead     uint64
	}{
		{"", "linearizable", 0}, // the zero value is the fence
		{ReadFenceIndex, "linearizable", 0},
		{ReadFenceBarrier, "strong", 1},
	} {
		t.Run(fmt.Sprintf("fence=%q", tc.fence), func(t *testing.T) {
			c, _ := newPartitionableCluster(t, testProfile, tc.fence)
			_, l := c.waitLeader()
			mustApply(t, l, cas("op-1", "kv:a", 0, 1))
			before := l.raft.LastIndex()
			const reads = 20
			for i := range reads {
				rec, info, err := fencedRead(l, "kv:a", 2*time.Second)
				if err != nil {
					t.Fatalf("read %d: %v", i, err)
				}
				expectValue(t, rec, 1, 1)
				if info.Consistency != tc.consistency || info.StalePossible || info.ServedBy != l.cfg.PodName {
					t.Fatalf("read %d info = %+v, want consistency %q", i, info, tc.consistency)
				}
			}
			if got := l.raft.LastIndex() - before; got != reads*tc.perRead {
				t.Fatalf("%d reads appended %d log entries, want %d", reads, got, reads*tc.perRead)
			}
		})
	}
}

func TestOpenRejectsUnknownReadFence(t *testing.T) {
	cfg := Config{PodName: "storage-1-0", GroupID: "shard-1", Shard: 1, DataDir: t.TempDir(), FSMDir: t.TempDir(),
		BindAddr: "127.0.0.1:0", AdvertiseAddr: "localhost:0", Profile: testProfile, ReadFence: "lease"}
	n, err := Open(testCtx(t), cfg)
	if err == nil {
		_ = n.Close()
		t.Fatal(`Open accepted ReadFence "lease"`)
	}
}

// Safety: a leader cut off from both followers must not serve a fenced read
// started after the cut, even while it still reports Leader. The majority
// elects a new leader and commits kv:a=2; no read on the old leader ever
// returns kv:a=1 as linearizable. Every refusal is retryable and arrives
// within the read's deadline.
func TestFencedReadIsolatedLeaderRefuses(t *testing.T) {
	c, net := newPartitionableCluster(t, slowLease, ReadFenceIndex)
	li, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))
	if rec, _, err := fencedRead(l, "kv:a", 2*time.Second); err != nil {
		t.Fatalf("fenced read before the partition: %v", err)
	} else {
		expectValue(t, rec, 1, 1)
	}

	var others []raft.ServerAddress
	for i, n := range c.nodes {
		if i != li {
			others = append(others, n.Addr())
		}
	}
	net.isolate(l.Addr(), others...)

	const deadline = 300 * time.Millisecond
	type result struct {
		asLeader bool
		elapsed  time.Duration
		rec      store.Record
		err      error
	}
	var (
		mu      sync.Mutex
		results []result
	)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			asLeader := l.raft.State() == raft.Leader
			start := time.Now()
			rec, _, err := fencedRead(l, "kv:a", deadline)
			mu.Lock()
			results = append(results, result{asLeader, time.Since(start), rec, err})
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// The majority side elects a leader and commits kv:a=2.
	var nl *Node
	for end := time.Now().Add(waitFor); nl == nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("the majority never elected a new leader")
		}
		for i, n := range c.nodes {
			if i != li && n.raft.State() == raft.Leader {
				nl = n
			}
		}
	}
	mustApply(t, nl, cas("op-2", "kv:a", 1, 2))
	time.Sleep(200 * time.Millisecond) // reads on the old leader after the write
	close(stop)
	<-done

	asLeader := 0
	for i, r := range results {
		if r.asLeader {
			asLeader++
		}
		if r.err == nil {
			t.Fatalf("read %d on the isolated leader (leader at start: %v) served version %d value %s as linearizable",
				i, r.asLeader, r.rec.Version, r.rec.Value)
		}
		if !retryable(r.err) {
			t.Fatalf("read %d failed with %v, want NotLeader or Unavailable (retryable)", i, r.err)
		}
		if r.elapsed > deadline+200*time.Millisecond {
			t.Fatalf("read %d took %s, past its %s deadline", i, r.elapsed, deadline)
		}
	}
	if asLeader == 0 {
		t.Fatalf("no read started while the isolated node still reported Leader (%d reads); the fence was never exercised", len(results))
	}
	t.Logf("%d reads on the isolated leader, %d started while it reported Leader, all refused", len(results), asLeader)
}

// isHeartbeat matches the request raft v1.8.0's heartbeat loop sends: no
// entries, no previous entry, no commit index (replication.go:390-395).
func isHeartbeat(req any) bool {
	ae, ok := req.(*raft.AppendEntriesRequest)
	return ok && len(ae.Entries) == 0 && ae.PrevLogEntry == 0 && ae.LeaderCommitIndex == 0
}

// raft v1.8.0's VerifyLeader counts any successful heartbeat response that
// arrives after it is registered (replication.go:437, future.go:274-293),
// including the response to a heartbeat sent before the read began. Here
// one follower is cut off; the other's heartbeat response is held until
// after the read starts, and every later RPC to it is dropped. VerifyLeader
// alone succeeds on that old acknowledgement; the fence must not.
func TestFencedReadStaleAckNotCounted(t *testing.T) {
	c, net := newPartitionableCluster(t, slowLease, ReadFenceIndex)
	li, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))
	if rec, _, err := fencedRead(l, "kv:a", 2*time.Second); err != nil {
		t.Fatalf("fenced read before the fault: %v", err)
	} else {
		expectValue(t, rec, 1, 1)
	}
	fi, f1 := c.follower(li)
	var f2 *Node
	for i, n := range c.nodes {
		if i != li && i != fi {
			f2 = n
		}
	}
	net.isolate(l.Addr(), f2.Addr())

	held, release := make(chan struct{}), make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	hook := func(from, to raft.ServerAddress, req any) {
		if from != l.Addr() || to != f1.Addr() || !isHeartbeat(req) {
			return
		}
		first := false
		holdOnce.Do(func() {
			first = true
			net.block(l.Addr(), f1.Addr()) // no fresh acknowledgement from f1
			close(held)
		})
		if first {
			<-release
		}
	}
	net.after.Store(&hook)
	select {
	case <-held:
	case <-time.After(waitFor):
		t.Fatal("no heartbeat response from f1 was held")
	}
	// The held response answers a heartbeat sent before this point.
	if l.raft.State() != raft.Leader {
		t.Fatal("leader stepped down before the read; the lease is too short for this test")
	}

	type readResult struct {
		rec  store.Record
		info ReadInfo
		err  error
	}
	read := make(chan readResult, 1)
	go func() {
		rec, info, err := fencedRead(l, "kv:a", 3*time.Second)
		read <- readResult{rec, info, err}
	}()
	verified := make(chan error, 1)
	time.Sleep(50 * time.Millisecond) // the fenced read is past readStart
	go func() { verified <- l.raft.VerifyLeader().Error() }()
	time.Sleep(50 * time.Millisecond) // both are waiting for votes
	releaseOnce.Do(func() { close(release) })

	// Control: plain VerifyLeader accepts the pre-read acknowledgement.
	select {
	case err := <-verified:
		if err != nil {
			t.Fatalf("control: VerifyLeader failed (%v); the held acknowledgement did not reach it, so the test proves nothing", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("control: VerifyLeader did not resolve on the held acknowledgement")
	}
	r := <-read
	if r.err == nil {
		t.Fatalf("fenced read served version %d on an acknowledgement sent before it began: %+v", r.rec.Version, r.info)
	}
	if !retryable(r.err) {
		t.Fatalf("fenced read failed with %v, want NotLeader or Unavailable (retryable)", r.err)
	}
}

// Read-your-writes: a committed write is visible to the next fenced read,
// on the same leader and on the next leader straight after a transfer.
func TestFencedReadYourWrites(t *testing.T) {
	c, _ := newPartitionableCluster(t, testProfile, ReadFenceIndex)
	_, l := c.waitLeader()
	v := uint64(0)
	write := func(l *Node) {
		t.Helper()
		mustApply(t, l, cas(fmt.Sprintf("op-%d", v+1), "kv:a", v, int(v+1)))
		v++
	}
	for range 50 {
		write(l)
		rec, _, err := fencedRead(l, "kv:a", 2*time.Second)
		if err != nil {
			t.Fatalf("fenced read after write %d: %v", v, err)
		}
		expectValue(t, rec, v, int(v))
	}
	for round := range 5 {
		_, l := c.waitLeader()
		write(l)
		stepDown(t, l)
		if _, _, err := fencedRead(l, "kv:a", time.Second); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("round %d: fenced read on the old leader after it stepped down: %v, want ErrNotLeader", round, err)
		}
		rec, info := mustFencedRead(t, c, "kv:a")
		expectValue(t, rec, v, int(v))
		if info.Consistency != "linearizable" {
			t.Fatalf("round %d: read info %+v", round, info)
		}
	}
}

// A new leader must not serve a fenced read until an entry of its own term
// has committed and applied: before that its commit index is what it heard
// as a follower and may lag the old leader's (commitment.go:100). The
// follower's first append of the new term is held, so the no-op cannot
// commit. Concurrent first reads wait (or refuse, retryably); none serves.
// After the release every one of them sees the old leader's last write.
func TestFencedReadNewLeaderBeforeOwnTermCommit(t *testing.T) {
	e := newHeldElection(t, func(ae *raft.AppendEntriesRequest, _ uint64) bool { return len(ae.Entries) > 0 })

	short := time.Now()
	if _, _, err := fencedRead(e.leader, "kv:a", 300*time.Millisecond); err == nil || !retryable(err) {
		t.Fatalf("fenced read before the new term committed: %v, want a retryable refusal", err)
	}
	if d := time.Since(short); d > 500*time.Millisecond {
		t.Fatalf("refusal took %s, past the 300ms deadline", d)
	}

	type readResult struct {
		rec  store.Record
		info ReadInfo
		err  error
	}
	const readers = 8
	results := make(chan readResult, readers)
	for range readers {
		go func() {
			rec, info, err := fencedRead(e.leader, "kv:a", waitFor)
			results <- readResult{rec, info, err}
		}()
	}
	var early []readResult
	timeout := time.After(300 * time.Millisecond)
collect:
	for {
		select {
		case r := <-results:
			if r.err == nil {
				t.Fatalf("fenced read served %+v before the new leader's own term committed", r.info)
			}
			if !retryable(r.err) {
				t.Fatalf("fenced read before the new term committed: %v, want retryable", r.err)
			}
			early = append(early, r)
		case <-timeout:
			break collect
		}
	}
	if s := e.leader.Status(); s.State != raft.Leader.String() || s.CommitIndex >= s.LastLogIndex {
		t.Fatalf("the hold did not keep the no-op uncommitted: %+v", s)
	}
	e.release()
	for range readers - len(early) {
		r := <-results
		if r.err != nil {
			t.Fatalf("waiting fenced read after the new term committed: %v", r.err)
		}
		expectValue(t, r.rec, 5, 5)
	}
	for range early { // the ones that refused succeed when retried
		rec, _, err := fencedRead(e.leader, "kv:a", 2*time.Second)
		if err != nil {
			t.Fatalf("retried fenced read: %v", err)
		}
		expectValue(t, rec, 5, 5)
	}
}

// The fence waits for SQLite to finish, not for Raft to hand the entry to
// the FSM (C1): raft.AppliedIndex runs ahead while the FSM is paused.
func TestFencedReadWaitsForSQLiteNotRaft(t *testing.T) {
	c, _ := newPartitionableCluster(t, testProfile, ReadFenceIndex)
	_, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))

	var paused atomic.Bool
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	l.sm.setBeforeApply(func(uint64) {
		if paused.Load() {
			<-release
		}
	})
	paused.Store(true)
	written := make(chan store.Result, 1)
	go func() {
		res, err := l.Apply(context.Background(), cas("op-2", "kv:a", 1, 2))
		if err != nil {
			t.Errorf("paused write: %v", err)
		}
		written <- res
	}()
	for deadline := time.Now().Add(waitFor); ; time.Sleep(5 * time.Millisecond) {
		s := l.Status()
		if s.RaftAppliedIndex > s.AppliedIndex {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("raft applied index never ran ahead of the paused FSM: %+v", s)
		}
	}

	type readResult struct {
		rec  store.Record
		info ReadInfo
		err  error
	}
	read := make(chan readResult, 1)
	go func() {
		rec, info, err := fencedRead(l, "kv:a", waitFor)
		read <- readResult{rec, info, err}
	}()
	select {
	case r := <-read:
		t.Fatalf("fenced read returned before SQLite applied the committed write: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	paused.Store(false)
	releaseOnce.Do(func() { close(release) })
	res := <-written
	r := <-read
	if r.err != nil {
		t.Fatalf("fenced read: %v", r.err)
	}
	expectValue(t, r.rec, 2, 2)
	if r.info.AppliedIndex < res.AppliedIndex || r.info.Consistency != "linearizable" {
		t.Fatalf("read info %+v, write applied at %d", r.info, res.AppliedIndex)
	}
}

// The captured commit index is often an entry that never reaches
// FSM.Apply: the new term's no-op, or a barrier. The FSM's own applied
// index never equals it, so the fence must wait for the last command at or
// below it instead of hanging until the deadline.
func TestFencedReadIndexOnNonCommandEntry(t *testing.T) {
	e := newHeldElection(t, func(ae *raft.AppendEntriesRequest, _ uint64) bool { return len(ae.Entries) > 0 })
	e.release()
	l := e.leader
	entryAtCommit := func() raft.LogType {
		t.Helper()
		for deadline := time.Now().Add(waitFor); ; time.Sleep(5 * time.Millisecond) {
			if ci := l.raft.CommitIndex(); ci == l.raft.LastIndex() {
				var entry raft.Log
				if err := l.logs.GetLog(ci, &entry); err != nil {
					t.Fatalf("log %d: %v", ci, err)
				}
				return entry.Type
			}
			if time.Now().After(deadline) {
				t.Fatal("commit index never reached the last log index")
			}
		}
	}
	if typ := entryAtCommit(); typ != raft.LogNoop {
		t.Fatalf("entry at the commit index is %v, want the new term's no-op", typ)
	}
	check := func(what string) {
		t.Helper()
		start := time.Now()
		rec, info, err := fencedRead(l, "kv:a", 2*time.Second)
		if err != nil {
			t.Fatalf("fenced read with the commit index on %s: %v", what, err)
		}
		expectValue(t, rec, 5, 5)
		if d := time.Since(start); d > time.Second {
			t.Fatalf("fenced read with the commit index on %s took %s", what, d)
		}
		if s := l.Status(); info.AppliedIndex != s.AppliedIndex || info.AppliedIndex >= s.CommitIndex {
			t.Fatalf("read observed %d; FSM at %d, commit index %d", info.AppliedIndex, s.AppliedIndex, s.CommitIndex)
		}
	}
	check("a no-op")
	if _, _, err := readKV(testCtx(t), l, ReadStrong, "kv:a"); err != nil {
		t.Fatalf("strong read: %v", err)
	}
	if typ := entryAtCommit(); typ != raft.LogBarrier {
		t.Fatalf("entry at the commit index after a strong read is %v, want a barrier", typ)
	}
	check("a barrier")
}

// The captured read index is compacted away while the fence waits: the
// read is held after the quorum check and before the index is mapped to a
// command, the leader commits more writes and takes a snapshot that
// truncates the log past the index (TrailingLogs 4). The fence must map
// the index through the snapshot and serve the current value, neither
// failing nor hanging.
func TestFencedReadAcrossCompaction(t *testing.T) {
	c, _ := newPartitionableCluster(t, testProfile, ReadFenceIndex)
	_, l := c.waitLeader()
	mustApply(t, l, cas("op-0", "kv:a", 0, 1))

	captured := make(chan uint64, 1)
	release := make(chan struct{})
	var armed atomic.Bool
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	hook := func(readIndex uint64) {
		if armed.CompareAndSwap(true, false) {
			captured <- readIndex
			<-release
		}
	}
	l.beforeFenceWait.Store(&hook)
	armed.Store(true)

	type readResult struct {
		rec  store.Record
		info ReadInfo
		err  error
	}
	read := make(chan readResult, 1)
	go func() {
		rec, info, err := fencedRead(l, "kv:a", waitFor)
		read <- readResult{rec, info, err}
	}()
	var readIndex uint64
	select {
	case readIndex = <-captured:
	case <-time.After(waitFor):
		t.Fatal("the fence never reached its wait")
	}

	v := uint64(1)
	for ; v < 20; v++ {
		mustApply(t, l, cas(fmt.Sprintf("op-%d", v), "kv:a", v, int(v+1)))
	}
	snap, err := l.Snapshot(testCtx(t))
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	first, err := l.logs.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if first <= readIndex {
		t.Fatalf("log still starts at %d, not past the read index %d (snapshot %+v)", first, readIndex, snap)
	}
	releaseOnce.Do(func() { close(release) })

	r := <-read
	if r.err != nil {
		t.Fatalf("fenced read across compaction (read index %d, log from %d): %v", readIndex, first, r.err)
	}
	expectValue(t, r.rec, v, int(v))
	if r.info.AppliedIndex < snap.AppliedIndex {
		t.Fatalf("read observed %d, before the snapshot's %d", r.info.AppliedIndex, snap.AppliedIndex)
	}
}

// A fence that cannot complete (no follower answers) never serves, and
// returns a retryable error within the caller's deadline.
func TestFencedReadDeadline(t *testing.T) {
	c, _ := newPartitionableCluster(t, slowLease, ReadFenceIndex)
	li, l := c.waitLeader()
	mustApply(t, l, cas("op-1", "kv:a", 0, 1))
	if _, _, err := fencedRead(l, "kv:a", 2*time.Second); err != nil {
		t.Fatalf("fenced read with a quorum: %v", err)
	}
	for i := range c.nodes {
		if i != li {
			c.kill(i)
		}
	}
	if l.raft.State() != raft.Leader {
		t.Fatal("leader stepped down before the read; the lease is too short for this test")
	}
	const deadline = 200 * time.Millisecond
	start := time.Now()
	_, _, err := fencedRead(l, "kv:a", deadline)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("fenced read served with no follower reachable")
	}
	if !retryable(err) {
		t.Fatalf("fenced read: %v, want NotLeader or Unavailable (retryable)", err)
	}
	if elapsed > deadline+150*time.Millisecond {
		t.Fatalf("fenced read returned after %s, deadline %s", elapsed, deadline)
	}
}

// Stale reads do not depend on the fence setting; a fenced read on a
// follower is NotLeader with the leader's hint.
func TestStaleReadUnaffectedByReadFence(t *testing.T) {
	for _, fence := range []ReadFence{ReadFenceIndex, ReadFenceBarrier} {
		t.Run(string(fence), func(t *testing.T) {
			c, _ := newPartitionableCluster(t, testProfile, fence)
			li, l := c.waitLeader()
			mustApply(t, l, cas("op-1", "kv:a", 0, 1))
			waitAllCaughtUp(t, c, l)
			_, f := c.follower(li)
			rec, info, err := readKV(testCtx(t), f, ReadStale, "kv:a")
			if err != nil {
				t.Fatalf("stale read on a follower: %v", err)
			}
			expectValue(t, rec, 1, 1)
			if info.Consistency != "stale" || !info.StalePossible || info.ServedBy != f.cfg.PodName {
				t.Fatalf("stale read info = %+v", info)
			}
			_, _, err = fencedRead(f, "kv:a", time.Second)
			var nle *NotLeaderError
			if !errors.As(err, &nle) || nle.LeaderID != l.Identity().ServerID() {
				t.Fatalf("fenced read on a follower: %v, want NotLeaderError naming %s", err, l.Identity().ServerID())
			}
		})
	}
}

// BenchmarkStrongRead compares a strong read (a barrier through the log)
// with a fenced linearizable read on a 3-node loopback cluster, one reader
// and GOMAXPROCS readers. entries/op is log entries appended per read.
//
//	go test ./internal/raftstore -run '^$' -bench StrongRead -benchtime 2000x -count 5
func BenchmarkStrongRead(b *testing.B) {
	c, _ := newPartitionableCluster(b, testProfile, ReadFenceIndex)
	_, l := c.waitLeader()
	mustApply(b, l, cas("op-1", "kv:a", 0, 1))
	for _, m := range []struct {
		name string
		mode ReadMode
	}{{"barrier", ReadStrong}, {"fence", ReadLinearizable}} {
		read := func() error {
			_, _, err := readKV(context.Background(), l, m.mode, "kv:a")
			return err
		}
		b.Run(m.name+"/serial", func(b *testing.B) {
			before := l.raft.LastIndex()
			for b.Loop() {
				if err := read(); err != nil {
					b.Fatalf("%s read: %v", m.name, err)
				}
			}
			b.ReportMetric(float64(l.raft.LastIndex()-before)/float64(b.N), "entries/op")
		})
		b.Run(m.name+"/parallel", func(b *testing.B) {
			before := l.raft.LastIndex()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := read(); err != nil {
						b.Errorf("%s read: %v", m.name, err) // FailNow is not allowed off the benchmark goroutine
						return
					}
				}
			})
			b.ReportMetric(float64(l.raft.LastIndex()-before)/float64(b.N), "entries/op")
		})
	}
}
