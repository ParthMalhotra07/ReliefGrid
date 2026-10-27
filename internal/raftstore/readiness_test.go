package raftstore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// A restarted follower knows its leader from the first heartbeat, before any
// append tells it how far the leader has committed: heartbeats carry no
// commit index (raft v1.8.0 net_transport.go), and Raft's own commit index
// starts at 0 after a restart. Readiness judged against that local commit
// index called the replica caught up at appliedIndex 0 (issue #22). Holding
// the follower's first append open makes that window deterministic: the
// leader is known, nothing has been replicated, and the replica must not be
// caught up until its FSM covers the leader's commit index.
func TestRestartedFollowerNotCaughtUpBeforeLeaderCommit(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	fi, _ := c.follower(li)
	for i := range 10 {
		mustApply(t, l, cas(fmt.Sprintf("op-a-%d", i), "kv:a", uint64(i), i+1))
	}
	c.kill(fi)
	var last uint64
	for i := range 20 {
		last = mustApply(t, l, cas(fmt.Sprintf("op-b-%d", i), "kv:a", uint64(10+i), 11+i)).AppliedIndex
	}

	held, release := make(chan struct{}), make(chan struct{})
	var once bool
	c.cfgs[fi].beforeRPC = func(rpc raft.RPC) {
		if ae, ok := rpc.Command.(*raft.AppendEntriesRequest); ok && len(ae.Entries) > 0 && !once {
			once = true
			close(held)
			<-release
		}
	}
	c.open(fi)
	f := c.nodes[fi]
	select {
	case <-held:
	case <-time.After(waitFor):
		t.Fatal("the leader never sent the restarted follower an append")
	}

	// The leader is known (heartbeats pass), nothing is replicated yet.
	deadline := time.Now().Add(waitFor)
	for f.Status().LeaderID == "" {
		if time.Now().After(deadline) {
			t.Fatalf("restarted follower never learned the leader: %+v", f.Status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The held append was observed before Raft saw it, so the leader's
	// commit index is known and lies past this replica's log: the commit
	// rule alone must keep it from being judged against entries it lacks.
	if s := f.Status(); s.LeaderCommitIndex <= f.raft.LastIndex() {
		close(release)
		t.Fatalf("held append's commit index %d is not past the follower's last index %d: %+v", s.LeaderCommitIndex, f.raft.LastIndex(), s)
	}
	for range 20 { // 200 ms: several heartbeats at testProfile's rate
		if s := f.Status(); s.CaughtUp {
			close(release)
			t.Fatalf("caught up before any append arrived: appliedIndex %d, leader applied %d: %+v", s.AppliedIndex, last, s)
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(release)
	deadline = time.Now().Add(waitFor)
	for {
		s := f.Status()
		if s.CaughtUp {
			if s.AppliedIndex < last {
				t.Fatalf("caught up at appliedIndex %d below the leader's %d: %+v", s.AppliedIndex, last, s)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never caught up after the append was released: %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s := f.Status(); s.LeaderCommitIndex < last {
		t.Fatalf("leaderCommitIndex %d is below the leader's last write %d: %+v", s.LeaderCommitIndex, last, s)
	}
}

// Every replica reports the leader's commit index: the leader its own, a
// follower the one the leader's appends carry (within CommitTimeout of a
// commit, even with no further writes).
func TestLeaderCommitIndexReported(t *testing.T) {
	c := newBootstrappedCluster(t, testProfile)
	_, l := c.waitLeader()
	for i := range 5 {
		mustApply(t, l, cas(fmt.Sprintf("op-%d", i), "kv:a", uint64(i), i+1))
	}
	deadline := time.Now().Add(waitFor)
	for {
		want := l.Status().CommitIndex
		ok := l.Status().LeaderCommitIndex == want
		for _, n := range c.nodes {
			s := n.Status()
			ok = ok && s.LeaderCommitIndex == want && s.CaughtUp
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for i, n := range c.nodes {
				t.Logf("node %d: %+v", i, n.Status())
			}
			t.Fatalf("not every replica reports leaderCommitIndex %d and caughtUp", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The fence keeps the newest term and the highest commit index seen in any
// term: an append from a deposed leader (a lower term) never replaces the
// term, and a new leader that has not yet learned the old commit index
// never lowers it (every leader's commit index is really committed).
func TestCommitFence(t *testing.T) {
	var f commitFence
	if term, commit := f.get(); term != 0 || commit != 0 {
		t.Fatalf("empty fence = %d/%d", term, commit)
	}
	for _, step := range []struct {
		term, commit         uint64
		wantTerm, wantCommit uint64
	}{
		{3, 10, 3, 10},
		{3, 8, 3, 10},  // reordered append in the same term
		{3, 12, 3, 12}, // progress
		{2, 50, 3, 12}, // a deposed leader
		{4, 11, 4, 12}, // a new leader may start lower: its term, the old commit
		{4, 0, 4, 12},  // no commit index carried
		{4, 13, 4, 13}, // the new leader's progress
	} {
		f.observe(step.term, step.commit)
		if term, commit := f.get(); term != step.wantTerm || commit != step.wantCommit {
			t.Fatalf("after observe(%d, %d): fence %d/%d, want %d/%d", step.term, step.commit, term, commit, step.wantTerm, step.wantCommit)
		}
	}
}

// heldElection is a group whose leader was killed, with an append of the
// new term held on the surviving follower: hold picks it (old is the dead
// leader's commit index). The hook runs after the transport has observed
// the append and before Raft sees it. release lets it through.
type heldElection struct {
	c        *testCluster
	leader   *Node // the new leader
	follower *Node
	release  func()
}

func newHeldElection(t *testing.T, hold func(ae *raft.AppendEntriesRequest, old uint64) bool) heldElection {
	t.Helper()
	c := newBootstrappedCluster(t, testProfile)
	li, l := c.waitLeader()
	for i := range 5 {
		mustApply(t, l, cas(fmt.Sprintf("op-%d", i), "kv:a", uint64(i), i+1))
	}
	var armed atomic.Bool
	var oldTerm, oldCommit atomic.Uint64
	held, release := make(chan struct{}), make(chan struct{})
	var heldOnce, releaseOnce sync.Once
	hook := func(rpc raft.RPC) {
		ae, ok := rpc.Command.(*raft.AppendEntriesRequest)
		if !ok || !armed.Load() || ae.Term <= oldTerm.Load() || !hold(ae, oldCommit.Load()) {
			return
		}
		heldOnce.Do(func() { close(held) })
		<-release
	}
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) }) // before the nodes close
	for i := range c.nodes {
		if i != li {
			c.cfgs[i].beforeRPC = hook
			c.restart(i)
			waitAllCaughtUp(t, c, l)
		}
	}
	st := l.Status()
	oldTerm.Store(st.Term)
	oldCommit.Store(st.CommitIndex)
	armed.Store(true)
	c.kill(li)
	select {
	case <-held:
	case <-time.After(waitFor):
		t.Fatal("no append of the new term was held")
	}
	ni, nl := c.waitLeader()
	_, f := c.follower(ni)
	return heldElection{c: c, leader: nl, follower: f, release: func() { releaseOnce.Do(func() { close(release) }) }}
}

// waitAllCaughtUp waits until every live replica has applied what l has.
func waitAllCaughtUp(t *testing.T, c *testCluster, l *Node) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for {
		want, ok := l.Status().AppliedIndex, true
		for _, n := range c.nodes {
			if n != nil {
				s := n.Status()
				ok = ok && s.CaughtUp && s.AppliedIndex == want
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for i, n := range c.nodes {
				if n != nil {
					t.Logf("node %d: %+v", i, n.Status())
				}
			}
			t.Fatalf("replicas never caught up to applied index %d", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitCaughtUp waits until n reports caught up.
func waitCaughtUp(t *testing.T, n *Node) Status {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for {
		s := n.Status()
		if s.CaughtUp {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("never caught up: %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A newly elected leader's commit index is what it last heard as a
// follower: raft v1.8.0 advances it only once an entry of the new term (its
// no-op) has a quorum (commitment.go), so it may lag what the old leader
// committed. Holding the follower's first append of the new term keeps the
// no-op uncommitted; until it commits, the new leader is not caught up.
func TestNewLeaderNotReadyUntilOwnTermCommits(t *testing.T) {
	e := newHeldElection(t, func(ae *raft.AppendEntriesRequest, _ uint64) bool { return len(ae.Entries) > 0 })
	for range 20 { // 200 ms
		s := e.leader.Status()
		if s.State != raft.Leader.String() {
			t.Fatalf("leadership moved while the no-op was held: %+v", s)
		}
		if s.CommitIndex >= s.LastLogIndex {
			t.Fatalf("the new term's no-op is already committed: %+v", s)
		}
		if s.CaughtUp {
			t.Fatalf("new leader caught up before its own term committed: commit %d, last log %d: %+v", s.CommitIndex, s.LastLogIndex, s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.release()
	s := waitCaughtUp(t, e.leader)
	if s.LeaderCommitIndex != s.CommitIndex || s.CommitIndex < s.LastLogIndex {
		t.Fatalf("leader caught up at commit %d (leaderCommit %d) below its no-op at %d", s.CommitIndex, s.LeaderCommitIndex, s.LastLogIndex)
	}
	waitCaughtUp(t, e.follower)
}

// A follower never claims to be caught up past its own commit index, even
// where its log already holds the entries: Raft advances the local commit
// index only after the append's consistency check, so entries past it may
// be a stale suffix that the leader's log does not share. Here the follower
// holds the new term's no-op (not a command) and the leader's commit index
// covers it, but the append that commits it locally is held.
func TestFollowerNotCaughtUpPastOwnCommit(t *testing.T) {
	e := newHeldElection(t, func(ae *raft.AppendEntriesRequest, old uint64) bool { return ae.LeaderCommitIndex > old })
	deadline := time.Now().Add(waitFor)
	for {
		s := e.follower.Status()
		if s.LeaderCommitIndex > s.CommitIndex && s.LeaderCommitIndex <= s.LastLogIndex {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never held the leader's commit ahead of its own: %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
	for range 20 { // 200 ms
		if s := e.follower.Status(); s.CaughtUp {
			t.Fatalf("caught up at leader commit %d past its own commit %d: %+v", s.LeaderCommitIndex, s.CommitIndex, s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.release()
	if s := waitCaughtUp(t, e.follower); s.CommitIndex < s.LeaderCommitIndex {
		t.Fatalf("caught up with commit %d below the leader's %d", s.CommitIndex, s.LeaderCommitIndex)
	}
}
