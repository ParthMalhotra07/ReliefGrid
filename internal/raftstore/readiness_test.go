package raftstore

import (
	"fmt"
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

// The fence keeps the newest term's highest commit index: an append from a
// deposed leader (a lower term) never lowers or replaces it.
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
		{4, 11, 4, 11}, // a new leader may start lower: its term wins
		{4, 0, 4, 11},  // no commit index carried
	} {
		f.observe(step.term, step.commit)
		if term, commit := f.get(); term != step.wantTerm || commit != step.wantCommit {
			t.Fatalf("after observe(%d, %d): fence %d/%d, want %d/%d", step.term, step.commit, term, commit, step.wantTerm, step.wantCommit)
		}
	}
}
