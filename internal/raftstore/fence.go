package raftstore

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// The read fence (v3.1 §13, S4.1): a linearizable read without a log entry.
//
//  1. Term gate. A new leader's commit index is what it heard as a
//     follower until an entry of its own term commits (commitment.go:100
//     in raft v1.8.0), so it may lag what the previous leader committed.
//     No read is served in a term until the entry at the commit index is
//     from that term. If the no-op has not committed yet, the first reader
//     of the term commits one barrier, shared by every concurrent reader.
//  2. Read index. The commit index, captured after the read began.
//  3. Quorum. VerifyLeader, plus a quorum of acceptances of requests sent
//     after the read began (ackTracker): VerifyLeader alone can count an
//     answer to a heartbeat sent earlier.
//  4. Same term. A term or role change at any point aborts the read as
//     NotLeader; nothing from an earlier term is reused.
//  5. SQLite. Wait until the FSM has committed the last command at or
//     below the read index (commandTarget): no-op, barrier and
//     configuration entries never reach FSM.Apply, and raft.AppliedIndex
//     only counts entries handed to the FSM (C1).
//
// Every failure is NotLeader or ErrUnavailable within ctx; there is no
// fallback to a stale answer.

// termGate is one term's first-read barrier.
type termGate struct {
	term uint64
	done chan struct{}
	err  error // set before done closes
}

// readFence returns once fn may read the FSM linearizably.
func (n *Node) readFence(ctx context.Context) error {
	start := time.Now()
	term := n.raft.CurrentTerm()
	if n.raft.State() != raft.Leader {
		return n.notLeader()
	}
	if err := n.openTerm(ctx, term); err != nil {
		return err
	}
	readIndex := n.raft.CommitIndex()
	if err := n.verifyQuorumSince(ctx, term, start); err != nil {
		return err
	}
	if err := n.sameTerm(term); err != nil {
		return err
	}
	if h := n.beforeFenceWait.Load(); h != nil {
		(*h)(readIndex)
	}
	target, err := n.commandTarget(readIndex)
	if err != nil {
		return fmt.Errorf("%w: read index %d: %w", ErrUnavailable, readIndex, err)
	}
	if err := n.sm.waitApplied(ctx, target); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return n.sameTerm(term)
}

// sameTerm is nil while this node is still the leader of term.
func (n *Node) sameTerm(term uint64) error {
	if n.raft.State() != raft.Leader || n.raft.CurrentTerm() != term {
		return n.notLeader()
	}
	return nil
}

// openTerm returns once an entry of term is committed here (the predicate
// readiness uses, committedInTerm). Until then the first caller commits a
// barrier and every concurrent caller waits for the same one; a failed
// barrier is forgotten so the next read tries again.
func (n *Node) openTerm(ctx context.Context, term uint64) error {
	if n.committedInTerm(n.raft.CommitIndex(), term) {
		return nil
	}
	n.gateMu.Lock()
	g := n.gate
	if g == nil || g.term != term {
		g = &termGate{term: term, done: make(chan struct{})}
		n.gate = g
		// Not bound to this read's ctx: other readers share it. The future
		// ends when it commits, when leadership is lost, or on shutdown.
		go func() {
			g.err = n.raft.Barrier(0).Error()
			if g.err != nil {
				n.gateMu.Lock()
				if n.gate == g {
					n.gate = nil
				}
				n.gateMu.Unlock()
			}
			close(g.done)
		}()
	}
	n.gateMu.Unlock()
	select {
	case <-g.done:
	case <-ctx.Done():
		return fmt.Errorf("%w: waiting for term %d's first commit: %w", ErrUnavailable, term, ctx.Err())
	}
	if err := n.leaderErr(g.err); err != nil {
		return fmt.Errorf("term %d gate: %w", term, err)
	}
	if err := n.sameTerm(term); err != nil {
		return err
	}
	if !n.committedInTerm(n.raft.CommitIndex(), term) {
		return fmt.Errorf("%w: term %d has no committed entry after its barrier", ErrUnavailable, term)
	}
	return nil
}

// verifyQuorumSince returns once a quorum of the latest configuration's
// voters, this node included, has accepted a request of term sent at or
// after since. Each round is a VerifyLeader, which also makes every
// follower answer a fresh heartbeat (raft.go:966-987).
func (n *Node) verifyQuorumSince(ctx context.Context, term uint64, since time.Time) error {
	for {
		if err := n.leaderErr(wait(ctx, n.raft.VerifyLeader())); err != nil {
			return fmt.Errorf("verify leader: %w", err)
		}
		if err := n.sameTerm(term); err != nil {
			return err
		}
		if n.quorumAcceptedSince(term, since) {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: no quorum answered after the read began: %w", ErrUnavailable, err)
		}
	}
}

func (n *Node) quorumAcceptedSince(term uint64, since time.Time) bool {
	f := n.raft.GetConfiguration()
	if f.Error() != nil {
		return false
	}
	voters, acks := 0, 0
	self := n.id.ServerID()
	for _, s := range f.Configuration().Servers {
		if s.Suffrage != raft.Voter {
			continue
		}
		voters++
		if s.ID == self || n.trans.acks.acceptedSince(s.ID, term, since) {
			acks++
		}
	}
	return voters > 0 && acks >= voters/2+1
}
