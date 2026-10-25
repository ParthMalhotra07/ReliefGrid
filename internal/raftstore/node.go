package raftstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

var (
	// ErrNotLeader: this node is not the leader, and the command was never
	// appended to its log. The error is a *NotLeaderError with a hint.
	ErrNotLeader = errors.New("raftstore: not the leader")
	// ErrBadBootstrap: the bootstrap configuration omits this node or lists
	// it at an address other than the one it advertises. Nothing was written.
	ErrBadBootstrap = errors.New("raftstore: bad bootstrap configuration")
	// ErrHasState: Bootstrap refused because Raft state already exists.
	ErrHasState = errors.New("raftstore: raft state exists; refusing to bootstrap")
	// ErrUnavailable: a read could not be fenced (no quorum, shutting down).
	// Nothing was changed; the caller may retry.
	ErrUnavailable = errors.New("raftstore: unavailable")
)

// NotLeaderError carries the current leader as this node knows it; both
// fields are empty when no leader is known.
type NotLeaderError struct {
	LeaderID   raft.ServerID
	LeaderAddr raft.ServerAddress
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("raftstore: not the leader (leader %q at %q)", e.LeaderID, e.LeaderAddr)
}

func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

// ReadMode selects how a read is fenced.
type ReadMode int

const (
	// ReadStrong goes through the log (M5): the leader commits a barrier
	// and reads after the FSM has applied everything before it.
	ReadStrong ReadMode = iota
	// ReadStale reads the local FSM with no fence; any replica serves it.
	ReadStale
)

// ReadInfo describes how a read was served.
type ReadInfo struct {
	Consistency   string // "strong" or "stale"
	ServedBy      string // pod name
	AppliedIndex  uint64 // FSM position the read observed
	StalePossible bool
}

// Status is a node's management view.
type Status struct {
	Identity
	ServerID   raft.ServerID
	Address    raft.ServerAddress
	State      string // Follower, Candidate, Leader, Shutdown
	Term       uint64
	LeaderID   raft.ServerID
	LeaderAddr raft.ServerAddress

	CommitIndex  uint64
	LastLogIndex uint64
	// RaftAppliedIndex is raft.AppliedIndex(): entries queued to the FSM.
	// Diagnostic only; never use it as "visible to reads".
	RaftAppliedIndex uint64
	// AppliedIndex is the last command the SQLite FSM has committed.
	AppliedIndex uint64

	HasState bool // any term, log or snapshot on disk
	CaughtUp bool // the FSM has completed every command up to CommitIndex
}

// Node is one running replica.
type Node struct {
	cfg   Config
	id    Identity
	raft  *raft.Raft
	sm    *stateMachine
	logs  *raftboltdb.BoltStore // log and stable store
	snaps *raft.FileSnapshotStore
	trans *raft.NetworkTransport

	// hasState caches raft.HasExistingState once it is true; Raft state is
	// never removed while the node runs.
	hasState atomic.Bool

	mu        sync.Mutex
	mapped    indexMapping // last committed Raft index mapped to a command index
	snapCache snapshotTarget
	closed    bool
}

// indexMapping caches commandTarget for a committed index: committed entries
// never change, so a later walk can stop there.
type indexMapping struct{ raftIndex, target uint64 }

// snapshotTarget caches the header of the newest snapshot on disk.
type snapshotTarget struct {
	id           string
	raftIndex    uint64 // Raft's last included index (may be a barrier or no-op)
	appliedIndex uint64 // last command included in the image
}

const retainSnapshots = 2

// Open starts a replica: it loads or creates the identity, opens the Raft
// stores on the PVC, rebuilds the FSM in FSMDir and starts Raft. It never
// bootstraps; a fresh disk waits with an empty configuration.
func Open(ctx context.Context, cfg Config) (_ *Node, err error) {
	if cfg.PodName == "" || cfg.GroupID == "" || cfg.DataDir == "" || cfg.FSMDir == "" {
		return nil, errors.New("raftstore: PodName, GroupID, DataDir and FSMDir are required")
	}
	if filepath.Clean(cfg.DataDir) == filepath.Clean(cfg.FSMDir) {
		return nil, errors.New("raftstore: FSMDir must not be DataDir; the FSM directory is wiped on start")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Info, Output: os.Stderr})
	}
	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, fmt.Errorf("raftstore: create %s: %w", raftDir, err)
	}

	// n is not the named result: "return nil, err" would clear it before
	// this cleanup runs.
	n := &Node{cfg: cfg}
	defer func() {
		if err != nil {
			n.closeStores()
		}
	}()
	if n.logs, err = raftboltdb.New(raftboltdb.Options{Path: filepath.Join(raftDir, "raft.db")}); err != nil {
		return nil, fmt.Errorf("raftstore: open log store: %w", err)
	}
	// NewFileSnapshotStore keeps its files in DataDir/snapshots.
	if n.snaps, err = raft.NewFileSnapshotStoreWithLogger(cfg.DataDir, retainSnapshots, logger.Named("snapshots")); err != nil {
		return nil, fmt.Errorf("raftstore: open snapshot store: %w", err)
	}
	hasState, err := raft.HasExistingState(n.logs, n.logs, n.snaps)
	if err != nil {
		return nil, fmt.Errorf("raftstore: inspect raft state: %w", err)
	}
	if n.id, err = loadOrCreateIdentity(cfg.DataDir, Identity{PodName: cfg.PodName, GroupID: cfg.GroupID, Shard: cfg.Shard}, n.logs, hasState); err != nil {
		return nil, err
	}
	if n.sm, err = newStateMachine(ctx, cfg.FSMDir, cfg.GroupID, cfg.Shard); err != nil {
		return nil, err
	}
	n.sm.beforeSwap = cfg.beforeRestoreSwap
	if n.trans, err = newDNSTransport(cfg.BindAddr, cfg.AdvertiseAddr, logger.Named("transport")); err != nil {
		return nil, err
	}
	rc := cfg.raftConfig(n.id.ServerID())
	rc.Logger = logger
	// NewRaft restores the latest snapshot into the fresh FSM before it
	// returns; the log suffix replays once the commit index is known.
	if n.raft, err = raft.NewRaft(rc, n.sm, n.logs, n.logs, n.snaps, n.trans); err != nil {
		return nil, fmt.Errorf("raftstore: start raft: %w", err)
	}
	return n, nil
}

func (n *Node) closeStores() {
	if n.trans != nil {
		_ = n.trans.Close()
	}
	if n.logs != nil {
		_ = n.logs.Close()
	}
	if n.sm != nil {
		_ = n.sm.close()
	}
}

func (n *Node) Identity() Identity { return n.id }

// Addr is the advertised Raft address.
func (n *Node) Addr() raft.ServerAddress { return n.trans.LocalAddr() }

// Bootstrap installs servers as the group's first configuration. It is the
// only way a group is formed. It returns ErrHasState if this node has any
// Raft state, and ErrBadBootstrap for a configuration that omits this node
// or lists it at another address.
func (n *Node) Bootstrap(ctx context.Context, servers []raft.Server) error {
	self := false
	for _, s := range servers {
		if s.ID == n.id.ServerID() {
			if s.Address != n.Addr() {
				return fmt.Errorf("%w: it lists %s at %s, but the node advertises %s", ErrBadBootstrap, s.ID, s.Address, n.Addr())
			}
			self = true
		}
	}
	if !self {
		return fmt.Errorf("%w: it does not include this node (%s)", ErrBadBootstrap, n.id.ServerID())
	}
	has, err := raft.HasExistingState(n.logs, n.logs, n.snaps)
	if err != nil {
		return fmt.Errorf("raftstore: inspect raft state: %w", err)
	}
	if has {
		return ErrHasState
	}
	err = wait(ctx, n.raft.BootstrapCluster(raft.Configuration{Servers: servers}))
	if errors.Is(err, raft.ErrCantBootstrap) {
		return fmt.Errorf("%w: %w", ErrHasState, err)
	}
	if err != nil {
		return fmt.Errorf("raftstore: bootstrap: %w", err)
	}
	return nil
}

// Apply proposes one command (a store.CreateEntity, store.TryClaim, ...)
// and returns its committed result. ErrNotLeader means it was never
// appended. Any other failure, including ctx ending, wraps
// store.ErrUnknownOutcome: the command may still commit.
func (n *Node) Apply(ctx context.Context, cmd any) (store.Result, error) {
	entry, err := fsm.Encode(cmd)
	if err != nil {
		return store.Result{}, err
	}
	f := n.raft.Apply(entry, enqueueTimeout(ctx))
	err = wait(ctx, f)
	if errors.Is(err, raft.ErrNotLeader) {
		return store.Result{}, n.notLeader()
	}
	if err != nil {
		return store.Result{}, fmt.Errorf("%w: %w", store.ErrUnknownOutcome, err)
	}
	res, ok := f.Response().(store.Result)
	if !ok {
		return store.Result{}, fmt.Errorf("%w: FSM returned %T", store.ErrUnknownOutcome, f.Response())
	}
	return res, nil
}

// Read runs fn against the FSM. A strong read needs this node to be the
// leader (ErrNotLeader otherwise) and a committed barrier (ErrUnavailable if
// it cannot get one). Restore cannot swap the database while fn runs.
func (n *Node) Read(ctx context.Context, mode ReadMode, fn func(context.Context, Reader) error) (ReadInfo, error) {
	info := ReadInfo{Consistency: "stale", ServedBy: n.cfg.PodName, StalePossible: true}
	switch mode {
	case ReadStale:
	case ReadStrong:
		if n.raft.State() != raft.Leader {
			return ReadInfo{}, n.notLeader()
		}
		// The barrier commits in this term and its future is answered on the
		// FSM goroutine after every earlier command has applied.
		err := wait(ctx, n.raft.Barrier(enqueueTimeout(ctx)))
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return ReadInfo{}, n.notLeader()
		}
		if err != nil {
			return ReadInfo{}, fmt.Errorf("%w: barrier: %w", ErrUnavailable, err)
		}
		info = ReadInfo{Consistency: "strong", ServedBy: n.cfg.PodName}
	default:
		return ReadInfo{}, fmt.Errorf("raftstore: unknown read mode %d", mode)
	}
	err := n.sm.read(func(f *fsm.FSM) error {
		info.AppliedIndex = f.AppliedIndex()
		return fn(ctx, f)
	})
	return info, err
}

func (n *Node) notLeader() error {
	addr, id := n.raft.LeaderWithID()
	return &NotLeaderError{LeaderID: id, LeaderAddr: addr}
}

// Term is the current Raft term, without the cost of Status.
func (n *Node) Term() uint64 { return n.raft.CurrentTerm() }

// leadershipPoll is how often WaitApplyAfter checks for a leadership change.
const leadershipPoll = 20 * time.Millisecond

// WaitApplyAfter blocks until the FSM has applied a command after index,
// this node's role or term changes, or ctx ends, whichever is first. It
// holds no FSM, SQL or snapshot/apply lock, so a long-poll never delays
// Apply or Restore. The caller reads again afterwards; waking is a hint,
// not a result.
func (n *Node) WaitApplyAfter(ctx context.Context, index uint64) {
	state, term := n.raft.State(), n.raft.CurrentTerm()
	tick := time.NewTicker(leadershipPoll)
	defer tick.Stop()
	for {
		ch := n.sm.appliedChanged()
		if n.sm.appliedIndex() > index {
			return
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return
		case <-tick.C:
			if n.raft.State() != state || n.raft.CurrentTerm() != term {
				return
			}
		}
	}
}

func (n *Node) Status() Status {
	stats := n.raft.Stats()
	num := func(k string) uint64 {
		v, _ := strconv.ParseUint(stats[k], 10, 64)
		return v
	}
	leaderAddr, leaderID := n.raft.LeaderWithID()
	has := n.hasState.Load()
	if !has {
		var err error
		if has, err = raft.HasExistingState(n.logs, n.logs, n.snaps); err == nil && has {
			n.hasState.Store(true)
		}
	}
	s := Status{
		Identity:         n.id,
		ServerID:         n.id.ServerID(),
		Address:          n.Addr(),
		State:            n.raft.State().String(),
		Term:             num("term"),
		LeaderID:         leaderID,
		LeaderAddr:       leaderAddr,
		CommitIndex:      num("commit_index"),
		LastLogIndex:     num("last_log_index"),
		RaftAppliedIndex: n.raft.AppliedIndex(),
		AppliedIndex:     n.sm.appliedIndex(),
		HasState:         has,
	}
	if has && leaderID != "" {
		target, err := n.commandTarget(s.CommitIndex)
		s.CaughtUp = err == nil && s.AppliedIndex >= target
	}
	return s
}

// WaitApplied blocks until the FSM has completed every command at or below
// raftIndex. Barrier, no-op and configuration entries never reach the FSM,
// so raftIndex is first mapped to the last command entry at or below it.
// An index in the compacted range is satisfied once the FSM holds the newest
// snapshot's image (its last included command).
func (n *Node) WaitApplied(ctx context.Context, raftIndex uint64) error {
	// The entry may not have reached this replica's log yet.
	for n.raft.LastIndex() < raftIndex {
		select {
		case <-ctx.Done():
			return fmt.Errorf("raftstore: log at %d, waiting for %d: %w", n.raft.LastIndex(), raftIndex, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	target, err := n.commandTarget(raftIndex)
	if err != nil {
		return err
	}
	return n.sm.waitApplied(ctx, target)
}

// commandTarget maps a Raft index to the last command index at or below it.
func (n *Node) commandTarget(raftIndex uint64) (uint64, error) {
	commit := n.raft.CommitIndex()
	n.mu.Lock()
	cached := n.mapped
	n.mu.Unlock()

	target, err := n.walkToCommand(raftIndex, cached)
	if err != nil {
		return 0, err
	}
	if raftIndex <= commit && raftIndex > cached.raftIndex {
		n.mu.Lock()
		if raftIndex > n.mapped.raftIndex {
			n.mapped = indexMapping{raftIndex: raftIndex, target: target}
		}
		n.mu.Unlock()
	}
	return target, nil
}

func (n *Node) walkToCommand(i uint64, cached indexMapping) (uint64, error) {
	for ; i > 0; i-- {
		if cached.raftIndex > 0 && i == cached.raftIndex {
			return cached.target, nil
		}
		first, err := n.logs.FirstIndex()
		if err != nil {
			return 0, fmt.Errorf("raftstore: first log index: %w", err)
		}
		if first == 0 || i < first {
			return n.compactedTarget(i)
		}
		var l raft.Log
		if err := n.logs.GetLog(i, &l); err != nil {
			if errors.Is(err, raft.ErrLogNotFound) { // compacted since FirstIndex
				return n.compactedTarget(i)
			}
			return 0, fmt.Errorf("raftstore: read log %d: %w", i, err)
		}
		if l.Type == raft.LogCommand {
			return i, nil
		}
	}
	return 0, nil
}

// compactedTarget is the command index the FSM must reach to cover Raft
// index i, which is no longer in the log. Raft's snapshot index may be a
// barrier the FSM never records, so the target is the last command included
// in the newest snapshot's image, read from its header.
func (n *Node) compactedTarget(i uint64) (uint64, error) {
	metas, err := n.snaps.List()
	if err != nil {
		return 0, fmt.Errorf("raftstore: list snapshots: %w", err)
	}
	if len(metas) == 0 || i > metas[0].Index {
		return 0, fmt.Errorf("raftstore: index %d is neither in the log nor in a snapshot", i)
	}
	newest := metas[0]
	n.mu.Lock()
	cached := n.snapCache
	n.mu.Unlock()
	if cached.id == newest.ID {
		return cached.appliedIndex, nil
	}
	_, rc, err := n.snaps.Open(newest.ID)
	if err != nil {
		return 0, fmt.Errorf("raftstore: open snapshot %s: %w", newest.ID, err)
	}
	defer func() { _ = rc.Close() }()
	h, err := readSnapshotHeader(bufio.NewReaderSize(rc, maxHeaderBytes))
	if err != nil {
		return 0, err
	}
	n.mu.Lock()
	n.snapCache = snapshotTarget{id: newest.ID, raftIndex: newest.Index, appliedIndex: h.AppliedIndex}
	n.mu.Unlock()
	return h.AppliedIndex, nil
}

// Snapshot asks Raft to take a snapshot now and waits for it. Nothing new
// to snapshot is not an error.
func (n *Node) Snapshot(ctx context.Context) error {
	err := wait(ctx, n.raft.Snapshot())
	if err != nil && !errors.Is(err, raft.ErrNothingNewToSnapshot) {
		return fmt.Errorf("raftstore: snapshot: %w", err)
	}
	return nil
}

// Close shuts Raft down without a leadership transfer and closes the
// transport and stores. The node cannot be reused; Open the same dirs again.
func (n *Node) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.mu.Unlock()
	err := n.raft.Shutdown().Error()
	return errors.Join(err, n.trans.Close(), n.logs.Close(), n.sm.close())
}

// wait waits for a Raft future or ctx, whichever comes first.
func wait(ctx context.Context, f raft.Future) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enqueueTimeout bounds how long Raft may take to accept a proposal; zero
// means no limit.
func enqueueTimeout(ctx context.Context) time.Duration {
	if d, ok := ctx.Deadline(); ok {
		return max(time.Until(d), time.Millisecond)
	}
	return 0
}
