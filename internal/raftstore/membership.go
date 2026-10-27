package raftstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

// Configuration is the group's membership as this replica knows it (#21):
// raft's latest configuration and the log index it was written at.
type Configuration struct {
	Index uint64
	// Committed: the configuration is committed. raft v1.8.0 exposes only
	// the latest configuration, but allows at most one uncommitted change
	// at a time, so the latest is committed once its index is within the
	// commit index.
	Committed bool
	Servers   []Server
}

// Server is one member: Suffrage is "Voter" or "Nonvoter".
type Server struct {
	ID       raft.ServerID
	Address  raft.ServerAddress
	Suffrage string
}

// Configuration returns the latest configuration and whether it is committed.
func (n *Node) Configuration(ctx context.Context) (Configuration, error) {
	f := n.raft.GetConfiguration()
	if err := wait(ctx, f); err != nil {
		return Configuration{}, fmt.Errorf("raftstore: get configuration: %w", err)
	}
	return n.configuration(f.Configuration(), n.raft.CommitIndex())
}

func (n *Node) configuration(c raft.Configuration, commit uint64) (Configuration, error) {
	index, err := n.configIndex(c)
	if err != nil {
		return Configuration{}, err
	}
	out := Configuration{Index: index, Committed: index > 0 && index <= commit}
	for _, s := range c.Servers {
		out.Servers = append(out.Servers, Server{ID: s.ID, Address: s.Address, Suffrage: s.Suffrage.String()})
	}
	return out, nil
}

// configScan caches configIndex's last answer: the index of the newest
// configuration entry found, and how far the log had been scanned.
type configScan struct {
	mu      sync.Mutex
	index   uint64 // the newest configuration entry found (0: none yet)
	scanned uint64 // every entry up to here has been looked at
}

// configIndex is the log index of c, the latest configuration. raft v1.8.0's
// GetConfiguration leaves its Index at 0 (api.go fills only latest), so the
// index is found where raft keeps it: the newest LogConfiguration entry, or
// the newest snapshot's configuration once that entry is compacted. It is
// the index raft compares a change's prevIndex with.
func (n *Node) configIndex(c raft.Configuration) (uint64, error) {
	n.cfgScan.mu.Lock()
	defer n.cfgScan.mu.Unlock()
	first, err := n.logs.FirstIndex()
	if err != nil {
		return 0, fmt.Errorf("raftstore: first log index: %w", err)
	}
	last, err := n.logs.LastIndex()
	if err != nil {
		return 0, fmt.Errorf("raftstore: last log index: %w", err)
	}
	// A cached answer holds if its entry is still c and nothing newer than
	// the scanned point is a configuration. A follower's log suffix can be
	// replaced, so any doubt rescans from the end.
	from := last
	if n.cfgScan.index > 0 && n.cfgScan.scanned <= last && n.isConfigEntry(n.cfgScan.index, c) {
		if i, ok, err := n.newestConfigEntry(last, n.cfgScan.scanned+1, c); err != nil {
			return 0, err
		} else if ok {
			n.cfgScan.index = i
		}
		n.cfgScan.scanned = last
		return n.cfgScan.index, nil
	}
	if first > 0 {
		if i, ok, err := n.newestConfigEntry(from, first, c); err != nil {
			return 0, err
		} else if ok {
			n.cfgScan.index, n.cfgScan.scanned = i, last
			return i, nil
		}
	}
	metas, err := n.snaps.List()
	if err != nil {
		return 0, fmt.Errorf("raftstore: list snapshots: %w", err)
	}
	if len(metas) > 0 && sameServers(metas[0].Configuration, c) {
		n.cfgScan.index, n.cfgScan.scanned = metas[0].ConfigurationIndex, last
		return metas[0].ConfigurationIndex, nil
	}
	return 0, nil // no configuration yet (an empty disk)
}

// newestConfigEntry scans from hi down to lo for a configuration entry; ok
// only if the newest one found is c.
func (n *Node) newestConfigEntry(hi, lo uint64, c raft.Configuration) (uint64, bool, error) {
	for i := hi; i >= lo && i > 0; i-- {
		var l raft.Log
		if err := n.logs.GetLog(i, &l); err != nil {
			if errors.Is(err, raft.ErrLogNotFound) {
				return 0, false, nil // compacted under the scan
			}
			return 0, false, fmt.Errorf("raftstore: read log %d: %w", i, err)
		}
		if l.Type == raft.LogConfiguration {
			return i, sameServers(raft.DecodeConfiguration(l.Data), c), nil
		}
	}
	return 0, false, nil
}

func (n *Node) isConfigEntry(i uint64, c raft.Configuration) bool {
	var l raft.Log
	return n.logs.GetLog(i, &l) == nil && l.Type == raft.LogConfiguration && sameServers(raft.DecodeConfiguration(l.Data), c)
}

func sameServers(a, b raft.Configuration) bool { return slices.Equal(a.Servers, b.Servers) }

// RemoveServer removes id from the group through this node, which must be
// the leader, if the latest configuration is still the one at prevIndex. It
// returns once the change has committed, with its index.
func (n *Node) RemoveServer(ctx context.Context, id raft.ServerID, prevIndex uint64) (uint64, error) {
	return n.change(ctx, prevIndex, func() raft.IndexFuture { return n.raft.RemoveServer(id, prevIndex, enqueueTimeout(ctx)) })
}

// AddNonvoter adds id at addr as a nonvoter: it receives the log but does
// not vote or count toward a quorum.
func (n *Node) AddNonvoter(ctx context.Context, id raft.ServerID, addr raft.ServerAddress, prevIndex uint64) (uint64, error) {
	return n.change(ctx, prevIndex, func() raft.IndexFuture { return n.raft.AddNonvoter(id, addr, prevIndex, enqueueTimeout(ctx)) })
}

// AddVoter adds id at addr as a voter, or promotes the nonvoter id.
func (n *Node) AddVoter(ctx context.Context, id raft.ServerID, addr raft.ServerAddress, prevIndex uint64) (uint64, error) {
	return n.change(ctx, prevIndex, func() raft.IndexFuture { return n.raft.AddVoter(id, addr, prevIndex, enqueueTimeout(ctx)) })
}

// change submits one membership change. A follower refuses before anything
// is proposed. On ctx's end the outcome is unknown: the caller re-reads the
// configuration rather than assuming the change failed. prevIndex 0 is
// refused: raft reads it as "no check" and would apply the change to
// whatever the configuration has become.
func (n *Node) change(ctx context.Context, prevIndex uint64, submit func() raft.IndexFuture) (uint64, error) {
	if prevIndex == 0 {
		return 0, fmt.Errorf("%w: a membership change needs the prevIndex it was decided from (0 would apply unconditionally)", ErrConfigurationChanged)
	}
	if n.raft.State() != raft.Leader {
		return 0, n.notLeader()
	}
	f := submit()
	err := wait(ctx, f)
	switch {
	case err == nil:
		return f.Index(), nil
	case err == raft.ErrNotLeader || err == raft.ErrLeadershipLost: //nolint:errorlint // raft returns these sentinels unwrapped
		return 0, n.notLeader()
	// raft v1.8.0 reports a stale prevIndex only as text (configuration.go:
	// "configuration changed since %v (latest is %v)").
	case strings.Contains(err.Error(), "configuration changed since"):
		return 0, fmt.Errorf("%w: %s", ErrConfigurationChanged, err.Error())
	}
	return 0, fmt.Errorf("raftstore: membership change: %w", err)
}
