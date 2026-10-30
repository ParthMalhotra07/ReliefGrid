package raftstore

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// Profile selects the Raft snapshot settings (M4) and, optionally, timeouts.
// A zero duration keeps the library default.
type Profile struct {
	Name              string
	SnapshotThreshold uint64
	TrailingLogs      uint64
	SnapshotInterval  time.Duration

	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
}

var (
	// ProfileDemo compacts often enough that a demo dataset actually takes
	// snapshots and installs them on a lagging follower (M4).
	ProfileDemo = Profile{Name: "demo", SnapshotThreshold: 1024, TrailingLogs: 512, SnapshotInterval: 30 * time.Second}
	// ProfileLoad keeps the library's snapshot defaults, for load tests.
	ProfileLoad = Profile{Name: "load", SnapshotThreshold: 8192, TrailingLogs: 10240, SnapshotInterval: 120 * time.Second}
)

// ProfileByName returns the named profile ("demo" or "load").
func ProfileByName(name string) (Profile, error) {
	for _, p := range []Profile{ProfileDemo, ProfileLoad} {
		if p.Name == name {
			return p, nil
		}
	}
	return Profile{}, fmt.Errorf("raftstore: unknown profile %q (want demo or load)", name)
}

// Config describes one replica of one shard's Raft group.
type Config struct {
	PodName string // StatefulSet pod name, e.g. storage-1-0
	GroupID string // Raft group, e.g. shard-1
	Shard   int

	// DataDir is on the PVC: identity, Raft log and stable store, snapshots.
	DataDir string
	// FSMDir is on an emptyDir: the derived SQLite FSM, wiped on Open (M3).
	FSMDir string

	// BindAddr is the Raft listen address, e.g. ":7000".
	BindAddr string
	// AdvertiseAddr is the stable DNS host:port peers dial (M2), never a pod
	// IP. A port of 0 is replaced with the bound port (for tests).
	AdvertiseAddr string

	Profile Profile
	// Logger receives Raft's own logs; nil logs at info to stderr.
	Logger hclog.Logger

	// Admit, if set, decides whether this disk may open its Raft transport
	// (M7). Open calls it once the disk's identity exists and before the
	// transport listens; it blocks until admitted or fails Open with its
	// error, leaving nothing open. hasState reports existing Raft state.
	Admit func(ctx context.Context, id Identity, hasState bool) error

	// beforeRestoreSwap, tests only, runs in Restore after the image is
	// staged and before the database is swapped in.
	beforeRestoreSwap func()

	// beforeRPC, tests only, runs on every incoming RPC that reaches Raft
	// through the transport's consumer (not heartbeats), after the commit
	// fence has observed it and before Raft sees it.
	beforeRPC func(raft.RPC)
}

// raftConfig builds the library config for this node from the profile.
func (c Config) raftConfig(id raft.ServerID) *raft.Config {
	rc := raft.DefaultConfig()
	rc.LocalID = id
	p := c.Profile
	if p.SnapshotThreshold > 0 {
		rc.SnapshotThreshold = p.SnapshotThreshold
	}
	if p.TrailingLogs > 0 {
		rc.TrailingLogs = p.TrailingLogs
	}
	if p.SnapshotInterval > 0 {
		rc.SnapshotInterval = p.SnapshotInterval
	}
	if p.HeartbeatTimeout > 0 {
		rc.HeartbeatTimeout = p.HeartbeatTimeout
	}
	if p.ElectionTimeout > 0 {
		rc.ElectionTimeout = p.ElectionTimeout
	}
	if p.LeaderLeaseTimeout > 0 {
		rc.LeaderLeaseTimeout = p.LeaderLeaseTimeout
	}
	return rc
}
