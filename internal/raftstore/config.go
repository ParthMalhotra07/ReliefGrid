package raftstore

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"go.opentelemetry.io/otel/trace"
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

// ReadFence selects how a ReadLinearizable read is served
// (STRONG_READ_FENCE).
type ReadFence string

const (
	// ReadFenceIndex is the read fence: a quorum check and a wait for the
	// FSM, no log entry. The zero value means this.
	ReadFenceIndex ReadFence = "fence"
	// ReadFenceBarrier serves linearizable reads as strong (a barrier in
	// the log), for comparison and as a fallback.
	ReadFenceBarrier ReadFence = "barrier"
)

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
	// ReadFence chooses how ReadLinearizable is served; "" is ReadFenceIndex.
	ReadFence ReadFence
	// Logger receives Raft's own logs; nil logs at info to stderr.
	Logger hclog.Logger
	// TracerProvider records raft.apply spans (S4.4); nil uses the global
	// provider (a no-op unless obs.Init enabled tracing).
	TracerProvider trace.TracerProvider
	// Listen opens the Raft listener on BindAddr; nil (production) uses
	// net.ListenConfig.Listen. Tests set it to listen on a port they hold
	// reserved (internal/testnet), so the port cannot be taken before the
	// first open or between a stop and a restart (issues #58, #75).
	Listen func(ctx context.Context, network, address string) (net.Listener, error)

	// Admit, if set, decides whether this disk may open its Raft transport
	// (M7). Open calls it once the disk's identity exists and before the
	// transport listens; it blocks until admitted or fails Open with its
	// error, leaving nothing open. hasState reports existing Raft state.
	Admit func(ctx context.Context, id Identity, hasState bool) error

	// beforeRestoreSwap, tests only, runs in Restore after the image is
	// staged and before the database is swapped in.
	beforeRestoreSwap func()

	// seams, tests only, are the S4.6b test boundaries (seams.go).
	seams *seamHooks

	// beforeRPC, tests only, runs on every incoming RPC that reaches Raft
	// through the transport's consumer (not heartbeats), after the commit
	// fence has observed it and before Raft sees it.
	beforeRPC func(raft.RPC)

	// dropSend, tests only, decides whether an outgoing RPC (req is the
	// *raft.AppendEntriesRequest, *raft.RequestVoteRequest, ... to target)
	// fails before it is sent. While it is set, pipelined replication is
	// off so that every append passes through it.
	dropSend func(target raft.ServerAddress, req any) bool
	// afterSend, tests only, runs after an outgoing RPC has succeeded and
	// before Raft sees the response; blocking in it delays the response.
	afterSend func(target raft.ServerAddress, req any)
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
