package main

import (
	"fmt"
	"slices"
)

// nodeStatus is the part of a storage node's GET /mgmt/status the Job uses.
type nodeStatus struct {
	PodName     string `json:"podName"`
	GroupID     string `json:"groupId"`
	Shard       int    `json:"shard"`
	ServerID    string `json:"serverId"`
	Address     string `json:"address"`
	State       string `json:"state"`
	Term        uint64 `json:"term"`
	LeaderID    string `json:"leaderId"`
	CommitIndex uint64 `json:"commitIndex"`
	HasState    bool   `json:"hasState"`
	CaughtUp    bool   `json:"caughtUp"`
	RingVersion uint64 `json:"ringVersion"`
}

type action int

const (
	actWait      action = iota // not enough peers reachable yet; poll again
	actRefuse                  // permanent: never create a group from this state
	actBootstrap               // record peerIds (if PENDING), then bootstrap peers[0]
	actFence                   // a group exists: wait for its quorum fence, then record BOOTSTRAPPED
)

func (a action) String() string {
	return [...]string{"wait", "refuse", "bootstrap", "fence"}[a]
}

// decide is the whole bootstrap policy for one shard, given the manifest and
// the statuses of the peers that answered (keyed by podName; absent =
// unreachable). It never infers that a new group should be created from a
// timeout or from empty disks under a manifest that already records one.
func decide(m manifest, sts map[string]nodeStatus) (action, string) {
	for _, p := range m.Peers {
		s, ok := sts[p.PodName]
		if !ok {
			continue
		}
		if why := mismatch(m, p, s); why != "" {
			return actRefuse, why
		}
		// Re-issuing a bootstrap is safe only with the IDs it was recorded
		// with; a new incarnation means a disk was replaced in between.
		if m.Phase == phaseBootstrapping && !slices.Contains(m.PeerIDs, s.ServerID) {
			return actRefuse, fmt.Sprintf("%s is %s, not one of the recorded %v: its disk changed during bootstrap", p.PodName, s.ServerID, m.PeerIDs)
		}
	}
	reachable, withState := 0, 0
	for _, p := range m.Peers {
		if s, ok := sts[p.PodName]; ok {
			reachable++
			if s.HasState {
				withState++
			}
		}
	}
	switch m.Phase {
	case phasePending:
		if withState > 0 {
			return actRefuse, "the manifest is PENDING but a peer already has Raft state"
		}
		if reachable < len(m.Peers) {
			return actWait, fmt.Sprintf("%d of %d peers reachable", reachable, len(m.Peers))
		}
		return actBootstrap, "all peers empty and reachable"
	case phaseBootstrapping:
		if withState > 0 {
			return actFence, "a bootstrap from an earlier run took effect"
		}
		if reachable < len(m.Peers) {
			return actWait, fmt.Sprintf("%d of %d peers reachable, none has state", reachable, len(m.Peers))
		}
		return actBootstrap, "all recorded peers still empty: re-issue the same bootstrap"
	default: // phaseBootstrapped
		if withState > 0 {
			return actFence, "verify the existing group"
		}
		if reachable < m.majority() {
			return actWait, fmt.Sprintf("%d of %d peers reachable", reachable, len(m.Peers))
		}
		return actRefuse, "the manifest records a bootstrapped group but no reachable peer has Raft state (wiped disks); a new group needs storage-reset"
	}
}

// mismatch is non-empty if a node is not the peer the manifest describes.
func mismatch(m manifest, p peer, s nodeStatus) string {
	switch {
	case s.PodName != p.PodName:
		return fmt.Sprintf("%s answered as %q", p.PodName, s.PodName)
	case s.GroupID != m.GroupID || s.Shard != m.Shard:
		return fmt.Sprintf("%s is %s shard %d, the manifest is %s shard %d", p.PodName, s.GroupID, s.Shard, m.GroupID, m.Shard)
	case s.Address != p.Address:
		return fmt.Sprintf("%s advertises %s, the manifest has %s", p.PodName, s.Address, p.Address)
	case s.RingVersion != m.RingVersion:
		return fmt.Sprintf("%s has ringVersion %d, the manifest has %d", p.PodName, s.RingVersion, m.RingVersion)
	}
	return ""
}

// fenced reports whether the group has applied a quorum-backed
// initialization fence: a majority of the recorded voters answer with Raft
// state, name the same leader (one of the recorded voters), and have caught
// up; and the leader's commit index is past the bootstrap configuration
// entry (index 1), which needs a majority to acknowledge its first entry.
func fenced(m manifest, sts map[string]nodeStatus) (bool, string) {
	leaders := map[string]int{}
	var leaderCommit uint64
	for _, p := range m.Peers {
		s, ok := sts[p.PodName]
		if !ok || !s.HasState || !s.CaughtUp || s.LeaderID == "" {
			continue
		}
		leaders[s.LeaderID]++
		if s.State == "Leader" && s.ServerID == s.LeaderID {
			leaderCommit = s.CommitIndex
		}
	}
	for id, n := range leaders {
		if n < m.majority() {
			continue
		}
		if !slices.Contains(m.PeerIDs, id) {
			return false, fmt.Sprintf("leader %s is not a recorded voter", id)
		}
		if leaderCommit < 2 {
			return false, fmt.Sprintf("leader %s has commit index %d", id, leaderCommit)
		}
		return true, fmt.Sprintf("leader %s, %d of %d caught up, commit index %d", id, n, len(m.Peers), leaderCommit)
	}
	return false, "no majority agrees on a caught-up leader"
}
