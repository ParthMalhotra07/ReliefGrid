package scripts

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

// lagStatus is the part of a replica's /mgmt/status the lag sample reads.
type lagStatus struct {
	State            string `json:"state"`
	Term             uint64 `json:"term"`
	CommitIndex      uint64 `json:"commitIndex"`
	LastLogIndex     uint64 `json:"lastLogIndex"`
	RaftAppliedIndex uint64 `json:"raftAppliedIndex"`
	AppliedIndex     uint64 `json:"appliedIndex"`
}

// runLag sources chaos/scripts/lib.sh with the cluster stubbed: st prints
// the given status per pod, group_state names storage-shard-1-0 as leader.
// It runs body and returns its stdout.
func runLag(t *testing.T, leader, follower *lagStatus, body string) string {
	t.Helper()
	js := func(s *lagStatus) string {
		if s == nil {
			return ""
		}
		b, _ := json.Marshal(s)
		return string(b)
	}
	script := `set -u
EXPERIMENT=delay-follower DESCRIPTION=test
. ../chaos/scripts/lib.sh
k() { echo "cluster call: k $*" >&2; return 1; }
kn() { echo "cluster call: kn $*" >&2; return 1; }
group_state() { echo "storage-shard-1-0 2"; }
st() {
	case "$1" in
	storage-shard-1-0) printf '%s' '` + js(leader) + `' ;;
	storage-shard-1-1) printf '%s' '` + js(follower) + `' ;;
	esac
}
peers=(storage-shard-1-0 storage-shard-1-2) target_pod=storage-shard-1-1
` + body
	out, err := exec.CommandContext(t.Context(), "bash", "-c", script).CombinedOutput()
	if strings.Contains(string(out), "cluster call:") {
		t.Fatalf("the lag sample touched the cluster:\n%s", out)
	}
	if err != nil {
		t.Fatalf("bash: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

// The delay-follower lag is Raft-level: the leader's commit index minus the
// follower's Raft applied index. Barriers (one per strong read) are Raft
// entries the FSM never sees, so a follower whose Raft log is at the
// leader's commit is not lagging however far its FSM appliedIndex (the
// last command) trails; a follower whose Raft log stalled still is.
func TestChaosLagIsRaftLevel(t *testing.T) {
	leader := &lagStatus{State: "Leader", Term: 2, CommitIndex: 120, LastLogIndex: 120, RaftAppliedIndex: 120, AppliedIndex: 100}
	for _, tc := range []struct {
		name     string
		follower *lagStatus
		want     string
	}{
		// 20 barriers after the last command: every replica's FSM stays at
		// 100 while Raft moves to 120. Not lag.
		{"barrier-only progress", &lagStatus{State: "Follower", Term: 2, CommitIndex: 120, LastLogIndex: 120, RaftAppliedIndex: 120, AppliedIndex: 100}, "0"},
		// The follower's Raft log stopped at 80: 40 committed entries
		// (commands and barriers alike) it has not got.
		{"stalled follower", &lagStatus{State: "Follower", Term: 2, CommitIndex: 80, LastLogIndex: 80, RaftAppliedIndex: 80, AppliedIndex: 80}, "40"},
		// Received but not yet known committed: still behind.
		{"replicated, commit not yet learned", &lagStatus{State: "Follower", Term: 2, CommitIndex: 110, LastLogIndex: 120, RaftAppliedIndex: 110, AppliedIndex: 100}, "10"},
		// The two statuses are read one after the other: a follower read
		// later can be past the leader's sampled commit. Never negative.
		{"follower sampled after a newer commit", &lagStatus{State: "Follower", Term: 2, CommitIndex: 123, LastLogIndex: 123, RaftAppliedIndex: 123, AppliedIndex: 123}, "0"},
		// A status that can't be read is no sample, not zero lag.
		{"follower unreadable", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := runLag(t, leader, tc.follower, `lag_sample "$target_pod"`); got != tc.want {
				t.Errorf("lag_sample = %q, want %q", got, tc.want)
			}
		})
	}
}

// After the settle the judge fails a follower still behind (finalLag > 0):
// barrier-only progress must leave finalLag at 0, a stalled follower must not.
func TestChaosFinalLagIgnoresBarriers(t *testing.T) {
	leader := &lagStatus{State: "Leader", Term: 2, CommitIndex: 120, LastLogIndex: 120, RaftAppliedIndex: 120, AppliedIndex: 100}
	for _, tc := range []struct {
		name     string
		follower *lagStatus
		want     float64
	}{
		{"barrier-only progress", &lagStatus{State: "Follower", Term: 2, CommitIndex: 120, LastLogIndex: 120, RaftAppliedIndex: 120, AppliedIndex: 100}, 0},
		{"stalled follower", &lagStatus{State: "Follower", Term: 2, CommitIndex: 80, LastLogIndex: 80, RaftAppliedIndex: 80, AppliedIndex: 80}, 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := runLag(t, leader, tc.follower, `delay_max_lag=40 delay_samples=50; delay_final_lag; printf '%s' "$harness_delay"`)
			var d map[string]any
			if err := json.Unmarshal([]byte(out), &d); err != nil {
				t.Fatalf("harness_delay is not JSON: %v\n%s", err, out)
			}
			if d["finalLag"] != tc.want || d["maxLag"] != float64(40) {
				t.Errorf("harness_delay = %s, want finalLag %v", out, tc.want)
			}
		})
	}
}
