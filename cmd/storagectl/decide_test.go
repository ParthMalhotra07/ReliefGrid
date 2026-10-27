package main

import (
	"fmt"
	"strings"
	"testing"
)

func testManifest(phase string) manifest {
	m := manifest{Name: "storage-shard-1-manifest", GroupID: "shard-1", Shard: 1, RingVersion: 1, Phase: phase}
	for i := range 3 {
		pod := fmt.Sprintf("storage-shard-1-%d", i)
		m.Peers = append(m.Peers, peer{PodName: pod, Address: pod + ".storage-shard-1.reliefgrid.svc.cluster.local:7000"})
		if phase != phasePending {
			m.PeerIDs = append(m.PeerIDs, pod+"/inc-"+fmt.Sprint(i))
		}
	}
	return m
}

// statuses gives every listed peer index an empty (or, with state, joined)
// status matching the manifest.
func statuses(m manifest, withState bool, idx ...int) map[string]nodeStatus {
	sts := map[string]nodeStatus{}
	for _, i := range idx {
		p := m.Peers[i]
		sts[p.PodName] = nodeStatus{
			PodName: p.PodName, GroupID: m.GroupID, Shard: m.Shard, Address: p.Address, RingVersion: m.RingVersion,
			ServerID: fmt.Sprintf("%s/inc-%d", p.PodName, i), HasState: withState,
		}
	}
	return sts
}

func TestDecide(t *testing.T) {
	pending, bootstrapping, bootstrapped := testManifest(phasePending), testManifest(phaseBootstrapping), testManifest(phaseBootstrapped)
	mixed := statuses(bootstrapping, false, 1, 2)
	mixed[bootstrapping.Peers[0].PodName] = statuses(bootstrapping, true, 0)[bootstrapping.Peers[0].PodName]

	tests := []struct {
		name string
		m    manifest
		sts  map[string]nodeStatus
		want action
	}{
		{"pending, all empty and reachable", pending, statuses(pending, false, 0, 1, 2), actBootstrap},
		{"pending, one unreachable", pending, statuses(pending, false, 0, 1), actWait},
		{"pending, none reachable", pending, statuses(pending, false), actWait},
		{"pending, a peer has state", pending, statuses(pending, true, 0, 1, 2), actRefuse},
		{"bootstrapping, earlier bootstrap took effect", bootstrapping, mixed, actFence},
		{"bootstrapping, all still empty: re-issue", bootstrapping, statuses(bootstrapping, false, 0, 1, 2), actBootstrap},
		{"bootstrapping, empty and one unreachable", bootstrapping, statuses(bootstrapping, false, 0, 1), actWait},
		{"bootstrapped, healthy", bootstrapped, statuses(bootstrapped, true, 0, 1, 2), actFence},
		{"bootstrapped, one down", bootstrapped, statuses(bootstrapped, true, 0, 1), actFence},
		{"bootstrapped, every disk wiped", bootstrapped, statuses(bootstrapped, false, 0, 1, 2), actRefuse},
		{"bootstrapped, a majority wiped, one down", bootstrapped, statuses(bootstrapped, false, 0, 1), actRefuse},
		{"bootstrapped, only one reachable and empty", bootstrapped, statuses(bootstrapped, false, 0), actWait},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, why := decide(tt.m, tt.sts); got != tt.want {
				t.Fatalf("decide = %v (%s), want %v", got, why, tt.want)
			}
		})
	}
}

func TestDecideRefusesMismatchedPeers(t *testing.T) {
	m := testManifest(phasePending)
	pod := m.Peers[2].PodName
	for name, mutate := range map[string]func(*nodeStatus){
		"other group":       func(s *nodeStatus) { s.GroupID = "shard-2" },
		"other shard":       func(s *nodeStatus) { s.Shard = 2 },
		"other address":     func(s *nodeStatus) { s.Address = "10.244.1.7:7000" },
		"other ringVersion": func(s *nodeStatus) { s.RingVersion = 2 },
		"other pod":         func(s *nodeStatus) { s.PodName = "storage-shard-1-9" },
	} {
		t.Run(name, func(t *testing.T) {
			sts := statuses(m, false, 0, 1, 2)
			s := sts[pod]
			mutate(&s)
			sts[pod] = s
			if got, why := decide(m, sts); got != actRefuse {
				t.Fatalf("decide = %v (%s), want refuse", got, why)
			}
		})
	}
}

func TestDecideRefusesNewIncarnationDuringBootstrap(t *testing.T) {
	m := testManifest(phaseBootstrapping)
	sts := statuses(m, false, 0, 1, 2)
	s := sts[m.Peers[1].PodName]
	s.ServerID = m.Peers[1].PodName + "/wiped"
	sts[m.Peers[1].PodName] = s
	if got, why := decide(m, sts); got != actRefuse {
		t.Fatalf("decide = %v (%s), want refuse: re-issuing would name an ID that no longer exists", got, why)
	}
}

func TestFenced(t *testing.T) {
	m := testManifest(phaseBootstrapping)
	leaderID := m.PeerIDs[0]
	joined := func(caughtUp bool, commit uint64, idx ...int) map[string]nodeStatus {
		sts := statuses(m, true, idx...)
		for pod, s := range sts {
			s.LeaderID, s.CaughtUp, s.State = leaderID, caughtUp, "Follower"
			if s.ServerID == leaderID {
				s.State, s.CommitIndex = "Leader", commit
				s.Configuration = committedVoters(m.PeerIDs, 1)
			}
			sts[pod] = s
		}
		return sts
	}
	tests := []struct {
		name string
		sts  map[string]nodeStatus
		want bool
	}{
		{"all caught up, commit past bootstrap", joined(true, 2, 0, 1, 2), true},
		{"majority caught up", joined(true, 5, 0, 1), true},
		{"only the leader", joined(true, 5, 0), false},
		{"commit still at the bootstrap entry", joined(true, 1, 0, 1, 2), false},
		{"not caught up", joined(false, 5, 0, 1, 2), false},
		{"majority without the leader's status", joined(true, 5, 1, 2), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, why := fenced(m, tt.sts); got != tt.want {
				t.Fatalf("fenced = %v (%s), want %v", got, why, tt.want)
			}
		})
	}

	// A stale leader's commit index must not stand in for the leader the
	// majority names: peers 0 and 1 follow peer 0 (commit 1, its no-op not
	// yet committed); peer 2 still claims leadership with commit 5.
	stale := joined(true, 1, 0, 1)
	p2 := statuses(m, true, 2)[m.Peers[2].PodName]
	p2.State, p2.LeaderID, p2.CaughtUp, p2.CommitIndex = "Leader", p2.ServerID, true, 5
	stale[m.Peers[2].PodName] = p2
	if ok, why := fenced(m, stale); ok {
		t.Fatalf("fenced on a stale leader's commit index: %s", why)
	}

	stranger := joined(true, 5, 0, 1, 2)
	for pod, s := range stranger {
		s.LeaderID = "storage-shard-1-0/other"
		stranger[pod] = s
	}
	if ok, why := fenced(m, stranger); ok {
		t.Fatalf("fenced with an unrecorded leader: %s", why)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	m := testManifest(phaseBootstrapping)
	got, err := parseManifest(m.Name, "7", m.data())
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != m.Phase || got.GroupID != m.GroupID || len(got.PeerIDs) != 3 || got.Peers[2] != m.Peers[2] || got.ResourceVersion != "7" {
		t.Fatalf("round trip: %+v", got)
	}
	d := m.data()
	delete(d, "peerIds")
	if _, err := parseManifest(m.Name, "7", d); err == nil {
		t.Fatal("BOOTSTRAPPING without peerIds parsed")
	}
	d = testManifest(phasePending).data()
	d["phase"] = "DONE"
	if _, err := parseManifest(m.Name, "7", d); err == nil {
		t.Fatal("unknown phase parsed")
	}
}

// committedVoters is a committed configuration of ids, all voters.
func committedVoters(ids []string, index uint64) *raftConfiguration {
	c := &raftConfiguration{Index: index, Committed: true}
	for _, id := range ids {
		c.Servers = append(c.Servers, configServer{ID: id, Address: "x:7000", Suffrage: suffrageVoter})
	}
	return c
}

// #21: the fence also inspects the leader's committed Raft configuration:
// exactly the recorded voters, committed. A leader that reports none, an
// uncommitted one, or a different membership is not a fence.
func TestFencedInspectsCommittedConfiguration(t *testing.T) {
	m := testManifest(phaseBootstrapping)
	base := func(cfg *raftConfiguration) map[string]nodeStatus {
		sts := statuses(m, true, 0, 1, 2)
		for pod, s := range sts {
			s.LeaderID, s.CaughtUp, s.State = m.PeerIDs[0], true, "Follower"
			if s.ServerID == m.PeerIDs[0] {
				s.State, s.CommitIndex, s.Configuration = "Leader", 5, cfg
			}
			sts[pod] = s
		}
		return sts
	}
	uncommitted := committedVoters(m.PeerIDs, 1)
	uncommitted.Committed = false
	nonvoter := committedVoters(m.PeerIDs, 1)
	nonvoter.Servers[2].Suffrage = suffrageNonvoter
	for _, tc := range []struct {
		name string
		cfg  *raftConfiguration
		want bool
		why  string
	}{
		{"the recorded voters, committed", committedVoters(m.PeerIDs, 1), true, ""},
		{"no configuration reported", nil, false, "configuration"},
		{"not committed", uncommitted, false, "not committed"},
		{"a missing voter", committedVoters(m.PeerIDs[:2], 1), false, "voters"},
		{"an extra voter", committedVoters(append(append([]string{}, m.PeerIDs...), "storage-shard-1-3/x"), 1), false, "voters"},
		{"a voter is a nonvoter", nonvoter, false, "voters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, why := fenced(m, base(tc.cfg))
			if got != tc.want || tc.why != "" && !strings.Contains(why, tc.why) {
				t.Fatalf("fenced = %v (%s), want %v naming %q", got, why, tc.want, tc.why)
			}
		})
	}
}

// The M7 fields round-trip through the ConfigMap: admitted, removed and an
// in-progress replacement; a malformed one is an error.
func TestManifestRoundTripAdmission(t *testing.T) {
	m := testManifest(phaseBootstrapped)
	m.Admitted = map[string]string{"storage-shard-1-2": "new-2"}
	m.Removed = []string{"storage-shard-1-2/inc-2"}
	m.Replace = &replaceRecord{PodName: "storage-shard-1-2", OldID: "storage-shard-1-2/inc-2", NewID: "storage-shard-1-2/new-2",
		Phase: replaceNonvoter, ConfigIndex: 12}
	got, err := parseManifest(m.Name, "3", m.data())
	if err != nil {
		t.Fatal(err)
	}
	if got.Admitted["storage-shard-1-2"] != "new-2" || len(got.Removed) != 1 || got.Replace == nil || *got.Replace != *m.Replace {
		t.Fatalf("round trip: admitted %v removed %v replace %+v", got.Admitted, got.Removed, got.Replace)
	}
	d := m.data()
	d["replace"] = `{"podName":"storage-shard-1-2","phase":"SOMETIME"}`
	if _, err := parseManifest(m.Name, "3", d); err == nil {
		t.Fatal("unknown replace phase parsed")
	}
	d = m.data()
	d["admitted"] = `["storage-shard-1-2"]`
	if _, err := parseManifest(m.Name, "3", d); err == nil {
		t.Fatal("malformed admitted parsed")
	}
}
