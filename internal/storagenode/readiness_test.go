package storagenode

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

// A replica that has not caught up with the leader's commit index refuses
// ordinary data reads it would serve from its own FSM (stale reads): its
// answer could be an old version or a false "absent". Strong reads go
// through the leader's log and are unaffected; mgmt reads (dump, counts)
// are diagnostics and are not gated.
func TestStaleReadRefusedUntilCaughtUp(t *testing.T) {
	rg := loadRing(t)
	key, _ := keyOn(t, rg, testShard, "", "kv:cfg")
	stale := call{method: http.MethodGet, path: "/internal/data/" + key + "?consistency=stale"}

	n := newFakeNode(t)
	n.status = raftstore.Status{LeaderID: "storage-1-0/x", AppliedIndex: 3, LeaderCommitIndex: 40}
	rec := do(t, newTestServer(t, n).dataRoutes(), stale)
	e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.STORAGEUNAVAILABLE)
	if !e.Retryable || e.OutcomeUnknown || rec.Header().Get("Retry-After") == "" || !strings.Contains(e.Message, "caught up") {
		t.Fatalf("not caught up = %+v (Retry-After %q)", e, rec.Header().Get("Retry-After"))
	}
	if len(n.modes) != 0 {
		t.Fatalf("read the local FSM while not caught up: modes %v", n.modes)
	}

	// A strong read is not gated here: it goes to Read, which sends a
	// follower's to the leader and makes a leader wait for its FSM.
	do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: "/internal/data/" + key})
	if len(n.modes) != 1 || n.modes[0] != raftstore.ReadStrong {
		t.Fatalf("strong read modes = %v, want one ReadStrong", n.modes)
	}

	n.status.CaughtUp = true
	if rec := do(t, newTestServer(t, n).dataRoutes(), stale); rec.Code == http.StatusServiceUnavailable {
		t.Fatalf("caught up, stale read = %d %s", rec.Code, rec.Body.String())
	}
}

// /readyz says why it is not ready, against the leader's commit index.
func TestReadyzNamesLeaderCommit(t *testing.T) {
	n := newFakeNode(t)
	n.status = raftstore.Status{LeaderID: "storage-1-0/x", CommitIndex: 0, AppliedIndex: 0, LeaderCommitIndex: 40}
	rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodGet, path: "/readyz"})
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "leader commit 40") {
		t.Fatalf("readyz = %d %s, want 503 naming leader commit 40", rec.Code, rec.Body.String())
	}
}

// The not-leader hint names the leader's advertised data address as the
// coordinator registered it (HTTP_ADVERTISE), when the coordinator's leader
// is the one Raft names; until then it falls back to the Raft host with
// this replica's HTTP port (issue #30).
func TestLeaderHintUsesAdvertisedAddress(t *testing.T) {
	rg := loadRing(t)
	key, _ := keyOn(t, rg, testShard, "", "kv:cfg")
	put := call{method: http.MethodPut, path: "/internal/data/" + key, header: map[string]string{"If-Match": `"1"`},
		body: map[string]any{"operationId": "op-1", "at": t0, "ringVersion": 1, "value": 2}}
	nle := &raftstore.NotLeaderError{LeaderID: "storage-1-0/5f0c", LeaderAddr: "storage-1-0.storage-1.ns.svc.cluster.local:7000"}
	fallback := "http-of-storage-1-0.storage-1.ns.svc.cluster.local:7000"

	for _, tc := range []struct {
		name    string
		leaders []internalapi.ShardLeader
		want    string
	}{
		{"no coordinator answer yet", nil, fallback},
		{"the coordinator's leader", []internalapi.ShardLeader{
			{Shard: testShard, LeaderId: "storage-1-0/5f0c", Address: "storage-1-0.adv:9090", Term: 7},
			{Shard: testShard + 1, LeaderId: "storage-2-0/aa", Address: "storage-2-0.adv:9090", Term: 3},
		}, "storage-1-0.adv:9090"},
		{"the coordinator names another leader", []internalapi.ShardLeader{
			{Shard: testShard, LeaderId: "storage-1-2/77", Address: "storage-1-2.adv:9090", Term: 6},
		}, fallback},
		{"only other shards", []internalapi.ShardLeader{
			{Shard: testShard + 1, LeaderId: "storage-1-0/5f0c", Address: "wrong:1", Term: 7},
		}, fallback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newFakeNode(t)
			n.term = 7
			n.applyErr = nle
			s := newTestServer(t, n)
			if tc.leaders != nil {
				s.learnLeaders(internalapi.HeartbeatResponse{RingVersion: 1, Leaders: tc.leaders})
			}
			e := expectError(t, do(t, s.dataRoutes(), put), http.StatusServiceUnavailable, internalapi.NOTLEADER)
			if e.LeaderHint == nil || e.LeaderHint.NodeId != "storage-1-0" || e.LeaderHint.Address != tc.want {
				t.Fatalf("leaderHint = %+v, want storage-1-0 at %s", e.LeaderHint, tc.want)
			}
			if n.statuses != 0 {
				t.Fatalf("leaderHint called Status %d times", n.statuses)
			}
		})
	}
}

// A heartbeat answer's leader hints are kept for not-leader hints; they
// change nothing in Raft (the fake coordinator names storage-1-2).
func TestHeartbeatAnswerFeedsLeaderHint(t *testing.T) {
	n := heartbeatNode(t)
	c := newFakeCoordinator(t)
	s := &Server{s: newTestServer(t, n)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.RunHeartbeats(ctx, HeartbeatConfig{CoordinatorURL: c.srv.URL, Address: "storage-1-1.storage-1-headless:8080", Interval: 20 * time.Millisecond})
	}()
	eventually(t, "a leader hint from the heartbeat answer", func() bool {
		return s.s.advertised("storage-1-2/"+testIncarnation) == "elsewhere:8080"
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RunHeartbeats: %v", err)
	}
}

func TestMgmtTransferLeader(t *testing.T) {
	t.Run("operator only", func(t *testing.T) {
		for _, tc := range []struct {
			name       string
			configured string
			header     map[string]string
			status     int
		}{
			{"no token configured", "", operatorAuth, http.StatusForbidden},
			{"no Authorization header", testToken, nil, http.StatusUnauthorized},
			{"wrong token", testToken, map[string]string{"Authorization": "Bearer nope"}, http.StatusUnauthorized},
		} {
			n := newFakeNode(t)
			s := newTestServer(t, n)
			s.operatorToken = tc.configured
			rec := do(t, s.mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/transfer-leader", header: tc.header})
			if rec.Code != tc.status || n.stepDowns != 0 {
				t.Fatalf("%s: status %d after %d step-downs, want %d and none", tc.name, rec.Code, n.stepDowns, tc.status)
			}
		}
	})
	t.Run("leader steps down", func(t *testing.T) {
		n := newFakeNode(t)
		n.status = raftstore.Status{State: "Follower", Term: 8, LeaderID: "storage-1-2/z"}
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/transfer-leader", header: operatorAuth})
		if rec.Code != http.StatusOK || n.stepDowns != 1 {
			t.Fatalf("status %d (%s) after %d step-downs", rec.Code, rec.Body.String(), n.stepDowns)
		}
		got := decodeJSON[transferResponse](t, rec)
		if want := (transferResponse{PodName: testPod, State: "Follower", Term: 8, LeaderID: "storage-1-2/z"}); got != want {
			t.Fatalf("answer = %+v, want %+v", got, want)
		}
	})
	t.Run("follower is NOT_LEADER", func(t *testing.T) {
		n := newFakeNode(t)
		n.stepDownErr = raftstore.ErrNotLeader
		n.status = raftstore.Status{State: "Follower", LeaderID: "storage-1-0/x"}
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/transfer-leader", header: operatorAuth})
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "NOT_LEADER") || !strings.Contains(rec.Body.String(), "storage-1-0/x") {
			t.Fatalf("follower = %d %s, want 409 NOT_LEADER naming the leader", rec.Code, rec.Body.String())
		}
	})
	t.Run("transfer that does not finish", func(t *testing.T) {
		n := newFakeNode(t)
		n.stepDownErr = errors.New("raftstore: still leader after 5 leadership transfers")
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/transfer-leader", header: operatorAuth})
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status %d (%s), want 503", rec.Code, rec.Body.String())
		}
	})
}
