package storagenode

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
)

// memberNode is a fakeNode that also takes membership changes.
type memberNode struct {
	*fakeNode
	cfg     raftstore.Configuration
	changes []string
	err     error
	index   uint64
}

func (n *memberNode) record(op string, id raft.ServerID, prev uint64) (uint64, error) {
	n.changes = append(n.changes, op+" "+string(id))
	if n.err != nil {
		return 0, n.err
	}
	if prev != n.cfg.Index {
		return 0, raftstore.ErrConfigurationChanged
	}
	return n.index, nil
}

func (n *memberNode) RemoveServer(_ context.Context, id raft.ServerID, prev uint64) (uint64, error) {
	return n.record("remove", id, prev)
}

func (n *memberNode) AddNonvoter(_ context.Context, id raft.ServerID, _ raft.ServerAddress, prev uint64) (uint64, error) {
	return n.record("add-nonvoter", id, prev)
}

func (n *memberNode) AddVoter(_ context.Context, id raft.ServerID, _ raft.ServerAddress, prev uint64) (uint64, error) {
	return n.record("add-voter", id, prev)
}

func (n *memberNode) Configuration(context.Context) (raftstore.Configuration, error) {
	return n.cfg, nil
}

func threeVoters() raftstore.Configuration {
	c := raftstore.Configuration{Index: 7, Committed: true}
	for _, id := range []string{"storage-1-0/a", "storage-1-1/b", "storage-1-2/c"} {
		c.Servers = append(c.Servers, raftstore.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(id[:11] + ":7000"), Suffrage: raft.Voter.String()})
	}
	return c
}

// #21: /mgmt/status carries the Raft configuration this replica knows, with
// its index and whether it is committed.
func TestStatusReportsConfiguration(t *testing.T) {
	n := newFakeNode(t)
	n.status = raftstore.Status{State: "Leader", HasState: true, Configuration: threeVoters()}
	s := newTestServer(t, n)
	rec := do(t, s.mgmtRoutes(), call{method: http.MethodGet, path: "/mgmt/status"})
	got := decodeJSON[statusResponse](t, rec)
	if got.Configuration == nil || got.Configuration.Index != 7 || !got.Configuration.Committed || len(got.Configuration.Servers) != 3 ||
		got.Configuration.Servers[2].ID != "storage-1-2/c" || got.Configuration.Servers[2].Suffrage != "Voter" {
		t.Fatalf("status configuration = %+v", got.Configuration)
	}
}

// POST /mgmt/membership is operator-only and goes to raftstore with the
// caller's prevIndex. A follower answers 409 NOT_LEADER, a stale index 409
// CONFIG_CHANGED, so storagectl can re-read and retry; nothing else is
// inferred from a failure.
func TestMembershipEndpoint(t *testing.T) {
	body := func(op, id string, prev uint64) map[string]any {
		return map[string]any{"op": op, "id": id, "address": "storage-1-2.x:7000", "prevIndex": prev}
	}
	auth := map[string]string{"Authorization": "Bearer " + testToken}
	for _, tc := range []struct {
		name   string
		err    error
		body   map[string]any
		header map[string]string
		status int
		want   string // in the body
		change string // recorded by the node, "" for none
	}{
		{"remove", nil, body("remove", "storage-1-2/c", 7), auth, http.StatusOK, `"index":8`, "remove storage-1-2/c"},
		{"add a nonvoter", nil, body("add-nonvoter", "storage-1-2/d", 7), auth, http.StatusOK, `"index":8`, "add-nonvoter storage-1-2/d"},
		{"promote", nil, body("add-voter", "storage-1-2/d", 7), auth, http.StatusOK, `"index":8`, "add-voter storage-1-2/d"},
		{"stale index", nil, body("remove", "storage-1-2/c", 6), auth, http.StatusConflict, "CONFIG_CHANGED", "remove storage-1-2/c"},
		{"on a follower", raftstore.ErrNotLeader, body("remove", "storage-1-2/c", 7), auth, http.StatusConflict, "NOT_LEADER", "remove storage-1-2/c"},
		{"outcome unknown", context.DeadlineExceeded, body("remove", "storage-1-2/c", 7), auth, http.StatusServiceUnavailable, "re-read", "remove storage-1-2/c"},
		{"unknown op", nil, body("demote", "storage-1-2/c", 7), auth, http.StatusBadRequest, "op", ""},
		{"no address for an add", nil, map[string]any{"op": "add-voter", "id": "storage-1-2/d", "prevIndex": 7}, auth, http.StatusBadRequest, "address", ""},
		{"no token", nil, body("remove", "storage-1-2/c", 7), nil, http.StatusUnauthorized, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := &memberNode{fakeNode: newFakeNode(t), cfg: threeVoters(), err: tc.err, index: 8}
			n.status = raftstore.Status{State: "Leader", Configuration: n.cfg}
			s := newTestServer(t, n)
			rec := do(t, s.mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/membership", body: tc.body, header: tc.header})
			if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("%d %s, want %d containing %q", rec.Code, rec.Body.String(), tc.status, tc.want)
			}
			if got := strings.Join(n.changes, ","); got != tc.change {
				t.Fatalf("changes %q, want %q", got, tc.change)
			}
		})
	}
}

// While its disk waits for admission a pod serves management only: status
// names it Unadmitted with the reason, readiness fails, liveness passes, and
// every operator action that needs Raft is refused.
func TestGatedManagement(t *testing.T) {
	id := raftstore.Identity{PodName: "storage-shard-1-2", Incarnation: "11111111-1111-4111-8111-111111111111", GroupID: "shard-1", Shard: 1}
	g := NewGated(id, false, GatedConfig{RingVersion: 1, OperatorToken: testToken})
	g.SetReason("not admitted: storage-shard-1-2 is not in the manifest's admitted")
	h := g.MgmtRoutes()

	got := decodeJSON[statusResponse](t, do(t, h, call{method: http.MethodGet, path: "/mgmt/status"}))
	if got.State != StateUnadmitted || got.ServerID != "storage-shard-1-2/11111111-1111-4111-8111-111111111111" || got.HasState ||
		got.Admitted || !strings.Contains(got.AdmissionReason, "not admitted") || got.CaughtUp {
		t.Fatalf("gated status = %+v", got)
	}
	if rec := do(t, h, call{method: http.MethodGet, path: "/readyz"}); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz %d", rec.Code)
	}
	if rec := do(t, h, call{method: http.MethodGet, path: "/healthz"}); rec.Code != http.StatusOK {
		t.Fatalf("healthz %d", rec.Code)
	}
	auth := map[string]string{"Authorization": "Bearer " + testToken}
	for _, path := range []string{"/mgmt/bootstrap", "/mgmt/membership", "/mgmt/snapshot", "/mgmt/transfer-leader"} {
		rec := do(t, h, call{method: http.MethodPost, path: path, header: auth, body: map[string]any{}})
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "UNADMITTED") {
			t.Fatalf("%s while gated: %d %s, want 409 UNADMITTED", path, rec.Code, rec.Body.String())
		}
	}
}
