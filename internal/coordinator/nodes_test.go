package coordinator_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

func adminNodes(t *testing.T, h *harness) map[string]internalapi.NodeStatus {
	t.Helper()
	rec := h.do(call{method: http.MethodGet, path: "/admin/nodes", header: operatorAuth})
	if rec.Code != http.StatusOK {
		t.Fatalf("/admin/nodes: %d %s", rec.Code, rec.Body.String())
	}
	out := map[string]internalapi.NodeStatus{}
	for _, n := range decodeJSON[internalapi.AdminNodes](t, rec).Nodes {
		out[n.NodeId] = n
	}
	return out
}

// Registration records the node and returns the ring and its shard peers. It
// never calls the node (no bootstrap, M7); fakeNode fails on any /mgmt call.
func TestRegister(t *testing.T) {
	h := newHarness(t, nil)
	h.registerAll()
	n := h.nodes[1][0]
	rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/register", body: n.registration()})
	if rec.Code != http.StatusOK {
		t.Fatalf("re-register: %d %s", rec.Code, rec.Body.String())
	}
	resp := decodeJSON[internalapi.RegisterResponse](t, rec)
	if !resp.Accepted || resp.Ring.RingVersion != h.rg.Version() || resp.Ring.Checksum != h.rg.Checksum() || resp.Ring.Shards != 3 || resp.Ring.VnodesPerShard != 128 {
		t.Fatalf("register response = %+v", resp)
	}
	peers := map[string]string{}
	for _, p := range resp.Peers {
		peers[p.NodeId] = p.Address
	}
	if len(peers) != 2 || peers[h.nodes[1][1].pod] != h.nodes[1][1].host() || peers[h.nodes[1][2].pod] != h.nodes[1][2].host() {
		t.Fatalf("peers = %+v, want the two other shard-1 replicas", resp.Peers)
	}
	if h.allRequests() != 0 {
		t.Fatalf("registration sent %d requests to storage nodes", h.allRequests())
	}

	for name, mutate := range map[string]func(*internalapi.NodeRegistration){
		"no nodeId":        func(r *internalapi.NodeRegistration) { r.NodeId = "" },
		"bad nodeId":       func(r *internalapi.NodeRegistration) { r.NodeId = "Storage_1" },
		"shard outside":    func(r *internalapi.NodeRegistration) { r.ShardId = 3 },
		"bad incarnation":  func(r *internalapi.NodeRegistration) { r.Incarnation = "not-a-uuid" },
		"no address":       func(r *internalapi.NodeRegistration) { r.Address = "" },
		"group mismatches": func(r *internalapi.NodeRegistration) { r.GroupId = "shard-2" },
	} {
		reg := n.registration()
		mutate(&reg)
		if rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/register", body: reg}); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s, want 400", name, rec.Code, rec.Body.String())
		}
	}
	if rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/register", body: `{"nodeId":"storage-1-0","extra":1}`}); rec.Code != http.StatusBadRequest {
		t.Errorf("unknown field: %d, want 400", rec.Code)
	}
}

// A heartbeat needs a matching registration: 404 for an unknown node or a
// different disk incarnation (a wiped disk must register again).
func TestHeartbeatRequiresRegistration(t *testing.T) {
	h := newHarness(t, nil)
	n := h.nodes[0][0]
	expectError(t, h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: n.heartbeat(nil, 1)}), http.StatusNotFound, internalapi.NOTFOUND)
	h.registerAll()
	hb := n.heartbeat(nil, 1)
	hb.Incarnation = "11111111-1111-4111-8111-111111111111"
	expectError(t, h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: hb}), http.StatusNotFound, internalapi.NOTFOUND)
}

// Plan §15: SUSPECT after 3 s and DOWN after 5 s without a heartbeat; a
// heartbeat makes the node UP again. The node's own health is shown apart.
func TestNodeStateFollowsHeartbeatAge(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	n := h.nodes[2][1]
	state := func() internalapi.NodeState { return adminNodes(t, h)[n.pod].State }

	if s := adminNodes(t, h)[n.pod]; s.State != internalapi.UP || s.Health != internalapi.HealthOk || s.LastHeartbeatAt != t0 || s.Address != n.host() || s.Incarnation != n.incarnation {
		t.Fatalf("fresh node = %+v", s)
	}
	h.clock.advance(2999 * time.Millisecond)
	if s := state(); s != internalapi.UP {
		t.Fatalf("after 2.999 s: %s, want UP", s)
	}
	h.clock.advance(time.Millisecond)
	if s := state(); s != internalapi.SUSPECT {
		t.Fatalf("after 3 s: %s, want SUSPECT", s)
	}
	h.clock.advance(2 * time.Second)
	if s := state(); s != internalapi.DOWN {
		t.Fatalf("after 5 s: %s, want DOWN", s)
	}

	hb := n.heartbeat(h.nodes[2][0], 1)
	hb.Health = internalapi.HealthDegraded
	if rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: hb}); rec.Code != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body.String())
	}
	if s := adminNodes(t, h)[n.pod]; s.State != internalapi.UP || s.Health != internalapi.HealthDegraded || s.LastHeartbeatAt != t0+5000 {
		t.Fatalf("after a new heartbeat = %+v, want UP/degraded", s)
	}
	if h.allRequests() != 0 {
		t.Fatalf("state changes sent %d requests to storage nodes", h.allRequests())
	}
}

func TestNodeStateThresholdsAreConfigurable(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) { c.SuspectAfter, c.DownAfter = time.Second, 2*time.Second })
	h.ready()
	n := h.nodes[0][2]
	h.clock.advance(time.Second)
	if s := adminNodes(t, h)[n.pod].State; s != internalapi.SUSPECT {
		t.Fatalf("after 1 s: %s, want SUSPECT", s)
	}
	h.clock.advance(time.Second)
	if s := adminNodes(t, h)[n.pod].State; s != internalapi.DOWN {
		t.Fatalf("after 2 s: %s, want DOWN", s)
	}
	if _, err := coordinator.New(coordinator.Config{Ring: h.rg, SuspectAfter: 5 * time.Second, DownAfter: 3 * time.Second}); err == nil {
		t.Fatal("SuspectAfter >= DownAfter accepted")
	}
	if _, err := coordinator.New(coordinator.Config{}); err == nil {
		t.Fatal("no ring accepted")
	}
}

// The heartbeat answer carries one leader hint per shard: the registered
// address of the leader with the highest term reported. A lower-term report
// never replaces a higher-term hint.
func TestHeartbeatResponseCarriesLeaderHints(t *testing.T) {
	h := newHarness(t, nil)
	h.registerAll()
	h.elect(0, 1, 4)
	h.elect(1, 2, 7)

	rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: h.nodes[1][0].heartbeat(h.nodes[1][0], 6)})
	if rec.Code != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body.String())
	}
	resp := decodeJSON[internalapi.HeartbeatResponse](t, rec)
	if resp.RingVersion != h.rg.Version() || resp.Checksum != h.rg.Checksum() {
		t.Fatalf("ring hint = %d %s", resp.RingVersion, resp.Checksum)
	}
	leaders := map[int]internalapi.ShardLeader{}
	for _, l := range resp.Leaders {
		leaders[l.Shard] = l
	}
	want := map[int]*fakeNode{0: h.nodes[0][1], 1: h.nodes[1][2]}
	terms := map[int]uint64{0: 4, 1: 7}
	if len(leaders) != 2 {
		t.Fatalf("leaders = %+v, want shards 0 and 1 only (shard 2 has none)", resp.Leaders)
	}
	for s, n := range want {
		if l := leaders[s]; l.LeaderId != n.serverID() || l.Address != n.host() || l.Term != terms[s] {
			t.Errorf("shard %d leader = %+v, want %s at %s term %d", s, l, n.serverID(), n.host(), terms[s])
		}
	}

	// A leader that is not registered has no address and is not a hint.
	ghost := newFakeNode(t, 2, 9)
	h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: h.nodes[2][0].heartbeat(ghost, 3)})
	resp = decodeJSON[internalapi.HeartbeatResponse](t, h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: h.nodes[2][0].heartbeat(nil, 3)}))
	for _, l := range resp.Leaders {
		if l.Shard == 2 {
			t.Fatalf("unregistered leader became a hint: %+v", l)
		}
	}
}
