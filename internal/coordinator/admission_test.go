package coordinator_test

import (
	"net/http"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

func registered(t *testing.T, h *harness, n *fakeNode) internalapi.RegisterResponse {
	t.Helper()
	rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/register", body: n.registration()})
	if rec.Code != http.StatusOK {
		t.Fatalf("register %s: %d %s", n.pod, rec.Code, rec.Body.String())
	}
	return decodeJSON[internalapi.RegisterResponse](t, rec)
}

func adminAdmitted(t *testing.T, h *harness, pod string) bool {
	t.Helper()
	rec := h.do(call{method: http.MethodGet, path: "/admin/nodes", header: operatorAuth})
	for _, n := range decodeJSON[internalapi.AdminNodes](t, rec).Nodes {
		if n.NodeId == pod {
			return n.Admitted
		}
	}
	t.Fatalf("%s is not listed", pod)
	return false
}

// #20, M7 at registration: a node is admitted only once its ServerID is in
// its shard's committed Raft configuration, as the leader reports it. A
// wiped disk re-registering under the same pod name with a new incarnation
// is unadmitted, never routed to, until replace adds it.
func TestAdmissionFollowsCommittedConfiguration(t *testing.T) {
	h := newHarness(t, nil)
	row := h.nodes[1]
	if resp := registered(t, h, row[0]); !resp.Accepted || resp.Admitted {
		t.Fatalf("before any committed configuration: %+v, want accepted, not admitted", resp)
	}
	h.registerAll()
	for s := range h.nodes {
		h.elect(s, 0, 1)
	}
	if resp := registered(t, h, row[2]); !resp.Admitted || !adminAdmitted(t, h, row[2].pod) {
		t.Fatalf("a member of the committed configuration: %+v, want admitted", resp)
	}

	// storage-1-2's disk is wiped: same pod, new incarnation.
	oldInc := row[2].incarnation
	row[2].incarnation = "99999999-9999-4999-8999-999999999999"
	if resp := registered(t, h, row[2]); !resp.Accepted || resp.Admitted || adminAdmitted(t, h, row[2].pod) {
		t.Fatalf("a wiped disk: %+v, want accepted, not admitted", resp)
	}
	key, _ := keyOn(t, h.rg, 1, "kv", "adm")
	row[0].setHandler(failWith(http.StatusServiceUnavailable, internalapi.STORAGEUNAVAILABLE, false))
	row[1].setHandler(failWith(http.StatusServiceUnavailable, internalapi.STORAGEUNAVAILABLE, false))
	h.do(call{method: http.MethodGet, path: "/internal/data/" + key})
	if got := len(row[2].requests()); got != 0 {
		t.Fatalf("the unadmitted node got %d routed requests", got)
	}

	// An uncommitted configuration naming it does not admit it.
	pending := committedConfig(row[0], row[1], row[2])
	pending.Committed, pending.Index = false, 4
	hb := row[0].heartbeat(row[0], 1)
	hb.Configuration = pending
	h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: hb})
	if adminAdmitted(t, h, row[2].pod) {
		t.Fatal("admitted on an uncommitted configuration")
	}

	// replace adds it (a nonvoter first): committed, it is admitted.
	added := committedConfig(row[0], row[1], row[2])
	added.Index, added.Servers[2].Suffrage = 5, internalapi.RaftServerSuffrageNonvoter
	hb.Configuration = added
	h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: hb})
	if !adminAdmitted(t, h, row[2].pod) {
		t.Fatal("not admitted once the committed configuration names it")
	}
	row[2].incarnation = oldInc
	if registered(t, h, row[2]).Admitted {
		t.Fatal("the old incarnation was admitted after its removal")
	}
}

// With no leader known, the routing fallback probes voters only: a
// nonvoter (a replacement catching up) can never lead, so it is admitted
// but never dialled for a routed call.
func TestRoutingFallbackSkipsNonvoters(t *testing.T) {
	h := newHarness(t, nil)
	h.registerAll()
	row := h.nodes[1]
	cfg := committedConfig(row...)
	cfg.Servers[2].Suffrage = internalapi.RaftServerSuffrageNonvoter
	for _, n := range row { // no leader known
		hb := n.heartbeat(nil, 3)
		hb.Configuration = cfg
		h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: hb})
	}
	if !adminAdmitted(t, h, row[2].pod) {
		t.Fatal("the nonvoter is not admitted")
	}
	row[0].setHandler(failWith(http.StatusServiceUnavailable, internalapi.STORAGEUNAVAILABLE, false))
	row[1].setHandler(failWith(http.StatusServiceUnavailable, internalapi.STORAGEUNAVAILABLE, false))
	key, _ := keyOn(t, h.rg, 1, "kv", "nv")
	h.do(call{method: http.MethodGet, path: "/internal/data/" + key})
	if len(row[0].requests()) == 0 || len(row[1].requests()) == 0 {
		t.Fatalf("the voters were not probed: %d, %d", len(row[0].requests()), len(row[1].requests()))
	}
	if got := len(row[2].requests()); got != 0 {
		t.Fatalf("the nonvoter got %d routed requests", got)
	}
}
