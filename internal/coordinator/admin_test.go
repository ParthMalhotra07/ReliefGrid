package coordinator_test

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

var adminPaths = []string{"/admin/ring", "/admin/nodes", "/admin/locate/req:r-1", "/admin/distribution"}

// /admin needs the operator bearer token: 401 without it or with a wrong
// one, 403 when the coordinator has no token configured.
func TestAdminRequiresOperatorToken(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	for _, p := range adminPaths {
		for _, hdr := range []map[string]string{nil, {"Authorization": "Bearer wrong"}, {"Authorization": "Basic " + testToken}, {"Authorization": testToken}} {
			expectError(t, h.do(call{method: http.MethodGet, path: p, header: hdr}), http.StatusUnauthorized, internalapi.UNAUTHORIZED)
		}
		if rec := h.do(call{method: http.MethodGet, path: p, header: operatorAuth}); rec.Code != http.StatusOK {
			t.Errorf("%s with the token: %d %s", p, rec.Code, rec.Body.String())
		}
	}
	open := newHarness(t, func(c *coordinator.Config) { c.OperatorToken = "" })
	for _, p := range adminPaths {
		expectError(t, open.do(call{method: http.MethodGet, path: p, header: operatorAuth}), http.StatusForbidden, internalapi.FORBIDDEN)
	}
}

func TestAdminRing(t *testing.T) {
	h := newHarness(t, nil)
	rec := h.do(call{method: http.MethodGet, path: "/admin/ring", header: operatorAuth})
	resp := decodeJSON[internalapi.AdminRing](t, rec)
	if resp.Ring.Checksum != h.rg.Checksum() || resp.Ring.RingVersion != 1 || !slices.Equal(resp.TokensPerShard, []int{128, 128, 128}) {
		t.Fatalf("admin ring = %+v", resp)
	}
}

// /admin/nodes lists every registered node, ordered by shard then pod, with
// the coordinator's state next to the node's own health.
func TestAdminNodesListsEveryNode(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	rec := h.do(call{method: http.MethodGet, path: "/admin/nodes", header: operatorAuth})
	nodes := decodeJSON[internalapi.AdminNodes](t, rec).Nodes
	var got []string
	for _, n := range nodes {
		got = append(got, n.NodeId)
		if n.State != internalapi.UP || n.Admitted != true || n.Term != 1 || n.LeaderId == nil {
			t.Errorf("node %+v", n)
		}
	}
	var want []string
	for s := range 3 {
		for i := range 3 {
			want = append(want, fmt.Sprintf("storage-%d-%d", s, i))
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
}

// Logical placement: the ring's shard and hash for the key, the leader the
// coordinator believes in (learned from a NOT_LEADER hint too), and the
// shard's replicas, shown apart.
func TestAdminLocate(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	key, _ := keyOn(t, h.rg, 2, "req:", "r")
	loc := func() internalapi.Placement {
		rec := h.do(call{method: http.MethodGet, path: "/admin/locate/" + url.PathEscape(key), header: operatorAuth})
		if rec.Code != http.StatusOK {
			t.Fatalf("locate: %d %s", rec.Code, rec.Body.String())
		}
		return decodeJSON[internalapi.Placement](t, rec)
	}
	p := loc()
	wantReplicas := []string{"storage-2-0", "storage-2-1", "storage-2-2"}
	if p.Key != key || p.Shard != 2 || p.RingVersion != 1 || p.Hash != fmt.Sprintf("%016x", xxhash.Sum64String(key)) ||
		p.LeaderId == nil || *p.LeaderId != h.nodes[2][0].serverID() || !slices.Equal(p.Replicas, wantReplicas) {
		t.Fatalf("placement = %+v", p)
	}

	h.nodes[2][0].setHandler(notLeader(h.nodes[2][1], 2))
	if rec := h.do(routedCalls(t, h, 2)[0].call); rec.Code != http.StatusOK {
		t.Fatalf("write: %d %s", rec.Code, rec.Body.String())
	}
	if p := loc(); p.LeaderId == nil || *p.LeaderId != h.nodes[2][1].serverID() {
		t.Fatalf("leader after a NOT_LEADER hint = %v, want %s", p.LeaderId, h.nodes[2][1].serverID())
	}

	expectError(t, h.do(call{method: http.MethodGet, path: "/admin/locate/req:", header: operatorAuth}), http.StatusBadRequest, internalapi.INVALID)
}

// Distribution counts each shard once, from its current leader's heartbeat:
// followers' counts are ignored, the age of the counts is shown, and a shard
// whose leader reported none is countsKnown=false (never "0 records").
func TestAdminDistributionUsesLeaderCountsOnce(t *testing.T) {
	h := newHarness(t, nil)
	h.registerAll()
	beat := func(n, leader *fakeNode, term uint64, c *internalapi.RecordCounts) {
		hb := n.heartbeat(leader, term)
		hb.RecordCounts = c
		if rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: hb}); rec.Code != http.StatusOK {
			t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body.String())
		}
	}
	for s := range 2 {
		l := h.nodes[s][1]
		beat(h.nodes[s][0], l, 3, &internalapi.RecordCounts{Request: 1, Resource: 1})                       // a lagging follower
		beat(l, l, 3, &internalapi.RecordCounts{Request: 1000 + s, Resource: 2000, Kv: 300, Tombstones: 7}) // the leader
		beat(h.nodes[s][2], l, 3, &internalapi.RecordCounts{Request: 999, Resource: 1999, Kv: 300})
	}
	for _, n := range h.nodes[2] { // shard 2: no leader known
		beat(n, nil, 3, &internalapi.RecordCounts{Request: 5})
	}
	h.clock.advance(1500 * time.Millisecond)

	rec := h.do(call{method: http.MethodGet, path: "/admin/distribution", header: operatorAuth})
	if rec.Code != http.StatusOK {
		t.Fatalf("distribution: %d %s", rec.Code, rec.Body.String())
	}
	d := decodeJSON[internalapi.Distribution](t, rec)
	if d.RingVersion != 1 || len(d.Shards) != 3 {
		t.Fatalf("distribution = %+v", d)
	}
	for _, s := range d.Shards[:2] {
		want := 1000 + s.Shard + 2000 + 300
		if !s.CountsKnown || s.Records != want || s.Replicas != 3 || s.Counts == nil || s.Counts.Tombstones != 7 ||
			s.LeaderId == nil || *s.LeaderId != h.nodes[s.Shard][1].serverID() || s.HeartbeatAgeMs == nil || *s.HeartbeatAgeMs != 1500 {
			t.Errorf("shard %d = %+v (counts %+v), want %d records from the leader, age 1500 ms", s.Shard, s, s.Counts, want)
		}
	}
	if s := d.Shards[2]; s.Shard != 2 || s.CountsKnown || s.Records != 0 {
		t.Errorf("shard 2 without a leader = %+v, want countsKnown=false", s)
	}
}
