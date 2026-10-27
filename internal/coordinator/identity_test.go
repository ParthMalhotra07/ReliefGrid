package coordinator_test

import (
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// An address is not an identity (issue #46): a freed port or a reused pod
// IP can be answered by another process. The coordinator accepts an answer
// only from the server it dialled, as X-ReliefGrid-Server-Id and
// X-ReliefGrid-Shard report it.

// An answer from a node of another shard is discarded and never counted as
// proposed: the shard's next member serves the call.
func TestAnswerFromAnotherShardIsSkipped(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	other := h.nodes[0][0]
	for _, r := range routedCalls(t, h, 1) {
		t.Run(r.name, func(t *testing.T) {
			h.nodes[1][0].setClaim(&identity{serverID: other.serverID(), shard: "0"})
			defer h.nodes[1][0].setClaim(&identity{serverID: h.nodes[1][0].serverID(), shard: "1"})
			before := len(h.nodes[1][1].requests())
			rec := h.do(r.call)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s with a shard-0 answer at the leader's address: %d %s, want 200 from the next member", r.name, rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get(internalapi.HeaderServerID); got != h.nodes[1][1].serverID() {
				t.Fatalf("answer came from %q, want %s", got, h.nodes[1][1].serverID())
			}
			if len(h.nodes[1][1].requests()) != before+1 {
				t.Fatalf("next member was not tried")
			}
		})
	}
}

// When every member's address answers as another shard, nothing was
// proposed: a mutation ends in 503 NO_LEADER, not OUTCOME_UNKNOWN. An
// address that answered as another server is not dialled again in the same
// call, so the call ends once every candidate has, without waiting out the
// budget (where a deadline mid-attempt would make it unknown).
func TestEveryAnswerFromAnotherShardIsNoLeader(t *testing.T) {
	const budget = 2 * time.Second
	h := newHarness(t, func(c *coordinator.Config) { c.Budget = budget })
	h.ready()
	for i, n := range h.nodes[1] {
		n.setClaim(&identity{serverID: h.nodes[2][i].serverID(), shard: "2"})
	}
	r := routedCalls(t, h, 1)[0] // create
	start := time.Now()
	e := expectError(t, h.do(r.call), http.StatusServiceUnavailable, internalapi.NOLEADER)
	if d := time.Since(start); d > budget/2 {
		t.Fatalf("took %s; want it to end once every member answered as another shard", d)
	}
	if e.OutcomeUnknown {
		t.Fatalf("error = %+v; an answer from another shard was never proposed here", e)
	}
	for _, n := range h.nodes[1] {
		if got := len(n.requests()); got != 1 {
			t.Fatalf("%s dialled %d times, want once", n.pod, got)
		}
	}
}

// An answer that claims this shard but another server, or carries no
// identity, may come from a replica of this shard that proposed it: a
// mutation that gets no verified answer is OUTCOME_UNKNOWN; a read is a
// failed attempt (NO_LEADER).
func TestUnverifiedAnswerIsUnknownForMutations(t *testing.T) {
	h0 := newHarness(t, nil)
	cases := map[string]*identity{
		"another server ID":    {serverID: "storage-1-0/00000000-0000-4000-8000-00000000ffff", shard: "1"},
		"another pod":          {serverID: h0.nodes[1][1].serverID(), shard: "1"},
		"no identity headers":  nil,
		"no server ID":         {serverID: "", shard: "1"},
		"unparseable shard":    {serverID: "storage-1-0/00000000-0000-4000-8000-000000000010", shard: "one"},
		"server ID with no /":  {serverID: "storage-1-0", shard: "1"},
		"empty shard, real ID": {serverID: "storage-1-0/00000000-0000-4000-8000-000000000010", shard: ""},
	}
	for name, claim := range cases {
		for _, r := range routedCalls(t, h0, 1) {
			t.Run(name+"/"+r.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, func(c *coordinator.Config) { c.Budget = 200 * time.Millisecond })
				h.ready()
				h.nodes[1][0].setClaim(claim)
				for _, n := range h.nodes[1][1:] {
					n.setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))
				}
				want := internalapi.NOLEADER
				if r.mutate {
					want = internalapi.OUTCOMEUNKNOWN
				}
				rec := h.do(r.call)
				e := expectError(t, rec, http.StatusServiceUnavailable, want)
				if e.OutcomeUnknown != r.mutate {
					t.Fatalf("error = %+v, want outcomeUnknown=%v", e, r.mutate)
				}
				if len(h.nodes[1][0].requests()) == 0 {
					t.Fatalf("the leader's address was never tried")
				}
			})
		}
	}
}

// A NOT_LEADER hint may name a pod that has not registered; its answer must
// come from that pod.
func TestHintedLeaderMustAnswerAsItself(t *testing.T) {
	for name, tc := range map[string]struct {
		claim func(n *fakeNode) *identity
		want  int
	}{
		"as itself": {func(n *fakeNode) *identity { return &identity{serverID: n.serverID(), shard: "1"} }, http.StatusOK},
		"as another": {func(*fakeNode) *identity {
			return &identity{serverID: "storage-1-8/00000000-0000-4000-8000-000000000018", shard: "1"}
		}, http.StatusServiceUnavailable},
		"anonymously":   {func(*fakeNode) *identity { return nil }, http.StatusServiceUnavailable},
		"another shard": {func(n *fakeNode) *identity { return &identity{serverID: n.serverID(), shard: "0"} }, http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(c *coordinator.Config) { c.Budget = 200 * time.Millisecond })
			h.ready()
			fresh := newFakeNode(t, 1, 9) // never registered
			fresh.setClaim(tc.claim(fresh))
			h.nodes[1][0].setHandler(notLeader(fresh, 2))
			for _, n := range h.nodes[1][1:] {
				n.setHandler(notLeader(fresh, 2))
			}
			rec := h.do(routedCalls(t, h, 1)[0].call)
			if rec.Code != tc.want {
				t.Fatalf("create through a hint to an unregistered pod answering %s: %d %s, want %d", name, rec.Code, rec.Body.String(), tc.want)
			}
			if len(fresh.requests()) == 0 {
				t.Fatalf("the hinted address was never tried")
			}
		})
	}
}

// A scan page from a node that is not the dialled member is a failed
// shard, never that shard's records.
func TestScanPageFromAnotherNodeFailsTheShard(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) { c.Budget = 200 * time.Millisecond })
	h.ready()
	for s := range 3 {
		scanAnswer(h, s)
	}
	for i, n := range h.nodes[2] {
		n.setClaim(&identity{serverID: h.nodes[0][i].serverID(), shard: "2"})
	}
	rec := h.do(call{method: http.MethodGet, path: scanPath})
	if rec.Code != http.StatusOK {
		t.Fatalf("scan: %d %s", rec.Code, rec.Body.String())
	}
	resp := decodeJSON[internalapi.ScanResponse](t, rec)
	if !resp.Partial || resp.FailedShards == nil || !slices.Equal(*resp.FailedShards, []int{2}) ||
		!slices.Equal(scanKeys(resp), []string{"res:s0-a", "res:s0-b", "res:s1-a", "res:s1-b"}) {
		t.Fatalf("scan with shard 2 answered by impostors = %+v; want partial, failedShards [2]", resp)
	}
}
