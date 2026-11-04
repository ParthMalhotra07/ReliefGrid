package coordinator_test

import (
	"net/http"
	"slices"
	"strings"
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
//
// The other members answer as another shard (never proposed), so the call
// ends once each address has been dialled once, not at the budget: an
// attempt the deadline cuts off after it was sent is unknown on its own
// (TestSentAttemptTheLeaderNeverRanIsUnknown), which would let this test
// pass without the leader's answer, or fail if the leader's server had not
// run the request yet (issue #91).
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
				h := newHarness(t, nil)
				h.ready()
				h.nodes[1][0].setClaim(claim)
				for i, n := range h.nodes[1][1:] {
					n.setClaim(&identity{serverID: h.nodes[2][i+1].serverID(), shard: "2"})
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
				if !strings.Contains(e.Message, "answered as another server") {
					t.Fatalf("message = %q; want the call to end once every member answered, not at the budget", e.Message)
				}
				for _, n := range h.nodes[1] {
					if got := len(n.requests()); got != 1 {
						t.Fatalf("%s dialled %d times, want once", n.pod, got)
					}
				}
			})
		}
	}
}

// A NOT_LEADER hint names the leader's pod and, from a current storage
// node, its full serverId: the answer at the hinted address must come from
// that server. A hint without serverId (or with one for another pod) falls
// back to any incarnation of the pod. This holds on every round, not only
// the attempt right after the hint.
func TestHintedLeaderMustAnswerAsItself(t *testing.T) {
	const otherIncarnation = "00000000-0000-4000-8000-0000000000ff"
	full := func(n *fakeNode) internalapi.LeaderHint {
		id := n.serverID()
		return internalapi.LeaderHint{NodeId: n.pod, ServerId: &id, Address: n.host()}
	}
	podOnly := func(n *fakeNode) internalapi.LeaderHint {
		return internalapi.LeaderHint{NodeId: n.pod, Address: n.host()}
	}
	foreignID := func(n *fakeNode) internalapi.LeaderHint {
		id := "storage-1-8/" + otherIncarnation // not n's pod: ignored
		return internalapi.LeaderHint{NodeId: n.pod, ServerId: &id, Address: n.host()}
	}
	as := func(id func(n *fakeNode) string, shard string) func(*fakeNode) *identity {
		return func(n *fakeNode) *identity { return &identity{serverID: id(n), shard: shard} }
	}
	itself := func(n *fakeNode) string { return n.serverID() }
	reincarnated := func(n *fakeNode) string { return n.pod + "/" + otherIncarnation }
	another := func(*fakeNode) string { return "storage-1-8/" + otherIncarnation }

	for name, tc := range map[string]struct {
		hint  func(n *fakeNode) internalapi.LeaderHint
		claim func(n *fakeNode) *identity
		want  int
	}{
		"serverId, as itself":                  {full, as(itself, "1"), http.StatusOK},
		"serverId, same pod another disk":      {full, as(reincarnated, "1"), http.StatusServiceUnavailable},
		"serverId, as another pod":             {full, as(another, "1"), http.StatusServiceUnavailable},
		"serverId, anonymously":                {full, func(*fakeNode) *identity { return nil }, http.StatusServiceUnavailable},
		"serverId, another shard":              {full, as(itself, "0"), http.StatusServiceUnavailable},
		"pod only, as itself":                  {podOnly, as(itself, "1"), http.StatusOK},
		"pod only, same pod another disk":      {podOnly, as(reincarnated, "1"), http.StatusOK},
		"pod only, as another pod":             {podOnly, as(another, "1"), http.StatusServiceUnavailable},
		"serverId of another pod, as itself":   {foreignID, as(itself, "1"), http.StatusOK},
		"serverId of another pod, as that pod": {foreignID, as(another, "1"), http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(c *coordinator.Config) { c.Budget = 200 * time.Millisecond })
			h.ready()
			fresh := newFakeNode(t, 1, 9) // never registered
			fresh.setClaim(tc.claim(fresh))
			for _, n := range h.nodes[1] {
				n.setHandler(notLeaderAs(tc.hint(fresh)))
			}
			rec := h.do(routedCalls(t, h, 1)[0].call)
			if rec.Code != tc.want {
				t.Fatalf("create through a hint to an unregistered pod (%s): %d %s, want %d", name, rec.Code, rec.Body.String(), tc.want)
			}
			if len(fresh.requests()) == 0 {
				t.Fatalf("the hinted address was never tried")
			}
		})
	}
}

// A hint's serverId is checked for a registered pod too: if Raft names
// another incarnation of the pod than the one registered, an answer at the
// registered address from the registered incarnation is not the leader's.
func TestHintedServerIDOverridesRegistration(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) { c.Budget = 200 * time.Millisecond })
	h.ready()
	target := h.nodes[1][1]
	id := target.pod + "/00000000-0000-4000-8000-0000000000ff"
	for _, n := range []*fakeNode{h.nodes[1][0], h.nodes[1][2]} {
		n.setHandler(notLeaderAs(internalapi.LeaderHint{NodeId: target.pod, ServerId: &id, Address: target.host()}))
	}
	e := expectError(t, h.do(routedCalls(t, h, 1)[0].call), http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN)
	if !e.OutcomeUnknown {
		t.Fatalf("error = %+v, want outcomeUnknown", e)
	}
	if len(target.requests()) == 0 {
		t.Fatalf("the hinted pod was never tried")
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
