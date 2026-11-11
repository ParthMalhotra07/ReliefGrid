package coordinator_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// routed is one routed call and the shard that must serve it.
type routed struct {
	name   string
	spec   string // "METHOD /path/template" in contracts/internal.yaml
	call   call
	shard  int
	mutate bool
}

// routedCalls builds one call per routed contract route, each owned by the
// given shard.
func routedCalls(t *testing.T, h *harness, shard int) []routed {
	t.Helper()
	kv, _ := keyOn(t, h.rg, shard, "", "kv:route")
	_, req := keyOn(t, h.rg, shard, store.RequestKeyPrefix, "q")
	_, res := keyOn(t, h.rg, shard, store.ResourceKeyPrefix, "r")
	body := func(extra string) string {
		return `{"operationId":"op-route","at":1792800000000,"ringVersion":1` + extra + `}`
	}
	esc := url.PathEscape
	q := url.QueryEscape
	return []routed{
		{"create", "PUT /internal/data/{key}", call{method: http.MethodPut, path: "/internal/data/" + esc(kv), header: map[string]string{"If-None-Match": "*"}, body: body(`,"value":1`)}, shard, true},
		{"compare-and-set", "PUT /internal/data/{key}", call{method: http.MethodPut, path: "/internal/data/" + esc(kv), header: map[string]string{"If-Match": `"4"`}, body: body(`,"value":2`)}, shard, true},
		{"read", "GET /internal/data/{key}", call{method: http.MethodGet, path: "/internal/data/" + esc(kv) + "?consistency=stale"}, shard, false},
		{"delete", "DELETE /internal/data/{key}", call{method: http.MethodDelete, path: "/internal/data/" + esc(kv) + "?operationId=op-del&at=1792800000000&ringVersion=1", header: map[string]string{"If-Match": `"5"`}}, shard, true},
		{"transition request", "POST /internal/requests/{id}/transition", call{method: http.MethodPost, path: "/internal/requests/" + esc(req) + "/transition", body: body(`,"expectedVersion":1,"expectedStatus":"PENDING","expectedAttemptId":"","to":"WAITING"`)}, shard, true},
		{"retry workflow", "POST /internal/requests/{id}/retry", call{method: http.MethodPost, path: "/internal/requests/" + esc(req) + "/retry", body: body(`,"expectedVersion":1,"nextAttemptAt":1792800030000`)}, shard, true},
		{"transition resource", "POST /internal/resources/{id}/transition", call{method: http.MethodPost, path: "/internal/resources/" + esc(res) + "/transition", body: body(`,"expectedVersion":1,"status":"OFFLINE"`)}, shard, true},
		{"claim", "POST /internal/resources/{id}/claim", call{method: http.MethodPost, path: "/internal/resources/" + esc(res) + "/claim", body: body(`,"requestId":"q-x","attempt":{"attemptId":"a-1","resourceId":"` + res + `","fingerprint":"fp"},"type":"WATER"`)}, shard, true},
		{"close attempt", "POST /internal/resources/{id}/close-attempt", call{method: http.MethodPost, path: "/internal/resources/" + esc(res) + "/close-attempt", body: body(`,"requestId":"q-x","attempt":{"attemptId":"a-1","resourceId":"` + res + `","fingerprint":"fp"},"disposition":"AVAILABLE","reason":"x"`)}, shard, true},
		{"closure", "GET /internal/resources/{id}/closures/{attemptId}", call{method: http.MethodGet, path: "/internal/resources/" + esc(res) + "/closures/a-1"}, shard, false},
		{"pending outbox", "GET /internal/outbox", call{method: http.MethodGet, path: fmt.Sprintf("/internal/outbox?shard=%d&limit=10&wait=1", shard)}, shard, false},
		{"mark published", "POST /internal/outbox/published", call{method: http.MethodPost, path: fmt.Sprintf("/internal/outbox/published?shard=%d", shard), body: body(`,"eventIds":["ev-1"]`)}, shard, true},
		{"operation", "GET /internal/operations/{operationId}", call{method: http.MethodGet, path: "/internal/operations/op-1?key=" + q(kv)}, shard, false},
		{"age request", "POST /internal/requests/{id}/age", call{method: http.MethodPost, path: "/internal/requests/" + esc(req) + "/age", body: body(`,"expectedVersion":1,"agingIntervalMS":1000,"outbox":[]`)}, shard, true},
	}
}

// Every routed route in the contract has a case above; the others are
// served by the coordinator itself and tested on their own.
func TestRoutedCallsCoverTheContract(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/internal.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	self := []string{"POST /internal/nodes/register", "POST /internal/nodes/heartbeat", "GET /internal/ring", "GET /internal/scan",
		"GET /admin/ring", "GET /admin/nodes", "GET /admin/locate/{key}", "GET /admin/distribution", "GET /admin/ring/simulate"}
	h := newHarness(t, nil)
	var covered []string
	for _, r := range routedCalls(t, h, 0) {
		covered = append(covered, r.spec)
	}
	for path, ops := range spec.Paths {
		for method := range ops {
			if method == "parameters" {
				continue
			}
			op := strings.ToUpper(method) + " " + path
			if !slices.Contains(covered, op) && !slices.Contains(self, op) {
				t.Errorf("contract route %s has no routing test", op)
			}
		}
	}
}

// Each routed call reaches only the leader of the owning shard, with method,
// path, query, conditional headers and body unchanged, and the leader's
// answer (status, body, ETag, Retry-After) comes back unchanged.
func TestEveryRouteGoesToTheOwningShardLeader(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	h.elect(1, 2, 2) // shard 1 is led by ordinal 2
	for s := range 3 {
		for _, r := range routedCalls(t, h, s) {
			t.Run(fmt.Sprintf("%s/shard-%d", r.name, s), func(t *testing.T) {
				leader := h.nodes[s][0]
				if s == 1 {
					leader = h.nodes[s][2]
				}
				before := map[*fakeNode]int{}
				for _, row := range h.nodes {
					for _, n := range row {
						before[n] = len(n.requests())
					}
				}
				leader.setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
					w.Header().Set("ETag", `"9"`)
					w.Header().Set("Retry-After", "2")
					writeJSON(w, http.StatusPreconditionFailed, map[string]any{"code": "VERSION_CONFLICT", "message": "from " + leader.pod})
				})
				defer leader.setHandler(nil)

				rec := h.do(r.call)
				if rec.Code != http.StatusPreconditionFailed || !strings.Contains(rec.Body.String(), "from "+leader.pod) ||
					rec.Header().Get("ETag") != `"9"` || rec.Header().Get("Retry-After") != "2" {
					t.Fatalf("response = %d %q (ETag %q, Retry-After %q); want the leader's answer unchanged",
						rec.Code, rec.Body.String(), rec.Header().Get("ETag"), rec.Header().Get("Retry-After"))
				}
				for _, row := range h.nodes {
					for _, n := range row {
						got := n.requests()[before[n]:]
						if n != leader && len(got) != 0 {
							t.Fatalf("%s (shard %d) received %d requests; only %s should", n.pod, n.shard, len(got), leader.pod)
						}
						if n == leader && len(got) != 1 {
							t.Fatalf("leader %s received %d requests, want 1", n.pod, len(got))
						}
					}
				}
				got := leader.requests()[before[leader]]
				wantURL, _ := url.Parse(r.call.path)
				if got.method != r.call.method || got.path != wantURL.EscapedPath() || got.query != wantURL.RawQuery {
					t.Fatalf("forwarded %s %s?%s, want %s %s?%s", got.method, got.path, got.query, r.call.method, wantURL.EscapedPath(), wantURL.RawQuery)
				}
				for k, v := range r.call.header {
					if got.header.Get(k) != v {
						t.Fatalf("header %s = %q, want %q", k, got.header.Get(k), v)
					}
				}
				if want, _ := r.call.body.(string); string(got.body) != want {
					t.Fatalf("forwarded body %q, want the caller's bytes %q", got.body, want)
				}
			})
		}
	}
}

// A routed key that cannot be placed is refused before any forward.
func TestUnroutableKeyIsInvalid(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	for _, c := range []call{
		{method: http.MethodGet, path: "/internal/data/req:"},
		{method: http.MethodGet, path: "/internal/operations/op-1"}, // no ?key
		{method: http.MethodGet, path: "/internal/outbox?shard=3&limit=1"},
		{method: http.MethodPost, path: "/internal/outbox/published?shard=x", body: `{}`},
	} {
		expectError(t, h.do(c), http.StatusBadRequest, internalapi.INVALID)
	}
	if n := h.allRequests(); n != 0 {
		t.Fatalf("%d requests forwarded", n)
	}
}

// Verify (S1.4): a stale leader hint gets NOT_LEADER with a hint; the
// coordinator retries at the hinted leader with the same operationId and
// the same bytes, and routes the next call there directly.
func TestStaleLeaderHintRetriesNewLeaderWithSameOperation(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	old, cur := h.nodes[2][0], h.nodes[2][1]
	old.setHandler(notLeader(cur, 2))
	r := routedCalls(t, h, 2)[7] // claim
	if r.name != "claim" {
		t.Fatalf("case %q", r.name)
	}
	rec := h.do(r.call)
	if rec.Code != http.StatusOK {
		t.Fatalf("claim through a stale hint: %d %s", rec.Code, rec.Body.String())
	}
	o, c := old.requests(), cur.requests()
	if len(o) != 1 || len(c) != 1 {
		t.Fatalf("old leader got %d, new leader %d requests; want 1 and 1", len(o), len(c))
	}
	if !bytes.Equal(o[0].body, c[0].body) || !bytes.Contains(c[0].body, []byte(`"operationId":"op-route"`)) {
		t.Fatalf("retry body %q differs from the first %q", c[0].body, o[0].body)
	}

	// The hint is learned: the next call goes straight to the new leader.
	if rec := h.do(r.call); rec.Code != http.StatusOK || len(old.requests()) != 1 || len(cur.requests()) != 2 {
		t.Fatalf("second call: %d; old=%d new=%d requests", rec.Code, len(old.requests()), len(cur.requests()))
	}
}

// With no leader hint (no heartbeat named one, or the hinted leader is
// DOWN), the coordinator probes the shard's members, UP before DOWN, and
// follows the first NOT_LEADER hint. Only that shard's members are called.
func TestDiscoveryProbesMembersWithoutAHint(t *testing.T) {
	h := newHarness(t, nil)
	h.registerAll()
	for _, n := range h.nodes[0] { // shard 0 reports no leader, but its committed configuration (M7)
		hb := n.heartbeat(nil, 3)
		hb.Configuration = committedConfig(h.nodes[0]...)
		h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: hb})
	}
	for _, n := range h.nodes[0] {
		if n != h.nodes[0][2] {
			n.setHandler(notLeader(h.nodes[0][2], 3))
		}
	}
	r := routedCalls(t, h, 0)[0]
	if rec := h.do(r.call); rec.Code != http.StatusOK {
		t.Fatalf("create with no hint: %d %s", rec.Code, rec.Body.String())
	}
	if len(h.nodes[0][2].requests()) != 1 {
		t.Fatalf("leader got %d requests", len(h.nodes[0][2].requests()))
	}
	for s := 1; s < 3; s++ {
		for _, n := range h.nodes[s] {
			if len(n.requests()) != 0 {
				t.Fatalf("%s of shard %d was probed for a shard-0 key", n.pod, s)
			}
		}
	}

	// Shard 1: the heartbeat leader went DOWN; a live member is tried first.
	h2 := newHarness(t, nil)
	h2.ready()
	h2.clock.advance(6 * time.Second)
	for _, n := range h2.nodes[1][1:] {
		h2.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: n.heartbeat(h2.nodes[1][0], 1)})
	}
	h2.nodes[1][1].setHandler(notLeader(h2.nodes[1][2], 2))
	if rec := h2.do(routedCalls(t, h2, 1)[0].call); rec.Code != http.StatusOK {
		t.Fatalf("create with a DOWN leader: %d %s", rec.Code, rec.Body.String())
	}
	if n := len(h2.nodes[1][0].requests()); n != 0 {
		t.Fatalf("DOWN leader was tried first (%d requests)", n)
	}
}

// CONTRIBUTING.md, Invariants, 5: once any attempt may have been proposed (OUTCOME_UNKNOWN, a
// dropped connection, a timeout after sending), exhausting the budget gives
// 503 OUTCOME_UNKNOWN, even if every later attempt was refused unproposed.
// Every attempt carries the same bytes.
func TestUnknownOutcomeIsStickyAcrossRetries(t *testing.T) {
	for name, first := range map[string]func(http.ResponseWriter, *http.Request, []byte){
		"storage OUTCOME_UNKNOWN": failWith(http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN, true),
		"connection dropped":      dropConnection,
		"unexpected 500":          failWith(http.StatusInternalServerError, "", false),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(c *coordinator.Config) { c.Budget = 400 * time.Millisecond })
			h.ready()
			shard := h.nodes[1]
			var calls atomic.Int32
			shard[0].setHandler(func(w http.ResponseWriter, r *http.Request, b []byte) {
				if calls.Add(1) == 1 {
					first(w, r, b)
					return
				}
				failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false)(w, r, b)
			})
			for _, n := range shard[1:] {
				n.setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))
			}
			r := routedCalls(t, h, 1)[4] // transition request
			e := expectError(t, h.do(r.call), http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN)
			if !e.OutcomeUnknown || !e.Retryable {
				t.Fatalf("error = %+v, want outcomeUnknown and retryable", e)
			}
			var bodies [][]byte
			for _, n := range shard {
				for _, s := range n.requests() {
					bodies = append(bodies, s.body)
				}
			}
			if len(bodies) < 2 {
				t.Fatalf("%d attempts within the budget, want retries", len(bodies))
			}
			for _, b := range bodies {
				if !bytes.Equal(b, bodies[0]) {
					t.Fatalf("attempt body %q differs from %q", b, bodies[0])
				}
			}
		})
	}
}

// refusalGate is the coordinator's client transport in the budget-boundary
// tests (issue #155). The routing loop retries until the budget's deadline,
// so on its own whether the last attempt was sent before the budget ended is
// a race between the jittered backoff and the deadline (about 1 run in 100
// under -race). The gate decides it: attempts pass through until every node
// in members has answered, and every later attempt is either held before it
// is sent (send=false: as if no connection could be made) or sent (the
// test's handler then decides what the node does).
type refusalGate struct {
	inner   *http.Transport
	send    bool
	closed  chan struct{} // closed once every member has answered
	held    atomic.Int32  // attempts held unsent
	mu      sync.Mutex
	pending map[string]bool // members (by host) that have not answered yet
}

func newRefusalGate(t *testing.T, send bool) *refusalGate {
	inner := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(inner.CloseIdleConnections)
	return &refusalGate{inner: inner, send: send, closed: make(chan struct{}), pending: map[string]bool{}}
}

// watch sets the members that must answer before the gate closes; call it
// before the first routed request.
func (g *refusalGate) watch(members []*fakeNode) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, n := range members {
		g.pending[n.host()] = true
	}
}

func (g *refusalGate) isClosed() bool {
	select {
	case <-g.closed:
		return true
	default:
		return false
	}
}

func (g *refusalGate) RoundTrip(r *http.Request) (*http.Response, error) {
	if g.isClosed() && !g.send {
		g.held.Add(1)
		<-r.Context().Done() // nothing is written: the node cannot have proposed
		return nil, r.Context().Err()
	}
	resp, err := g.inner.RoundTrip(r)
	if err == nil {
		g.mu.Lock()
		if g.pending[r.URL.Host] {
			delete(g.pending, r.URL.Host)
			if len(g.pending) == 0 {
				close(g.closed)
			}
		}
		g.mu.Unlock()
	}
	return resp, err
}

// When nothing was proposed (NOT_LEADER, NO_LEADER)
// the budget ends in 503 NO_LEADER: retryable, outcomeUnknown=false, with
// Retry-After, and within the budget.
//
// Keep every node listening and explicitly refusing throughout the test:
// closing a listener frees its port for an unrelated server (issue #119).
// Once every member has refused, later attempts are held unsent until the
// budget ends, so no attempt can be cut off after it was sent (issue #155).
// A sent attempt cut off by the budget is correctly unknown:
// TestBudgetCutOffAfterRefusalsIsUnknown covers that case.
func TestNeverProposedExhaustionIsNoLeader(t *testing.T) {
	const budget = time.Second // headroom for slow runners
	gate := newRefusalGate(t, false)
	h := newHarness(t, func(c *coordinator.Config) { c.Budget, c.Client = budget, &http.Client{Transport: gate} })
	h.ready()
	shard := h.nodes[2]
	gate.watch(shard)
	shard[0].setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))
	shard[1].setHandler(notLeader(shard[0], 1))
	shard[2].setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))

	start := time.Now()
	rec := h.do(routedCalls(t, h, 2)[0].call)
	elapsed := time.Since(start)
	e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.NOLEADER)
	if e.OutcomeUnknown || !e.Retryable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("error = %+v (Retry-After %q)", e, rec.Header().Get("Retry-After"))
	}
	if gate.held.Load() == 0 {
		t.Fatal("no attempt was held after every member refused; the budget end was not on an unsent attempt")
	}
	// Check before the direct probes so only routed attempts count here.
	for _, n := range shard {
		if len(n.requests()) == 0 {
			t.Fatalf("%s received no routed attempts; want every refusal path exercised", n.pod)
		}
	}
	if elapsed > budget+500*time.Millisecond {
		t.Fatalf("answered after %s; budget %s", elapsed, budget)
	}

	// Issue #119: a closed listener releases its port to unrelated servers.
	// Require explicit, identified refusals on repeated calls, rather than
	// relying on a transport error to mean that nothing was proposed.
	client := &http.Client{Timeout: budget}
	for i, n := range shard {
		t.Run(n.pod+"/keeps refusing", func(t *testing.T) {
			want := internalapi.NOLEADER
			if i == 1 {
				want = internalapi.NOTLEADER
			}
			for attempt := range 2 {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, n.srv.URL+"/internal/data/probe", nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("refusal probe %d: %v; want an explicit %s answer with node identity", attempt+1, err, want)
				}
				var e internalapi.Error
				err = json.NewDecoder(resp.Body).Decode(&e)
				_ = resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != http.StatusServiceUnavailable || e.Code != want || e.OutcomeUnknown || !e.Retryable ||
					resp.Header.Get(internalapi.HeaderServerID) != n.serverID() || resp.Header.Get(internalapi.HeaderShard) != "2" {
					t.Fatalf("refusal probe %d: status=%d error=%+v headers=%v; want identified %s from %s", attempt+1, resp.StatusCode, e, resp.Header, want, n.serverID())
				}
			}
		})
	}
}

// Issue #155: every member refused unproposed, then one more mutation
// attempt was sent to a refusing node and the budget ended before it
// answered. That node may have just become leader and proposed it, so the
// budget ends in OUTCOME_UNKNOWN (CONTRIBUTING.md, Invariants, 5), not
// NO_LEADER. AttemptTimeout equals the budget, so the budget's deadline is
// what cuts the attempt off, as in the CI failure.
func TestBudgetCutOffAfterRefusalsIsUnknown(t *testing.T) {
	const budget = 400 * time.Millisecond
	gate := newRefusalGate(t, true)
	h := newHarness(t, func(c *coordinator.Config) {
		c.Budget, c.AttemptTimeout, c.Client = budget, budget, &http.Client{Transport: gate}
	})
	h.ready()
	shard := h.nodes[2]
	gate.watch(shard)
	var cutOff atomic.Int32
	refuseThenHang := func(refuse func(http.ResponseWriter, *http.Request, []byte)) func(http.ResponseWriter, *http.Request, []byte) {
		return func(w http.ResponseWriter, r *http.Request, b []byte) {
			if gate.isClosed() { // every member has refused: this attempt was sent
				cutOff.Add(1)
				<-r.Context().Done()
				return
			}
			refuse(w, r, b)
		}
	}
	shard[0].setHandler(refuseThenHang(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false)))
	shard[1].setHandler(refuseThenHang(notLeader(shard[0], 1)))
	shard[2].setHandler(refuseThenHang(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false)))

	start := time.Now()
	e := expectError(t, h.do(routedCalls(t, h, 2)[0].call), http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN)
	if !e.OutcomeUnknown || !e.Retryable {
		t.Fatalf("error = %+v, want outcomeUnknown and retryable", e)
	}
	if d := time.Since(start); d > budget+500*time.Millisecond {
		t.Fatalf("answered after %s; budget %s", d, budget)
	}
	if n := cutOff.Load(); n != 1 {
		t.Fatalf("%d attempts reached a node after every member refused, want exactly the one the budget cut off", n)
	}
}

// A storage node that hangs is bounded by AttemptTimeout and the budget; a
// mutation sent to it may have been proposed, so the answer is unknown. A
// read that times out is not (reads change nothing).
func TestHungNodeIsBoundedByTheBudget(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) {
		c.Budget, c.AttemptTimeout = 400*time.Millisecond, 150*time.Millisecond
	})
	h.ready()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	for _, n := range h.nodes[0] {
		n.setHandler(func(w http.ResponseWriter, r *http.Request, _ []byte) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
	}
	start := time.Now()
	expectError(t, h.do(routedCalls(t, h, 0)[1].call), http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN)
	if d := time.Since(start); d > 700*time.Millisecond {
		t.Fatalf("mutation answered after %s; budget 400ms", d)
	}
	rec := h.do(routedCalls(t, h, 0)[2].call) // read
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("hung read: %d %s", rec.Code, rec.Body.String())
	}
	if e := decodeJSON[internalapi.Error](t, rec); e.OutcomeUnknown || e.Code == internalapi.OUTCOMEUNKNOWN {
		t.Fatalf("read reported %s outcomeUnknown=%v; a read is never an unknown outcome", e.Code, e.OutcomeUnknown)
	}
}

// A mutation whose request was written to the leader is unknown when the
// budget ends, even if the leader's server never got as far as running it
// (a starved process, as under parallel -race load in issue #91): the
// coordinator cannot tell that from a request in flight. Here the leader's
// server holds every connection before the handler, so the attempt always
// outlives the budget, which AttemptTimeout here equals.
func TestSentAttemptTheLeaderNeverRanIsUnknown(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) { c.Budget = 200 * time.Millisecond })
	leader := h.nodes[1][0]
	leader.srv.Close()
	release := make(chan struct{})
	leader.srv = httptest.NewUnstartedServer(http.HandlerFunc(leader.serve))
	leader.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateActive { // a request has started arriving
			<-release
		}
	}
	leader.srv.Start()
	t.Cleanup(leader.srv.Close)
	t.Cleanup(func() { close(release) }) // runs first: Close waits for active connections
	h.ready()

	e := expectError(t, h.do(routedCalls(t, h, 1)[0].call), http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN)
	if !e.OutcomeUnknown {
		t.Fatalf("error = %+v, want outcomeUnknown", e)
	}
	for _, n := range h.nodes[1] {
		if got := len(n.requests()); got != 0 {
			t.Fatalf("%s ran %d requests; the leader's attempt should hold the whole budget", n.pod, got)
		}
	}
}

// A read changes nothing, so no read failure is an unknown outcome: a
// dropped connection, a 5xx or even a storage OUTCOME_UNKNOWN on a read
// ends in 503 NO_LEADER, retryable, with
// outcomeUnknown=false and Retry-After. Only mutations become unknown.
func TestReadFailuresAreNeverUnknown(t *testing.T) {
	for name, fail := range map[string]func(http.ResponseWriter, *http.Request, []byte){
		"connection dropped":      dropConnection,
		"500":                     failWith(http.StatusInternalServerError, "", false),
		"502 without a body":      func(w http.ResponseWriter, _ *http.Request, _ []byte) { w.WriteHeader(http.StatusBadGateway) },
		"storage OUTCOME_UNKNOWN": failWith(http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN, true),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(c *coordinator.Config) { c.Budget = 300 * time.Millisecond })
			h.ready()
			for _, n := range h.nodes[0] {
				n.setHandler(fail)
			}
			for _, r := range routedCalls(t, h, 0) {
				if r.mutate {
					continue
				}
				rec := h.do(r.call)
				e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.NOLEADER)
				if e.OutcomeUnknown || !e.Retryable || rec.Header().Get("Retry-After") == "" {
					t.Fatalf("%s: error = %+v (Retry-After %q), want retryable and not unknown", r.name, e, rec.Header().Get("Retry-After"))
				}
			}
		})
	}
}

// A committed rejection is an answer, not a failure: it is returned at once
// and never retried elsewhere.
func TestCommittedRejectionIsNotRetried(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	h.nodes[0][0].setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSON(w, http.StatusConflict, internalapi.Error{Code: internalapi.DOMAINCONFLICT, Message: "illegal",
			Result: &store.Result{Outcome: store.OutcomeDomainConflict, OperationID: "op-route", Shard: 0}})
	})
	expectError(t, h.do(routedCalls(t, h, 0)[4].call), http.StatusConflict, internalapi.DOMAINCONFLICT)
	if n := h.allRequests(); n != 1 {
		t.Fatalf("%d requests for one committed rejection, want 1", n)
	}
}

// RING_VERSION_MISMATCH: a caller with another ringVersion is refused before
// any forward, with the coordinator's version; a storage node's mismatch is
// returned as is (never proposed) and not retried on other members.
func TestRingVersionMismatch(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	kv, _ := keyOn(t, h.rg, 1, "", "kv:ring")
	for _, c := range []call{
		{method: http.MethodPut, path: "/internal/data/" + kv, header: map[string]string{"If-None-Match": "*"}, body: `{"operationId":"op","at":1,"ringVersion":2,"value":1}`},
		{method: http.MethodDelete, path: "/internal/data/" + kv + "?operationId=op&at=1&ringVersion=2", header: map[string]string{"If-Match": `"1"`}},
		{method: http.MethodPost, path: "/internal/outbox/published?shard=1", body: `{"operationId":"op","at":1,"ringVersion":2,"eventIds":["e"]}`},
	} {
		e := expectError(t, h.do(c), http.StatusConflict, internalapi.RINGVERSIONMISMATCH)
		if e.RingVersion == nil || *e.RingVersion != 1 || !e.Retryable || e.OutcomeUnknown {
			t.Fatalf("%s %s: error = %+v", c.method, c.path, e)
		}
	}
	if n := h.allRequests(); n != 0 {
		t.Fatalf("%d mismatched requests forwarded", n)
	}

	two := uint64(2)
	h.nodes[1][0].setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSON(w, http.StatusConflict, internalapi.Error{Code: internalapi.RINGVERSIONMISMATCH, Message: "node has 2", Retryable: true, RingVersion: &two})
	})
	e := expectError(t, h.do(routedCalls(t, h, 1)[0].call), http.StatusConflict, internalapi.RINGVERSIONMISMATCH)
	if e.OutcomeUnknown || e.RingVersion == nil || *e.RingVersion != 2 {
		t.Fatalf("storage mismatch = %+v", e)
	}
	if n := h.allRequests(); n != 1 {
		t.Fatalf("storage mismatch retried: %d requests", n)
	}
}

func TestGetRing(t *testing.T) {
	h := newHarness(t, nil)
	rec := h.do(call{method: http.MethodGet, path: "/internal/ring"})
	if rec.Code != http.StatusOK {
		t.Fatalf("/internal/ring: %d %s", rec.Code, rec.Body.String())
	}
	r := decodeJSON[internalapi.Ring](t, rec)
	if r.RingVersion != 1 || r.Hash != internalapi.Xxhash64 || r.Shards != 3 || r.VnodesPerShard != 128 || r.Checksum != h.rg.Checksum() {
		t.Fatalf("ring = %+v", r)
	}
}

// A shard with no registered member (a restarted coordinator) is not "every
// member DOWN": the call keeps its budget, and members that register while
// it waits serve it.
func TestUnregisteredShardKeepsTheBudget(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) { c.Budget = 2 * time.Second })
	register := func(s int) {
		for _, n := range h.nodes[s] {
			if rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/register", body: n.registration()}); rec.Code != http.StatusOK {
				t.Fatalf("register %s: %d %s", n.pod, rec.Code, rec.Body.String())
			}
		}
		h.elect(s, 0, 1)
	}
	register(0)
	register(2)

	done := h.doAsync(routedCalls(t, h, 1)[0].call)
	time.Sleep(150 * time.Millisecond)
	select {
	case rec := <-done:
		t.Fatalf("shard 1 (no member registered) answered %d %s at once; want it to wait within the budget", rec.Code, rec.Body.String())
	default:
	}
	register(1)
	rec := <-done
	if rec.Code != http.StatusOK {
		t.Fatalf("after shard 1 registered: %d %s, want 200", rec.Code, rec.Body.String())
	}
	if len(h.nodes[1][0].requests()) != 1 {
		t.Fatalf("shard 1 leader got %d requests, want 1", len(h.nodes[1][0].requests()))
	}
}

// Members that go DOWN while a call is retrying end it at the next round
// instead of after the budget, and an unknown outcome seen before that
// stays unknown.
func TestMembersGoingDownMidRequestFailFast(t *testing.T) {
	for name, tc := range map[string]struct {
		first func(http.ResponseWriter, *http.Request, []byte)
		want  internalapi.ErrorCode
	}{
		"never proposed":        {failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false), internalapi.NOLEADER},
		"unknown stays unknown": {failWith(http.StatusServiceUnavailable, internalapi.OUTCOMEUNKNOWN, true), internalapi.OUTCOMEUNKNOWN},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(c *coordinator.Config) { c.Budget = 2 * time.Second })
			h.ready()
			var once sync.Once
			for i, n := range h.nodes[1] {
				n.setHandler(func(w http.ResponseWriter, r *http.Request, b []byte) {
					once.Do(func() { h.clock.advance(6 * time.Second) }) // heartbeats stop: every member DOWN
					if i == 0 {
						tc.first(w, r, b)
						return
					}
					failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false)(w, r, b)
				})
			}
			start := time.Now()
			e := expectError(t, h.do(routedCalls(t, h, 1)[4].call), http.StatusServiceUnavailable, tc.want)
			if d := time.Since(start); d > 500*time.Millisecond {
				t.Fatalf("answered after %s; members went DOWN mid-request, want < 500ms of a 2s budget", d)
			}
			if !strings.Contains(e.Message, "DOWN") || e.OutcomeUnknown != (tc.want == internalapi.OUTCOMEUNKNOWN) {
				t.Fatalf("error = %+v", e)
			}
		})
	}
}
