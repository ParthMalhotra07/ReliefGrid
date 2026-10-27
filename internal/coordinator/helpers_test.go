package coordinator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	t0        = int64(1_792_800_000_000)
	testToken = "op-secret"
)

var operatorAuth = map[string]string{"Authorization": "Bearer " + testToken}

func loadRing(t *testing.T) *ring.Ring {
	t.Helper()
	rg, err := ring.Load("../../contracts/ring.yaml")
	if err != nil {
		t.Fatalf("load ring: %v", err)
	}
	return rg
}

// keyOn returns prefix+id for the first id "<base>-<n>" the ring places on
// shard, and the id.
func keyOn(t *testing.T, rg *ring.Ring, shard int, prefix, base string) (key, id string) {
	t.Helper()
	for n := range 10_000 {
		id = fmt.Sprintf("%s-%d", base, n)
		owner, err := rg.Owner(prefix + id)
		if err != nil {
			t.Fatal(err)
		}
		if owner == shard {
			return prefix + id, id
		}
	}
	t.Fatalf("no %s key on shard %d", prefix, shard)
	return "", ""
}

// clock is a settable time source for heartbeat ages.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// seen is one request a fake storage node received.
type seen struct {
	method, path, query string
	header              http.Header
	body                []byte
}

// fakeNode is a storage replica: a real HTTP server that records every
// request and answers with handle (default: 200 with a committed OK).
type fakeNode struct {
	t           *testing.T
	pod         string
	shard       int
	incarnation string
	srv         *httptest.Server

	mu     sync.Mutex
	reqs   []seen
	handle func(w http.ResponseWriter, r *http.Request, body []byte)
	// claim is the identity n answers with, as a real storage node does on
	// every data route; nil sends no identity headers.
	claim *identity
}

// identity is the X-ReliefGrid-Server-Id and X-ReliefGrid-Shard a node
// answers with.
type identity struct{ serverID, shard string }

func newFakeNode(t *testing.T, shard, ordinal int) *fakeNode {
	t.Helper()
	n := &fakeNode{t: t, pod: fmt.Sprintf("storage-%d-%d", shard, ordinal), shard: shard,
		incarnation: fmt.Sprintf("00000000-0000-4000-8000-0000000000%d%d", shard, ordinal)}
	n.claim = &identity{serverID: n.serverID(), shard: fmt.Sprint(shard)}
	n.srv = httptest.NewServer(http.HandlerFunc(n.serve))
	t.Cleanup(n.srv.Close)
	return n
}

func (n *fakeNode) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	if strings.HasPrefix(r.URL.Path, "/mgmt") {
		// The coordinator observes; it never bootstraps, promotes or
		// reconfigures a storage node.
		n.t.Errorf("coordinator called %s %s on %s", r.Method, r.URL.Path, n.pod)
	}
	n.mu.Lock()
	n.reqs = append(n.reqs, seen{method: r.Method, path: r.URL.EscapedPath(), query: r.URL.RawQuery, header: r.Header.Clone(), body: body})
	h, claim := n.handle, n.claim
	n.mu.Unlock()
	if claim != nil {
		w.Header().Set(internalapi.HeaderServerID, claim.serverID)
		w.Header().Set(internalapi.HeaderShard, claim.shard)
	}
	if h != nil {
		h(w, r, body)
		return
	}
	writeJSON(w, http.StatusOK, internalapi.MutationResponse{RingVersion: 1,
		Result: store.Result{Outcome: store.OutcomeOK, OperationID: "op", Key: "k", Version: 1, Shard: n.shard, AppliedIndex: 7}})
}

func (n *fakeNode) setHandler(h func(w http.ResponseWriter, r *http.Request, body []byte)) {
	n.mu.Lock()
	n.handle = h
	n.mu.Unlock()
}

// setClaim makes n answer as id (nil: with no identity headers).
func (n *fakeNode) setClaim(id *identity) {
	n.mu.Lock()
	n.claim = id
	n.mu.Unlock()
}

func (n *fakeNode) requests() []seen {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]seen(nil), n.reqs...)
}

func (n *fakeNode) host() string     { return strings.TrimPrefix(n.srv.URL, "http://") }
func (n *fakeNode) serverID() string { return n.pod + "/" + n.incarnation }

func (n *fakeNode) registration() internalapi.NodeRegistration {
	return internalapi.NodeRegistration{NodeId: n.pod, GroupId: fmt.Sprintf("shard-%d", n.shard), ShardId: n.shard,
		Incarnation: n.incarnation, Address: n.host(), ProtocolVersion: 1, SchemaVersion: 1}
}

// heartbeat is n's heartbeat naming leader (nil: no leader known) at term.
func (n *fakeNode) heartbeat(leader *fakeNode, term uint64) internalapi.Heartbeat {
	hb := internalapi.Heartbeat{NodeId: n.pod, GroupId: fmt.Sprintf("shard-%d", n.shard), ShardId: n.shard,
		Incarnation: n.incarnation, Health: internalapi.HealthOk, Term: term, AppliedIndex: 10, CommitIndex: 10}
	if leader != nil {
		id := leader.serverID()
		hb.LeaderId = &id
	}
	return hb
}

func notLeader(hint *fakeNode, term uint64) func(http.ResponseWriter, *http.Request, []byte) {
	return func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSON(w, http.StatusServiceUnavailable, internalapi.Error{Code: internalapi.NOTLEADER, Message: "not leader", Retryable: true,
			LeaderHint: &internalapi.LeaderHint{NodeId: hint.pod, Address: hint.host(), Term: &term}})
	}
}

func failWith(status int, code internalapi.ErrorCode, unknown bool) func(http.ResponseWriter, *http.Request, []byte) {
	return func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, status, internalapi.Error{Code: code, Message: string(code), Retryable: true, OutcomeUnknown: unknown})
	}
}

// dropConnection reads the request and closes the connection without a
// response: the command may have been proposed.
func dropConnection(w http.ResponseWriter, _ *http.Request, _ []byte) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// harness is a coordinator with fake storage nodes, 3 per shard.
type harness struct {
	t     *testing.T
	rg    *ring.Ring
	clock *clock
	c     *coordinator.Coordinator
	h     http.Handler
	nodes [][]*fakeNode // [shard][ordinal]
}

func newHarness(t *testing.T, mod func(*coordinator.Config)) *harness {
	t.Helper()
	rg := loadRing(t)
	clk := &clock{now: time.UnixMilli(t0)}
	cfg := coordinator.Config{Ring: rg, OperatorToken: testToken, Now: clk.Now,
		Budget: time.Second, AttemptTimeout: 500 * time.Millisecond}
	if mod != nil {
		mod(&cfg)
	}
	// A test that shortens Budget below the default attempt keeps one
	// attempt within the budget, as New requires.
	cfg.AttemptTimeout = min(cfg.AttemptTimeout, cfg.Budget)
	c, err := coordinator.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &harness{t: t, rg: rg, clock: clk, c: c, h: c.Handler()}
	for s := range rg.Shards() {
		var row []*fakeNode
		for i := range 3 {
			row = append(row, newFakeNode(t, s, i))
		}
		h.nodes = append(h.nodes, row)
	}
	return h
}

// registerAll registers every fake node.
func (h *harness) registerAll() {
	h.t.Helper()
	for _, row := range h.nodes {
		for _, n := range row {
			if rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/register", body: n.registration()}); rec.Code != http.StatusOK {
				h.t.Fatalf("register %s: %d %s", n.pod, rec.Code, rec.Body.String())
			}
		}
	}
}

// elect has every member of shard heartbeat that leader leads at term.
func (h *harness) elect(shard, leader int, term uint64) {
	h.t.Helper()
	l := h.nodes[shard][leader]
	for _, n := range h.nodes[shard] {
		if rec := h.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: n.heartbeat(l, term)}); rec.Code != http.StatusOK {
			h.t.Fatalf("heartbeat %s: %d %s", n.pod, rec.Code, rec.Body.String())
		}
	}
}

// ready registers every node and elects ordinal 0 of every shard at term 1.
func (h *harness) ready() {
	h.t.Helper()
	h.registerAll()
	for s := range h.nodes {
		h.elect(s, 0, 1)
	}
}

func (h *harness) allRequests() int {
	total := 0
	for _, row := range h.nodes {
		for _, n := range row {
			total += len(n.requests())
		}
	}
	return total
}

type call struct {
	method, path string
	header       map[string]string
	body         any // marshalled unless it is a string or []byte
}

func (h *harness) do(c call) *httptest.ResponseRecorder {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rec := httptest.NewRecorder()
	h.h.ServeHTTP(rec, h.request(ctx, c))
	return rec
}

// doAsync serves c in another goroutine; the answer arrives on the channel.
func (h *harness) doAsync(c call) <-chan *httptest.ResponseRecorder {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	req := h.request(ctx, c)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		defer cancel()
		rec := httptest.NewRecorder()
		h.h.ServeHTTP(rec, req)
		done <- rec
	}()
	return done
}

func (h *harness) request(ctx context.Context, c call) *http.Request {
	h.t.Helper()
	var body io.Reader
	switch b := c.body.(type) {
	case nil:
	case string:
		body = strings.NewReader(b)
	case []byte:
		body = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			h.t.Fatal(err)
		}
		body = bytes.NewReader(raw)
	}
	req := httptest.NewRequestWithContext(ctx, c.method, c.path, body)
	for k, v := range c.header {
		req.Header.Set(k, v)
	}
	return req
}

func decodeJSON[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %T from %d %q: %v", v, rec.Code, rec.Body.String(), err)
	}
	return v
}

func expectError(t *testing.T, rec *httptest.ResponseRecorder, status int, code internalapi.ErrorCode) internalapi.Error {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d (%s), want %d %s", rec.Code, rec.Body.String(), status, code)
	}
	e := decodeJSON[internalapi.Error](t, rec)
	if e.Code != code {
		t.Fatalf("code = %s (%s), want %s", e.Code, e.Message, code)
	}
	return e
}
