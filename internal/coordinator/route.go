package coordinator

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Backoff between rounds of attempts, once every candidate has been tried.
// Each wait is jittered over [backoff/2, backoff].
const (
	minBackoff = 20 * time.Millisecond
	maxBackoff = 200 * time.Millisecond
)

// jitter returns a random duration in [d/2, d], so concurrent calls to one
// shard do not retry in lockstep. The coordinator is outside the FSM, so
// randomness here does not touch determinism.
func jitter(d time.Duration) time.Duration { return d/2 + rand.N(d/2+1) }

// keyFunc extracts the routing key of a request; ok=false means it has none.
type keyFunc func(r *http.Request) (key string, ok bool)

func pathKey(r *http.Request) (string, bool)     { return r.PathValue("key"), true }
func requestKey(r *http.Request) (string, bool)  { return store.RequestKey(r.PathValue("id")), true }
func resourceKey(r *http.Request) (string, bool) { return store.ResourceKey(r.PathValue("id")), true }
func queryKey(r *http.Request) (string, bool) {
	k := r.URL.Query().Get("key")
	return k, k != ""
}

// byKey routes a request to the shard owning its key.
func (c *Coordinator) byKey(key keyFunc, mutation bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		k, ok := key(r)
		if !ok {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, "key is required")
			return
		}
		shard, err := c.ring.Owner(k)
		if err != nil {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, err.Error())
			return
		}
		c.routed(w, r, shard, mutation)
	}
}

// byShard routes an outbox request to the shard in ?shard=.
func (c *Coordinator) byShard(mutation bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		shard, err := strconv.Atoi(r.URL.Query().Get("shard"))
		if err != nil || shard < 0 || shard >= c.ring.Shards() {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("shard must be 0 to %d", c.ring.Shards()-1))
			return
		}
		c.routed(w, r, shard, mutation)
	}
}

// routed checks the caller's ringVersion, forwards r to shard's leader and
// writes the leader's answer unchanged, or the coordinator's 503.
func (c *Coordinator) routed(w http.ResponseWriter, r *http.Request, shard int, mutation bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "body: "+err.Error())
		return
	}
	if !c.checkRing(w, r, body) {
		return
	}
	ans, unknown, why := c.forward(r.Context(), shard, r, body, mutation)
	if ans == nil {
		slog.Warn("routing budget exhausted", "method", r.Method, "path", r.URL.Path, "shard", shard, "unknown", unknown, "last", why)
		writeUnavailable(w, unknown, fmt.Sprintf("shard %d: no leader answered within %s (last: %s)", shard, c.cfg.Budget, why))
		return
	}
	h := w.Header()
	for k, v := range forwardHeader(ans.header) {
		h[k] = v
	}
	w.WriteHeader(ans.status)
	if _, err := w.Write(ans.body); err != nil {
		slog.Debug("write forwarded answer", "err", err)
	}
}

// checkRing refuses a request whose ringVersion (query, or a JSON body's
// ringVersion) is not the coordinator's, before anything is forwarded.
func (c *Coordinator) checkRing(w http.ResponseWriter, r *http.Request, body []byte) bool {
	var got *uint64
	if v := r.URL.Query().Get("ringVersion"); v != "" {
		rv, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, "ringVersion must be an unsigned integer")
			return false
		}
		got = &rv
	}
	if len(bytes.TrimSpace(body)) > 0 {
		var b struct {
			RingVersion *uint64 `json:"ringVersion"`
		}
		if err := json.Unmarshal(body, &b); err != nil {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, "body is not a JSON command: "+err.Error())
			return false
		}
		if b.RingVersion != nil {
			got = b.RingVersion
		}
	}
	if got != nil && *got != c.ring.Version() {
		writeRingMismatch(w, c.ring.Version(), *got)
		return false
	}
	return true
}

// target is an address to dial and the server that must answer there: a
// registered member's ServerID, a NOT_LEADER hint's serverId, or only the
// pod a hint named when it gave no serverId.
type target struct {
	addr     string
	serverID string // podName/incarnation; "" if only pod is known
	pod      string
}

// errWrongShard marks an answer from a node of another shard: it cannot
// have proposed anything in this one.
var errWrongShard = errors.New("answered as another shard")

// verify checks that an answer came from t and shard (issue #46): an
// address is not an identity, since a freed port or a reused pod IP can be
// answered by another process. errWrongShard means nothing was proposed
// here; any other error is an answer from an unknown server of this shard,
// or one that did not say who it is, which may have proposed.
func verify(h http.Header, t target, shard int) error {
	sid, sh := h.Get(internalapi.HeaderServerID), h.Get(internalapi.HeaderShard)
	if sid == "" || sh == "" {
		return errors.New("answer has no server identity")
	}
	got, err := strconv.Atoi(sh)
	if err != nil {
		return fmt.Errorf("answer names shard %q", sh)
	}
	if got != shard {
		return fmt.Errorf("%w %d", errWrongShard, got)
	}
	pod, _, ok := strings.Cut(sid, "/")
	if !ok || t.serverID != "" && sid != t.serverID || pod != t.pod {
		return fmt.Errorf("answered as %s, want %s", sid, cmp.Or(t.serverID, t.pod))
	}
	return nil
}

// answer is a storage node's response, buffered.
type answer struct {
	status int
	header http.Header
	body   []byte
}

// forward sends r (with body) to shard's leader within the budget and
// returns the first answer that is not a routing failure: a committed
// outcome, a rejection, a validation error or the node's own
// RING_VERSION_MISMATCH. It follows NOT_LEADER hints, then the shard's
// members (UP, then SUSPECT, then DOWN), in rounds with backoff. An answer
// that does not name the server dialled is a failed attempt (verify), and
// that address is not dialled again: once every candidate has answered as
// another server the call ends without waiting out the budget.
//
// With no answer it returns unknown=true if r is a mutation and any attempt
// may have been proposed: headers were written and then the connection
// failed, timed out, the node answered OUTCOME_UNKNOWN or another 5xx, or
// an answer came from an unverified server of this shard.
// That stays true whatever later attempts say (CONTRIBUTING.md,
// Invariants, 5). A read is
// never unknown.
//
// A shard whose every registered member is DOWN by heartbeat age fails at
// once, at the start or between rounds, instead of waiting out the budget;
// one member UP or SUSPECT keeps the full budget. A shard with no
// registered member (a restarted coordinator) waits for registrations.
func (c *Coordinator) forward(ctx context.Context, shard int, r *http.Request, body []byte, mutation bool) (ans *answer, unknown bool, why string) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Budget)
	defer cancel()
	why = "no storage node registered"
	tried := map[string]bool{} // by address
	wrong := map[string]bool{} // addresses that answered as another server: not dialled again
	var next target            // a NOT_LEADER hint to follow first
	backoff := minBackoff
	if c.allDown(shard) {
		return nil, false, fmt.Sprintf("every member of shard %d is DOWN (no heartbeat for %s)", shard, c.cfg.DownAfter)
	}
	for ctx.Err() == nil {
		t := next
		next = target{}
		if t.addr == "" || tried[t.addr] {
			t = c.pick(shard, tried)
		}
		if t.addr == "" { // every candidate tried this round
			clear(tried)
			maps.Copy(tried, wrong)
			if len(wrong) > 0 && c.pick(shard, tried).addr == "" {
				return nil, unknown, fmt.Sprintf("every member of shard %d answered as another server (last: %s)", shard, why)
			}
			select {
			case <-ctx.Done():
			case <-time.After(jitter(backoff)):
			}
			backoff = min(2*backoff, maxBackoff)
			if c.allDown(shard) {
				return nil, unknown, fmt.Sprintf("every member of shard %d is DOWN (last: %s)", shard, why)
			}
			continue
		}
		addr := t.addr
		tried[addr] = true

		a, sent, err := c.attempt(ctx, addr, r, body)
		if err != nil {
			unknown = unknown || mutation && sent
			why = fmt.Sprintf("%s: %v", addr, err)
			continue
		}
		if err := verify(a.header, t, shard); err != nil {
			slog.Warn("discarding an answer from an unexpected server", "addr", addr, "shard", shard, "status", a.status, "err", err)
			unknown = unknown || mutation && !errors.Is(err, errWrongShard)
			wrong[addr] = true
			why = fmt.Sprintf("%s: %v", addr, err)
			continue
		}
		if a.status < 500 {
			return a, false, ""
		}
		var e internalapi.Error
		_ = json.Unmarshal(a.body, &e)
		why = fmt.Sprintf("%s: %d %s %s", addr, a.status, e.Code, e.Message)
		switch e.Code {
		case internalapi.NOTLEADER:
			if e.LeaderHint != nil {
				next = c.hint(shard, *e.LeaderHint)
			}
		case internalapi.NOLEADER, internalapi.STORAGEUNAVAILABLE:
			// Never proposed: try elsewhere.
		default: // OUTCOME_UNKNOWN, or a 5xx with no known code
			unknown = unknown || mutation
		}
	}
	return nil, unknown, why
}

// attempt sends one forward, bounded by AttemptTimeout. sent reports
// whether the request headers were written: if not, the node cannot have
// proposed anything.
func (c *Coordinator) attempt(ctx context.Context, addr string, r *http.Request, body []byte) (a *answer, sent bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
	defer cancel()
	var wrote atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteHeaders: func() { wrote.Store(true) }})
	u := "http://" + addr + r.URL.EscapedPath()
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	var rd io.Reader = http.NoBody
	if len(body) > 0 {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, u, rd)
	if err != nil {
		return nil, false, err
	}
	req.Header = forwardHeader(r.Header)
	resp, err := c.cfg.Client.Do(req)
	if err != nil {
		return nil, wrote.Load(), err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, true, fmt.Errorf("read answer: %w", err)
	}
	return &answer{status: resp.StatusCode, header: resp.Header, body: b}, true, nil
}

// pick returns the next target to try for shard, or one with no address
// once every candidate is in tried: the believed leader unless it is DOWN,
// then the members UP before SUSPECT before DOWN.
func (c *Coordinator) pick(shard int, tried map[string]bool) target {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	leaderID, leaderAddr, hasLeader := c.resolve(shard)
	members := c.members(shard)
	if hasLeader && !tried[leaderAddr] {
		down := false
		for _, m := range members {
			if m.reg.Address == leaderAddr {
				down = c.state(m, now) == internalapi.DOWN
			}
		}
		if !down {
			l := c.leaders[shard]
			return target{addr: leaderAddr, serverID: cmp.Or(l.hintID, leaderID), pod: l.pod}
		}
	}
	for _, want := range []internalapi.NodeState{internalapi.UP, internalapi.SUSPECT, internalapi.DOWN} {
		for _, m := range members {
			if !tried[m.reg.Address] && c.state(m, now) == want {
				return target{addr: m.reg.Address, serverID: m.serverID(), pod: m.reg.NodeId}
			}
		}
	}
	return target{}
}

// hint learns a NOT_LEADER hint and returns the target to try next: the
// hinted pod as registered, else the hint's address. The answer must come
// from the hint's serverId, which Raft reports and so outranks the
// registration; a hint without one (or with one for another pod) accepts
// any incarnation of the pod. A hint may name a pod that has not registered
// yet (it just became leader of a restarted coordinator's table). A hint
// with no pod is not followed.
func (c *Coordinator) hint(shard int, h internalapi.LeaderHint) target {
	if h.NodeId == "" {
		return target{}
	}
	var hintID string
	if h.ServerId != nil && strings.HasPrefix(*h.ServerId, h.NodeId+"/") {
		hintID = *h.ServerId
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	term := c.leaders[shard].term
	if h.Term != nil {
		term = *h.Term
	}
	c.learn(shard, term, leader{pod: h.NodeId, addr: h.Address, hintID: hintID})
	if m := c.nodes[h.NodeId]; m != nil && m.reg.ShardId == shard {
		return target{addr: m.reg.Address, serverID: cmp.Or(hintID, m.serverID()), pod: m.reg.NodeId}
	}
	return target{addr: h.Address, serverID: hintID, pod: h.NodeId}
}

// allDown reports whether shard has registered members and every one is
// DOWN.
func (c *Coordinator) allDown(shard int) bool {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	members := c.members(shard)
	for _, m := range members {
		if c.state(m, now) != internalapi.DOWN {
			return false
		}
	}
	return len(members) > 0
}
