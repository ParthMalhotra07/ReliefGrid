package coordinator

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// Identity patterns from contracts/internal.yaml.
var (
	podNameRE     = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	incarnationRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// protocolVersion is the internal API version this coordinator speaks.
const protocolVersion = 1

// member is one registered storage node and what it last reported.
type member struct {
	reg      internalapi.NodeRegistration
	lastSeen time.Time              // last heartbeat, or registration if none yet
	hb       *internalapi.Heartbeat // nil until the first heartbeat
}

func (m *member) serverID() string { return m.reg.NodeId + "/" + m.reg.Incarnation }

// leader is the coordinator's belief about one shard's leader: a routing
// hint from heartbeats and NOT_LEADER answers, never authority.
type leader struct {
	term     uint64
	pod      string // "" if no leader is known
	serverID string // from a heartbeat; "" when learned from a NOT_LEADER hint
	addr     string // a NOT_LEADER hint's address, used only if pod is not registered
}

// state is m's heartbeat-age state (plan §15): an observation for routing
// order and the admin view only.
func (c *Coordinator) state(m *member, now time.Time) internalapi.NodeState {
	switch age := now.Sub(m.lastSeen); {
	case age >= c.cfg.DownAfter:
		return internalapi.DOWN
	case age >= c.cfg.SuspectAfter:
		return internalapi.SUSPECT
	}
	return internalapi.UP
}

func (c *Coordinator) checkIdentity(pod, group string, shard int, incarnation string) error {
	switch {
	case !podNameRE.MatchString(pod):
		return fmt.Errorf("nodeId %q is not a pod name", pod)
	case shard < 0 || shard >= c.ring.Shards():
		return fmt.Errorf("shardId %d is outside 0..%d", shard, c.ring.Shards()-1)
	case group != fmt.Sprintf("shard-%d", shard):
		return fmt.Errorf("groupId %q does not match shardId %d", group, shard)
	case !incarnationRE.MatchString(incarnation):
		return fmt.Errorf("incarnation %q is not a UUID", incarnation)
	}
	return nil
}

// register records a storage node and answers with the ring and the node's
// shard peers. It never calls the node: registration does not bootstrap,
// add a voter or move a key (M7).
func (c *Coordinator) register(w http.ResponseWriter, r *http.Request) {
	var reg internalapi.NodeRegistration
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&reg); err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "registration: "+err.Error())
		return
	}
	if err := c.checkIdentity(reg.NodeId, reg.GroupId, reg.ShardId, reg.Incarnation); err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, err.Error())
		return
	}
	if _, _, err := net.SplitHostPort(reg.Address); err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "address must be host:port")
		return
	}
	resp := internalapi.RegisterResponse{Ring: c.ringInfo(), Peers: []internalapi.Peer{}}
	if reg.ProtocolVersion != protocolVersion {
		reason := fmt.Sprintf("protocolVersion %d is not %d", reg.ProtocolVersion, protocolVersion)
		resp.Reason = &reason
		writeJSON(w, http.StatusOK, resp)
		return
	}

	now := c.cfg.Now()
	c.mu.Lock()
	if m := c.nodes[reg.NodeId]; m != nil && m.reg.Incarnation == reg.Incarnation && m.reg.ShardId == reg.ShardId {
		m.reg = reg // same disk; the address may have changed
	} else {
		c.nodes[reg.NodeId] = &member{reg: reg, lastSeen: now}
	}
	for _, m := range c.members(reg.ShardId) {
		if m.reg.NodeId != reg.NodeId {
			resp.Peers = append(resp.Peers, internalapi.Peer{NodeId: m.reg.NodeId, Address: m.reg.Address})
		}
	}
	c.mu.Unlock()

	resp.Accepted = true
	// Admission is not enforced until S4.2 (M7): every registration is
	// reported admitted. Registration still never bootstraps or adds a
	// voter; an empty disk joins Raft only through storagectl replace.
	resp.Admitted = true
	writeJSON(w, http.StatusOK, resp)
}

// heartbeat records a node's report and answers with every shard's leader
// hint. A node that is not registered with this incarnation gets 404 and
// must register again.
func (c *Coordinator) heartbeat(w http.ResponseWriter, r *http.Request) {
	var hb internalapi.Heartbeat
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&hb); err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "heartbeat: "+err.Error())
		return
	}
	if err := c.checkIdentity(hb.NodeId, hb.GroupId, hb.ShardId, hb.Incarnation); err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, err.Error())
		return
	}
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.nodes[hb.NodeId]
	if m == nil || m.reg.Incarnation != hb.Incarnation || m.reg.ShardId != hb.ShardId {
		writeError(w, http.StatusNotFound, internalapi.NOTFOUND, hb.NodeId+" is not registered with this incarnation")
		return
	}
	m.hb, m.lastSeen = &hb, now
	if hb.LeaderId != nil {
		pod, _, _ := strings.Cut(*hb.LeaderId, "/")
		c.learn(hb.ShardId, hb.Term, leader{pod: pod, serverID: *hb.LeaderId})
	} else {
		c.learn(hb.ShardId, hb.Term, leader{})
	}

	resp := internalapi.HeartbeatResponse{RingVersion: c.ring.Version(), Checksum: c.ring.Checksum(), Leaders: []internalapi.ShardLeader{}}
	for s := range c.leaders {
		if id, addr, ok := c.resolve(s); ok && id != "" {
			resp.Leaders = append(resp.Leaders, internalapi.ShardLeader{Shard: s, LeaderId: id, Address: addr, Term: c.leaders[s].term})
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// learn records l as shard's leader at term. A lower term never replaces a
// higher one; a report of no leader clears the hint only at a higher term
// (a new election), and at the same term a known leader is kept. Callers
// hold c.mu.
func (c *Coordinator) learn(shard int, term uint64, l leader) {
	cur := &c.leaders[shard]
	switch {
	case term < cur.term:
	case l.pod == "":
		if term > cur.term {
			*cur = leader{term: term}
		}
	case term > cur.term || cur.pod == "" || cur.pod == l.pod:
		if term == cur.term && cur.pod == l.pod && l.serverID == "" {
			l.serverID = cur.serverID // a hint confirms the leader; keep its ServerID
		}
		l.term = term
		*cur = l
	}
}

// resolve returns shard's believed leader: its ServerID ("" if only a
// NOT_LEADER hint named it) and address. The address is the registered one;
// a leader that is neither registered nor hinted with an address is
// unknown. Callers hold c.mu.
func (c *Coordinator) resolve(shard int) (serverID, addr string, ok bool) {
	l := c.leaders[shard]
	if l.pod == "" {
		return "", "", false
	}
	if m := c.nodes[l.pod]; m != nil {
		if m.reg.ShardId != shard || l.serverID != "" && l.serverID != m.serverID() {
			return "", "", false // another disk or shard: the report is about a node we do not know
		}
		return m.serverID(), m.reg.Address, true
	}
	if l.serverID == "" && l.addr != "" {
		return "", l.addr, true
	}
	return "", "", false
}

// members returns shard's registered nodes ordered by pod name. Callers
// hold c.mu.
func (c *Coordinator) members(shard int) []*member {
	var out []*member
	for _, m := range c.nodes {
		if m.reg.ShardId == shard {
			out = append(out, m)
		}
	}
	slices.SortFunc(out, func(a, b *member) int { return cmp.Compare(a.reg.NodeId, b.reg.NodeId) })
	return out
}

func (c *Coordinator) nodeStatus(m *member, now time.Time) internalapi.NodeStatus {
	st := internalapi.NodeStatus{NodeId: m.reg.NodeId, GroupId: m.reg.GroupId, ShardId: m.reg.ShardId, Incarnation: m.reg.Incarnation,
		Address: m.reg.Address, Admitted: true, State: c.state(m, now), Health: internalapi.HealthUnavailable}
	if m.hb != nil {
		st.Health, st.Term, st.LeaderId = m.hb.Health, m.hb.Term, m.hb.LeaderId
		st.AppliedIndex, st.CommitIndex = m.hb.AppliedIndex, m.hb.CommitIndex
		st.LastHeartbeatAt = m.lastSeen.UnixMilli()
	}
	return st
}
