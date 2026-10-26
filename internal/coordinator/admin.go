package coordinator

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// operator guards an /admin route with the operator bearer token: 403 if
// none is configured, 401 if it is missing or wrong.
func (c *Coordinator) operator(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if c.cfg.OperatorToken == "" {
			writeError(w, http.StatusForbidden, internalapi.FORBIDDEN, "no operator token is configured")
			return
		}
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(c.cfg.OperatorToken)) != 1 {
			writeError(w, http.StatusUnauthorized, internalapi.UNAUTHORIZED, "operator bearer token required")
			return
		}
		h(w, r)
	}
}

func (c *Coordinator) adminRing(w http.ResponseWriter, _ *http.Request) {
	per := make([]int, c.ring.Shards())
	for _, t := range c.ring.Tokens() {
		per[t.Shard]++
	}
	writeJSON(w, http.StatusOK, internalapi.AdminRing{Ring: c.ringInfo(), TokensPerShard: per})
}

// adminNodes lists every registered node by shard, then pod: the
// coordinator's heartbeat-age state next to the node's own health.
func (c *Coordinator) adminNodes(w http.ResponseWriter, _ *http.Request) {
	now := c.cfg.Now()
	c.mu.Lock()
	out := internalapi.AdminNodes{Nodes: []internalapi.NodeStatus{}}
	for s := range c.ring.Shards() {
		for _, m := range c.members(s) {
			out.Nodes = append(out.Nodes, c.nodeStatus(m, now))
		}
	}
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// adminLocate is a key's logical placement: ring shard and hash, the leader
// the coordinator believes in, and the shard's registered replicas.
func (c *Coordinator) adminLocate(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	shard, err := c.ring.Owner(key)
	if err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, err.Error())
		return
	}
	p := internalapi.Placement{Key: key, Shard: shard, RingVersion: c.ring.Version(),
		Hash: fmt.Sprintf("%016x", xxhash.Sum64String(key)), Replicas: []string{}}
	c.mu.Lock()
	if id, _, ok := c.resolve(shard); ok && id != "" {
		p.LeaderId = &id
	}
	for _, m := range c.members(shard) {
		p.Replicas = append(p.Replicas, m.reg.NodeId)
	}
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, p)
}

// adminDistribution counts each shard once, from the record counts in its
// believed leader's last heartbeat, with that heartbeat's age. A shard with
// no known leader, or a leader that reported no counts, is
// countsKnown=false, never "0 records".
func (c *Coordinator) adminDistribution(w http.ResponseWriter, _ *http.Request) {
	now := c.cfg.Now()
	out := internalapi.Distribution{RingVersion: c.ring.Version()}
	c.mu.Lock()
	for s := range c.ring.Shards() {
		members := c.members(s)
		row := struct {
			Counts         *internalapi.RecordCounts `json:"counts,omitempty"`
			CountsKnown    bool                      `json:"countsKnown"`
			HeartbeatAgeMs *int64                    `json:"heartbeatAgeMs,omitempty"`
			LeaderId       *internalapi.ServerID     `json:"leaderId,omitempty"`
			Records        int                       `json:"records"`
			Replicas       int                       `json:"replicas"`
			Shard          int                       `json:"shard"`
		}{Shard: s, Replicas: len(members)}
		if id, _, ok := c.resolve(s); ok && id != "" {
			row.LeaderId = &id
			if m := c.nodes[c.leaders[s].pod]; m.hb != nil && m.hb.RecordCounts != nil {
				counts := *m.hb.RecordCounts
				age := now.Sub(m.lastSeen).Milliseconds()
				row.Counts, row.CountsKnown, row.HeartbeatAgeMs = &counts, true, &age
				row.Records = counts.Request + counts.Resource + counts.Kv
			}
		}
		out.Shards = append(out.Shards, row)
	}
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}
