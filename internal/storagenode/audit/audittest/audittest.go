// Package audittest serves fake storage-node management endpoints
// (/mgmt/status and /mgmt/merkle) over digests built from given rows, for
// tests of the S4.5 audit client. It records every /mgmt/merkle query so a
// test can check the audit only ever asked for an exact index.
package audittest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
)

// Token is the operator token every fake pod expects.
const Token = "audit-token"

// Cluster is a set of fake pods sharing one lock.
type Cluster struct {
	mu   sync.Mutex
	Pods []*Pod

	// OnMerkle, if set, runs (under the lock) before each /mgmt/merkle
	// request is answered, so a test can move indexes mid-audit.
	OnMerkle func(c *Cluster, p *Pod)
	// OnStatus, if set, runs (under the lock) before each /mgmt/status.
	OnStatus func(c *Cluster, p *Pod)
}

// Pod is one fake replica. Fields may be changed under Cluster.Lock.
type Pod struct {
	Name             string
	GroupID          string
	Shard            int
	RingVersion      uint64
	SchemaVersion    string
	AppliedIndex     uint64
	RaftAppliedIndex uint64
	Rows             [][]any // records rows, key first, in key order
	Down             bool    // answer 503 to everything
	// Edit, if set, changes a 200 answer before it is sent: kind is
	// "status", "digest" or "bucket", and resp is the JSON object, whose
	// "digest" is a merkle.Digest. Tests use it to send malformed answers.
	Edit func(kind string, resp map[string]any)

	srv     *httptest.Server
	queries []string // raw query of every /mgmt/merkle request
}

// Lock and Unlock guard every pod's fields.
func (c *Cluster) Lock()   { c.mu.Lock() }
func (c *Cluster) Unlock() { c.mu.Unlock() }

// NewCluster starts one fake pod per name, all at applied index 9 with the
// same rows (key:000..key:{n-1}).
func NewCluster(t *testing.T, n int, names ...string) *Cluster {
	t.Helper()
	c := &Cluster{}
	for _, name := range names {
		p := &Pod{Name: name, GroupID: "shard-1", Shard: 1, RingVersion: 1, SchemaVersion: "1",
			AppliedIndex: 9, RaftAppliedIndex: 11, Rows: Rows(n)}
		p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { c.serve(p, w, r) }))
		t.Cleanup(p.srv.Close)
		c.Pods = append(c.Pods, p)
	}
	return c
}

// Rows are n records rows key:000.. with a JSON body.
func Rows(n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{fmt.Sprintf("key:%03d", i), int64(1), fmt.Sprintf(`{"n":%d}`, i)}
	}
	return rows
}

// Addrs are the pods' host:port, in order.
func (c *Cluster) Addrs() []string {
	var out []string
	for _, p := range c.Pods {
		out = append(out, strings.TrimPrefix(p.srv.URL, "http://"))
	}
	return out
}

// Queries are every /mgmt/merkle query string any pod received.
func (c *Cluster) Queries() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, p := range c.Pods {
		out = append(out, p.queries...)
	}
	return out
}

// ExpectExactQueries fails t if any pod was asked for a digest without an
// exact index, or at fsm.AnyIndex: an audit compares only at one known
// command index.
func (c *Cluster) ExpectExactQueries(t *testing.T) {
	t.Helper()
	for _, raw := range c.Queries() {
		q, err := url.ParseQuery(raw)
		if err != nil {
			t.Fatalf("query %q: %v", raw, err)
		}
		if !q.Has("at") {
			t.Fatalf("a digest was asked for without at: %q", raw)
		}
		at, err := strconv.ParseUint(q.Get("at"), 10, 64)
		if err != nil || at == fsm.AnyIndex {
			t.Fatalf("a digest was asked for at=%q (AnyIndex is %d)", q.Get("at"), fsm.AnyIndex)
		}
	}
}

// SetApplied sets every pod's applied index.
func (c *Cluster) SetApplied(i uint64) {
	for _, p := range c.Pods {
		p.AppliedIndex = i
	}
}

func (p *Pod) table(collect int) *merkle.Table {
	tb := merkle.NewTable("records", 1)
	if collect >= 0 {
		tb.Collect(collect)
	}
	for _, r := range p.Rows {
		if err := tb.Add(r); err != nil {
			panic(err)
		}
	}
	return tb
}

func (p *Pod) digest() merkle.Digest {
	empty := func(name string) merkle.TableDigest { return merkle.NewTable(name, 1).Digest() }
	return merkle.NewDigest(p.table(-1).Digest(), empty("operation_results"), empty("attempt_closures"), empty("outbox"))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// reply sends resp as a 200 answer of kind, through Edit if set.
func (p *Pod) reply(w http.ResponseWriter, kind string, resp map[string]any) {
	if p.Edit != nil {
		p.Edit(kind, resp)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (c *Cluster) serve(p *Pod, w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p.Down {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "down"})
		return
	}
	switch r.URL.Path {
	case "/mgmt/status":
		if c.OnStatus != nil {
			c.OnStatus(c, p)
		}
		p.reply(w, "status", map[string]any{
			"podName": p.Name, "groupId": p.GroupID, "shard": p.Shard, "ringVersion": p.RingVersion,
			"appliedIndex": p.AppliedIndex, "raftAppliedIndex": p.RaftAppliedIndex, "caughtUp": true,
		})
	case "/mgmt/merkle":
		if r.Header.Get("Authorization") != "Bearer "+Token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "UNAUTHORIZED"})
			return
		}
		p.queries = append(p.queries, r.URL.RawQuery)
		if c.OnMerkle != nil {
			c.OnMerkle(c, p)
		}
		q := r.URL.Query()
		if q.Has("at") {
			at, err := strconv.ParseUint(q.Get("at"), 10, 64)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad at"})
				return
			}
			if at != p.AppliedIndex {
				writeJSON(w, http.StatusConflict, map[string]any{"error": "mismatch", "code": "INDEX_MISMATCH", "appliedIndex": p.AppliedIndex})
				return
			}
		}
		if q.Has("bucket") {
			b, _ := strconv.Atoi(q.Get("bucket"))
			if q.Get("table") != "records" {
				p.reply(w, "bucket", map[string]any{"podName": p.Name, "appliedIndex": p.AppliedIndex,
					"table": q.Get("table"), "bucket": b, "hash": merkle.BucketHash(nil), "leaves": []merkle.Leaf{}})
				return
			}
			tb := p.table(b)
			leaves := tb.Leaves()
			if leaves == nil {
				leaves = []merkle.Leaf{}
			}
			p.reply(w, "bucket", map[string]any{"podName": p.Name, "ringVersion": p.RingVersion, "appliedIndex": p.AppliedIndex,
				"table": "records", "bucket": b, "hash": tb.Digest().Buckets[b], "leaves": leaves})
			return
		}
		p.reply(w, "digest", map[string]any{
			"podName": p.Name, "ringVersion": p.RingVersion,
			"summary": map[string]any{"schemaVersion": p.SchemaVersion, "appliedIndex": p.AppliedIndex, "tombstones": 0},
			"digest":  p.digest(),
		})
	default:
		http.NotFound(w, r)
	}
}
