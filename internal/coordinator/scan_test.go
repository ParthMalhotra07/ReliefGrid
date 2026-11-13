package coordinator_test

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const scanPath = "/internal/scan?kind=resource&type=WATER&statuses=AVAILABLE&statuses=OFFLINE&geohashPrefixes=ttn&geohashPrefixes=ttp&limit=50&consistency=stale"

// scanAnswer makes every member of shard answer a scan with two records.
func scanAnswer(h *harness, shard int) {
	for _, n := range h.nodes[shard] {
		n.setHandler(func(w http.ResponseWriter, r *http.Request, _ []byte) {
			writeJSON(w, http.StatusOK, internalapi.ScanResponse{
				RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStale,
				Records: []store.Record{
					{Key: fmt.Sprintf("res:s%d-a", shard), Kind: store.KindResource, Version: 1},
					{Key: fmt.Sprintf("res:s%d-b", shard), Kind: store.KindResource, Version: 2},
				},
				Positions: []store.ShardPosition{{Shard: shard, AppliedIndex: uint64(100 + shard)}},
			})
		})
	}
}

func scanKeys(resp internalapi.ScanResponse) []string {
	var keys []string
	for _, r := range resp.Records {
		keys = append(keys, r.Key)
	}
	return keys
}

// Every shard is asked the same query once, through its leader; the answer
// merges records in shard order and has one position per shard.
func TestScanGathersEveryShard(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	for s := range 3 {
		scanAnswer(h, s)
	}
	rec := h.do(call{method: http.MethodGet, path: scanPath})
	if rec.Code != http.StatusOK {
		t.Fatalf("scan: %d %s", rec.Code, rec.Body.String())
	}
	resp := decodeJSON[internalapi.ScanResponse](t, rec)
	want := []string{"res:s0-a", "res:s0-b", "res:s1-a", "res:s1-b", "res:s2-a", "res:s2-b"}
	if resp.Partial || resp.FailedShards != nil && len(*resp.FailedShards) != 0 || !slices.Equal(scanKeys(resp), want) ||
		len(resp.Positions) != 3 || resp.RingVersion != 1 || resp.Consistency != internalapi.ScanResponseConsistencyStale {
		t.Fatalf("scan = %+v", resp)
	}
	for s, row := range h.nodes {
		got := row[0].requests()
		if len(got) != 1 || got[0].path != "/internal/scan" {
			t.Fatalf("shard %d leader got %+v", s, got)
		}
		if q := got[0].query; q != scanPath[len("/internal/scan?"):] {
			t.Fatalf("shard %d query %q, want the caller's", s, q)
		}
		for _, n := range row[1:] {
			if len(n.requests()) != 0 {
				t.Fatalf("follower %s scanned", n.pod)
			}
		}
	}
}

// Verify (S1.4), CONTRIBUTING.md, Invariants, 7: a failed or unreachable shard makes the scan
// partial with failedShards, never an empty or complete result; the other
// shards' records and positions are still returned.
func TestScanWithAShardDownIsPartial(t *testing.T) {
	for name, fail := range map[string]func(h *harness){
		"no leader": func(h *harness) {
			for _, n := range h.nodes[1] {
				n.setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))
			}
		},
		"unreachable": func(h *harness) {
			for _, n := range h.nodes[1] {
				n.srv.Close()
			}
		},
		"storage failure": func(h *harness) {
			for _, n := range h.nodes[1] {
				n.setHandler(failWith(http.StatusServiceUnavailable, internalapi.STORAGEUNAVAILABLE, false))
			}
		},
		"garbled answer": func(h *harness) {
			for _, n := range h.nodes[1] {
				n.setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) { writeJSON(w, http.StatusOK, "nonsense") })
			}
		},
		"ring mismatch": func(h *harness) {
			two := uint64(2)
			for _, n := range h.nodes[1] {
				n.setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
					writeJSON(w, http.StatusConflict, internalapi.Error{Code: internalapi.RINGVERSIONMISMATCH, RingVersion: &two})
				})
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nil)
			h.ready()
			scanAnswer(h, 0)
			scanAnswer(h, 2)
			fail(h)
			rec := h.do(call{method: http.MethodGet, path: scanPath})
			if rec.Code != http.StatusOK {
				t.Fatalf("scan: %d %s", rec.Code, rec.Body.String())
			}
			resp := decodeJSON[internalapi.ScanResponse](t, rec)
			if !resp.Partial || resp.FailedShards == nil || !slices.Equal(*resp.FailedShards, []int{1}) {
				t.Fatalf("partial=%v failedShards=%v, want true [1]", resp.Partial, resp.FailedShards)
			}
			if want := []string{"res:s0-a", "res:s0-b", "res:s2-a", "res:s2-b"}; !slices.Equal(scanKeys(resp), want) {
				t.Fatalf("records = %v, want %v", scanKeys(resp), want)
			}
			var shards []int
			for _, p := range resp.Positions {
				shards = append(shards, p.Shard)
			}
			if !slices.Equal(shards, []int{0, 2}) {
				t.Fatalf("positions for shards %v, want [0 2]", shards)
			}
		})
	}

	// No shard answered: there is no partial result to return, so the scan
	// is 503 NO_LEADER (retryable, nothing was read), not a 200.
	t.Run("every shard down", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.ready()
		for _, row := range h.nodes {
			for _, n := range row {
				n.srv.Close()
			}
		}
		rec := h.do(call{method: http.MethodGet, path: scanPath})
		e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.NOLEADER)
		if !e.Retryable || e.OutcomeUnknown || rec.Header().Get("Retry-After") == "" {
			t.Fatalf("scan with no shard = %+v (Retry-After %q), want retryable NO_LEADER", e, rec.Header().Get("Retry-After"))
		}
	})
}

// hasMore marks a shard whose page came back full: the shard said so, or it
// returned exactly limit records. Other shards' positions do not have it.
func TestScanPropagatesHasMore(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	answer := func(shard, records int, hasMore bool) {
		for _, n := range h.nodes[shard] {
			n.setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				resp := internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStrong,
					Records: []store.Record{}, Positions: []store.ShardPosition{{Shard: shard, AppliedIndex: 5, HasMore: hasMore}}}
				for i := range records {
					resp.Records = append(resp.Records, store.Record{Key: fmt.Sprintf("kv:s%d-%d", shard, i), Kind: store.KindKV, Version: 1})
				}
				writeJSON(w, http.StatusOK, resp)
			})
		}
	}
	answer(0, 3, false) // full page, not flagged by the shard
	answer(1, 1, true)  // flagged by the shard
	answer(2, 2, false) // short page
	rec := h.do(call{method: http.MethodGet, path: "/internal/scan?kind=kv&limit=3"})
	if rec.Code != http.StatusOK {
		t.Fatalf("scan: %d %s", rec.Code, rec.Body.String())
	}
	resp := decodeJSON[internalapi.ScanResponse](t, rec)
	got := map[int]bool{}
	for _, p := range resp.Positions {
		got[p.Shard] = p.HasMore
	}
	if want := map[int]bool{0: true, 1: true, 2: false}; len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("hasMore by shard = %v, want %v", got, want)
	}
	if resp.Partial || len(resp.Records) != 6 {
		t.Fatalf("scan = %+v", resp)
	}
}

// A scan's own input is checked once, before any shard is asked.
func TestScanRejectsBadInputBeforeForwarding(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	for _, path := range []string{
		"/internal/scan?kind=resource",                  // no limit
		"/internal/scan?kind=resource&limit=0",          // below 1
		"/internal/scan?kind=resource&limit=1001",       // above 1000
		"/internal/scan?limit=10",                       // no kind
		"/internal/scan?kind=thing&limit=10",            // bad kind
		"/internal/scan?kind=kv&limit=10&consistency=x", // bad mode
	} {
		expectError(t, h.do(call{method: http.MethodGet, path: path}), http.StatusBadRequest, internalapi.INVALID)
	}
	e := expectError(t, h.do(call{method: http.MethodGet, path: "/internal/scan?kind=kv&limit=10&ringVersion=2"}), http.StatusConflict, internalapi.RINGVERSIONMISMATCH)
	if e.RingVersion == nil || *e.RingVersion != 1 {
		t.Fatalf("mismatch = %+v", e)
	}
	if n := h.allRequests(); n != 0 {
		t.Fatalf("%d scans forwarded", n)
	}
}

// A shard whose every member is DOWN by heartbeat age fails at once: the
// scan is partial within 500 ms (not after the 2 s budget) and nothing is
// sent to it; a mutation to it is 503 NO_LEADER at once. A shard with any
// member UP or SUSPECT still gets the full budget.
func TestShardAllDownFailsFast(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) { c.Budget = 2 * time.Second })
	h.ready()
	h.clock.advance(6 * time.Second) // every node DOWN
	for _, s := range []int{0, 2} {
		h.elect(s, 0, 1) // shards 0 and 2 UP again; shard 1 stays DOWN
		scanAnswer(h, s)
	}
	for _, n := range h.nodes[1] {
		n.setHandler(func(w http.ResponseWriter, r *http.Request, _ []byte) { <-r.Context().Done() }) // would hang
	}

	start := time.Now()
	rec := h.do(call{method: http.MethodGet, path: scanPath})
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("scan with an all-DOWN shard took %s, want < 500ms", d)
	}
	resp := decodeJSON[internalapi.ScanResponse](t, rec)
	if rec.Code != http.StatusOK || !resp.Partial || resp.FailedShards == nil || !slices.Equal(*resp.FailedShards, []int{1}) || len(resp.Records) != 4 {
		t.Fatalf("scan = %d %+v, want partial [1] with shards 0 and 2", rec.Code, resp)
	}

	start = time.Now()
	e := expectError(t, h.do(routedCalls(t, h, 1)[0].call), http.StatusServiceUnavailable, internalapi.NOLEADER)
	if d := time.Since(start); d > 500*time.Millisecond || e.OutcomeUnknown || !e.Retryable {
		t.Fatalf("mutation to an all-DOWN shard: %+v after %s", e, d)
	}
	for _, n := range h.nodes[1] {
		if got := len(n.requests()); got != 0 {
			t.Fatalf("%s (DOWN) received %d requests", n.pod, got)
		}
	}

	// One member SUSPECT: the shard is tried, and the budget applies.
	h2 := newHarness(t, func(c *coordinator.Config) { c.Budget = 600 * time.Millisecond })
	h2.ready()
	h2.clock.advance(6 * time.Second)
	for _, s := range []int{0, 2} {
		h2.elect(s, 0, 1)
		scanAnswer(h2, s)
	}
	h2.do(call{method: http.MethodPost, path: "/internal/nodes/heartbeat", body: h2.nodes[1][2].heartbeat(nil, 1)})
	h2.clock.advance(3500 * time.Millisecond) // storage-1-2 SUSPECT, the others DOWN; shards 0 and 2 SUSPECT
	for _, n := range h2.nodes[1] {
		n.setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))
	}
	start = time.Now()
	resp = decodeJSON[internalapi.ScanResponse](t, h2.do(call{method: http.MethodGet, path: scanPath}))
	if d := time.Since(start); d < 500*time.Millisecond || !resp.Partial {
		t.Fatalf("scan with a SUSPECT member returned after %s (partial=%v); want the full 600ms budget", d, resp.Partial)
	}
	if len(h2.nodes[1][2].requests()) == 0 {
		t.Fatal("the SUSPECT member was never tried")
	}
}

// A 4xx that every shard returns is the caller's mistake, not an outage: it
// passes through unchanged instead of becoming a retryable 503. If the
// shards disagree, or any shard answers, the failures stay per-shard.
func TestScanPassesThroughA4xxEveryShardAgreesOn(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	for s := range h.nodes {
		for _, n := range h.nodes[s] {
			n.setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
				writeJSON(w, http.StatusBadRequest, internalapi.Error{Code: internalapi.INVALID, Message: "statuses: BOGUS is not a resource status"})
			})
		}
	}
	e := expectError(t, h.do(call{method: http.MethodGet, path: scanPath}), http.StatusBadRequest, internalapi.INVALID)
	if e.Retryable || e.Message != "statuses: BOGUS is not a resource status" {
		t.Fatalf("passed-through error = %+v", e)
	}

	// Shard 1 answers: the others' 400 is a per-shard failure, partial.
	scanAnswer(h, 1)
	rec := h.do(call{method: http.MethodGet, path: scanPath})
	resp := decodeJSON[internalapi.ScanResponse](t, rec)
	if rec.Code != http.StatusOK || !resp.Partial || resp.FailedShards == nil || !slices.Equal(*resp.FailedShards, []int{0, 2}) {
		t.Fatalf("one shard answering = %d %+v, want partial [0 2]", rec.Code, resp)
	}

	// No shard answers, but the failures differ: 503, retryable.
	for _, n := range h.nodes[1] {
		n.setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))
	}
	expectError(t, h.do(call{method: http.MethodGet, path: scanPath}), http.StatusServiceUnavailable, internalapi.NOLEADER)
}

// A shard that returns more records than limit broke the contract; its page
// is a failure, never merged.
func TestScanRejectsAShardOverTheLimit(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	scanAnswer(h, 0)
	scanAnswer(h, 2)
	for _, n := range h.nodes[1] {
		n.setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			writeJSON(w, http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStale,
				Records:   []store.Record{{Key: "res:s1-a"}, {Key: "res:s1-b"}, {Key: "res:s1-c"}},
				Positions: []store.ShardPosition{{Shard: 1, AppliedIndex: 101}}})
		})
	}
	path := strings.Replace(scanPath, "limit=50", "limit=2", 1)
	rec := h.do(call{method: http.MethodGet, path: path})
	resp := decodeJSON[internalapi.ScanResponse](t, rec)
	if rec.Code != http.StatusOK || !resp.Partial || resp.FailedShards == nil || !slices.Equal(*resp.FailedShards, []int{1}) {
		t.Fatalf("shard 1 over the limit = %d %+v, want partial [1]", rec.Code, resp)
	}
	if slices.Contains(scanKeys(resp), "res:s1-a") {
		t.Fatalf("records of the over-limit shard were merged: %v", scanKeys(resp))
	}
}

// The merged consistency is the weakest any shard used: an unlabelled or
// unknown mode, then stale, then linearizable (the §13 fence), then strong
// (through the log).
func TestScanReportsTheWeakestConsistency(t *testing.T) {
	const (
		strong = internalapi.ScanResponseConsistencyStrong
		lin    = internalapi.ScanResponseConsistencyLinearizable
		stale  = internalapi.ScanResponseConsistencyStale
	)
	for _, tc := range []struct {
		modes [3]internalapi.ScanResponseConsistency
		want  internalapi.ScanResponseConsistency
	}{
		{[3]internalapi.ScanResponseConsistency{strong, strong, strong}, strong},
		{[3]internalapi.ScanResponseConsistency{lin, strong, strong}, lin},
		{[3]internalapi.ScanResponseConsistency{strong, lin, strong}, lin},
		{[3]internalapi.ScanResponseConsistency{strong, lin, stale}, stale},
		{[3]internalapi.ScanResponseConsistency{stale, strong, lin}, stale},
		// #166: a shard that does not say how it served its page (missing,
		// empty or an unknown mode) is weaker than any known mode, wherever
		// it sits, so the merged page carries its label and every
		// authoritative reader refuses it; a later shard never masks it.
		{[3]internalapi.ScanResponseConsistency{"", strong, lin}, ""},
		{[3]internalapi.ScanResponseConsistency{strong, "", lin}, ""},
		{[3]internalapi.ScanResponseConsistency{"eventual", strong, strong}, "eventual"},
		{[3]internalapi.ScanResponseConsistency{stale, "", strong}, ""},
	} {
		h := newHarness(t, nil)
		h.ready()
		for s, mode := range tc.modes {
			for _, n := range h.nodes[s] {
				n.setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
					writeJSON(w, http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: mode, Records: []store.Record{},
						Positions: []store.ShardPosition{{Shard: s, AppliedIndex: 1}}})
				})
			}
		}
		resp := decodeJSON[internalapi.ScanResponse](t, h.do(call{method: http.MethodGet, path: scanPath}))
		if resp.Consistency != tc.want {
			t.Errorf("shard modes %v: consistency = %s, want %s", tc.modes, resp.Consistency, tc.want)
		}
	}
}
