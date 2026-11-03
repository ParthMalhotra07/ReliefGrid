package coordinator_test

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func simulate(t *testing.T, h *harness, query string) internalapi.RingSimulation {
	t.Helper()
	rec := h.do(call{method: http.MethodGet, path: "/admin/ring/simulate?" + query, header: operatorAuth})
	if rec.Code != http.StatusOK {
		t.Fatalf("simulate?%s: %d %s", query, rec.Code, rec.Body.String())
	}
	return decodeJSON[internalapi.RingSimulation](t, rec)
}

// checkTotals: both per-shard rows and the matrix count every key once, and
// moved is the matrix off its diagonal.
func checkTotals(t *testing.T, s internalapi.RingSimulation) {
	t.Helper()
	sum := func(xs []int) (n int) {
		for _, x := range xs {
			n += x
		}
		return n
	}
	if sum(s.PerShardBefore) != s.Keys || sum(s.PerShardAfter) != s.Keys {
		t.Fatalf("per-shard sums %d and %d, want keys %d", sum(s.PerShardBefore), sum(s.PerShardAfter), s.Keys)
	}
	in, moved := 0, 0
	for from, row := range s.Matrix {
		for to, n := range row {
			if n < 1 {
				t.Fatalf("matrix[%s][%s] = %d: zero entries are omitted", from, to, n)
			}
			in += n
			if from != to {
				moved += n
			}
		}
	}
	if in != s.Keys || moved != s.Moved {
		t.Fatalf("matrix holds %d keys, %d moved; response says %d, %d", in, moved, s.Keys, s.Moved)
	}
	if s.Keys > 0 && s.MovedFraction != float64(s.Moved)/float64(s.Keys) {
		t.Fatalf("movedFraction %v, want %d/%d", s.MovedFraction, s.Moved, s.Keys)
	}
}

// Verify (S4.5b): growing 3 → 4 shards moves about a quarter of the keys,
// and only to the new shard, because the existing shards keep their tokens.
// The candidate is built by the ring's own builder, and the active ring is
// untouched afterwards.
func TestRingSimulateThreeToFour(t *testing.T) {
	h := newHarness(t, nil)
	s := simulate(t, h, "shards=4&sample=20000")
	checkTotals(t, s)
	want, err := ring.Build(2, 4, 128)
	if err != nil {
		t.Fatal(err)
	}
	if s.Source != internalapi.RingSimulationSourceSample || s.Keys != 20000 ||
		s.CurrentRing.Checksum != h.rg.Checksum() || s.CurrentRing.RingVersion != 1 || s.CurrentRing.Shards != 3 ||
		s.Candidate.RingVersion != 2 || s.Candidate.Shards != 4 || s.Candidate.VnodesPerShard != 128 || s.Candidate.Checksum != want.Checksum() {
		t.Fatalf("simulation header = %+v / %+v / %+v", s.Source, s.CurrentRing, s.Candidate)
	}
	if s.IdealFraction != 0.25 {
		t.Fatalf("idealFraction = %v, want 1 - 3/4", s.IdealFraction)
	}
	if s.MovedFraction < 0.20 || s.MovedFraction > 0.30 {
		t.Fatalf("movedFraction = %v, want near 0.25", s.MovedFraction)
	}
	for from, row := range s.Matrix {
		for to := range row {
			if to != from && to != "3" {
				t.Fatalf("keys moved %s → %s; only the new shard 3 may gain keys", from, to)
			}
		}
	}
	if len(s.PerShardBefore) != 3 || len(s.PerShardAfter) != 4 || s.PerShardAfter[3] != s.Moved {
		t.Fatalf("before %v after %v moved %d", s.PerShardBefore, s.PerShardAfter, s.Moved)
	}
	for i := range 3 {
		if s.PerShardAfter[i] > s.PerShardBefore[i] {
			t.Fatalf("shard %d grew from %d to %d", i, s.PerShardBefore[i], s.PerShardAfter[i])
		}
	}

	rec := h.do(call{method: http.MethodGet, path: "/admin/ring", header: operatorAuth})
	if r := decodeJSON[internalapi.AdminRing](t, rec); r.Ring.Shards != 3 || r.Ring.Checksum != h.rg.Checksum() {
		t.Fatalf("active ring after a simulation = %+v", r.Ring)
	}
	if n := h.allRequests(); n != 0 {
		t.Fatalf("a sample simulation sent %d requests to storage nodes", n)
	}
}

// The active ring's own numbers move nothing; the candidate is the same
// token map at the next ringVersion. Shrinking reports 1 - S'/S.
func TestRingSimulateNoChangeMovesNothing(t *testing.T) {
	h := newHarness(t, nil)
	s := simulate(t, h, "shards=3&vnodes=128&sample=5000")
	checkTotals(t, s)
	if s.Moved != 0 || s.MovedFraction != 0 || s.IdealFraction != 0 || !slices.Equal(s.PerShardBefore, s.PerShardAfter) {
		t.Fatalf("no-change simulation = %+v", s)
	}
	for from, row := range s.Matrix {
		if len(row) != 1 || row[from] == 0 {
			t.Fatalf("matrix row %s = %v, want only the diagonal", from, row)
		}
	}
	if def := simulate(t, h, "shards=3"); def.Keys != 10000 || def.Moved != 0 || def.Candidate.VnodesPerShard != 128 {
		t.Fatalf("defaults: keys %d moved %d vnodes %d, want 10000 0 128", def.Keys, def.Moved, def.Candidate.VnodesPerShard)
	}
	if shrink := simulate(t, h, "shards=2&sample=1000"); math.Abs(shrink.IdealFraction-1.0/3) > 1e-12 || shrink.PerShardAfter == nil || len(shrink.PerShardAfter) != 2 {
		t.Fatalf("3 → 2: idealFraction %v after %v", shrink.IdealFraction, shrink.PerShardAfter)
	}
}

// storedAnswer makes every member of shard serve a scan of each kind from
// keys, in key order, two records a page with hasMore, as a storage node
// pages. It honours after and refuses a scan without the ring version.
func storedAnswer(h *harness, shard int, keys map[store.Kind][]string) {
	for _, n := range h.nodes[shard] {
		n.setHandler(func(w http.ResponseWriter, r *http.Request, _ []byte) {
			q := r.URL.Query()
			if r.URL.Path != "/internal/scan" || q.Get("ringVersion") != "1" || q.Get("limit") == "" {
				writeJSON(w, http.StatusBadRequest, internalapi.Error{Code: internalapi.INVALID, Message: "unexpected " + r.URL.String()})
				return
			}
			kind := store.Kind(q.Get("kind"))
			all := slices.Sorted(slices.Values(keys[kind]))
			i, _ := slices.BinarySearch(all, q.Get("after"))
			if i < len(all) && all[i] == q.Get("after") {
				i++
			}
			end := min(i+2, len(all))
			recs := []store.Record{}
			for _, k := range all[i:end] {
				recs = append(recs, store.Record{Key: k, Kind: kind, Version: 1})
			}
			writeJSON(w, http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStrong,
				Records: recs, Positions: []store.ShardPosition{{Shard: shard, AppliedIndex: 50, HasMore: end < len(all)}}})
		})
	}
}

// storedKeys returns n keys of each kind that the active ring places on
// shard.
func storedKeys(t *testing.T, h *harness, shard, n int) map[store.Kind][]string {
	t.Helper()
	out := map[store.Kind][]string{}
	for kind, prefix := range map[store.Kind]string{store.KindRequest: "req:", store.KindResource: "res:", store.KindKV: "kv-"} {
		for i := 0; len(out[kind]) < n; i++ {
			k := fmt.Sprintf("%ss%d-%d", prefix, shard, i)
			if owner, _ := h.rg.Owner(k); owner == shard {
				out[kind] = append(out[kind], k)
			}
		}
	}
	return out
}

// source=stored counts every stored key of every kind, following each
// shard's pages to the end, at the current ringVersion.
func TestRingSimulateStoredScansEveryKey(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	var all []string
	for s := range 3 {
		keys := storedKeys(t, h, s, 5)
		storedAnswer(h, s, keys)
		for _, ks := range keys {
			all = append(all, ks...)
		}
	}
	got := simulate(t, h, "shards=4&source=stored")
	checkTotals(t, got)
	if got.Source != internalapi.RingSimulationSourceStored || got.Keys != len(all) || !slices.Equal(got.PerShardBefore, []int{15, 15, 15}) {
		t.Fatalf("stored simulation: source %s keys %d before %v, want stored %d [15 15 15]", got.Source, got.Keys, got.PerShardBefore, len(all))
	}
	four, _ := ring.Build(2, 4, 128)
	moved := 0
	for _, k := range all {
		if o, _ := four.Owner(k); o == 3 {
			moved++
		}
	}
	if got.Moved != moved {
		t.Fatalf("moved %d, want %d by placing the stored keys on the candidate", got.Moved, moved)
	}
	for s, row := range h.nodes {
		for _, kind := range []string{"request", "resource", "kv"} {
			var pages int
			for _, req := range row[0].requests() {
				q, _ := url.ParseQuery(req.query)
				if q.Get("kind") == kind {
					pages++
					if q.Get("consistency") == "stale" {
						t.Fatalf("shard %d scanned %s stale; a stored simulation is authoritative", s, kind)
					}
				}
			}
			if pages != 3 { // 5 keys, 2 a page
				t.Fatalf("shard %d %s: %d pages, want 3", s, kind, pages)
			}
		}
	}
}

// Verify (S4.5b), CONTRIBUTING.md, Invariants, 7: a stored scan that misses
// a shard is an error, never a simulation over fewer keys.
func TestRingSimulateStoredPartialScanIsAnError(t *testing.T) {
	h := newHarness(t, func(c *coordinator.Config) { c.Budget = 200 * time.Millisecond })
	h.ready()
	storedAnswer(h, 0, storedKeys(t, h, 0, 3))
	storedAnswer(h, 2, storedKeys(t, h, 2, 3))
	for _, n := range h.nodes[1] {
		n.setHandler(failWith(http.StatusServiceUnavailable, internalapi.NOLEADER, false))
	}
	rec := h.do(call{method: http.MethodGet, path: "/admin/ring/simulate?shards=4&source=stored", header: operatorAuth})
	e := expectError(t, rec, http.StatusServiceUnavailable, internalapi.NOLEADER)
	if !e.Retryable || e.OutcomeUnknown || !strings.Contains(e.Message, "shard 1") {
		t.Fatalf("partial stored scan = %+v, want retryable NO_LEADER naming shard 1", e)
	}

	// A shard that fails on a later page, after answering a first one, is
	// still a missed shard.
	h2 := newHarness(t, func(c *coordinator.Config) { c.Budget = 200 * time.Millisecond })
	h2.ready()
	for s := range 3 {
		storedAnswer(h2, s, storedKeys(t, h2, s, 3))
	}
	failAfterFirstPage(h2, 2)
	rec = h2.do(call{method: http.MethodGet, path: "/admin/ring/simulate?shards=4&source=stored", header: operatorAuth})
	expectError(t, rec, http.StatusServiceUnavailable, internalapi.NOLEADER)
}

// failAfterFirstPage wraps shard's current handler: first pages are served,
// any page with after fails with STORAGE_UNAVAILABLE.
func failAfterFirstPage(h *harness, shard int) {
	for _, n := range h.nodes[shard] {
		n.mu.Lock()
		inner := n.handle
		n.mu.Unlock()
		n.setHandler(func(w http.ResponseWriter, r *http.Request, b []byte) {
			if r.URL.Query().Get("after") != "" {
				failWith(http.StatusServiceUnavailable, internalapi.STORAGEUNAVAILABLE, false)(w, r, b)
				return
			}
			inner(w, r, b)
		})
	}
}

func TestRingSimulateRejectsBadInput(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	for _, q := range []string{
		"", "shards=", "shards=x", "shards=0", "shards=-1", "shards=4&vnodes=0", "shards=4&vnodes=x",
		"shards=4&sample=0", "shards=4&sample=1000001", "shards=4&source=bogus",
		"shards=4&source=stored&sample=10", "shards=1048576&vnodes=2",
	} {
		expectError(t, h.do(call{method: http.MethodGet, path: "/admin/ring/simulate?" + q, header: operatorAuth}), http.StatusBadRequest, internalapi.INVALID)
	}
	if n := h.allRequests(); n != 0 {
		t.Fatalf("bad input reached storage nodes: %d requests", n)
	}
}
