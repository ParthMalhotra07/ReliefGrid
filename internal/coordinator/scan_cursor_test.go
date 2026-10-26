package coordinator_test

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordinator"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// keyShard is a fake shard's data: a sorted key set that a scan pages with
// after/limit as a storage node does, and that tests change mid-walk.
type keyShard struct {
	shard int
	mu    sync.Mutex
	keys  []string // sorted
	down  bool
}

func (k *keyShard) insert(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if i, found := slices.BinarySearch(k.keys, key); !found {
		k.keys = slices.Insert(k.keys, i, key)
	}
}

func (k *keyShard) remove(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if i, found := slices.BinarySearch(k.keys, key); found {
		k.keys = slices.Delete(k.keys, i, i+1)
	}
}

func (k *keyShard) setDown(v bool) {
	k.mu.Lock()
	k.down = v
	k.mu.Unlock()
}

func (k *keyShard) snapshot() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.keys)
}

func (k *keyShard) serve(w http.ResponseWriter, r *http.Request, _ []byte) {
	q := r.URL.Query()
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.down {
		writeJSON(w, http.StatusServiceUnavailable, internalapi.Error{Code: internalapi.NOLEADER, Retryable: true})
		return
	}
	if q.Has("cursor") {
		writeJSON(w, http.StatusBadRequest, internalapi.Error{Code: internalapi.INVALID, Message: "a storage node takes after, never cursor"})
		return
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	i, _ := slices.BinarySearch(k.keys, q.Get("after"))
	if i < len(k.keys) && k.keys[i] == q.Get("after") {
		i++ // strictly after
	}
	recs := []store.Record{}
	for _, key := range k.keys[i:min(i+limit, len(k.keys))] {
		recs = append(recs, store.Record{Key: key, Kind: store.KindKV, Version: 1})
	}
	writeJSON(w, http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStrong, Records: recs,
		Positions: []store.ShardPosition{{Shard: k.shard, AppliedIndex: 9, HasMore: len(recs) == limit}}})
}

// keyShards gives every shard of h perShard[s] keys "kv:s<s>-<n>".
func keyShards(h *harness, perShard ...int) []*keyShard {
	var out []*keyShard
	for s, n := range perShard {
		ks := &keyShard{shard: s}
		for i := range n {
			ks.keys = append(ks.keys, fmt.Sprintf("kv:s%d-%06d", s, i*10))
		}
		for _, node := range h.nodes[s] {
			node.setHandler(ks.serve)
		}
		out = append(out, ks)
	}
	return out
}

// walk is the outcome of paging a scan until nextCursor is empty.
type walk struct {
	pages   []internalapi.ScanResponse
	byShard map[int][]string // keys in the order each shard returned them
	seen    map[string]int
}

// page asks for one page; cursor "" is the first page.
func scanPage(t *testing.T, h *harness, limit int, cursor string) internalapi.ScanResponse {
	t.Helper()
	path := fmt.Sprintf("/internal/scan?kind=kv&limit=%d", limit)
	if cursor != "" {
		path += "&cursor=" + url.QueryEscape(cursor)
	}
	rec := h.do(call{method: http.MethodGet, path: path})
	if rec.Code != http.StatusOK {
		t.Fatalf("scan page (cursor %q): %d %s", cursor, rec.Code, rec.Body.String())
	}
	return decodeJSON[internalapi.ScanResponse](t, rec)
}

func shardOfKey(key string) int {
	var s int
	_, _ = fmt.Sscanf(key, "kv:s%d-", &s)
	return s
}

// walkAll pages until nextCursor is absent, calling between(page) after
// each page; it fails past maxPages.
func walkAll(t *testing.T, h *harness, limit, maxPages int, between func(page int, resp internalapi.ScanResponse)) walk {
	t.Helper()
	w := walk{byShard: map[int][]string{}, seen: map[string]int{}}
	cursor := ""
	for page := 0; ; page++ {
		if page >= maxPages {
			t.Fatalf("still paging after %d pages", page)
		}
		resp := scanPage(t, h, limit, cursor)
		w.pages = append(w.pages, resp)
		for _, r := range resp.Records {
			w.seen[r.Key]++
			s := shardOfKey(r.Key)
			w.byShard[s] = append(w.byShard[s], r.Key)
		}
		if between != nil {
			between(page, resp)
		}
		if resp.NextCursor == nil || *resp.NextCursor == "" {
			if resp.Partial {
				t.Fatalf("page %d is partial %v but has no nextCursor", page, resp.FailedShards)
			}
			return w
		}
		cursor = *resp.NextCursor
	}
}

func (w walk) checkOrdered(t *testing.T) {
	t.Helper()
	for s, keys := range w.byShard {
		for i := 1; i < len(keys); i++ {
			if keys[i] <= keys[i-1] {
				t.Fatalf("shard %d returned %s after %s: not in key order", s, keys[i], keys[i-1])
			}
		}
	}
	for k, n := range w.seen {
		if n != 1 {
			t.Fatalf("%s returned %d times", k, n)
		}
	}
}

// Verify (S1.6): paging 5,000 keys per shard with limit 100 returns every key
// exactly once, in key order per shard, and ends with no nextCursor. The
// first page forwards no after; later pages send each shard after=its last
// key and never the cursor itself, and a shard that is exhausted is not
// asked again.
func TestScanCursorWalksEveryKeyOnce(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	shards := keyShards(h, 5000, 5000, 5000)
	w := walkAll(t, h, 100, 60, nil)
	w.checkOrdered(t)
	for s, ks := range shards {
		if want := ks.snapshot(); !slices.Equal(w.byShard[s], want) {
			t.Fatalf("shard %d: walked %d keys, want %d", s, len(w.byShard[s]), len(want))
		}
	}
	// 50 full pages, then one page that finds each shard exhausted.
	if len(w.pages) != 51 {
		t.Fatalf("walk took %d pages, want 51", len(w.pages))
	}
	for i, p := range w.pages {
		if p.Partial || p.RingVersion != 1 {
			t.Fatalf("page %d: partial=%v ringVersion=%d", i, p.Partial, p.RingVersion)
		}
	}

	for s, row := range h.nodes {
		reqs := row[0].requests()
		if len(reqs) != 51 {
			t.Fatalf("shard %d leader was asked %d times, want 51", s, len(reqs))
		}
		first, _ := url.ParseQuery(reqs[0].query)
		if first.Has("after") || first.Has("cursor") {
			t.Fatalf("shard %d first page query %q: want no after or cursor", s, reqs[0].query)
		}
		for i, r := range reqs[1:] {
			q, _ := url.ParseQuery(r.query)
			if want := w.byShard[s][100*(i+1)-1]; q.Get("after") != want || q.Has("cursor") || q.Get("kind") != "kv" || q.Get("limit") != "100" {
				t.Fatalf("shard %d page %d query %q: want kind, limit and after=%s, no cursor", s, i+1, r.query, want)
			}
		}
	}
}

// A shard that runs out early drops out of the cursor and is not asked
// again; the others carry on, and positions cover only the shards read.
func TestScanCursorSkipsExhaustedShards(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	keyShards(h, 1000, 250, 0)
	w := walkAll(t, h, 100, 20, nil)
	w.checkOrdered(t)
	if len(w.seen) != 1250 || len(w.pages) != 11 {
		t.Fatalf("walk saw %d keys in %d pages, want 1250 in 11", len(w.seen), len(w.pages))
	}
	if got := len(h.nodes[1][0].requests()); got != 3 { // 100, 100, 50: short page ends it
		t.Fatalf("shard 1 asked %d times, want 3", got)
	}
	if got := len(h.nodes[2][0].requests()); got != 1 {
		t.Fatalf("empty shard 2 asked %d times, want 1", got)
	}
	for i, p := range w.pages[3:] {
		var shards []int
		for _, pos := range p.Positions {
			shards = append(shards, pos.Shard)
		}
		if !slices.Equal(shards, []int{0}) {
			t.Fatalf("page %d positions for shards %v, want [0]", i+3, shards)
		}
	}
}

// Verify (S1.6): under concurrent inserts and deletes, every key present for
// the whole walk is returned exactly once, no key twice, each shard in
// order. Keys inserted or deleted during the walk may or may not appear.
func TestScanCursorUnderConcurrentWrites(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	shards := keyShards(h, 5000, 5000, 5000)
	// Stable keys are the even ones; the writer only deletes odd keys of the
	// initial set and inserts new keys between existing ones.
	var stable []string
	for _, ks := range shards {
		for i, k := range ks.snapshot() {
			if i%2 == 0 {
				stable = append(stable, k)
			}
		}
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		rng := rand.New(rand.NewPCG(1, 2))
		for {
			select {
			case <-stop:
				return
			default:
			}
			ks := shards[rng.IntN(3)]
			n := rng.IntN(5000)
			ks.remove(fmt.Sprintf("kv:s%d-%06d", ks.shard, (2*(n/2)+1)*10))     // an odd initial key
			ks.insert(fmt.Sprintf("kv:s%d-%06d", ks.shard, n*10+rng.IntN(9)+1)) // a new key between initial ones
		}
	}()
	w := walkAll(t, h, 100, 200, nil)
	close(stop)
	<-done

	w.checkOrdered(t)
	for _, k := range stable {
		if w.seen[k] != 1 {
			t.Fatalf("stable key %s returned %d times, want once", k, w.seen[k])
		}
	}
}

// Verify (S1.6): a shard failing mid-walk makes those pages partial with a
// nextCursor (even once every other shard is exhausted); the failed shard's
// position is carried, not advanced, so once it is back the walk resumes it
// where it stopped and every key is still returned exactly once.
func TestScanCursorResumesAFailedShard(t *testing.T) {
	for name, tc := range map[string]struct{ downFrom, downUntil int }{
		"mid-walk":               {downFrom: 10, downUntil: 14},
		"first page":             {downFrom: 0, downUntil: 2},
		"while another runs out": {downFrom: 20, downUntil: 50}, // shard 2 finishes at page 30
		"as the others finish":   {downFrom: 50, downUntil: 51}, // page 50: shard 0 empty, shard 2 done
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Each page that finds shard 1 down waits out the budget.
			h := newHarness(t, func(c *coordinator.Config) { c.Budget = 50 * time.Millisecond })
			h.ready()
			shards := keyShards(h, 5000, 5000, 3000)
			var down []int
			shards[1].setDown(tc.downFrom == 0)
			w := walkAll(t, h, 100, 200, func(page int, resp internalapi.ScanResponse) {
				failed := resp.FailedShards != nil && slices.Equal(*resp.FailedShards, []int{1})
				if resp.Partial != failed || resp.Partial != (page >= tc.downFrom && page < tc.downUntil) {
					t.Fatalf("page %d: partial=%v failed=%v; shard 1 down for pages [%d,%d)", page, resp.Partial, resp.FailedShards, tc.downFrom, tc.downUntil)
				}
				if resp.Partial {
					down = append(down, page)
					if resp.NextCursor == nil || *resp.NextCursor == "" {
						t.Fatalf("partial page %d has no nextCursor", page)
					}
					for _, p := range resp.Positions {
						if p.Shard == 1 {
							t.Fatalf("partial page %d has a position for the failed shard", page)
						}
					}
				}
				shards[1].setDown(page+1 >= tc.downFrom && page+1 < tc.downUntil)
			})
			w.checkOrdered(t)
			for s, ks := range shards {
				if want := ks.snapshot(); !slices.Equal(w.byShard[s], want) {
					t.Fatalf("shard %d: walked %d keys, want all %d once in order", s, len(w.byShard[s]), len(want))
				}
			}
			if len(down) != tc.downUntil-tc.downFrom {
				t.Fatalf("%d partial pages, want %d", len(down), tc.downUntil-tc.downFrom)
			}
		})
	}
}

// When the only shard left to read is down, nothing answered: the page is
// 503 NO_LEADER (retryable, nothing read), not an empty 200. Retrying the
// same cursor once it is back finishes the walk.
func TestScanCursorOnlyShardLeftDown(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	shards := keyShards(h, 0, 150, 0)
	first := scanPage(t, h, 100, "")
	if first.Partial || first.NextCursor == nil || len(first.Records) != 100 {
		t.Fatalf("first page = partial=%v %d records nextCursor=%v; want 100 records and a cursor", first.Partial, len(first.Records), first.NextCursor)
	}
	shards[1].setDown(true)
	path := "/internal/scan?kind=kv&limit=100&cursor=" + url.QueryEscape(*first.NextCursor)
	if e := expectError(t, h.do(call{method: http.MethodGet, path: path}), http.StatusServiceUnavailable, internalapi.NOLEADER); !e.Retryable {
		t.Fatalf("only shard down = %+v, want retryable", e)
	}
	shards[1].setDown(false)
	last := scanPage(t, h, 100, *first.NextCursor)
	if last.Partial || len(last.Records) != 50 || last.Records[0].Key != "kv:s1-001000" || last.NextCursor != nil && *last.NextCursor != "" {
		t.Fatalf("retried page = %d records, nextCursor %v; want the 50 from kv:s1-001000 and no cursor", len(last.Records), last.NextCursor)
	}
}

// Verify (S1.6): a cursor from another ring version is 409
// RING_VERSION_MISMATCH and a malformed one 400 INVALID; neither reaches a
// shard, and neither is read as "start from the first key".
func TestScanCursorRejectedBeforeForwarding(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	keyShards(h, 10, 10, 10)
	stale := store.EncodeScanCursor(store.ScanCursor{RingVersion: 2, Query: store.ScanQueryHash(store.ScanQuery{Kind: store.KindKV, Limit: 10}), After: map[int]string{0: "kv:s0-000050", 1: ""}})
	e := expectError(t, h.do(call{method: http.MethodGet, path: "/internal/scan?kind=kv&limit=10&cursor=" + url.QueryEscape(stale)}),
		http.StatusConflict, internalapi.RINGVERSIONMISMATCH)
	if e.RingVersion == nil || *e.RingVersion != 1 {
		t.Fatalf("stale cursor = %+v, want the server's ringVersion 1", e)
	}
	kv10 := store.ScanQueryHash(store.ScanQuery{Kind: store.KindKV, Limit: 10})
	for _, bad := range []string{
		"garbage",
		store.EncodeScanCursor(store.ScanCursor{RingVersion: 1, Query: kv10, After: map[int]string{3: "kv:x"}}), // no shard 3
	} {
		expectError(t, h.do(call{method: http.MethodGet, path: "/internal/scan?kind=kv&limit=10&cursor=" + url.QueryEscape(bad)}),
			http.StatusBadRequest, internalapi.INVALID)
	}
	if n := h.allRequests(); n != 0 {
		t.Fatalf("%d scans forwarded for rejected cursors", n)
	}
}

// A cursor is bound to its query: continuing it with another kind, type,
// status set, geohash cover or limit is 400 INVALID and reaches no shard.
// Reordering statuses or prefixes is the same query.
func TestScanCursorBoundToItsQuery(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	keyShards(h, 50, 50, 50)
	const base = "/internal/scan?kind=kv&type=WATER&statuses=AVAILABLE&statuses=OFFLINE&geohashPrefixes=ttn&geohashPrefixes=tsq&limit=10"
	rec := h.do(call{method: http.MethodGet, path: base})
	first := decodeJSON[internalapi.ScanResponse](t, rec)
	if rec.Code != http.StatusOK || first.NextCursor == nil {
		t.Fatalf("first page = %d %s", rec.Code, rec.Body.String())
	}
	cursor := "&cursor=" + url.QueryEscape(*first.NextCursor)
	before := h.allRequests()
	for _, other := range []string{
		"/internal/scan?kind=resource&type=WATER&statuses=AVAILABLE&statuses=OFFLINE&geohashPrefixes=ttn&geohashPrefixes=tsq&limit=10",
		"/internal/scan?kind=kv&type=FOOD&statuses=AVAILABLE&statuses=OFFLINE&geohashPrefixes=ttn&geohashPrefixes=tsq&limit=10",
		"/internal/scan?kind=kv&type=WATER&statuses=AVAILABLE&geohashPrefixes=ttn&geohashPrefixes=tsq&limit=10",
		"/internal/scan?kind=kv&type=WATER&statuses=AVAILABLE&statuses=OFFLINE&geohashPrefixes=ttn&limit=10",
		"/internal/scan?kind=kv&type=WATER&statuses=AVAILABLE&statuses=OFFLINE&geohashPrefixes=ttn&geohashPrefixes=tsq&limit=11",
	} {
		expectError(t, h.do(call{method: http.MethodGet, path: other + cursor}), http.StatusBadRequest, internalapi.INVALID)
	}
	if n := h.allRequests() - before; n != 0 {
		t.Fatalf("%d scans forwarded for a cursor of another query", n)
	}
	reordered := "/internal/scan?kind=kv&type=WATER&statuses=OFFLINE&statuses=AVAILABLE&geohashPrefixes=tsq&geohashPrefixes=ttn&limit=10"
	if rec := h.do(call{method: http.MethodGet, path: reordered + cursor}); rec.Code != http.StatusOK {
		t.Fatalf("same query, filters reordered: %d %s", rec.Code, rec.Body.String())
	}

	// after is the storage node's parameter, never the caller's.
	expectError(t, h.do(call{method: http.MethodGet, path: base + "&after=kv:a"}), http.StatusBadRequest, internalapi.INVALID)
}

// A shard answer that is out of key order, or that goes back to or before
// the cursor's position, is a failed shard: it would corrupt the cursor.
func TestScanCursorRejectsAShardOutOfOrder(t *testing.T) {
	h := newHarness(t, nil)
	h.ready()
	keyShards(h, 20, 20, 20)
	first := scanPage(t, h, 10, "")
	for _, n := range h.nodes[1] {
		n.setHandler(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
			writeJSON(w, http.StatusOK, internalapi.ScanResponse{RingVersion: 1, Consistency: internalapi.ScanResponseConsistencyStrong,
				Records:   []store.Record{{Key: "kv:s1-000050"}, {Key: "kv:s1-000200"}}, // 50 is at or before the cursor (90)
				Positions: []store.ShardPosition{{Shard: 1, AppliedIndex: 9}}})
		})
	}
	resp := scanPage(t, h, 10, *first.NextCursor)
	if !resp.Partial || resp.FailedShards == nil || !slices.Equal(*resp.FailedShards, []int{1}) || resp.NextCursor == nil {
		t.Fatalf("out-of-order shard 1 = partial=%v failed=%v next=%v, want partial [1] with a cursor", resp.Partial, resp.FailedShards, resp.NextCursor)
	}
	cur, err := store.DecodeScanCursor(*resp.NextCursor)
	if err != nil || cur.After[1] != "kv:s1-000090" {
		t.Fatalf("next cursor = %+v, %v; want shard 1 still after kv:s1-000090", cur, err)
	}
}
