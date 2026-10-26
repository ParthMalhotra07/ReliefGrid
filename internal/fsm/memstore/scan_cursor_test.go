package memstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// cursorKey is key n of shard s; byPrefix routes it there.
func cursorKey(s, n int) string { return fmt.Sprintf("kv:s%d-%06d", s, n) }

func byPrefix(key string) int {
	var s int
	_, _ = fmt.Sscanf(key, "kv:s%d-", &s)
	return s
}

// seededStore is a 3-shard memstore with perShard keys cursorKey(s, 10*i)
// on every shard, so new keys fit between them.
func seededStore(t *testing.T, perShard int, opts ...memstore.Option) *memstore.Store {
	t.Helper()
	s, err := memstore.New(context.Background(), append([]memstore.Option{memstore.WithRouter(byPrefix)}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	for sh := range 3 {
		for i := range perShard {
			k := cursorKey(sh, 10*i)
			if res, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: "c-" + k, At: t0}, Key: k, Value: json.RawMessage(`1`)}); err != nil || res.Outcome != store.OutcomeOK {
				t.Fatalf("create %s: %+v, %v", k, res, err)
			}
		}
	}
	return s
}

type walkResult struct {
	pages   []store.ScanResult
	byShard map[int][]string
	seen    map[string]int
}

// walkStore pages q until NextCursor is empty, calling between after each
// page; a partial page must carry a cursor.
func walkStore(t *testing.T, s store.Store, q store.ScanQuery, maxPages int, between func(page int, res store.ScanResult)) walkResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := walkResult{byShard: map[int][]string{}, seen: map[string]int{}}
	for page := 0; ; page++ {
		if page >= maxPages {
			t.Fatalf("still paging after %d pages", page)
		}
		res, err := s.Scan(ctx, q)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		w.pages = append(w.pages, res)
		for _, r := range res.Records {
			w.seen[r.Key]++
			w.byShard[byPrefix(r.Key)] = append(w.byShard[byPrefix(r.Key)], r.Key)
		}
		if between != nil {
			between(page, res)
		}
		if res.NextCursor == "" {
			if res.Partial {
				t.Fatalf("page %d is partial %v with no NextCursor", page, res.FailedShards)
			}
			return w
		}
		q.Cursor = res.NextCursor
	}
}

func (w walkResult) checkOnceInOrder(t *testing.T) {
	t.Helper()
	for s, keys := range w.byShard {
		for i := 1; i < len(keys); i++ {
			if keys[i] <= keys[i-1] {
				t.Fatalf("shard %d returned %s after %s", s, keys[i], keys[i-1])
			}
		}
	}
	for k, n := range w.seen {
		if n != 1 {
			t.Fatalf("%s returned %d times", k, n)
		}
	}
}

func initialKeys(shard, perShard int) []string {
	var out []string
	for i := range perShard {
		out = append(out, cursorKey(shard, 10*i))
	}
	return out
}

// Verify (S1.6): 5,000 keys per shard, limit 100: every key exactly once, in
// key order per shard, in 51 pages (the 51st finds every shard exhausted).
func TestScanCursorWalksEveryKeyOnce(t *testing.T) {
	t.Parallel() // seeding 15,000 keys dominates; separate stores seed side by side
	s := seededStore(t, 5000)
	w := walkStore(t, s, store.ScanQuery{Kind: store.KindKV, Limit: 100}, 60, nil)
	w.checkOnceInOrder(t)
	for sh := range 3 {
		if !slices.Equal(w.byShard[sh], initialKeys(sh, 5000)) {
			t.Fatalf("shard %d: walked %d keys, want all 5000 in order", sh, len(w.byShard[sh]))
		}
	}
	if len(w.pages) != 51 {
		t.Fatalf("walk took %d pages, want 51", len(w.pages))
	}
	for i, p := range w.pages {
		if p.Partial {
			t.Fatalf("page %d partial %v", i, p.FailedShards)
		}
	}
}

// Shards that run out drop out of the cursor: later pages have positions
// only for the shards still read.
func TestScanCursorDropsExhaustedShards(t *testing.T) {
	s := seededStore(t, 0)
	ctx := context.Background() // setup; walkStore sets its own deadline
	for i := range 250 {
		k := cursorKey(1, i)
		if _, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: "c-" + k, At: t0}, Key: k, Value: json.RawMessage(`1`)}); err != nil {
			t.Fatal(err)
		}
	}
	w := walkStore(t, s, store.ScanQuery{Kind: store.KindKV, Limit: 100}, 10, nil)
	w.checkOnceInOrder(t)
	if len(w.seen) != 250 || len(w.pages) != 3 {
		t.Fatalf("saw %d keys in %d pages, want 250 in 3", len(w.seen), len(w.pages))
	}
	for i, p := range w.pages[1:] {
		if len(p.Positions) != 1 || p.Positions[0].Shard != 1 {
			t.Fatalf("page %d positions %+v, want only shard 1", i+1, p.Positions)
		}
	}
}

// Verify (S1.6): with inserts and deletes committing concurrently, every key
// present for the whole walk is returned exactly once, none twice, in order.
func TestScanCursorUnderConcurrentWrites(t *testing.T) {
	t.Parallel() // seeding 15,000 keys dominates; separate stores seed side by side
	s := seededStore(t, 5000)
	var stable []string // even initial keys: never deleted
	for sh := range 3 {
		for i, k := range initialKeys(sh, 5000) {
			if i%2 == 0 {
				stable = append(stable, k)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var writes atomic.Int64
	go func() {
		rng := rand.New(rand.NewPCG(3, 4))
		for op := 0; ctx.Err() == nil; op++ {
			sh, n := rng.IntN(3), rng.IntN(2500)
			del := cursorKey(sh, 10*(2*n+1)) // an odd initial key
			ins := cursorKey(sh, 10*rng.IntN(5000)+rng.IntN(9)+1)
			if _, err := s.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: fmt.Sprintf("d-%d", op), At: t0}, Key: del, ExpectedVersion: 1, Delete: true}); err != nil && ctx.Err() == nil {
				done <- err
				return
			}
			if _, err := s.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: fmt.Sprintf("i-%d", op), At: t0}, Key: ins, Value: json.RawMessage(`2`)}); err != nil && ctx.Err() == nil {
				done <- err
				return
			}
			writes.Add(1)
		}
		done <- nil
	}()
	w := walkStore(t, s, store.ScanQuery{Kind: store.KindKV, Limit: 100}, 200, nil)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("writer: %v", err)
	}
	if writes.Load() == 0 {
		t.Fatal("no write committed during the walk")
	}
	w.checkOnceInOrder(t)
	for _, k := range stable {
		if w.seen[k] != 1 {
			t.Fatalf("stable key %s returned %d times, want once (%d writes during the walk)", k, w.seen[k], writes.Load())
		}
	}
}

// Verify (S1.6): a shard failing mid-walk gives partial pages with a cursor
// that carries its position; once it is back the walk resumes it where it
// stopped, and every key is returned exactly once.
func TestScanCursorResumesAFailedShard(t *testing.T) {
	t.Parallel() // seeding 15,000 keys dominates; separate stores seed side by side
	base := seededStore(t, 5000)
	var page atomic.Int64
	s := memstore.WithFaults(base, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			if p := page.Add(1) - 1; p >= 10 && p < 14 {
				return memstore.Fault{FailShards: []int{1}}
			}
		}
		return memstore.Fault{}
	})
	w := walkStore(t, s, store.ScanQuery{Kind: store.KindKV, Limit: 100}, 80, func(p int, res store.ScanResult) {
		if want := p >= 10 && p < 14; res.Partial != want || want && !slices.Equal(res.FailedShards, []int{1}) {
			t.Fatalf("page %d: partial=%v failed=%v, want partial=%v", p, res.Partial, res.FailedShards, want)
		}
	})
	w.checkOnceInOrder(t)
	for sh := range 3 {
		if !slices.Equal(w.byShard[sh], initialKeys(sh, 5000)) {
			t.Fatalf("shard %d: walked %d keys, want all 5000 once in order", sh, len(w.byShard[sh]))
		}
	}
}

// Verify (S1.6): a cursor from another ring version is ErrRingVersionMismatch;
// a malformed one, or one continued with other filters or another limit, is
// a plain error. None restarts the walk.
func TestScanCursorStaleOrMalformed(t *testing.T) {
	v1 := seededStore(t, 300)
	v2 := seededStore(t, 300, memstore.WithRingVersion(2))
	ctx := testCtx(t) // after seeding: the deadline is for the scans
	q := store.ScanQuery{Kind: store.KindKV, Limit: 100}
	first, err := v1.Scan(ctx, q)
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page = %+v, %v; want a NextCursor", first.Positions, err)
	}
	q.Cursor = first.NextCursor
	if res, err := v2.Scan(ctx, q); !errors.Is(err, store.ErrRingVersionMismatch) || len(res.Records) != 0 {
		t.Fatalf("v1 cursor on ring v2 = %d records, %v; want ErrRingVersionMismatch", len(res.Records), err)
	}
	if _, err := v1.Scan(ctx, q); err != nil {
		t.Fatalf("v1 cursor on ring v1: %v", err)
	}
	for name, change := range map[string]func(q *store.ScanQuery){
		"limit":    func(q *store.ScanQuery) { q.Limit = 99 },
		"kind":     func(q *store.ScanQuery) { q.Kind = store.KindResource },
		"prefixes": func(q *store.ScanQuery) { q.GeohashPrefixes = []string{"tt"} },
	} {
		other := q
		change(&other)
		if res, err := v1.Scan(ctx, other); err == nil || errors.Is(err, store.ErrRingVersionMismatch) || len(res.Records) != 0 {
			t.Fatalf("cursor continued with another %s = %d records, %v; want a plain error", name, len(res.Records), err)
		}
	}
	for _, bad := range []string{"garbage", store.EncodeScanCursor(store.ScanCursor{RingVersion: 1, Query: store.ScanQueryHash(q), After: map[int]string{5: "kv:x"}})} {
		q.Cursor = bad
		res, err := v1.Scan(ctx, q)
		if err == nil || errors.Is(err, store.ErrRingVersionMismatch) || errors.Is(err, store.ErrUnknownOutcome) || len(res.Records) != 0 {
			t.Fatalf("cursor %q = %d records, %v; want a plain error", bad, len(res.Records), err)
		}
	}
}
