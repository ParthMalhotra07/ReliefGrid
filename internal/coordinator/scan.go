package coordinator

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// scan is scatter-gather: the caller's query goes unchanged to every
// shard's leader at once, and the answers are merged in shard order.
// A shard that fails (no leader within the budget, an error, a garbled
// answer) is listed in failedShards with partial=true; its records are
// missing, never reported as an empty shard (CONTRIBUTING.md, Invariants,
// 7). If no shard answers there is nothing to return: 503 NO_LEADER,
// retryable, unless every shard refused the query with the same 4xx, which
// is the caller's mistake and passes through unchanged.
//
// A shard whose page is full (it said hasMore, or returned exactly limit
// records) has hasMore in its position; one that returned more than limit
// records, or keys out of order, broke the contract and is a failed shard.
// The merged consistency is the weakest any shard used.
//
// Paging (S1.6): a cursor is split per shard, and each shard it lists is
// asked for keys after its own position; shards it does not list are
// exhausted and not asked. nextCursor carries every shard whose page was
// full (after its last key) and every shard that failed (at the position it
// had), so a failed shard resumes where it stopped. A cursor from another
// ring version is 409 RING_VERSION_MISMATCH; one that is malformed, names a
// shard the ring does not have, or came from other filters or another limit
// is 400 INVALID.
func (c *Coordinator) scan(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	switch store.Kind(q.Get("kind")) {
	case store.KindRequest, store.KindResource, store.KindKV:
	default:
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "kind must be request, resource or kv")
		return
	}
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit < 1 || limit > store.MaxScanLimit {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("limit is required, 1 to %d records per shard", store.MaxScanLimit))
		return
	}
	switch q.Get("consistency") {
	case "", "strong", "linearizable", "stale":
	default:
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "consistency must be strong, linearizable or stale")
		return
	}
	if q.Has("after") {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "after is for storage nodes; page with cursor")
		return
	}
	if !c.checkRing(w, r, nil) {
		return
	}
	sq := store.ScanQuery{Kind: store.Kind(q.Get("kind")), Type: store.ResourceType(q.Get("type")),
		Statuses: q["statuses"], GeohashPrefixes: q["geohashPrefixes"], Limit: limit}
	var after map[int]string // nil on a first page: every shard from its first key
	asked := make([]int, c.ring.Shards())
	for s := range asked {
		asked[s] = s
	}
	if q.Has("cursor") {
		cur, ok := c.scanCursor(w, q.Get("cursor"), sq)
		if !ok {
			return
		}
		after = cur.After
		asked = slices.Sorted(maps.Keys(after))
	}

	answers := make([]shardAnswer, c.ring.Shards())
	var wg sync.WaitGroup
	for _, s := range asked {
		wg.Go(func() { answers[s] = c.scanShard(shardRequest(r, after, s), s, limit, after[s]) })
	}
	wg.Wait()

	out := internalapi.ScanResponse{RingVersion: c.ring.Version(), Records: []store.Record{}, Positions: []store.ShardPosition{}}
	var whys []string
	var failedShards []int
	next := map[int]string{}
	for _, s := range asked {
		a := answers[s]
		if a.resp == nil {
			failedShards = append(failedShards, s)
			whys = append(whys, fmt.Sprintf("shard %d: %s", s, a.why))
			next[s] = after[s] // resume where it was
			continue
		}
		recs := a.resp.Records
		out.Records = append(out.Records, recs...)
		pos := a.resp.Positions[0]
		pos.HasMore = pos.HasMore || len(recs) >= limit
		if pos.HasMore {
			next[s] = recs[len(recs)-1].Key
		}
		out.Positions = append(out.Positions, pos)
		if out.Consistency == "" || consistencyRank[a.resp.Consistency] < consistencyRank[out.Consistency] {
			out.Consistency = a.resp.Consistency // the weakest read used
		}
	}
	if len(out.Positions) == 0 {
		if rej := agreedRejection(answers); rej != nil {
			for k, v := range forwardHeader(rej.header) {
				w.Header()[k] = v
			}
			w.WriteHeader(rej.status)
			_, _ = w.Write(rej.body)
			return
		}
		writeUnavailable(w, false, "no shard answered the scan: "+fmt.Sprint(whys))
		return
	}
	if len(failedShards) > 0 {
		out.Partial, out.FailedShards = true, &failedShards
	}
	if len(next) > 0 {
		cur := store.EncodeScanCursor(store.ScanCursor{RingVersion: c.ring.Version(), Query: store.ScanQueryHash(sq), After: next})
		out.NextCursor = &cur
	}
	writeJSON(w, http.StatusOK, out)
}

// scanCursor decodes and checks a caller's cursor for query q, writing the
// refusal if it cannot continue this scan on this ring.
func (c *Coordinator) scanCursor(w http.ResponseWriter, s string, q store.ScanQuery) (store.ScanCursor, bool) {
	cur, err := store.DecodeScanCursor(s)
	if err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "cursor: "+err.Error())
		return store.ScanCursor{}, false
	}
	if have := c.ring.Version(); cur.RingVersion != have {
		writeJSON(w, http.StatusConflict, internalapi.Error{Code: internalapi.RINGVERSIONMISMATCH, Retryable: true, RingVersion: &have,
			Message: fmt.Sprintf("cursor was made under ring version %d, the coordinator has %d: restart the scan without a cursor", cur.RingVersion, have)})
		return store.ScanCursor{}, false
	}
	if cur.Query != store.ScanQueryHash(q) {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "cursor belongs to a scan with other filters or another limit")
		return store.ScanCursor{}, false
	}
	for shard := range cur.After {
		if shard >= c.ring.Shards() {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("cursor names shard %d; the ring has %d", shard, c.ring.Shards()))
			return store.ScanCursor{}, false
		}
	}
	return cur, true
}

// shardRequest is r as one shard should see it: unchanged on a first page;
// on a cursor page the cursor is replaced by that shard's after.
func shardRequest(r *http.Request, after map[int]string, shard int) *http.Request {
	if after == nil {
		return r
	}
	sr := r.Clone(r.Context())
	q := sr.URL.Query()
	q.Del("cursor")
	if k := after[shard]; k != "" {
		q.Set("after", k)
	}
	sr.URL.RawQuery = q.Encode()
	return sr
}

// consistencyRank orders read modes from weakest: a local stale read, the
// §13 linearizable fence, then strong (through the log, M5).
var consistencyRank = map[internalapi.ScanResponseConsistency]int{
	internalapi.ScanResponseConsistencyStale:        0,
	internalapi.ScanResponseConsistencyLinearizable: 1,
	internalapi.ScanResponseConsistencyStrong:       2,
}

// shardAnswer is one shard's scan page, or why it failed. rejected is set
// when the shard refused the query with a 4xx.
type shardAnswer struct {
	resp     *internalapi.ScanResponse
	rejected *answer
	why      string
}

// agreedRejection returns the 4xx every shard answered with the same status
// and code, or nil.
func agreedRejection(answers []shardAnswer) *answer {
	var first *answer
	var code internalapi.ErrorCode
	for _, a := range answers {
		if a.rejected == nil {
			return nil
		}
		var e internalapi.Error
		_ = json.Unmarshal(a.rejected.body, &e)
		if first == nil {
			first, code = a.rejected, e.Code
		} else if a.rejected.status != first.status || e.Code != code {
			return nil
		}
	}
	return first
}

// scanShard asks one shard's leader for keys after after; an answer without
// resp is a failure, and why says what failed.
func (c *Coordinator) scanShard(r *http.Request, shard, limit int, after string) shardAnswer {
	ans, _, why := c.forward(r.Context(), shard, r, nil, false)
	if ans == nil {
		return shardAnswer{why: why}
	}
	if ans.status != http.StatusOK {
		var e internalapi.Error
		_ = json.Unmarshal(ans.body, &e)
		a := shardAnswer{why: fmt.Sprintf("%d %s %s", ans.status, e.Code, e.Message)}
		if ans.status >= 400 && ans.status < 500 {
			a.rejected = ans
		}
		return a
	}
	var resp internalapi.ScanResponse
	if err := json.Unmarshal(ans.body, &resp); err != nil {
		return shardAnswer{why: "garbled answer: " + err.Error()}
	}
	if len(resp.Positions) != 1 || resp.Positions[0].Shard != shard || resp.Partial {
		return shardAnswer{why: fmt.Sprintf("answer is not one complete shard-%d page: positions %+v", shard, resp.Positions)}
	}
	if resp.Records == nil {
		return shardAnswer{why: "answer has no records list"}
	}
	if len(resp.Records) > limit {
		return shardAnswer{why: fmt.Sprintf("answer has %d records, over the limit %d", len(resp.Records), limit)}
	}
	if resp.Positions[0].HasMore && len(resp.Records) == 0 {
		return shardAnswer{why: "answer says hasMore with no records, so it gives no position to resume from"}
	}
	prev := after
	for _, rec := range resp.Records {
		if rec.Key <= prev {
			return shardAnswer{why: fmt.Sprintf("answer has %q after %q: not in key order past the cursor", rec.Key, prev)}
		}
		prev = rec.Key
	}
	return shardAnswer{resp: &resp}
}
