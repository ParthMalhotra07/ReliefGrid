package coordinator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// scan is scatter-gather: the caller's query goes unchanged to every
// shard's leader at once, and the answers are merged in shard order.
// A shard that fails (no leader within the budget, an error, a garbled
// answer) is listed in failedShards with partial=true; its records are
// missing, never reported as an empty shard (invariant 7). If no shard
// answers there is nothing to return: 503 NO_LEADER, retryable.
//
// A shard whose page is full (it said hasMore, or returned exactly limit
// records) has hasMore in its position.
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
	if !c.checkRing(w, r, nil) {
		return
	}

	type shardAnswer struct {
		resp *internalapi.ScanResponse
		why  string
	}
	answers := make([]shardAnswer, c.ring.Shards())
	var wg sync.WaitGroup
	for s := range answers {
		wg.Go(func() {
			resp, why := c.scanShard(r, s)
			answers[s] = shardAnswer{resp, why}
		})
	}
	wg.Wait()

	out := internalapi.ScanResponse{RingVersion: c.ring.Version(), Records: []store.Record{}, Positions: []store.ShardPosition{}}
	var whys []string
	var failedShards []int
	for s, a := range answers {
		if a.resp == nil {
			failedShards = append(failedShards, s)
			whys = append(whys, fmt.Sprintf("shard %d: %s", s, a.why))
			continue
		}
		out.Records = append(out.Records, a.resp.Records...)
		pos := a.resp.Positions[0]
		pos.HasMore = pos.HasMore || len(a.resp.Records) >= limit
		out.Positions = append(out.Positions, pos)
		if out.Consistency == "" || a.resp.Consistency == internalapi.ScanResponseConsistencyStale {
			out.Consistency = a.resp.Consistency // the weakest read used
		}
	}
	if len(out.Positions) == 0 {
		writeUnavailable(w, false, "no shard answered the scan: "+fmt.Sprint(whys))
		return
	}
	if len(failedShards) > 0 {
		out.Partial, out.FailedShards = true, &failedShards
	}
	writeJSON(w, http.StatusOK, out)
}

// scanShard asks one shard's leader; a nil answer is a failure, and why
// says what failed.
func (c *Coordinator) scanShard(r *http.Request, shard int) (*internalapi.ScanResponse, string) {
	ans, _, why := c.forward(r.Context(), shard, r, nil, false)
	if ans == nil {
		return nil, why
	}
	if ans.status != http.StatusOK {
		var e internalapi.Error
		_ = json.Unmarshal(ans.body, &e)
		return nil, fmt.Sprintf("%d %s %s", ans.status, e.Code, e.Message)
	}
	var resp internalapi.ScanResponse
	if err := json.Unmarshal(ans.body, &resp); err != nil {
		return nil, "garbled answer: " + err.Error()
	}
	if len(resp.Positions) != 1 || resp.Positions[0].Shard != shard || resp.Partial {
		return nil, fmt.Sprintf("answer is not one complete shard-%d page: positions %+v", shard, resp.Positions)
	}
	if resp.Records == nil {
		return nil, "answer has no records list"
	}
	return &resp, ""
}
