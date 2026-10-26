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
// missing, never reported as an empty shard (CONTRIBUTING.md, Invariants,
// 7). If no shard answers there is nothing to return: 503 NO_LEADER,
// retryable, unless every shard refused the query with the same 4xx, which
// is the caller's mistake and passes through unchanged.
//
// A shard whose page is full (it said hasMore, or returned exactly limit
// records) has hasMore in its position; one that returned more than limit
// records broke the contract and is a failed shard. The merged consistency
// is the weakest any shard used.
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

	answers := make([]shardAnswer, c.ring.Shards())
	var wg sync.WaitGroup
	for s := range answers {
		wg.Go(func() { answers[s] = c.scanShard(r, s, limit) })
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
	writeJSON(w, http.StatusOK, out)
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

// scanShard asks one shard's leader; an answer without resp is a failure,
// and why says what failed.
func (c *Coordinator) scanShard(r *http.Request, shard, limit int) shardAnswer {
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
	return shardAnswer{resp: &resp}
}
