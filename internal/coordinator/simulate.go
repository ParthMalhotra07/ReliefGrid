package coordinator

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/ring"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	defaultSimulationSample = 10_000
	maxSimulationSample     = 1_000_000
)

// adminRingSimulate reports how many keys would change shard if a ring with
// other numbers were published (S4.5b). The candidate comes from the same
// token builder as the active ring, at the next ringVersion; it is never
// loaded, so routing is untouched. Keys are a synthetic sample, or with
// source=stored every stored key from a complete scan: a scan that misses a
// shard is an error, never a count over fewer keys (CONTRIBUTING.md,
// Invariants, 7).
func (c *Coordinator) adminRingSimulate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	shards, err := strconv.Atoi(q.Get("shards"))
	if err != nil || shards < 1 {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "shards is required, a whole number of at least 1")
		return
	}
	vnodes := c.ring.VNodesPerShard()
	if q.Has("vnodes") {
		if vnodes, err = strconv.Atoi(q.Get("vnodes")); err != nil || vnodes < 1 {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, "vnodes must be a whole number of at least 1")
			return
		}
	}
	source := internalapi.RingSimulationSource(q.Get("source"))
	switch source {
	case "":
		source = internalapi.RingSimulationSourceSample
	case internalapi.RingSimulationSourceSample, internalapi.RingSimulationSourceStored:
	default:
		writeError(w, http.StatusBadRequest, internalapi.INVALID, "source must be sample or stored")
		return
	}
	sample := defaultSimulationSample
	if q.Has("sample") {
		if source == internalapi.RingSimulationSourceStored {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, "sample is for synthetic keys; source=stored places every stored key")
			return
		}
		if sample, err = strconv.Atoi(q.Get("sample")); err != nil || sample < 1 || sample > maxSimulationSample {
			writeError(w, http.StatusBadRequest, internalapi.INVALID, fmt.Sprintf("sample must be 1 to %d keys", maxSimulationSample))
			return
		}
	}
	candidate, err := ring.Build(c.ring.Version()+1, shards, vnodes)
	if err != nil {
		writeError(w, http.StatusBadRequest, internalapi.INVALID, err.Error())
		return
	}

	t := newTally(c.ring, candidate)
	if source == internalapi.RingSimulationSourceSample {
		for i := range sample {
			prefix := store.RequestKeyPrefix
			if i%2 == 1 {
				prefix = store.ResourceKeyPrefix
			}
			t.add(prefix + strconv.Itoa(i/2))
		}
	} else if !c.tallyStored(w, r, t) {
		return
	}
	writeJSON(w, http.StatusOK, t.result(source, c.ringInfo(), candidate))
}

// tallyStored adds every stored key of every kind on every shard to t,
// paging each shard's leader to its end at the current ringVersion, with
// the default (strong) read. It writes the 503 and returns false if any
// shard could not be read completely.
func (c *Coordinator) tallyStored(w http.ResponseWriter, r *http.Request, t *tally) bool {
	keys := make([][]string, c.ring.Shards())
	whys := make([]string, c.ring.Shards())
	var wg sync.WaitGroup
	for s := range keys {
		wg.Go(func() { keys[s], whys[s] = c.scanAll(r, s) })
	}
	wg.Wait()
	var failed []string
	for s, why := range whys {
		if why != "" {
			failed = append(failed, fmt.Sprintf("shard %d: %s", s, why))
		}
	}
	if len(failed) > 0 {
		writeUnavailable(w, false, fmt.Sprintf("stored keys scan is incomplete, so nothing is reported: %v", failed))
		return false
	}
	for _, ks := range keys {
		for _, k := range ks {
			t.add(k)
		}
	}
	return true
}

// scanAll returns every key of every kind stored on shard, or why the scan
// stopped short. scanShard checks each page is in key order past after, so
// every page makes progress.
func (c *Coordinator) scanAll(r *http.Request, shard int) (keys []string, why string) {
	for _, kind := range []store.Kind{store.KindRequest, store.KindResource, store.KindKV} {
		after := ""
		for {
			q := url.Values{"kind": {string(kind)}, "limit": {strconv.Itoa(store.MaxScanLimit)},
				"ringVersion": {strconv.FormatUint(c.ring.Version(), 10)}}
			if after != "" {
				q.Set("after", after)
			}
			req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, "/internal/scan?"+q.Encode(), nil)
			if err != nil {
				return nil, err.Error()
			}
			a := c.scanShard(req, shard, store.MaxScanLimit, after)
			if a.resp == nil {
				return nil, fmt.Sprintf("%s keys after %q: %s", kind, after, a.why)
			}
			for _, rec := range a.resp.Records {
				keys = append(keys, rec.Key)
			}
			recs := a.resp.Records
			if !a.resp.Positions[0].HasMore && len(recs) < store.MaxScanLimit {
				break
			}
			after = recs[len(recs)-1].Key
		}
	}
	return keys, ""
}

// tally places keys on the active and the candidate ring.
type tally struct {
	before, after *ring.Ring
	keys, moved   int
	perBefore     []int
	perAfter      []int
	matrix        map[string]map[string]int
}

func newTally(before, after *ring.Ring) *tally {
	return &tally{before: before, after: after, perBefore: make([]int, before.Shards()),
		perAfter: make([]int, after.Shards()), matrix: map[string]map[string]int{}}
}

// add places key on both rings. A key neither ring can route (an empty ID)
// is not counted: it cannot be stored, and the sample never makes one.
func (t *tally) add(key string) {
	from, err := t.before.Owner(key)
	if err != nil {
		return
	}
	to, _ := t.after.Owner(key) // the same key rules on every ring
	t.keys++
	t.perBefore[from]++
	t.perAfter[to]++
	if from != to {
		t.moved++
	}
	f, s := strconv.Itoa(from), strconv.Itoa(to)
	if t.matrix[f] == nil {
		t.matrix[f] = map[string]int{}
	}
	t.matrix[f][s]++
}

func (t *tally) result(source internalapi.RingSimulationSource, current internalapi.Ring, candidate *ring.Ring) internalapi.RingSimulation {
	out := internalapi.RingSimulation{Source: source, CurrentRing: current,
		Candidate: internalapi.Ring{RingVersion: candidate.Version(), Hash: internalapi.Xxhash64, Shards: candidate.Shards(),
			VnodesPerShard: candidate.VNodesPerShard(), Checksum: candidate.Checksum()},
		Keys: t.keys, Moved: t.moved, Matrix: t.matrix, PerShardBefore: t.perBefore, PerShardAfter: t.perAfter}
	if t.keys > 0 {
		out.MovedFraction = float64(t.moved) / float64(t.keys)
	}
	// The least movement for a shard-count change: the keys the new shards
	// must take, or the keys the removed shards held.
	switch s, s2 := float64(t.before.Shards()), float64(t.after.Shards()); {
	case s2 > s:
		out.IdealFraction = 1 - s/s2
	case s2 < s:
		out.IdealFraction = 1 - s2/s
	}
	return out
}
