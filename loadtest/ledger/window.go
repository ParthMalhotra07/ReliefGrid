package ledger

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
)

// Latency is a nearest-rank summary in milliseconds; zero with no samples.
type Latency struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

func latency(samples []float64) Latency {
	if len(samples) == 0 {
		return Latency{}
	}
	sort.Float64s(samples)
	rank := func(p float64) float64 {
		i := int(p*float64(len(samples))+0.999999) - 1
		return samples[min(max(i, 0), len(samples)-1)]
	}
	return Latency{N: len(samples), P50: rank(0.50), P99: rank(0.99), Max: samples[len(samples)-1]}
}

// ShardWindow is one shard's writes and reads around a fault.
type ShardWindow struct {
	Shard int `json:"shard"`
	// Acks by when they were acknowledged: before from, in [from, to], after to.
	AcksBefore int `json:"acksBefore"`
	AcksDuring int `json:"acksDuring"`
	AcksAfter  int `json:"acksAfter"`
	// AcksInsideFault: writes that started at or after from+grace and were
	// acknowledged by to, so committed while the fault fully held.
	AcksInsideFault int `json:"acksInsideFault"`
	// Reads that started in [from, to]; those served (no error); those
	// served that started at or after from+grace and ended by to.
	ReadsDuring            int `json:"readsDuring"`
	ReadsServedDuring      int `json:"readsServedDuring"`
	ReadsServedInsideFault int `json:"readsServedInsideFault"`
	// The same two, for probe reads only, by probe.
	ProbeReadsDuring       map[string]int `json:"probeReadsDuring"`
	ProbeServedInsideFault map[string]int `json:"probeServedInsideFault"`
	// FirstAckAfterFaultMs: from from to the first ack of a write that
	// started at or after from; FirstAckAfterHealMs likewise from to. -1: none.
	FirstAckAfterFaultMs int64 `json:"firstAckAfterFaultMs"`
	FirstAckAfterHealMs  int64 `json:"firstAckAfterHealMs"`
	// Latencies of the writes acknowledged and the reads started in [from, to].
	WriteLatencyMs Latency `json:"writeLatencyMs"`
	ReadLatencyMs  Latency `json:"readLatencyMs"`
	// ErrorsDuring: failed write attempts in [from, to] (recorded, not hidden).
	ErrorsDuring int `json:"errorsDuring"`
}

// WindowStats is the ledger lined up with a harness's fault: FromMs and
// ToMs are the fault's start and heal (unix ms, the harness's wall clock),
// GraceMs how long after FromMs the fault is taken to fully hold (a lease or
// an in-flight request may still finish just after it began).
type WindowStats struct {
	FromMs  int64         `json:"fromMs"`
	ToMs    int64         `json:"toMs"`
	GraceMs int64         `json:"graceMs"`
	Shards  []ShardWindow `json:"shards"`
}

// Window computes WindowStats per shard, with shardOf mapping each key to
// its shard. Wall ms is StartedAtMs + ns/1e6: the harness and the load share
// the host's wall clock on kind, so skew is a few ms, well inside GraceMs.
func (l *Ledger) Window(fromMs, toMs, graceMs int64, shardOf func(key string) (int, error)) (WindowStats, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.StartedAtMs == 0 {
		return WindowStats{}, errors.New("ledger: no startedAtMs, so the run can't be lined up with a fault")
	}
	wall := func(ns int64) int64 { return l.StartedAtMs + ns/1_000_000 }
	inside := fromMs + graceMs
	byShard := map[int]*ShardWindow{}
	var errs []error
	get := func(key string) *ShardWindow {
		s, err := shardOf(key)
		if err != nil {
			errs = append(errs, fmt.Errorf("ledger: shard of %s: %w", key, err))
			return nil
		}
		w := byShard[s]
		if w == nil {
			w = &ShardWindow{Shard: s, FirstAckAfterFaultMs: -1, FirstAckAfterHealMs: -1,
				ProbeReadsDuring: map[string]int{}, ProbeServedInsideFault: map[string]int{}}
			byShard[s] = w
		}
		return w
	}
	writeLat, readLat := map[int][]float64{}, map[int][]float64{}
	first := func(cur *int64, since, at int64) {
		if d := at - since; *cur < 0 || d < *cur {
			*cur = d
		}
	}
	for _, e := range l.Acked {
		if e.AckNs == 0 || e.Resolved {
			continue // not acknowledged while the run was being timed
		}
		w := get(e.Key)
		if w == nil {
			continue
		}
		start, at := wall(e.StartNs), wall(e.AckNs)
		switch {
		case at < fromMs:
			w.AcksBefore++
		case at <= toMs:
			w.AcksDuring++
			if e.StartNs > 0 {
				writeLat[w.Shard] = append(writeLat[w.Shard], float64(e.AckNs-e.StartNs)/1e6)
			}
		default:
			w.AcksAfter++
		}
		if e.StartNs == 0 {
			continue
		}
		if start >= inside && at <= toMs {
			w.AcksInsideFault++
		}
		if start >= fromMs {
			first(&w.FirstAckAfterFaultMs, fromMs, at)
		}
		if start >= toMs {
			first(&w.FirstAckAfterHealMs, toMs, at)
		}
	}
	for _, r := range l.Reads {
		start, end := wall(r.StartNs), wall(r.EndNs)
		if start < fromMs || start > toMs {
			continue
		}
		w := get(r.Key)
		if w == nil {
			continue
		}
		served := r.Error == ""
		w.ReadsDuring++
		readLat[w.Shard] = append(readLat[w.Shard], float64(r.EndNs-r.StartNs)/1e6)
		if r.Target != "" {
			w.ProbeReadsDuring[r.Target]++
		}
		if !served {
			continue
		}
		w.ReadsServedDuring++
		if start >= inside && end <= toMs {
			w.ReadsServedInsideFault++
			if r.Target != "" {
				w.ProbeServedInsideFault[r.Target]++
			}
		}
	}
	for _, e := range l.Errors {
		if e.At >= fromMs && e.At <= toMs {
			if w := get(e.Key); w != nil {
				w.ErrorsDuring++
			}
		}
	}
	out := WindowStats{FromMs: fromMs, ToMs: toMs, GraceMs: graceMs, Shards: []ShardWindow{}}
	for _, s := range slices.Sorted(maps.Keys(byShard)) {
		w := byShard[s]
		w.WriteLatencyMs, w.ReadLatencyMs = latency(writeLat[s]), latency(readLat[s])
		out.Shards = append(out.Shards, *w)
	}
	return out, errors.Join(errs...)
}
