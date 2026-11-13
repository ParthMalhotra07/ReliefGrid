package appmetrics

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

type observation struct{ count, age map[[2]string]float64 }

// Sample performs bounded remote work outside the scrape path. It commits new
// workflow gauges only after a COMPLETE cursor walk. A failed/partial page
// retains all previous values, marks them stale, and never reports empty work.
func (p *Process) Sample(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	next, answered, err := p.walk(ctx)
	for shard := range 3 {
		value := 0.
		if answered[shard] {
			value = 1
		}
		p.shardRead.WithLabelValues(strconv.Itoa(shard)).Set(value)
	}
	if err != nil {
		p.valid.Set(0)
	} else {
		for _, s := range states {
			for _, u := range urgencies {
				key := [2]string{s, u}
				p.counts.WithLabelValues(s, u).Set(next.count[key])
				p.ages.WithLabelValues(s, u).Set(next.age[key])
			}
		}
		p.lastSuccess.Set(float64(p.now().UnixMilli()) / 1000)
		p.valid.Set(1)
	}
	var wg sync.WaitGroup
	for shard := range 3 {
		wg.Go(func() { p.sampleOutbox(ctx, shard) })
	}
	wg.Wait()
	return err
}
func (p *Process) walk(ctx context.Context) (observation, map[int]bool, error) {
	out := observation{count: map[[2]string]float64{}, age: map[[2]string]float64{}}
	answered := map[int]bool{}
	expected := map[int]bool{0: true, 1: true, 2: true}
	seenCursor, seenRecords := map[string]bool{}, map[string]bool{}
	cursor := ""
	at := p.now().UnixMilli()
	for pages := 0; pages < 100; pages++ {
		page, err := p.source.Scan(ctx, store.ScanQuery{Kind: store.KindRequest, Statuses: states, Limit: 100, Cursor: cursor, Consistency: store.ConsistencyLinearizable})
		if err != nil {
			return out, answered, fmt.Errorf("workflow metrics scan: %w", err)
		}
		if page.Partial || len(page.FailedShards) > 0 {
			for _, s := range page.FailedShards {
				delete(answered, s)
			}
			return out, answered, fmt.Errorf("workflow metrics: partial scan")
		}
		current, more := map[int]bool{}, map[int]bool{}
		for _, pos := range page.Positions {
			if !expected[pos.Shard] || current[pos.Shard] {
				return out, answered, fmt.Errorf("workflow metrics: duplicate or unexpected shard")
			}
			current[pos.Shard] = true
			answered[pos.Shard] = true
			if pos.HasMore {
				more[pos.Shard] = true
			}
		}
		if len(current) != len(expected) || len(page.Records) > 100*len(expected) || (page.NextCursor == "") != (len(more) == 0) {
			return out, answered, fmt.Errorf("workflow metrics: incomplete continuation")
		}
		for _, r := range page.Records {
			if r.Request == nil || r.Deleted || seenRecords[r.Key] {
				return out, answered, fmt.Errorf("workflow metrics: malformed or repeated record")
			}
			seenRecords[r.Key] = true
			state := string(r.Request.Status)
			valid := false
			for _, s := range states {
				if s == state {
					valid = true
					break
				}
			}
			if !valid || r.Request.CreatedAt <= 0 {
				return out, answered, fmt.Errorf("workflow metrics: invalid state or creation timestamp")
			}
			key := [2]string{state, urgency(r.Request.Urgency)}
			out.count[key]++
			out.age[key] = max(out.age[key], float64(max(int64(0), at-r.Request.CreatedAt))/1000)
		}
		if page.NextCursor == "" {
			return out, answered, nil
		}
		if seenCursor[page.NextCursor] || page.NextCursor == cursor {
			return out, answered, fmt.Errorf("workflow metrics: cursor did not advance")
		}
		seenCursor[page.NextCursor] = true
		cursor = page.NextCursor
		expected = more
	}
	return out, answered, fmt.Errorf("workflow metrics: bounded walk exhausted")
}
func (p *Process) sampleOutbox(ctx context.Context, shard int) {
	label := strconv.Itoa(shard)
	rows, err := p.source.PendingOutbox(ctx, store.OutboxQuery{Shard: shard, Limit: 100, Consistency: store.ConsistencyLinearizable})
	if err != nil || len(rows) > 100 {
		p.batchValid.WithLabelValues(label).Set(0)
		return
	}
	age := 0.
	for _, e := range rows {
		if e.CreatedAt <= 0 {
			p.batchValid.WithLabelValues(label).Set(0)
			return
		}
		age = max(age, float64(max(int64(0), p.now().UnixMilli()-e.CreatedAt))/1000)
	}
	p.batch.WithLabelValues(label).Set(float64(len(rows)))
	p.batchAge.WithLabelValues(label).Set(age)
	more := 0.
	if len(rows) == 100 {
		more = 1
	}
	p.batchTruncated.WithLabelValues(label).Set(more)
	p.batchValid.WithLabelValues(label).Set(1)
}
