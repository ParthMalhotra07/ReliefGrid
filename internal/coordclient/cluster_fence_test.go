package coordclient_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// S4.1: a scan or outbox page that names no read mode goes through the read
// fence, end to end: client -> coordinator -> shard leader. The fence adds
// no log entry per read, so twenty default reads leave every shard leader's
// last log index where it was. The control proves the measurement can see a
// barrier: an explicit strong read, which stays available, commits one per
// read on every shard it touches.
func TestClusterDefaultReadsUseReadFence(t *testing.T) {
	c := newCluster(t, nil)
	ctx := clusterCtx(t)
	for s := range 3 {
		key := c.keyOn(s, "kv:fenced")
		if res, err := c.client.CreateEntity(ctx, store.CreateEntity{Meta: m(fmt.Sprint("op-fenced-", s)), Key: key, Value: json.RawMessage("1")}); err != nil || res.Outcome != store.OutcomeOK {
			t.Fatalf("create %s: %+v, %v", key, res, err)
		}
	}
	scan := func(mode store.Consistency) {
		t.Helper()
		got, err := c.client.Scan(ctx, store.ScanQuery{Kind: store.KindKV, Limit: 10, Consistency: mode})
		if err != nil || got.Partial || len(got.Records) != 3 {
			t.Fatalf("scan (mode %q) = %+v, %v; want the 3 keys from every shard", mode, got, err)
		}
	}
	outbox := func(mode store.Consistency) {
		t.Helper()
		for s := range 3 {
			if _, err := c.client.PendingOutbox(ctx, store.OutboxQuery{Shard: s, Limit: 10, Consistency: mode}); err != nil {
				t.Fatalf("outbox shard %d (mode %q): %v", s, mode, err)
			}
		}
	}
	// lastIndexes is each shard leader's pod and last log index.
	lastIndexes := func() (pods [3]string, idx [3]uint64) {
		for s := range 3 {
			l := c.raftLeader(s)
			pods[s], idx[s] = l.pod, l.node.Status().LastLogIndex
		}
		return pods, idx
	}
	// One default read first: the first fenced read of a term may commit
	// the term's no-op barrier if it has not committed yet (fence.go, term
	// gate). That is once per term, not per read.
	scan("")
	outbox("")

	const reads = 10
	pods, before := lastIndexes()
	for range reads {
		scan("")
		outbox("")
	}
	podsAfter, after := lastIndexes()
	if podsAfter != pods {
		t.Fatalf("a shard changed leader during the reads (%v -> %v); the log count is not comparable", pods, podsAfter)
	}
	for s := range 3 {
		if got := after[s] - before[s]; got != 0 {
			t.Errorf("shard %d: %d default scans and outbox pages appended %d log entries, want 0 (the read fence)", s, 2*reads, got)
		}
	}

	pods, before = lastIndexes()
	for range reads {
		scan(store.ConsistencyStrong)
	}
	podsAfter, after = lastIndexes()
	if podsAfter != pods {
		t.Fatalf("a shard changed leader during the strong reads (%v -> %v)", pods, podsAfter)
	}
	for s := range 3 {
		if got := after[s] - before[s]; got != reads {
			t.Errorf("shard %d: %d explicit strong scans appended %d log entries, want %d (one barrier each)", s, reads, got, reads)
		}
	}
}
