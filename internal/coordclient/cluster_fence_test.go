package coordclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// fencedCluster is a cluster with one kv key per shard, created by
// operation op-fenced-<shard>.
func fencedCluster(t *testing.T) (*cluster, context.Context, [3]string) {
	t.Helper()
	c := newCluster(t, nil)
	ctx := clusterCtx(t)
	var keys [3]string
	for s := range 3 {
		keys[s] = c.keyOn(s, "kv:fenced")
		if res, err := c.client.CreateEntity(ctx, store.CreateEntity{Meta: m(fmt.Sprint("op-fenced-", s)), Key: keys[s], Value: json.RawMessage("1")}); err != nil || res.Outcome != store.OutcomeOK {
			t.Fatalf("create %s: %+v, %v", keys[s], res, err)
		}
	}
	return c, ctx, keys
}

// logGrowth runs reads and returns how many entries each shard leader's log
// grew by meanwhile. A leader change makes the counts incomparable, so it
// fails the test rather than pass or fail on noise.
func (c *cluster) logGrowth(reads func()) [3]uint64 {
	c.t.Helper()
	measure := func() (pods [3]string, idx [3]uint64) {
		for s := range 3 {
			l := c.raftLeader(s)
			pods[s], idx[s] = l.pod, l.node.Status().LastLogIndex
		}
		return pods, idx
	}
	pods, before := measure()
	reads()
	podsAfter, after := measure()
	if podsAfter != pods {
		c.t.Fatalf("a shard changed leader during the reads (%v -> %v); the log count is not comparable", pods, podsAfter)
	}
	var grew [3]uint64
	for s := range 3 {
		grew[s] = after[s] - before[s]
	}
	return grew
}

const fencedReads = 10

// S4.1: a scan or outbox page that names no read mode goes through the read
// fence, end to end: client -> coordinator -> shard leader. The fence adds
// no log entry per read, so default reads leave every shard leader's last
// log index where it was. The control proves the measurement can see a
// barrier: an explicit strong read, which stays available, commits one per
// read on every shard it touches.
func TestClusterDefaultReadsUseReadFence(t *testing.T) {
	c, ctx, _ := fencedCluster(t)
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
	// One default read first: the first fenced read of a term may commit
	// the term's no-op barrier if it has not committed yet (fence.go, term
	// gate). That is once per term, not per read.
	scan("")
	outbox("")

	grew := c.logGrowth(func() {
		for range fencedReads {
			scan("")
			outbox("")
		}
	})
	for s, got := range grew {
		if got != 0 {
			t.Errorf("shard %d: %d default scans and outbox pages appended %d log entries, want 0 (the read fence)", s, 2*fencedReads, got)
		}
	}

	grew = c.logGrowth(func() {
		for range fencedReads {
			scan(store.ConsistencyStrong)
		}
	})
	for s, got := range grew {
		if got != fencedReads {
			t.Errorf("shard %d: %d explicit strong scans appended %d log entries, want %d (one barrier each)", s, fencedReads, got, fencedReads)
		}
	}
}

// S4.1: point reads (ReadRecord, Operation) on a client with no read mode
// go through the read fence too: no log entry per read. A client that asks
// for strong still gets it, one barrier per read.
func TestClusterDefaultPointReadsUseReadFence(t *testing.T) {
	c, ctx, keys := fencedCluster(t)
	pointReads := func(cl *coordclient.Client) {
		t.Helper()
		for s, key := range keys {
			if rec, err := cl.ReadRecord(ctx, key); err != nil || rec.Version != 1 {
				t.Fatalf("ReadRecord %s = %+v, %v; want version 1", key, rec, err)
			}
			if res, err := cl.Operation(ctx, key, fmt.Sprint("op-fenced-", s)); err != nil || res.Outcome != store.OutcomeOK {
				t.Fatalf("Operation op-fenced-%d = %+v, %v; want OK", s, res, err)
			}
		}
	}
	pointReads(c.client) // the term gate, as above

	grew := c.logGrowth(func() {
		for range fencedReads {
			pointReads(c.client)
		}
	})
	for s, got := range grew {
		if got != 0 {
			t.Errorf("shard %d: %d default point reads appended %d log entries, want 0 (the read fence)", s, 2*fencedReads, got)
		}
	}

	strong, err := coordclient.New(ctx, c.coord.URL, coordclient.WithReadConsistency(store.ConsistencyStrong))
	if err != nil {
		t.Fatalf("strong client: %v", err)
	}
	grew = c.logGrowth(func() {
		for range fencedReads {
			pointReads(strong)
		}
	})
	for s, got := range grew {
		if want := uint64(2 * fencedReads); got != want {
			t.Errorf("shard %d: %d explicit strong point reads appended %d log entries, want %d (one barrier each)", s, 2*fencedReads, got, want)
		}
	}
}
