package memstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store/storetest"
)

const t0 = int64(1_792_000_000_000)

func newStore(t *testing.T) *memstore.Store {
	t.Helper()
	s, err := memstore.New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// Verify (adapter): two same-version CAS calls, both in flight before either
// is applied; exactly one wins.
func TestConcurrentSameVersionCASExactlyOneWins(t *testing.T) {
	ctx := testCtx(t)
	base := newStore(t)
	if res, err := base.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: "create", At: t0}, Key: "kv:x", Value: json.RawMessage(`0`)}); err != nil || res.Outcome != store.OutcomeOK {
		t.Fatalf("create: %+v, %v", res, err)
	}

	var arrived sync.WaitGroup
	arrived.Add(2)
	s := memstore.WithFaults(base, func(c memstore.Call) memstore.Fault {
		if c.Method != "CompareAndSet" {
			return memstore.Fault{}
		}
		return memstore.Fault{Before: func(context.Context) error { arrived.Done(); arrived.Wait(); return nil }}
	})

	var wg sync.WaitGroup
	results := make([]store.Result, 2)
	errs := make([]error, 2)
	for i, v := range []string{`"a"`, `"b"`} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = s.CompareAndSet(ctx, store.CompareAndSet{
				Meta: store.Meta{OperationID: "cas-" + v, At: t0}, Key: "kv:x", ExpectedVersion: 1, Value: json.RawMessage(v),
			})
		}()
	}
	wg.Wait()

	wins := 0
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("call %d: %v", i, errs[i])
		}
		switch results[i].Outcome {
		case store.OutcomeOK:
			wins++
		case store.OutcomeVersionConflict:
		default:
			t.Fatalf("call %d: outcome %s", i, results[i].Outcome)
		}
	}
	if wins != 1 {
		t.Fatalf("%d winners, want exactly 1: %+v", wins, results)
	}
	if rec, err := base.ReadRecord(ctx, "kv:x"); err != nil || rec.Version != 2 {
		t.Fatalf("record = %+v, %v; want version 2", rec, err)
	}
}

// A lost response after commit is an unknown outcome; retrying the same
// command returns the committed result and claims nothing twice.
func TestDropResponseThenRetryReturnsCommittedResult(t *testing.T) {
	ctx := testCtx(t)
	base := newStore(t)
	r := &store.Resource{ID: "r1", Type: store.TypeWater, Status: store.ResourceAvailable, Reusable: true,
		AvailableFrom: t0 - 1, AvailableUntil: t0 + 1}
	if res, err := base.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: "c-r1", At: t0}, Key: store.ResourceKey("r1"), Resource: r}); err != nil || res.Outcome != store.OutcomeOK {
		t.Fatalf("create: %+v, %v", res, err)
	}

	drop := true
	s := memstore.WithFaults(base, func(c memstore.Call) memstore.Fault {
		if c.Method == "TryClaim" && drop {
			drop = false
			return memstore.Fault{DropResponse: true}
		}
		return memstore.Fault{}
	})
	claim := store.TryClaim{Meta: store.Meta{OperationID: "claim-a", At: t0}, ResourceKey: store.ResourceKey("r1"), RequestID: "q1",
		Attempt: store.Attempt{AttemptID: "a", ResourceID: "r1", Fingerprint: "fp-a"}, Type: store.TypeWater}

	if _, err := s.TryClaim(ctx, claim); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("dropped response err = %v, want ErrUnknownOutcome", err)
	}
	res, err := s.TryClaim(ctx, claim)
	if err != nil || res.Outcome != store.ClaimSuccess || !res.Replayed {
		t.Fatalf("retry = %+v, %v; want replayed SUCCESS", res, err)
	}
	if rec, _ := base.ReadRecord(ctx, store.ResourceKey("r1")); rec.Version != 2 {
		t.Fatalf("resource version = %d, want 2 (claimed exactly once)", rec.Version)
	}
}

// Invariant 7: a failed shard makes the scan partial, never empty-and-complete.
func TestScanWithFailedShardIsPartial(t *testing.T) {
	ctx := testCtx(t)
	base := newStore(t)
	for i, id := range []string{"r1", "r2", "r3", "r4", "r5", "r6"} {
		r := &store.Resource{ID: id, Type: store.TypeWater, Status: store.ResourceAvailable, AvailableFrom: t0, AvailableUntil: t0 + 1}
		if _, err := base.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: "c" + string(rune('0'+i)), At: t0}, Key: store.ResourceKey(id), Resource: r}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	q := store.ScanQuery{Kind: store.KindResource, Type: store.TypeWater, Limit: 100}
	full, err := base.Scan(ctx, q)
	if err != nil || full.Partial || len(full.Records) != 6 || len(full.Positions) != base.Shards() {
		t.Fatalf("full scan = partial=%v records=%d positions=%d, %v", full.Partial, len(full.Records), len(full.Positions), err)
	}

	s := memstore.WithFaults(base, func(c memstore.Call) memstore.Fault {
		if c.Method == "Scan" {
			return memstore.Fault{FailShards: []int{1}}
		}
		return memstore.Fault{}
	})
	part, err := s.Scan(ctx, q)
	if err != nil {
		t.Fatalf("partial scan: %v", err)
	}
	if !part.Partial || !reflect.DeepEqual(part.FailedShards, []int{1}) {
		t.Fatalf("scan with shard 1 down = partial=%v failed=%v, want partial=true failed=[1]", part.Partial, part.FailedShards)
	}
	if len(part.Records) >= len(full.Records) {
		t.Fatalf("partial scan returned %d of %d records; shard 1 rows should be missing", len(part.Records), len(full.Records))
	}
}

// The store.Store contract suite: coordclient runs the same one.
func TestStoreContract(t *testing.T) {
	s := newStore(t)
	storetest.Run(t, func(*testing.T) store.Store { return s })
}
