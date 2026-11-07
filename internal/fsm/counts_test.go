package fsm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Counts (S1.4 heartbeat recordCounts): live rows by kind, tombstones apart.
func TestCounts(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if c, err := h.f.Counts(ctx); err != nil || c != (fsm.Counts{}) {
		t.Fatalf("empty FSM counts = %+v, %v", c, err)
	}
	createResource(h, resource("r1", store.TypeWater, store.ResourceAvailable))
	createResource(h, resource("r2", store.TypeFood, store.ResourceOffline))
	expectOutcome(t, "q1", h.apply(store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater)}), store.OutcomeOK)
	for _, k := range []string{"kv:a", "kv:b", "kv:c"} {
		expectOutcome(t, k, h.apply(store.CompareAndSet{Meta: meta("c-" + k), Key: k, Value: json.RawMessage(`1`)}), store.OutcomeOK)
	}
	expectOutcome(t, "delete kv:c", h.apply(store.CompareAndSet{Meta: meta("d-kv:c"), Key: "kv:c", ExpectedVersion: 1, Delete: true}), store.OutcomeOK)
	// A rejection adds no row.
	expectOutcome(t, "exists", h.apply(store.CreateEntity{Meta: meta("c-q1-again"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater)}), store.OutcomeExists)

	c, err := h.f.Counts(ctx)
	if want := (fsm.Counts{Request: 1, Resource: 2, KV: 2, Tombstones: 1}); err != nil || c != want {
		t.Fatalf("counts = %+v, %v; want %+v", c, err, want)
	}
}

// OutboxStats (S5.2, #113): the exact unpublished count and the oldest
// unpublished row's proposer time, not a capped batch.
func TestOutboxStats(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if s, err := h.f.OutboxStats(ctx); err != nil || s != (fsm.OutboxStats{}) {
		t.Fatalf("empty FSM outbox stats = %+v, %v", s, err)
	}
	ev := func(id string) []store.OutboxEvent {
		return []store.OutboxEvent{{EventID: id, EventType: "request.created", SchemaVersion: 1, Exchange: "relief.events",
			RoutingKey: "request.created", Payload: json.RawMessage(`{}`)}}
	}
	// 150 events, more than PendingOutbox's batch of 100, at rising proposer times.
	for i := range 150 {
		id := fmt.Sprintf("q%03d", i)
		m := store.Meta{OperationID: "c-" + id, At: t0 + int64(i)*1000}
		expectOutcome(t, id, h.apply(store.CreateEntity{Meta: m, Key: store.RequestKey(id), Request: request(id, store.TypeWater), Outbox: ev("ev-" + id)}), store.OutcomeOK)
	}
	expectOutcome(t, "mark", h.apply(store.MarkOutboxPublished{Meta: meta("mark"), Shard: 0, EventIDs: []string{"ev-q000", "ev-q001"}}), store.OutcomeOK)
	s, err := h.f.OutboxStats(ctx)
	if want := (fsm.OutboxStats{Pending: 148, OldestCreatedAt: t0 + 2000, AppliedIndex: h.idx}); err != nil || s != want {
		t.Fatalf("outbox stats = %+v, %v; want %+v", s, err, want)
	}
}

// OutboxStats reads applied_index in the same SQLite snapshot as COUNT/MIN
// (#132), so the index it returns is the one the count was taken at, even
// while applies commit concurrently. Every apply here adds exactly one
// unpublished row, so a paired observation always has Pending == AppliedIndex.
func TestOutboxStatsPairedWithAppliedIndex(t *testing.T) {
	ctx := context.Background()
	f, err := fsm.Open(ctx, "file:"+filepath.Join(t.TempDir(), "fsm-0.db")) // WAL: reads run beside the writer
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	const n = 200
	done := make(chan error, 1)
	go func() {
		for i := range n {
			id := fmt.Sprintf("p%03d", i)
			entry, err := fsm.Encode(store.CreateEntity{Meta: meta("c-" + id), Key: store.RequestKey(id), Request: request(id, store.TypeWater),
				Outbox: []store.OutboxEvent{{EventID: "ev-" + id, EventType: "request.created", SchemaVersion: 1, Exchange: "relief.events",
					RoutingKey: "request.created", Payload: json.RawMessage(`{}`)}}})
			if err == nil {
				_, err = f.Apply(uint64(i+1), entry)
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for observed := 0; ; observed++ {
		s, err := f.OutboxStats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if uint64(s.Pending) != s.AppliedIndex {
			t.Fatalf("observation %d: pending %d at applied index %d; the pair is from two snapshots", observed, s.Pending, s.AppliedIndex)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			if s, err := f.OutboxStats(ctx); err != nil || s.Pending != n || s.AppliedIndex != n {
				t.Fatalf("final stats = %+v, %v; want %d pending at index %d", s, err, n, n)
			}
			return
		default:
		}
	}
}
