package fsm_test

import (
	"context"
	"encoding/json"
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
