package fsm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func keysOf(recs []store.Record) []string {
	keys := []string{}
	for _, r := range recs {
		keys = append(keys, r.Key)
	}
	return keys
}

// Scan with after returns only keys strictly after it, in key order, with
// the filters still applied; tombstones are skipped, and the after key
// itself need not exist.
func TestScanAfterKey(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		createResource(h, resource(id, store.TypeWater, store.ResourceAvailable))
	}
	createResource(h, resource("bb", store.TypeFood, store.ResourceAvailable)) // filtered out by type
	h.apply(store.CreateEntity{Meta: meta("c-kv"), Key: "kv:z", Value: json.RawMessage(`1`)})
	h.apply(store.CreateEntity{Meta: meta("c-kv-d"), Key: "kv:gone", Value: json.RawMessage(`1`)})
	h.apply(store.CompareAndSet{Meta: meta("del-kv"), Key: "kv:gone", ExpectedVersion: 1, Delete: true})

	water := store.ScanQuery{Kind: store.KindResource, Type: store.TypeWater, Limit: 100}
	for _, tc := range []struct {
		after string
		limit int
		want  []string
	}{
		{"", 100, []string{"res:a", "res:b", "res:c", "res:d", "res:e"}},
		{"res:b", 100, []string{"res:c", "res:d", "res:e"}},
		{"res:bb", 100, []string{"res:c", "res:d", "res:e"}}, // not a match itself
		{"res:b0", 100, []string{"res:c", "res:d", "res:e"}}, // absent key between res:b and res:bb
		{"res:c", 1, []string{"res:d"}},
		{"res:e", 100, []string{}},
		{"res:", 2, []string{"res:a", "res:b"}},
	} {
		q := water
		q.Limit = tc.limit
		got, err := h.f.Scan(ctx, q, tc.after)
		if err != nil {
			t.Fatalf("Scan after %q: %v", tc.after, err)
		}
		if k := keysOf(got); !reflect.DeepEqual(k, tc.want) {
			t.Errorf("Scan after %q limit %d = %v, want %v", tc.after, tc.limit, k, tc.want)
		}
	}

	kv, err := h.f.Scan(ctx, store.ScanQuery{Kind: store.KindKV, Limit: 10}, "kv:a")
	if err != nil || !reflect.DeepEqual(keysOf(kv), []string{"kv:z"}) {
		t.Fatalf("kv scan after kv:a = %v, %v; want [kv:z] (kv:gone is a tombstone)", keysOf(kv), err)
	}
}

// Paging with after = the last key of each page reads every key exactly
// once, in order, and the page after the last one is empty.
func TestScanAfterPagesEveryKeyOnce(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	var want []string
	for i := range 250 {
		k := fmt.Sprintf("kv:%04d", i)
		h.apply(store.CreateEntity{Meta: meta("c-" + k), Key: k, Value: json.RawMessage(`1`)})
		want = append(want, k)
	}
	var got []string
	after := ""
	for page := 0; ; page++ {
		if page > 10 {
			t.Fatalf("still paging after %d pages: %d keys so far", page, len(got))
		}
		recs, err := h.f.Scan(ctx, store.ScanQuery{Kind: store.KindKV, Limit: 30}, after)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if len(recs) == 0 {
			break
		}
		got = append(got, keysOf(recs)...)
		after = recs[len(recs)-1].Key
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paged %d keys, want %d in order (first %v)", len(got), len(want), got[:min(5, len(got))])
	}
}
