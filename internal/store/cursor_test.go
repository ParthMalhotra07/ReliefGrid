package store_test

import (
	"maps"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// A cursor survives a round trip, including keys with bytes a URL or JSON
// would mangle, and a shard still to be read from its first key ("").
func TestScanCursorRoundTrip(t *testing.T) {
	for _, c := range []store.ScanCursor{
		{RingVersion: 1, Query: "0123456789abcdef", After: map[int]string{0: "kv:a", 2: "res:s-77"}},
		{RingVersion: 7, After: map[int]string{1: ""}},
		{RingVersion: 1 << 62, After: map[int]string{0: "kv:a&b=c/d?e+f%20 \xff\x00", 1: "req:é"}},
	} {
		enc := store.EncodeScanCursor(c)
		if enc == "" || strings.ContainsAny(enc, "+/=&?% ") {
			t.Fatalf("Encode(%+v) = %q, want a non-empty URL-safe string", c, enc)
		}
		got, err := store.DecodeScanCursor(enc)
		if err != nil || got.RingVersion != c.RingVersion || got.Query != c.Query || !maps.Equal(got.After, c.After) {
			t.Fatalf("Decode(Encode(%+v)) = %+v, %v", c, got, err)
		}
	}
}

// No shard left to read is the end of the walk: the empty cursor.
func TestScanCursorExhaustedIsEmpty(t *testing.T) {
	for _, after := range []map[int]string{nil, {}} {
		if enc := store.EncodeScanCursor(store.ScanCursor{RingVersion: 1, After: after}); enc != "" {
			t.Fatalf("Encode with After=%v = %q, want \"\"", after, enc)
		}
	}
}

// Anything Encode would not produce is rejected, never read as "start over".
func TestScanCursorRejectsMalformed(t *testing.T) {
	for _, s := range []string{
		"",
		"not a cursor",
		"!!!!",
		strings.TrimSuffix(store.EncodeScanCursor(store.ScanCursor{RingVersion: 1, After: map[int]string{0: "kv:a"}}), "A") + "*",
		store.EncodeScanCursor(store.ScanCursor{RingVersion: 1, After: map[int]string{-1: "kv:a"}}),
		store.EncodeScanCursor(store.ScanCursor{RingVersion: 0, After: map[int]string{0: "kv:a"}}),
	} {
		if c, err := store.DecodeScanCursor(s); err == nil {
			t.Errorf("Decode(%q) = %+v, want an error", s, c)
		}
	}
}

// The query hash covers kind, type, statuses, geohash prefixes and limit,
// ignores the order of statuses and prefixes, and ignores the cursor.
func TestScanQueryHash(t *testing.T) {
	base := store.ScanQuery{Kind: store.KindResource, Type: store.TypeWater, Statuses: []string{"AVAILABLE", "OFFLINE"},
		GeohashPrefixes: []string{"ttn", "tsq"}, Limit: 100}
	h := store.ScanQueryHash(base)
	if len(h) != 16 {
		t.Fatalf("hash %q, want 16 hex characters", h)
	}
	same := base
	same.Statuses = []string{"OFFLINE", "AVAILABLE"}
	same.GeohashPrefixes = []string{"tsq", "ttn"}
	same.Cursor = "anything"
	if got := store.ScanQueryHash(same); got != h {
		t.Fatalf("reordered filters and a cursor changed the hash: %s != %s", got, h)
	}
	for name, change := range map[string]func(q *store.ScanQuery){
		"kind":     func(q *store.ScanQuery) { q.Kind = store.KindRequest },
		"type":     func(q *store.ScanQuery) { q.Type = store.TypeFood },
		"statuses": func(q *store.ScanQuery) { q.Statuses = []string{"AVAILABLE"} },
		"prefixes": func(q *store.ScanQuery) { q.GeohashPrefixes = []string{"ttn"} },
		"limit":    func(q *store.ScanQuery) { q.Limit = 99 },
		"split": func(q *store.ScanQuery) {
			q.Statuses, q.GeohashPrefixes = []string{"AVAILABLE", "OFFLINE", "ttn"}, []string{"tsq"}
		},
	} {
		q := base
		change(&q)
		if store.ScanQueryHash(q) == h {
			t.Errorf("changing %s kept the hash", name)
		}
	}
}
