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

// BenchmarkMerkle measures one full digest of a file database (as on a
// pod) holding n requests, n resources and their operation results and
// outbox rows: about 4n rows.
func BenchmarkMerkle(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entities=%d", 2*n), func(b *testing.B) {
			ctx := context.Background()
			f, err := fsm.Open(ctx, "file:"+filepath.Join(b.TempDir(), "fsm-0.db"))
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			idx := uint64(0)
			apply := func(cmd any) {
				entry, err := fsm.Encode(cmd)
				if err != nil {
					b.Fatal(err)
				}
				idx++
				if _, err := f.Apply(idx, entry); err != nil {
					b.Fatal(err)
				}
			}
			for i := range n {
				id := fmt.Sprintf("%06d", i)
				apply(store.CreateEntity{Meta: meta("c-r" + id), Key: store.ResourceKey("r" + id), Resource: resource("r"+id, store.TypeWater, store.ResourceAvailable)})
				apply(store.CreateEntity{Meta: meta("c-q" + id), Key: store.RequestKey("q" + id), Request: request("q"+id, store.TypeWater),
					Outbox: []store.OutboxEvent{{EventID: "ev-" + id, EventType: "request.created", SchemaVersion: 1, Payload: json.RawMessage(`{"id":"q` + id + `"}`)}}})
			}
			b.ResetTimer()
			for b.Loop() {
				r, err := f.Merkle(ctx, idx)
				if err != nil {
					b.Fatal(err)
				}
				rows := 0
				for _, t := range r.Digest.Tables {
					rows += t.Rows
				}
				b.ReportMetric(float64(rows), "rows")
			}
		})
	}
}
