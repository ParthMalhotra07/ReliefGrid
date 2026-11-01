package outbox_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// The HTTP adapter delegates both reads and commits to the real FSM. It checks
// the relay -> production coordclient wire path, not a second storage model.
func TestRelayThroughCoordinatorClientUsesStrongWaitAndShardBatch(t *testing.T) {
	ctx, s := fixtureStore(t)
	seed(t, ctx, s, 3)
	reads, marks := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var answer any
		switch r.Method + " " + r.URL.Path {
		case "GET /internal/ring":
			answer = map[string]any{"ringVersion": 1, "shards": 3, "vnodesPerShard": 128, "hash": "xxhash64", "checksum": "test-real-FSM-adapter"}
		case "GET /internal/outbox":
			reads++
			q := r.URL.Query()
			if q.Get("shard") != "0" || q.Get("limit") != "100" || q.Get("consistency") != "strong" || q.Get("wait") != "1" {
				t.Errorf("wrong wire read: %s", r.URL.RawQuery)
			}
			rows, err := s.PendingOutbox(r.Context(), store.OutboxQuery{Shard: 0, Limit: 100})
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			answer = map[string]any{"ringVersion": 1, "shard": 0, "events": rows, "read": map[string]any{"consistency": "strong"}}
		case "POST /internal/outbox/published":
			marks++
			var cmd store.MarkOutboxPublished
			if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			shard, err := strconv.Atoi(r.URL.Query().Get("shard"))
			if err != nil || shard != 0 {
				t.Errorf("wrong owning shard: %s", r.URL.RawQuery)
			}
			cmd.Shard = shard
			if len(cmd.EventIDs) != 3 || cmd.OperationID == "" || cmd.At <= 0 {
				t.Errorf("wrong mark command: %+v", cmd)
			}
			result, err := s.MarkOutboxPublished(r.Context(), cmd)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			answer = map[string]any{"result": result, "ringVersion": 1}
		default:
			t.Errorf("unexpected route: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		if err := json.NewEncoder(w).Encode(answer); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := coordclient.New(ctx, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := relay(t, client).Step(ctx, 0, &publisher{}); err != nil || report.Marked != 3 {
		t.Fatalf("HTTP relay: %+v %v", report, err)
	}
	if reads != 1 || marks != 1 {
		t.Fatalf("unexpected calls reads=%d marks=%d", reads, marks)
	}
	rows, err := s.PendingOutbox(ctx, store.OutboxQuery{Shard: 0, Limit: 100})
	if err != nil || len(rows) != 0 {
		t.Fatalf("wire mark failed to commit: %d %v", len(rows), err)
	}
}
