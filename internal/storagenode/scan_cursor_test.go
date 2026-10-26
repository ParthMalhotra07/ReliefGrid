package storagenode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// S1.6: after=K returns this shard's keys strictly after K, in key order;
// hasMore still flags a full page. Keys with URL-special bytes survive.
func TestScanAfter(t *testing.T) {
	n := newFakeNode(t)
	keys := []string{"kv:a", "kv:a&b", "kv:b", "kv:c d", "kv:e"}
	for i, k := range keys {
		n.seed(t, store.CreateEntity{Meta: meta(fmt.Sprintf("op-%d", i)), Key: k, Value: json.RawMessage(`1`)})
	}
	for _, tc := range []struct {
		after   string
		limit   int
		want    []string
		hasMore bool
	}{
		{"", 2, []string{"kv:a", "kv:a&b"}, true},
		{"kv:a", 10, []string{"kv:a&b", "kv:b", "kv:c d", "kv:e"}, false},
		{"kv:a&b", 2, []string{"kv:b", "kv:c d"}, true},
		{"kv:c d", 1, []string{"kv:e"}, true},
		{"kv:e", 10, []string{}, false},
	} {
		path := fmt.Sprintf("/internal/scan?kind=kv&limit=%d", tc.limit)
		if tc.after != "" {
			path += "&after=" + url.QueryEscape(tc.after)
		}
		rec := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: path})
		resp := decodeJSON[internalapi.ScanResponse](t, rec)
		got := []string{}
		for _, r := range resp.Records {
			got = append(got, r.Key)
		}
		if rec.Code != http.StatusOK || !slices.Equal(got, tc.want) || len(resp.Positions) != 1 || resp.Positions[0].HasMore != tc.hasMore {
			t.Errorf("after %q limit %d = %d %v hasMore=%v, want %v hasMore=%v", tc.after, tc.limit, rec.Code, got, resp.Positions, tc.want, tc.hasMore)
		}
	}
}

// A storage node never decodes a cursor: that is the coordinator's job. One
// that reaches a node is refused, never read as "from the first key".
func TestScanRefusesACursor(t *testing.T) {
	n := newFakeNode(t)
	cur := store.EncodeScanCursor(store.ScanCursor{RingVersion: 1, After: map[int]string{testShard: "kv:a"}})
	expectError(t, do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodGet, path: "/internal/scan?kind=kv&limit=10&cursor=" + url.QueryEscape(cur+"x")}),
		http.StatusBadRequest, internalapi.INVALID)
	if len(n.modes) != 0 {
		t.Fatal("a scan with a cursor was read")
	}
}
