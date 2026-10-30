package storagenode

import (
	"net/http"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
)

// Every data-route answer names the replica that served it, so the
// coordinator can tell it from another process on a reused address
// (issue #46): committed results, errors and unknown routes alike.
func TestDataRoutesNameTheServer(t *testing.T) {
	rg := loadRing(t)
	mine, _ := keyOn(t, rg, testShard, "", "kv:mine")
	theirs, _ := keyOn(t, rg, 0, "", "kv:theirs")
	h := newTestServer(t, newFakeNode(t)).dataRoutes()
	for name, c := range map[string]call{
		"committed":     {method: http.MethodPut, path: "/internal/data/" + mine, header: map[string]string{"If-None-Match": "*"}, body: map[string]any{"operationId": "op-1", "at": t0, "ringVersion": 1, "value": 1}},
		"read":          {method: http.MethodGet, path: "/internal/data/" + mine},
		"foreign key":   {method: http.MethodGet, path: "/internal/data/" + theirs},
		"ring mismatch": {method: http.MethodPut, path: "/internal/data/" + mine, header: map[string]string{"If-None-Match": "*"}, body: map[string]any{"operationId": "op-2", "at": t0, "ringVersion": 9, "value": 1}},
		"unknown route": {method: http.MethodGet, path: "/internal/nope"},
	} {
		rec := do(t, h, c)
		if got := rec.Header().Get(internalapi.HeaderServerID); got != testServerID {
			t.Errorf("%s (%d): %s = %q, want %q", name, rec.Code, internalapi.HeaderServerID, got, testServerID)
		}
		if got := rec.Header().Get(internalapi.HeaderShard); got != "1" {
			t.Errorf("%s (%d): %s = %q, want %q", name, rec.Code, internalapi.HeaderShard, got, "1")
		}
	}
}
