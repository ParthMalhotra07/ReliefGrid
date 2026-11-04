//go:build !seams

package storagenode

import (
	"net/http"
	"testing"
)

// The default build has no seam routes and does not claim to be a seams
// build: production images cannot arm a test boundary.
func TestSeamRoutesAbsentByDefault(t *testing.T) {
	h := newTestServer(t, newFakeNode(t)).mgmtRoutes()
	for _, c := range []call{
		{method: http.MethodPost, path: "/mgmt/test/seams", header: operatorAuth, body: map[string]any{"kind": "snapshot-persist", "timeoutMs": 1000}},
		{method: http.MethodGet, path: "/mgmt/test/seams/1", header: operatorAuth},
		{method: http.MethodPost, path: "/mgmt/test/seams/1/release", header: operatorAuth},
	} {
		if rec := do(t, h, c); rec.Code != http.StatusNotFound {
			t.Errorf("%s %s: %d, want 404", c.method, c.path, rec.Code)
		}
	}
	st := decodeJSON[map[string]any](t, do(t, h, call{method: http.MethodGet, path: "/mgmt/status"}))
	if _, ok := st["seams"]; ok {
		t.Fatalf("status %v reports seams in the default build", st)
	}
}
