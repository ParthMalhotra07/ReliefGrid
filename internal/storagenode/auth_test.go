package storagenode

import (
	"github.com/ParthMalhotra07/ReliefGrid/internal/auth"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDataAuthRunsBeforeHandlerWithIdentity(t *testing.T) {
	n := newFakeNode(t)
	s := newTestServer(t, n)
	s.metrics = newNodeMetrics(0, "storage-shard-0-0", time.Now)
	s.internalAuth = &auth.ReceiverConfig{Token: "node-current", PreviousToken: "node-old"}
	for _, token := range []string{"", "app-token", "node-current", "node-old"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/internal/data/kv:auth", nil)
		if token != "" {
			req.Header.Set(auth.Header, token)
		}
		rec := httptest.NewRecorder()
		s.dataRoutes().ServeHTTP(rec, req)
		if rec.Header().Get(internalapi.HeaderServerID) != testServerID {
			t.Fatal("auth answer lost node identity")
		}
		allowed := token == "node-current" || token == "node-old"
		if allowed {
			if rec.Code == 401 {
				t.Fatal("valid token refused")
			}
		} else {
			expectError(t, rec, 401, internalapi.UNAUTHORIZED)
		}
	}
	if len(n.applied) != 0 {
		t.Fatal("unauthenticated or malformed requests reached Apply")
	}
	for _, path := range []string{"/healthz", "/readyz"} {
		rec := httptest.NewRecorder()
		s.mgmtRoutes().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), "GET", path, nil))
		if rec.Code == 401 {
			t.Fatalf("probe protected: %s", path)
		}
	}
	rec := httptest.NewRecorder()
	s.metricsRoutes().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), "GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("metrics %d", rec.Code)
	}
}
