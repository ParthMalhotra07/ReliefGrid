package main

import (
	"net/http/httptest"
	"testing"
)

func TestAPIReadinessTracksInitializationAndDrain(t *testing.T) {
	h := &apiHealth{}
	check := func(path string, want int) {
		t.Helper()
		w := httptest.NewRecorder()
		h.handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), "GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s=%d,want%d", path, w.Code, want)
		}
	}
	check("/healthz", 200)
	check("/readyz", 503)
	h.initialized.Store(true)
	check("/readyz", 200)
	h.draining.Store(true)
	check("/readyz", 503)
	check("/healthz", 200)
}
