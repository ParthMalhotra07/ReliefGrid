package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPlaceholderRoutesDoNotExposeInternalOrAPIFallbacks(t *testing.T) {
	for _, tc := range []struct {
		path   string
		status int
	}{{"/", 200}, {"/readyz", 200}, {"/healthz", 200}, {"/internal/ring", 404}, {"/admin", 404}, {"/api/v1/requests", 404}} {
		w := httptest.NewRecorder()
		handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, tc.path, nil))
		if w.Code != tc.status {
			t.Errorf("%s: status=%d want=%d", tc.path, w.Code, tc.status)
		}
		if tc.path == "/" && (!strings.Contains(w.Body.String(), "ReliefGrid") || w.Header().Get("Content-Security-Policy") == "") {
			t.Fatal("placeholder content or security policy missing")
		}
	}
}
