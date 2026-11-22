package main

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestStreetMapAllowsOnlyTheTileHostForImages(t *testing.T) {
	w := httptest.NewRecorder()
	handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	directives := strings.Split(w.Header().Get("Content-Security-Policy"), ";")
	for _, want := range []string{
		"img-src 'self' data: https://tile.openstreetmap.org",
		"script-src 'self'",
		"connect-src 'self'",
		"worker-src 'self'",
	} {
		if !slices.ContainsFunc(directives, func(got string) bool { return strings.TrimSpace(got) == want }) {
			t.Errorf("missing exact CSP directive: %s", want)
		}
	}
	if got := w.Header().Get("Referrer-Policy"); got != "same-origin" {
		t.Errorf("Referrer-Policy = %q, want same-origin", got)
	}
}
