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

func TestBundledFontIsServedAsWoff2FromSelf(t *testing.T) {
	entries, err := assets.ReadDir("static")
	if err != nil {
		t.Fatal(err)
	}
	name := ""
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".woff2") {
			name = entry.Name()
		}
	}
	if name == "" {
		t.Fatal("no bundled .woff2 font in static/")
	}
	w := httptest.NewRecorder()
	handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/assets/"+name, nil))
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "font/woff2" {
		t.Fatalf("font served with %d %q", w.Code, w.Header().Get("Content-Type"))
	}
	if !strings.Contains(w.Header().Get("Content-Security-Policy"), "font-src 'self'") {
		t.Fatal("font-src must stay 'self'")
	}
}
