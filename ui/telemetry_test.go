package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestQueueTelemetryRequiresCompleteCountsAndKeepsCredentialsServerSide(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "incomplete"}[bad], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u, p, ok := r.BasicAuth()
				if !ok || u != "viewer" || p != "secret" {
					t.Error("server-side auth missing")
				}
				if strings.Contains(r.URL.EscapedPath(), "%2F") == false {
					t.Error("vhost not encoded")
				}
				w.Header().Set("Content-Type", "application/json")
				if bad {
					_, _ = w.Write([]byte(`{}`))
					return
				}
				if strings.HasSuffix(r.URL.Path, "matching.dead") {
					_, _ = w.Write([]byte(`{"messages":2}`))
				} else {
					_, _ = w.Write([]byte(`{"messages_ready":3,"messages_unacknowledged":1}`))
				}
			}))
			defer server.Close()
			telemetry, err := newQueueTelemetry(strings.Replace(server.URL, "://", "://viewer:secret@", 1))
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			telemetry.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ui/status", nil))
			if strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "viewer") || strings.Contains(w.Body.String(), server.URL) {
				t.Fatal("broker credentials or address exposed")
			}
			var s queueSnapshot
			if err = json.Unmarshal(w.Body.Bytes(), &s); err != nil {
				t.Fatal(err)
			}
			if bad {
				if s.Available || s.Ready != nil || s.Dead != nil {
					t.Fatal("incomplete observation became zero backlog")
				}
			} else if !s.Available || s.Ready == nil || *s.Ready != 3 || s.Dead == nil || *s.Dead != 2 {
				t.Fatalf("bad queue snapshot: %+v", s)
			}
		})
	}
}
func TestQueueTelemetryRemoteCallHonorsCallerCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	telemetry, err := newQueueTelemetry(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := time.Now()
	s := telemetry.sample(ctx)
	if s.Available || time.Since(start) > time.Second {
		t.Fatal("cancelled queue observation was not bounded")
	}
}
