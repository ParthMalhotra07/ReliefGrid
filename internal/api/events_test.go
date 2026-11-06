package api_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Reconnects have no durable replay cursor. Every connection must promptly
// tell the UI to refresh authoritative state, rather than silently heartbeat.
func TestSSEConnectRequiresAuthoritativeRefresh(t *testing.T) {
	_, store := realStore(t)
	h := handler(t, store, now, "dispatcher-a")
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "event: refresh" {
			return
		}
	}
	t.Fatalf("connected SSE did not request authoritative refresh: %v", scanner.Err())
}
