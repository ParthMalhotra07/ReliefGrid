package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
)

// Exercise cancellation through run, rather than setting apiHealth directly.
// Two readiness probes at the chart's two-second interval must fit before
// the listener closes; healthy requests remain serviceable in that window.
func TestRunCancellationPublishesReadinessBeforeClosingListener(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), 12*time.Second)
	defer stop()
	s, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.addr = "127.0.0.1:0"
	admission, cancel := context.WithCancel(ctx)
	defer cancel()
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() { done <- run(admission, cfg, s, ready) }()
	var addr string
	select {
	case addr = <-ready:
	case <-ctx.Done():
		t.Fatal("API did not start")
	}
	client := &http.Client{Timeout: time.Second}
	get := func(path string, want int) {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
		if e != nil {
			t.Fatal(e)
		}
		response, e := client.Do(req)
		if e != nil {
			t.Fatalf("%s listener closed before readiness propagated: %v", path, e)
		}
		_ = response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("%s HTTP%d want%d", path, response.StatusCode, want)
		}
	}
	get("/readyz", http.StatusOK)
	cancelled := time.Now()
	cancel()
	// Observe the actual transition to 503, allowing scheduling of run's select.
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	observed := false
	for time.Since(cancelled) < time.Second {
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", nil)
		if e != nil {
			t.Fatal(e)
		}
		response, e := client.Do(req)
		if e != nil {
			t.Fatalf("readyz was not observable after cancellation: %v", e)
		}
		_ = response.Body.Close()
		if response.StatusCode == http.StatusServiceUnavailable {
			observed = true
			break
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("unexpected readyz status%d", response.StatusCode)
		}
		<-ticker.C
	}
	if !observed {
		t.Fatal("run never published readiness503")
	}
	for time.Since(cancelled) < 4*time.Second {
		get("/readyz", http.StatusServiceUnavailable)
		get("/healthz", http.StatusOK)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("readiness window did not finish")
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("API did not complete bounded shutdown")
	}
	t.Log("cancelled run served readiness503 and health200 through the propagation window")
}
