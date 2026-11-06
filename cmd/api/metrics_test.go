package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
)

func TestProcessMetricsUseSeparateListenerAndStopWithAPI(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend, err := memstore.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	metricsAddress := "127.0.0.1:0"
	cfg, err := loadConfig(func(key string) string {
		switch key {
		case "HTTP_ADDR":
			return "127.0.0.1:0"
		case "METRICS_ADDR":
			return metricsAddress
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	metricsReady := make(chan string, 1)
	cfg.metricsReady = metricsReady
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() { done <- run(ctx, cfg, backend, ready) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("metrics/API shutdown leaked")
		}
	})
	var public string
	select {
	case public = <-ready:
	case err := <-done:
		t.Fatalf("API startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("API startup timeout")
	}
	select {
	case metricsAddress = <-metricsReady:
	case <-time.After(time.Second):
		t.Fatal("no actual metrics address")
	}
	client := &http.Client{Timeout: time.Second}
	call := func(address string) (int, string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/metrics", nil)
		if err != nil {
			return 0, "", err
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, "", err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode, string(body), err
	}
	code, body, err := call(metricsAddress)
	if err != nil {
		t.Fatalf("private metrics listener is unavailable: %v", err)
	}
	if code != 200 || !strings.Contains(body, "relief_metrics_observation_success") {
		t.Fatalf("private metrics missing health observation: HTTP%d %s", code, body)
	}
	if code, _, err = call(public); err != nil || code != 404 {
		t.Fatalf("operator metrics exposed on public listener: HTTP%d %v", code, err)
	}
}
