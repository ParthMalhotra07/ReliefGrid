package main

import (
	"bufio"
	"context"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Exercise the production run wrapper (including #107's tracing writer), not
// just api.New: a long-lived stream must not pin the 20s shutdown drain.
func TestRunStreamRefreshesAndEndsOnSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store, err := memstore.New(t.Context(), memstore.WithShards(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.addr = "127.0.0.1:0"
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, store, ready) }()
	var address string
	select {
	case address = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("API startup did not finish")
	}
	requestCtx, requestCancel := context.WithTimeout(context.Background(), apiReadinessPropagation+4*time.Second)
	defer requestCancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, "http://"+address+"/api/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	reader := bufio.NewScanner(resp.Body)
	found := false
	for reader.Scan() {
		if strings.TrimSpace(reader.Text()) == "event: refresh" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("traced API stream did not request refresh: %v", reader.Err())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(apiReadinessPropagation + 2*time.Second):
		t.Fatal("active SSE pinned API drain")
	}
}
