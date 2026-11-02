package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthShowsBrokerOutageWithoutRestartingInitializedProcess(t *testing.T) {
	h := &healthState{}
	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), "GET", path, nil))
		return w
	}
	if w := get("/readyz"); w.Code != 503 {
		t.Fatalf("uninitialized ready=%d", w.Code)
	}
	h.initialized.Store(true)
	h.relayRunning.Store(true)
	for _, connected := range []bool{false, true, false} {
		h.consumerConnected.Store(connected)
		for _, path := range []string{"/healthz", "/readyz"} {
			w := get(path)
			var body map[string]bool
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusOK || body["consumerConnected"] != connected || !body["relayRunning"] {
				t.Fatalf("process status=%d body=%s", w.Code, w.Body.String())
			}
		}
	}
	h.relayRunning.Store(false)
	if w := get("/readyz"); w.Code != 503 {
		t.Fatalf("stopped relay ready=%d", w.Code)
	}
}

func TestHealthServerStopsWithItsWorkerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serveHealth(ctx, listener, &healthState{}) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("health listener leaked after cancellation")
	}
	if conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", listener.Addr().String()); err == nil {
		_ = conn.Close()
		t.Fatal("health listener still accepting after shutdown")
	}
}

func TestHealthReportsReconcilerAndWakeupWorkers(t *testing.T) {
	h := &healthState{}
	h.reconcilerRunning.Store(true)
	h.wakeupConnected.Store(true)
	w := httptest.NewRecorder()
	h.handler().ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), "GET", "/healthz", nil))
	var body map[string]bool
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body["reconcilerRunning"] || !body["wakeupConnected"] {
		t.Fatalf("recovery workers invisible: %s", w.Body.String())
	}
}
