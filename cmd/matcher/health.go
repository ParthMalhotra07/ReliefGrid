package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Process initialization is separate from shard availability and broker health.
// A temporary dependency outage is handled by bounded worker reconnects, not a
// liveness failure. The response exposes that state without claiming ownership.
type healthState struct {
	initialized       atomic.Bool
	relayRunning      atomic.Bool
	consumerConnected atomic.Bool
	reconcilerRunning atomic.Bool
	wakeupConnected   atomic.Bool
}

func (h *healthState) handler() http.Handler {
	mux := http.NewServeMux()
	for _, path := range []string{"/healthz", "/readyz"} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			if r.URL.Path == "/readyz" && (!h.initialized.Load() || !h.relayRunning.Load()) {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"initialized": h.initialized.Load(), "relayRunning": h.relayRunning.Load(), "consumerConnected": h.consumerConnected.Load(), "reconcilerRunning": h.reconcilerRunning.Load(), "wakeupConnected": h.wakeupConnected.Load()})
		})
	}
	return mux
}

func serveHealth(ctx context.Context, listener net.Listener, state *healthState) error {
	srv := &http.Server{Handler: state.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	var err error
	served := false
	select {
	case err = <-done:
		served = true
	case <-ctx.Done():
	}
	state.initialized.Store(false)
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopErr := srv.Shutdown(shutdown)
	if stopErr != nil {
		stopErr = errors.Join(stopErr, srv.Close())
	}
	// Cancellation can win before Serve registers the listener with Server.
	// Close our owned listener explicitly and join Serve before returning.
	_ = listener.Close()
	if !served {
		err = <-done
	}
	if stopErr != nil {
		return fmt.Errorf("matcher health shutdown: %w", stopErr)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("matcher health server: %w", err)
	}
	return ctx.Err()
}
