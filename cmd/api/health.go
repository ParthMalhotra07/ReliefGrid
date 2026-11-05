package main

import (
	"net/http"
	"sync/atomic"
)

// Dependency outages remain per-route errors; health/readiness report process
// initialization and local draining, never the availability of every shard.
type apiHealth struct{ initialized, draining atomic.Bool }

func (h *apiHealth) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/readyz" && (!h.initialized.Load() || h.draining.Load()) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
