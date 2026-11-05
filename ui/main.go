package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

//go:embed static
var assets embed.FS

func handler() http.Handler { telemetry, _ := newQueueTelemetry(""); return dispatchHandler(telemetry) }
func dispatchHandler(telemetry *queueTelemetry) http.Handler {
	mux := http.NewServeMux()
	serve := func(name string, w http.ResponseWriter, r *http.Request) {
		body, err := assets.ReadFile("static/" + name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; worker-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		switch {
		case strings.HasSuffix(name, ".html"):
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
		case strings.HasSuffix(name, ".js"):
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		case strings.HasSuffix(name, ".css"):
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
		case strings.HasSuffix(name, ".svg"):
			w.Header().Set("Content-Type", "image/svg+xml")
		case strings.HasSuffix(name, ".png"):
			w.Header().Set("Content-Type", "image/png")
		default:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		}
		if strings.HasPrefix(name, "app-") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		_, _ = w.Write(body)
	}
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { serve("index.html", w, r) })
	mux.HandleFunc("GET /sw.js", func(w http.ResponseWriter, r *http.Request) { serve("sw.js", w, r) })
	mux.HandleFunc("GET /assets/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "manifest.json" || name == "index.html" || name == "sw.js" || strings.ContainsAny(name, "/\\") {
			http.NotFound(w, r)
			return
		}
		serve(name, w, r)
	})
	mux.Handle("GET /ui/status", telemetry)
	for _, path := range []string{"/healthz", "/readyz"} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	}
	return mux
}

func run(ctx context.Context, address string) error {
	telemetry, err := newQueueTelemetry(os.Getenv("UI_BROKER_MANAGEMENT_URL"))
	if err != nil {
		return err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("UI listen: %w", err)
	}
	srv := &http.Server{Handler: dispatchHandler(telemetry), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	served := false
	select {
	case err = <-done:
		served = true
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopErr := srv.Shutdown(shutdown)
	if stopErr != nil {
		stopErr = errors.Join(stopErr, srv.Close())
	}
	_ = listener.Close()
	if !served {
		err = <-done
	}
	if stopErr != nil {
		return fmt.Errorf("UI shutdown: %w", stopErr)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("UI serve: %w", err)
	}
	return ctx.Err()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	address := os.Getenv("HTTP_ADDR")
	if address == "" {
		address = ":8080"
	}
	if err := run(ctx, address); err != nil && ctx.Err() == nil {
		slog.Error("UI stopped", "error", err)
		os.Exit(1)
	}
}
