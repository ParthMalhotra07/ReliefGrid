// Command api serves the public /api/v1 contract through the coordinator.
// HTTP_ADDR defaults to :8080; COORDINATOR_URL defaults to
// http://storage-coordinator:8080. CALLER_NAMESPACE names one trusted local
// dispatcher. STREAM_TOKEN optionally protects /events. BUDGET_MS (5000) and
// CALL_TIMEOUT_MS (4000) bound HTTP work and each remote call respectively.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api"
	"github.com/ParthMalhotra07/ReliefGrid/internal/appmetrics"
	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/obs"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Two readiness failures at the chart's 2s probe interval, plus propagation.
// 5s propagation + 20s Shutdown stays within the pod's 30s grace.
const apiReadinessPropagation = 5 * time.Second
const apiShutdownDrain = 20 * time.Second

type config struct {
	metricsReady     chan<- string // optional in-process lifecycle observation, never an HTTP route
	metricsAddress   string
	addr             string
	coordinator      string
	internalAuthFile string
	api              api.Config
	amqp             string
}

func loadConfig(getenv func(string) string) (config, error) {
	get := func(key, fallback string) string {
		if value := getenv(key); value != "" {
			return value
		}
		return fallback
	}
	cfg := config{addr: get("HTTP_ADDR", ":8080"), coordinator: get("COORDINATOR_URL", "http://storage-coordinator:8080")}
	if _, _, err := net.SplitHostPort(cfg.addr); err != nil {
		return config{}, fmt.Errorf("HTTP_ADDR: %w", err)
	}
	// Chart-disabled deployments set no path. A configured sender file is
	// validated by coordclient before its initial ring request.
	cfg.internalAuthFile = getenv("INTERNAL_AUTH_TOKEN_FILE")
	cfg.metricsAddress = getenv("METRICS_ADDR")
	if cfg.metricsAddress != "" {
		if _, _, err := net.SplitHostPort(cfg.metricsAddress); err != nil {
			return config{}, fmt.Errorf("METRICS_ADDR: %w", err)
		}
	}
	u, err := url.Parse(cfg.coordinator)
	if err != nil {
		return config{}, fmt.Errorf("COORDINATOR_URL: %w", err)
	}
	if u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return config{}, fmt.Errorf("COORDINATOR_URL must be an HTTP(S) endpoint without credentials, query or fragment")
	}
	cfg.api.CallerNamespace = get("CALLER_NAMESPACE", "local-dispatcher")
	cfg.api.StreamToken = getenv("STREAM_TOKEN")
	cfg.amqp = getenv("AMQP_URL")
	if cfg.amqp != "" {
		broker, parseErr := url.Parse(cfg.amqp)
		if parseErr != nil || broker.Hostname() == "" || (broker.Scheme != "amqp" && broker.Scheme != "amqps") || broker.RawQuery != "" || broker.Fragment != "" {
			return config{}, fmt.Errorf("AMQP_URL must be an AMQP(S) endpoint")
		}
	}
	for _, field := range []struct {
		key      string
		fallback string
		target   *time.Duration
	}{
		{"BUDGET_MS", "5000", &cfg.api.Budget},
		{"CALL_TIMEOUT_MS", "4000", &cfg.api.CallTimeout},
	} {
		value, err := strconv.ParseInt(get(field.key, field.fallback), 10, 32)
		if err != nil || value <= 0 {
			return config{}, fmt.Errorf("%s must be positive integer milliseconds", field.key)
		}
		*field.target = time.Duration(value) * time.Millisecond
	}
	if cfg.api.CallTimeout > cfg.api.Budget {
		return config{}, fmt.Errorf("CALL_TIMEOUT_MS must not exceed BUDGET_MS")
	}
	return cfg, nil
}

// The optional backend is a test seam for the real SQLite memstore. The binary
// always passes nil, connects through coordclient and exposes no fake-store flag.
func run(ctx context.Context, cfg config, backend store.Store, ready chan<- string) (runErr error) {
	if backend == nil {
		startup, cancel := context.WithTimeout(ctx, cfg.api.Budget)
		client, err := coordclient.New(startup, cfg.coordinator, coordclient.WithTimeout(cfg.api.CallTimeout), coordclient.WithInternalAuthFile(cfg.internalAuthFile))
		cancel()
		if err != nil {
			return fmt.Errorf("connect coordinator: %w", err)
		}
		backend = client
	}
	if cfg.metricsAddress != "" {
		metrics, err := appmetrics.New(backend, "api", nil)
		if err != nil {
			return err
		}
		stop, err := metrics.Start(ctx, cfg.metricsAddress)
		if err != nil {
			return err
		}
		defer func() { runErr = errors.Join(runErr, stop()) }()
		if cfg.metricsReady != nil {
			select {
			case cfg.metricsReady <- metrics.Address():
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		backend = metrics.Store()
		cfg.api.Observer = metrics
	}
	cfg.api.Store = backend
	cfg.api.Events = api.NewEventHub()
	cfg.api.StreamContext = ctx
	h, err := api.New(cfg.api)
	if err != nil {
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("API hostname: %w", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("API listen: %w", err)
	}
	projectionCtx, stopProjection := context.WithCancel(ctx)
	defer stopProjection()
	if cfg.amqp != "" {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = mq.ProjectEvents(projectionCtx, cfg.amqp, cfg.api.Events.Publish, cfg.api.Events.SetAvailable)
		}()
		defer func() { stopProjection(); <-done }()
	}
	state := &apiHealth{}
	mux := http.NewServeMux()
	// Only the public API is traced (S4.4): probes would fill Jaeger. The
	// span continues the caller's traceparent; with tracing off the no-op
	// span still carries it, so events keep an incoming traceparent.
	mux.Handle("/", obs.Handler(h))
	mux.Handle("GET /healthz", state.handler())
	mux.Handle("GET /readyz", state.handler())
	// Diagnostic response metadata lets the scaling demo observe Service routing.
	observed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-ReliefGrid-Pod", hostname)
		mux.ServeHTTP(w, r)
	})
	// Stop admission on SIGTERM, but preserve admitted request contexts until
	// Shutdown has drained them or reached its bounded deadline.
	workCtx, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()
	srv := &http.Server{Handler: observed, ReadHeaderTimeout: cfg.api.Budget, ReadTimeout: cfg.api.Budget,
		BaseContext: func(net.Listener) context.Context { return workCtx }}
	done := make(chan error, 1)
	state.initialized.Store(true)
	go func() { done <- srv.Serve(listener) }()
	slog.InfoContext(ctx, "API listening", "addr", listener.Addr().String())
	if ready != nil {
		select {
		case ready <- listener.Addr().String():
		case <-ctx.Done():
		}
	}
	served := false
	select {
	case err = <-done:
		served = true
	case <-ctx.Done():
	}
	state.draining.Store(true)
	if ctx.Err() != nil {
		// Keep readyz observable while endpoints/Traefik remove this pod.
		propagation := time.NewTimer(apiReadinessPropagation)
		<-propagation.C
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), apiShutdownDrain)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, srv.Close())
	}
	// Join Serve even when cancellation wins before Serve registers the listener.
	_ = listener.Close()
	if !served {
		err = <-done
	}
	if shutdownErr != nil {
		return fmt.Errorf("API shutdown: %w", shutdownErr)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("API serve: %w", err)
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	cfg, err := loadConfig(os.Getenv)
	if err == nil {
		err = withTracing(ctx, "api", func() error { return run(ctx, cfg, nil, nil) })
	}
	if err != nil {
		slog.Error("API stopped", "error", err)
		os.Exit(1)
	}
}

// withTracing runs fn with tracing set up from the standard OTEL_*
// variables (S4.4): with no OTLP endpoint only the W3C propagator, so
// traceparent still flows. Spans are flushed when fn returns.
func withTracing(ctx context.Context, service string, fn func() error) error {
	enabled := obs.EnabledFromEnv()
	shutdown, err := obs.Init(ctx, obs.Config{ServiceName: service, Enabled: enabled})
	if err != nil {
		return err
	}
	slog.Info("tracing", "enabled", enabled)
	err = fn()
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if serr := shutdown(flushCtx); serr != nil {
		slog.Warn("flushing traces", "err", serr)
	}
	return err
}
